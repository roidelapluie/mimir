// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"golang.org/x/sys/unix"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/filepool"
)

// This is a benchmark-only format prototype, not an extension accepted by the V4 reader.
// Its directory is appended after the unchanged V4 symbol section. A future TOC would locate it.
// Header: magic (8), sampling factor (4), symbol count (4), uint64 offsets, CRC32 (4).
// Offsets have the same section-relative meaning as the current sparse protobuf.
type symbolDirectoryFixture struct {
	dir, file, sidecar string
	id                 ulid.ULID
	count              int
	offsets            []int
	directoryOffset    int64
	directoryBytes     int
}

type symbolDirectoryCase struct {
	name         string
	count, width int
	source       string
}

func symbolDirectoryCases() []symbolDirectoryCase {
	return []symbolDirectoryCase{
		{name: "repository", source: "testdata/index_format_v2/index"},
		{name: "100k_64B", count: 100000, width: 64},
		{name: "1m_16B", count: 1000000, width: 16},
		{name: "1m_64B", count: 1000000, width: 64},
		{name: "5m_64B", count: 5000000, width: 64},
	}
}

func newSymbolDirectoryFixture(tb testing.TB, tc symbolDirectoryCase) symbolDirectoryFixture {
	tb.Helper()
	f := symbolDirectoryFixture{dir: tb.TempDir(), id: ulid.MustNew(1, nil), count: tc.count}
	var payload []byte
	if tc.source != "" {
		src, err := os.ReadFile(tc.source)
		require.NoError(tb, err)
		toc, err := index.NewTOCFromByteSlice(realByteSlice(src))
		require.NoError(tb, err)
		require.Equal(tb, byte(index.FormatV2), src[4])
		size := binary.BigEndian.Uint32(src[toc.Symbols:])
		payload = src[toc.Symbols+4 : toc.Symbols+4+uint64(size)]
		f.count = int(binary.BigEndian.Uint32(payload))
	} else {
		payload = make([]byte, 4, 4+tc.count*(tc.width+1))
		binary.BigEndian.PutUint32(payload, uint32(tc.count))
		for i := 0; i < tc.count; i++ {
			symbol := fmt.Sprintf("%016x", i)
			if tc.width > 16 {
				symbol += fmt.Sprintf("%0*x", tc.width-16, uint64(i)*0x9e3779b97f4a7c15)
			}
			payload = binary.AppendUvarint(payload, uint64(len(symbol)))
			payload = append(payload, symbol...)
		}
	}
	section := binary.BigEndian.AppendUint64(nil, uint64(len(payload)))
	section = append(section, payload...)
	section = binary.BigEndian.AppendUint32(section, crc32.Checksum(payload, castagnoliTable))
	f.directoryOffset = int64(len(section))
	blockDir := filepath.Join(f.dir, f.id.String())
	require.NoError(tb, os.MkdirAll(blockDir, 0700))
	f.file = filepath.Join(blockDir, "symbols-v4")
	require.NoError(tb, os.WriteFile(f.file, section, 0600))
	factory := streamencoding.NewFilePoolDecbufFactory(f.file, 0, filepool.NewFilePoolMetrics(nil))
	n, offsets, err := streamindex.SparseValuesFromSymbolsTableWithVersion(tb.Context(), factory, 0, true, index.FormatV4)
	require.NoError(tb, err)
	require.Equal(tb, f.count, n)
	require.NoError(tb, factory.Close())
	f.offsets = offsets
	directory := append([]byte(nil), "SYMDIR01"...)
	directory = binary.BigEndian.AppendUint32(directory, 32)
	directory = binary.BigEndian.AppendUint32(directory, uint32(f.count))
	for _, off := range offsets {
		directory = binary.BigEndian.AppendUint64(directory, uint64(off))
	}
	directory = binary.BigEndian.AppendUint32(directory, crc32.Checksum(directory, castagnoliTable))
	f.directoryBytes = len(directory)
	out, err := os.OpenFile(f.file, os.O_APPEND|os.O_WRONLY, 0600)
	require.NoError(tb, err)
	_, err = out.Write(directory)
	require.NoError(tb, err)
	require.NoError(tb, out.Sync())
	require.NoError(tb, out.Close())
	f.sidecar = filepath.Join(blockDir, block.SparseIndexHeaderFilename)
	require.NoError(tb, writeSparseHeaderProtoToDisk(f.sidecar, &indexheaderpb.Sparse{
		Symbols:             streamindex.SparseSymbolsToProto(f.count, offsets),
		PostingsOffsetTable: streamindex.SparsePostingsOffsetsTableToProto(map[string]*streamindex.SparseTableOffsetsForLabel{}, 32),
	}, log.NewNopLogger()))
	return f
}

func (f symbolDirectoryFixture) loadEmbedded() (int, []int, error) {
	in, err := os.Open(f.file)
	if err != nil {
		return 0, nil, err
	}
	data := make([]byte, f.directoryBytes)
	_, err = in.ReadAt(data, f.directoryOffset)
	closeErr := in.Close()
	if err != nil {
		return 0, nil, err
	}
	if closeErr != nil {
		return 0, nil, closeErr
	}
	if len(data) < 20 || string(data[:8]) != "SYMDIR01" || binary.BigEndian.Uint32(data[8:]) != 32 || crc32.Checksum(data[:len(data)-4], castagnoliTable) != binary.BigEndian.Uint32(data[len(data)-4:]) {
		return 0, nil, fmt.Errorf("invalid directory")
	}
	count := int(binary.BigEndian.Uint32(data[12:]))
	samples := (count + 31) / 32
	if samples != (len(data)-20)/8 || len(data) != 20+8*samples {
		return 0, nil, fmt.Errorf("invalid directory size")
	}
	offsets := make([]int, samples)
	var previous uint64
	for i := range offsets {
		off := binary.BigEndian.Uint64(data[16+i*8:])
		if off < 12 || off >= uint64(f.directoryOffset)-4 || i > 0 && off <= previous {
			return 0, nil, fmt.Errorf("invalid symbol offset")
		}
		offsets[i] = int(off)
		previous = off
	}
	return count, offsets, nil
}

func (f symbolDirectoryFixture) loaders(tb testing.TB) map[string]func() (int, []int, error) {
	factory := streamencoding.NewFilePoolDecbufFactory(f.file, 0, filepool.NewFilePoolMetrics(nil))
	tb.Cleanup(func() { require.NoError(tb, factory.Close()) })
	return map[string]func() (int, []int, error){
		"scan": func() (int, []int, error) {
			return streamindex.SparseValuesFromSymbolsTableWithVersion(tb.Context(), factory, 0, false, index.FormatV4)
		},
		"scan_crc": func() (int, []int, error) {
			return streamindex.SparseValuesFromSymbolsTableWithVersion(tb.Context(), factory, 0, true, index.FormatV4)
		},
		"sidecar": func() (int, []int, error) {
			n, o, _, err := LoadSparseIndexHeaderFromDisk(tb.Context(), f.id, f.dir, 32, log.NewNopLogger())
			return n, o, err
		},
		"embedded": f.loadEmbedded,
	}
}

func TestSymbolDirectoryPrototype(t *testing.T) {
	for _, tc := range []symbolDirectoryCase{{name: "empty"}, {name: "boundary", count: 65, width: 64}, {name: "repository", source: "testdata/index_format_v2/index"}} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSymbolDirectoryFixture(t, tc)
			factory := streamencoding.NewFilePoolDecbufFactory(f.file, 0, filepool.NewFilePoolMetrics(nil))
			defer func() { require.NoError(t, factory.Close()) }()
			reference, err := streamindex.NewSymbolsTableReader(index.FormatV4, factory, 0, f.count, f.offsets)
			require.NoError(t, err)
			for name, load := range f.loaders(t) {
				t.Run(name, func(t *testing.T) {
					n, offsets, err := load()
					require.NoError(t, err)
					require.Equal(t, f.count, n)
					require.Equal(t, f.offsets, offsets)
					reader, err := streamindex.NewSymbolsTableReader(index.FormatV4, factory, 0, n, offsets)
					require.NoError(t, err)
					for i := 0; i < n; i++ {
						want, err := reference.Lookup(uint32(i))
						require.NoError(t, err)
						got, err := reader.Lookup(uint32(i))
						require.NoError(t, err)
						require.Equal(t, want, got)
					}
				})
			}
			in, err := os.OpenFile(f.file, os.O_RDWR, 0600)
			require.NoError(t, err)
			_, err = in.WriteAt([]byte("BAD"), f.directoryOffset)
			require.NoError(t, err)
			require.NoError(t, in.Close())
			_, _, err = f.loadEmbedded()
			require.EqualError(t, err, "invalid directory")
		})
	}
}

var symbolDirectorySink []int

func BenchmarkSymbolDirectory(b *testing.B) {
	for _, tc := range symbolDirectoryCases() {
		b.Run(tc.name, func(b *testing.B) {
			f := newSymbolDirectoryFixture(b, tc)
			loaders := f.loaders(b)
			for _, name := range []string{"scan", "scan_crc", "sidecar", "embedded"} {
				b.Run(name, func(b *testing.B) {
					load := loaders[name]
					b.ReportAllocs()
					b.ResetTimer()
					for i := 0; i < b.N; i++ {
						n, o, err := load()
						if err != nil || n != f.count {
							b.Fatalf("count=%d err=%v", n, err)
						}
						symbolDirectorySink = o
					}
				})
			}
		})
	}
}

// TestSymbolDirectoryCold measures advisory page-cache eviction, without global cache dropping.
// Run explicitly with SYMBOL_DIRECTORY_COLD=1; these are component timings, not full gateway startup.
func TestSymbolDirectoryCold(t *testing.T) {
	if os.Getenv("SYMBOL_DIRECTORY_COLD") != "1" {
		t.Skip("opt-in disk experiment")
	}
	for _, tc := range symbolDirectoryCases() {
		f := newSymbolDirectoryFixture(t, tc)
		loaders := f.loaders(t)
		stat, err := os.Stat(f.sidecar)
		require.NoError(t, err)
		t.Logf("SIZE %s symbols=%d symbol_bytes=%d embedded_bytes=%d sidecar_bytes=%d retained_offset_bytes=%d", tc.name, f.count, f.directoryOffset, f.directoryBytes, stat.Size(), 8*len(f.offsets))
		results := map[string][]int64{}
		names := []string{"scan", "scan_crc", "sidecar", "embedded"}
		for run := 0; run < 7; run++ {
			for j := range names {
				name := names[(j+run)%len(names)]
				for _, path := range []string{f.file, f.sidecar} {
					file, err := os.Open(path)
					require.NoError(t, err)
					require.NoError(t, file.Sync())
					require.NoError(t, unix.Fadvise(int(file.Fd()), 0, 0, unix.FADV_DONTNEED))
					require.NoError(t, file.Close())
				}
				start := time.Now()
				n, offsets, err := loaders[name]()
				elapsed := time.Since(start)
				require.NoError(t, err)
				require.Equal(t, f.count, n)
				require.Equal(t, f.offsets, offsets)
				results[name] = append(results[name], elapsed.Nanoseconds())
			}
		}
		for _, name := range names {
			values := results[name]
			slices.Sort(values)
			t.Logf("COLD %s %s median_ns=%d min_ns=%d max_ns=%d", tc.name, name, values[3], values[0], values[6])
		}
	}
}
