// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"
)

type v4PlanningBucket struct {
	objstore.Bucket
	requests                   atomic.Int64
	bytes                      atomic.Int64
	indexBytes, directoryBytes int
	headerDir                  string
}

func (b *v4PlanningBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	r, err := b.Bucket.GetRange(ctx, name, off, length)
	if err != nil {
		return nil, err
	}
	b.requests.Add(1)
	return &v4PlanningReadCloser{ReadCloser: r, bytes: &b.bytes}, nil
}

type v4PlanningReadCloser struct {
	io.ReadCloser
	bytes *atomic.Int64
}

func (r *v4PlanningReadCloser) Read(p []byte) (int, error) {
	n, err := r.ReadCloser.Read(p)
	r.bytes.Add(int64(n))
	return n, err
}

func newPostingsPageFixture(tb testing.TB, version, headerVersion, count int) (*StreamBinaryReader, *v4PlanningBucket) {
	tb.Helper()
	values := make([]string, count)
	for i := range values {
		values[i] = fmt.Sprintf("pod-%04d", i)
	}
	return newPostingsValuesFixture(tb, version, headerVersion, values)
}

func newPostingsValuesFixture(tb testing.TB, version, headerVersion int, values []string) (*StreamBinaryReader, *v4PlanningBucket) {
	tb.Helper()
	ctx := tb.Context()
	filename := filepath.Join(tb.TempDir(), "index")
	w, err := index.NewWriterWithOptions(ctx, filename, index.WriterOptions{FormatVersion: version})
	require.NoError(tb, err)
	symbols := append([]string{"cluster", "pod", "prod"}, values...)
	slices.Sort(symbols)
	for _, s := range symbols {
		require.NoError(tb, w.AddSymbol(s))
	}
	for i, value := range values {
		require.NoError(tb, w.AddSeries(storage.SeriesRef(i), labels.FromStrings("cluster", "prod", "pod", value)))
	}
	require.NoError(tb, w.Close())
	raw, err := os.ReadFile(filename)
	require.NoError(tb, err)
	bucket := &v4PlanningBucket{Bucket: objstore.NewInMemBucket(), indexBytes: len(raw)}
	if version == index.FormatV4 {
		toc, err := index.NewTOCFromByteSlice(realByteSlice(raw))
		require.NoError(tb, err)
		bucket.directoryBytes = int(binary.BigEndian.Uint64(raw[toc.PostingsTable+16:]))
	}
	tb.Cleanup(func() { require.NoError(tb, bucket.Close()) })
	id := ulid.MustNew(1, nil)
	require.NoError(tb, bucket.Upload(ctx, id.String()+"/index", bytes.NewReader(raw)))
	bucket.headerDir = tb.TempDir()
	r, err := NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bucket), bucket.headerDir, Config{Version: headerVersion}, 32, log.NewNopLogger(), NewStreamBinaryReaderMetrics(nil))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, r.Close()) })
	bucket.requests.Store(0)
	bucket.bytes.Store(0)
	return r, bucket
}

func BenchmarkPostingsPageReads(b *testing.B) {
	for _, version := range []int{index.FormatV2, index.FormatV4} {
		b.Run(fmt.Sprintf("format=%d", version), func(b *testing.B) {
			for _, headerVersion := range []int{BinaryFormatV1, BinaryFormatV2} {
				b.Run(fmt.Sprintf("header=%d", headerVersion), func(b *testing.B) {
					for _, op := range []string{"exact", "interior", "boundary", "group-boundary", "prefix", "enumerate", "regex"} {
						b.Run(op, func(b *testing.B) {
							r, bucket := newPostingsPageFixture(b, version, headerVersion, 4096)
							ctx := b.Context()
							lookupValues := make([]string, 4096)
							for i := range lookupValues {
								lookupValues[i] = fmt.Sprintf("pod-%04d", i)
							}
							next := 0
							b.ReportAllocs()
							for b.Loop() {
								switch op {
								case "exact":
									next = (next + 7919) % len(lookupValues)
									_, err := r.PostingsOffset(ctx, "pod", lookupValues[next])
									require.NoError(b, err)
								case "enumerate":
									values, err := r.LabelValuesOffsets(ctx, "pod", "", nil)
									require.NoError(b, err)
									require.Len(b, values, 4096)
								case "regex":
									_, err := r.LabelValuesOffsets(ctx, "pod", "", func(value string) bool { return strings.Contains(value, "0") })
									require.NoError(b, err)
								case "interior":
									_, err := r.PostingsOffset(ctx, "pod", "pod-0126")
									require.NoError(b, err)
								case "boundary":
									_, err := r.PostingsOffset(ctx, "pod", "pod-0127")
									require.NoError(b, err)
								case "group-boundary":
									_, err := r.PostingsOffset(ctx, "cluster", "prod")
									require.NoError(b, err)
								case "prefix":
									_, err := r.LabelValuesOffsets(ctx, "pod", "pod-010", nil)
									require.NoError(b, err)
								}
							}
							b.ReportMetric(float64(bucket.requests.Load())/float64(b.N), "requests/op")
							b.ReportMetric(float64(bucket.bytes.Load())/float64(b.N), "read-bytes/op")
						})
					}
				})
			}
		})
	}
}

func BenchmarkPostingsLookupWorkloads(b *testing.B) {
	for _, shape := range []string{"short", "url", "cardinality"} {
		count := 4096
		if shape == "cardinality" {
			count = 65536
		}
		values := make([]string, count)
		for i := range values {
			values[i] = fmt.Sprintf("pod-%08d", i)
			if shape == "url" {
				values[i] = "https://service.example.org/api/v1/namespaces/production/workloads/" + values[i]
			}
		}
		for _, version := range []int{index.FormatV2, index.FormatV4} {
			b.Run(fmt.Sprintf("shape=%s/format=%d", shape, version), func(b *testing.B) {
				for _, operation := range []string{"exact", "prefix", "enumerate"} {
					b.Run(operation, func(b *testing.B) {
						r, bucket := newPostingsValuesFixture(b, version, BinaryFormatV2, values)
						prefix := values[100][:len(values[100])-1]
						next := 0
						b.ReportAllocs()
						for b.Loop() {
							switch operation {
							case "exact":
								next = (next + 7919) % len(values)
								_, err := r.PostingsOffset(b.Context(), "pod", values[next])
								if err != nil {
									b.Fatal(err)
								}
							case "prefix":
								result, err := r.LabelValuesOffsets(b.Context(), "pod", prefix, nil)
								if err != nil {
									b.Fatal(err)
								}
								if len(result) != 10 {
									b.Fatalf("got %d values", len(result))
								}
							case "enumerate":
								result, err := r.LabelValuesOffsets(b.Context(), "pod", "", nil)
								if err != nil {
									b.Fatal(err)
								}
								if len(result) != count {
									b.Fatalf("got %d values", len(result))
								}
							}
						}
						b.ReportMetric(float64(bucket.requests.Load())/float64(b.N), "requests/op")
						b.ReportMetric(float64(bucket.bytes.Load())/float64(b.N), "read-bytes/op")
						b.ReportMetric(float64(bucket.indexBytes), "index-bytes")
						b.ReportMetric(float64(bucket.directoryBytes), "directory-bytes")
					})
				}
			})
		}
	}
}

func BenchmarkPostingsReaderOpen(b *testing.B) {
	for _, count := range []int{4096, 65536} {
		for _, version := range []int{index.FormatV2, index.FormatV4} {
			b.Run(fmt.Sprintf("count=%d/format=%d", count, version), func(b *testing.B) {
				values := make([]string, count)
				for i := range values {
					values[i] = fmt.Sprintf("pod-%08d", i)
				}
				_, bucket := newPostingsValuesFixture(b, version, BinaryFormatV2, values)
				metrics := NewStreamBinaryReaderMetrics(nil)
				b.ReportAllocs()
				for b.Loop() {
					r, err := NewStreamBinaryReader(b.Context(), ulid.MustNew(1, nil), objstore.WithNoopInstr(bucket), bucket.headerDir, Config{Version: BinaryFormatV2}, 32, log.NewNopLogger(), metrics)
					if err != nil {
						b.Fatal(err)
					}
					if err = r.Close(); err != nil {
						b.Fatal(err)
					}
				}
				b.ReportMetric(float64(bucket.requests.Load())/float64(b.N), "requests/op")
				b.ReportMetric(float64(bucket.bytes.Load())/float64(b.N), "read-bytes/op")
			})
		}
	}
}

func BenchmarkPostingsRetainedMetadata(b *testing.B) {
	for _, version := range []int{index.FormatV2, index.FormatV4} {
		b.Run(fmt.Sprintf("format=%d", version), func(b *testing.B) {
			values := make([]string, 65536)
			for i := range values {
				values[i] = fmt.Sprintf("pod-%08d", i)
			}
			_, bucket := newPostingsValuesFixture(b, version, BinaryFormatV2, values)
			readers := make([]*StreamBinaryReader, 0, b.N)
			metrics := NewStreamBinaryReaderMetrics(nil)
			runtime.GC()
			// Drain victim caches before measuring retained reader memory.
			runtime.GC()
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			b.ResetTimer()
			for range b.N {
				r, err := NewStreamBinaryReader(b.Context(), ulid.MustNew(1, nil), objstore.WithNoopInstr(bucket), bucket.headerDir, Config{Version: BinaryFormatV2}, 32, log.NewNopLogger(), metrics)
				if err != nil {
					b.Fatal(err)
				}
				readers = append(readers, r)
			}
			b.StopTimer()
			runtime.GC()
			// Drain victim caches before measuring retained reader memory.
			runtime.GC()
			runtime.ReadMemStats(&after)
			b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc))/float64(b.N), "retained-B/reader")
			runtime.KeepAlive(readers)
			for _, r := range readers {
				if err := r.Close(); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
