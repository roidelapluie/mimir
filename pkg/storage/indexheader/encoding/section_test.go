// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"encoding/binary"
	"hash/crc32"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/filepool"
)

func TestNewDecbufAt64(t *testing.T) {
	for _, tc := range []struct {
		name    string
		length  uint64
		corrupt bool
		want    error
	}{
		{name: "valid", length: 6},
		{name: "overflow", length: math.MaxUint64, want: ErrInvalidSize},
		{name: "truncated", length: 100, want: ErrInvalidSize},
		{name: "bad checksum", length: 6, corrupt: true, want: ErrInvalidChecksum},
	} {
		t.Run(tc.name, func(t *testing.T) {
			data := make([]byte, 3+8+6+4+3)
			binary.BigEndian.PutUint64(data[3:], tc.length)
			copy(data[11:], "abcdef")
			binary.BigEndian.PutUint32(data[17:], crc32.Checksum(data[11:17], table))
			if tc.corrupt {
				data[17] ^= 1
			}
			file := filepath.Join(t.TempDir(), "section")
			require.NoError(t, os.WriteFile(file, data, 0600))
			factory := NewFilePoolDecbufFactory(file, 0, filepool.NewFilePoolMetrics(nil))
			t.Cleanup(func() { require.NoError(t, factory.Close()) })
			d := NewDecbufAt64(t.Context(), factory, 3, table)
			defer func() { require.NoError(t, d.Close()) }()
			require.ErrorIs(t, d.Err(), tc.want)
			if tc.want != nil {
				return
			}
			require.Equal(t, 8, d.Offset())
			require.Equal(t, 10, d.Len())
			require.Equal(t, []byte("abcdef"), d.Bytes(6))
			d.ResetAt(8)
			require.Equal(t, byte('a'), d.Byte())
			d.ResetAt(18)
			require.Zero(t, d.Len())
			require.Empty(t, d.Bytes(1))
			require.ErrorIs(t, d.Err(), ErrInvalidSize)
		})
	}
}

func TestNewDecbufAt64WideSection(t *testing.T) {
	if strconv.IntSize < 64 {
		t.Skip("wide index sections require 64-bit offsets")
	}
	for _, tc := range []struct {
		name           string
		offset, length int64
		checksum       bool
	}{
		{name: "wide payload", offset: 3, length: 1<<32 + 64},
		{name: "wide offset", offset: 1<<32 + 128, length: 6, checksum: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			filename := filepath.Join(t.TempDir(), "wide-section")
			f, err := os.Create(filename)
			require.NoError(t, err)
			require.NoError(t, f.Truncate(tc.offset+8+tc.length+4))
			var header [8]byte
			binary.BigEndian.PutUint64(header[:], uint64(tc.length))
			_, err = f.WriteAt(header[:], tc.offset)
			require.NoError(t, err)
			marker := []byte("abcdef")
			_, err = f.WriteAt(marker, tc.offset+8+tc.length-int64(len(marker)))
			require.NoError(t, err)
			if tc.checksum {
				var sum [4]byte
				binary.BigEndian.PutUint32(sum[:], crc32.Checksum(marker, table))
				_, err = f.WriteAt(sum[:], tc.offset+8+tc.length)
				require.NoError(t, err)
			}
			require.NoError(t, f.Close())
			factory := NewFilePoolDecbufFactory(filename, 0, filepool.NewFilePoolMetrics(nil))
			t.Cleanup(func() { require.NoError(t, factory.Close()) })
			var checksumTable *crc32.Table
			if tc.checksum {
				checksumTable = table
			}
			d := NewDecbufAt64(t.Context(), factory, int(tc.offset), checksumTable)
			defer func() { require.NoError(t, d.Close()) }()
			require.NoError(t, d.Err())
			require.Equal(t, int(tc.length)+4, d.Len())
			d.ResetAt(8 + int(tc.length) - len(marker))
			require.Equal(t, marker, d.Bytes(len(marker)))
			require.NoError(t, d.Err())
			require.Equal(t, 8+int(tc.length), d.Offset())
			require.Equal(t, 4, d.Len())
			d.ResetAt(8 + int(tc.length) + 4)
			require.NoError(t, d.Err())
			require.Zero(t, d.Len())
			d.ResetAt(8 + int(tc.length) + 5)
			require.ErrorIs(t, d.Err(), ErrInvalidSize)
		})
	}
}

func TestNewDecbufAt64NegativeOffset(t *testing.T) {
	d := NewDecbufAt64(t.Context(), nil, -1, nil)
	require.ErrorIs(t, d.Err(), ErrInvalidSize)
}

func TestSectionReaderNegativeOffsets(t *testing.T) {
	r := &sectionReader{size: 10}
	require.ErrorIs(t, r.ResetAt(-1), ErrInvalidSize)
	require.ErrorIs(t, r.Skip(-1), ErrInvalidSize)
	_, err := r.Peek(-1)
	require.ErrorIs(t, err, ErrInvalidSize)
}
