// SPDX-License-Identifier: AGPL-3.0-only

package indexheader

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"testing"

	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestV4PostingsEstimates(t *testing.T) {
	r, bucket := newPostingsPageFixture(t, index.FormatV4, BinaryFormatV2, 4096)
	require.Equal(t, 32, r.postingsOffsetTable.PostingsOffsetsInMemSampling())
	for _, tc := range []struct {
		name, prefix         string
		values               []string
		pages, postingsBytes uint64
	}{
		{name: "pod", pages: 128, postingsBytes: 4096 * 16},
		{name: "pod", values: []string{"pod-0126", "pod-0127", "pod-0127"}, pages: 1, postingsBytes: 32 * 16},
		{name: "pod", values: []string{"pod-0127", "pod-0128"}, pages: 2, postingsBytes: 64 * 16},
		{name: "pod", prefix: "pod-010", pages: 1, postingsBytes: 32 * 16},
		{name: "pod", prefix: "pod-012", pages: 2, postingsBytes: 64 * 16},
		{name: "pod", prefix: "pod-999"},
		{name: "pod", prefix: "missing"},
		{name: "missing"},
		{name: "pod", values: []string{}},
		{name: "pod", values: []string{"pod-9999"}},
	} {
		t.Run(tc.name+tc.prefix, func(t *testing.T) {
			bucket.requests.Store(0)
			estimate, supported, err := r.EstimatePostings(t.Context(), tc.name, tc.prefix, tc.values)
			require.NoError(t, err)
			require.True(t, supported)
			require.Equal(t, tc.pages, estimate.Pages)
			require.Equal(t, tc.postingsBytes, estimate.PostingsBytes)
			require.Equal(t, int64(0), bucket.requests.Load())
			if tc.pages != 0 {
				require.Positive(t, estimate.OffsetBytes)
			}
		})
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, _, err := r.EstimatePostings(ctx, "pod", "", nil)
	require.ErrorIs(t, err, context.Canceled)
}

func TestV4PostingsPageReadCounts(t *testing.T) {
	r, bucket := newPostingsPageFixture(t, index.FormatV4, BinaryFormatV2, 4096)
	for _, tc := range []struct{ name, value string }{
		{"pod", "pod-0126"},
		{"pod", "pod-0127"},
		{"pod", "pod-0255"},
		{"pod", "pod-4095"},
		{"cluster", "prod"},
	} {
		t.Run(tc.name+tc.value, func(t *testing.T) {
			bucket.requests.Store(0)
			bucket.bytes.Store(0)
			estimate, _, err := r.EstimatePostings(t.Context(), tc.name, "", []string{tc.value})
			require.NoError(t, err)
			rng, err := r.PostingsOffset(t.Context(), tc.name, tc.value)
			require.NoError(t, err)
			require.Greater(t, rng.End, rng.Start)
			require.Equal(t, int64(estimate.OffsetBytes), bucket.bytes.Load())
			require.Equal(t, int64(1), bucket.requests.Load())
		})
	}
	for _, tc := range []struct {
		prefix          string
		count, requests int
	}{
		{"pod-010", 10, 1}, {"pod-012", 10, 1}, {"pod-", 4096, 1}, {"pod-999", 0, 0}, {"missing", 0, 0},
	} {
		t.Run(tc.prefix, func(t *testing.T) {
			bucket.requests.Store(0)
			values, err := r.LabelValuesOffsets(t.Context(), "pod", tc.prefix, nil)
			require.NoError(t, err)
			require.Len(t, values, tc.count)
			require.Equal(t, int64(tc.requests), bucket.requests.Load())
		})
	}
}

func TestV4PostingsScanBatches(t *testing.T) {
	values := make([]string, 65536)
	for i := range values {
		values[i] = fmt.Sprintf("pod-%08d", i)
	}
	r, bucket := newPostingsValuesFixture(t, index.FormatV4, BinaryFormatV2, values)
	estimate, _, err := r.EstimatePostings(t.Context(), "pod", "", nil)
	require.NoError(t, err)
	require.Equal(t, uint64(2048), estimate.Pages)
	result, err := r.LabelValuesOffsets(t.Context(), "pod", "", nil)
	require.NoError(t, err)
	require.Len(t, result, len(values))
	require.Equal(t, int64(2), bucket.requests.Load())
	require.Equal(t, int64(estimate.OffsetBytes), bucket.bytes.Load())
}

func TestV4PostingsScanBatchCorruption(t *testing.T) {
	r, bucket := newPostingsPageFixture(t, index.FormatV4, BinaryFormatV2, 4096)
	object := ulid.MustNew(1, nil).String() + "/index"
	reader, err := bucket.Get(t.Context(), object)
	require.NoError(t, err)
	data, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	toc, err := index.NewTOCFromByteSlice(realByteSlice(data))
	require.NoError(t, err)
	root := binary.BigEndian.Uint64(data[toc.PostingsTable+8:])
	size := binary.BigEndian.Uint64(data[toc.PostingsTable+16:])
	d := encoding.Decbuf{B: data[root : root+size-4]}
	corrupted := false
	for range d.Be32() {
		name := d.UvarintStr()
		_ = d.UvarintStr()
		for page := range d.Be32() {
			offset, size := d.Be64(), d.Be32()
			_ = d.Be32()
			_ = d.Be64()
			_ = d.UvarintStr()
			if name == "pod" && page == 1 {
				data[offset+uint64(size)-1] ^= 1
				corrupted = true
			}
		}
	}
	require.NoError(t, d.Err())
	require.True(t, corrupted)
	require.NoError(t, bucket.Upload(t.Context(), object, bytes.NewReader(data)))
	_, err = r.LabelValuesOffsets(t.Context(), "pod", "", nil)
	require.ErrorIs(t, err, encoding.ErrInvalidChecksum)
}

func TestV4PostingsEnumerationOwnership(t *testing.T) {
	for _, version := range []int{BinaryFormatV1, BinaryFormatV2} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			r, _ := newPostingsPageFixture(t, index.FormatV4, version, 4096)
			values, err := r.LabelValuesOffsets(t.Context(), "pod", "", nil)
			require.NoError(t, err)
			for i := range 4096 {
				_, err = r.PostingsOffset(t.Context(), "pod", fmt.Sprintf("pod-%04d", i))
				require.NoError(t, err)
			}
			for i, value := range values {
				require.Equal(t, fmt.Sprintf("pod-%04d", i), value.LabelValue)
			}
		})
	}
}
