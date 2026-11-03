// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"encoding/binary"
	"hash/crc32"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPagedPostingsDecodeValidation(t *testing.T) {
	data, g := postingsValidationFixture()
	values := []string{"pod-a", "pod-b", "pod-c"}
	for _, tc := range []struct {
		name      string
		mutate    func([]byte)
		errorText string
	}{
		{name: "valid"},
		{name: "first offset", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[20:], 1) }, errorText: "invalid postings value offsets"},
		{name: "decreasing offset", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[28:], 4) }, errorText: "invalid postings value offsets"},
		{name: "offset beyond data", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[28:], 16) }, errorText: "invalid postings value offsets"},
		{name: "last offset", mutate: func(b []byte) { binary.BigEndian.PutUint32(b[32:], 14) }, errorText: "invalid postings value offsets"},
		{name: "unsorted middle", mutate: func(b []byte) { b[48] = 'z' }, errorText: "unsorted postings values"},
		{name: "first prefix", mutate: func(b []byte) { b[39] = 'a' }, errorText: "unsorted postings values"},
		{name: "last prefix", mutate: func(b []byte) { b[49] = 'z' }, errorText: "unsorted postings values"},
		{name: "overlapping postings", mutate: func(b []byte) { b[37] = 7 }, errorText: "postings offsets do not match page bounds"},
		{name: "postings outside bounds", mutate: func(b []byte) { b[38] = 48 }, errorText: "postings offset outside postings section"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := append([]byte(nil), data...)
			if tc.mutate != nil {
				tc.mutate(raw)
			}
			binary.BigEndian.PutUint32(raw[len(raw)-4:], crc32.Checksum(raw[:len(raw)-4], castagnoliTable))
			table := &pagedPostingsTable{}
			page, err := table.decode(g, 0, raw)
			if tc.errorText != "" {
				require.EqualError(t, err, tc.errorText)
				return
			}
			require.NoError(t, err)
			for i, value := range values {
				require.Equal(t, value, page.value(i))
			}
		})
	}
}

func postingsValidationFixture() ([]byte, *postingsGroup) {
	values := []string{"pod-a", "pod-b", "pod-c"}
	data := make([]byte, 20)
	binary.BigEndian.PutUint32(data, 3)
	binary.BigEndian.PutUint32(data[4:], 15)
	binary.BigEndian.PutUint64(data[8:], 100)
	data[16] = 1
	for _, offset := range []uint32{0, 5, 10, 15} {
		data = binary.BigEndian.AppendUint32(data, offset)
	}
	data = append(data, 0, 16, 32)
	for _, value := range values {
		data = append(data, value...)
	}
	data = binary.BigEndian.AppendUint32(data, 0)
	directory := binary.BigEndian.AppendUint64(nil, 0)
	directory = binary.BigEndian.AppendUint32(directory, uint32(len(data)))
	directory = binary.BigEndian.AppendUint32(directory, 3)
	directory = binary.BigEndian.AppendUint64(directory, 100)
	directory = append(directory, 0)
	g := &postingsGroup{prefix: "pod-", count: 3, data: directory, pages: []int{0}, postingsEnd: 148}
	return data, g
}

func FuzzPagedPostingsDecode(f *testing.F) {
	seed, _ := postingsValidationFixture()
	f.Add(seed)
	f.Fuzz(func(t *testing.T, raw []byte) {
		if len(raw) < 33 || len(raw) > 65536 {
			return
		}
		data := append([]byte(nil), raw...)
		binary.BigEndian.PutUint32(data[len(data)-4:], crc32.Checksum(data[:len(data)-4], castagnoliTable))
		_, g := postingsValidationFixture()
		binary.BigEndian.PutUint32(g.data[8:], uint32(len(data)))
		table := &pagedPostingsTable{}
		page, err := table.decode(g, 0, data)
		if err != nil {
			return
		}
		for i := range page.count {
			value := page.value(i)
			require.True(t, strings.HasPrefix(value, g.prefix))
			if i > 0 {
				require.Less(t, page.value(i-1), value)
			}
		}
	})
}
