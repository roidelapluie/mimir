// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/prometheus/prometheus/blob/main/tsdb/index/index.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"fmt"
	"hash/crc32"

	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
	"github.com/grafana/mimir/pkg/storage/indexheader/indexheaderpb"
)

func SparseValuesFromSymbolsTable(
	ctx context.Context,
	decbufFactory streamencoding.DecbufFactory,
	tableOffset int,
	doChecksum bool,
) (allSymbolsCount int, sparseSymbolsOffsets []int, err error) {
	return SparseValuesFromSymbolsTableWithVersion(ctx, decbufFactory, tableOffset, doChecksum, index.FormatV2)
}

// SparseValuesFromSymbolsTableWithVersion samples symbols using the selected index framing.
func SparseValuesFromSymbolsTableWithVersion(ctx context.Context, decbufFactory streamencoding.DecbufFactory, tableOffset int, doChecksum bool, version int) (allSymbolsCount int, sparseSymbolsOffsets []int, err error) {
	var decbuf streamencoding.Decbuf
	if version == index.FormatV4 {
		var table *crc32.Table
		if doChecksum {
			table = castagnoliTable
		}
		decbuf = streamencoding.NewDecbufAt64(ctx, decbufFactory, tableOffset, table)
	} else if doChecksum {
		decbuf = decbufFactory.NewDecbufAtChecked(ctx, tableOffset, castagnoliTable)
	} else {
		decbuf = decbufFactory.NewDecbufAtUnchecked(ctx, tableOffset)
	}

	defer runutil.CloseWithErrCapture(&err, &decbuf, "decode symbols table")
	if err := decbuf.Err(); err != nil {
		return -1, nil, fmt.Errorf("init symbol table decoding buffer: %w", decbuf.Err())
	}

	// Symbols table format:
	// ┌────────────────────┬─────────────────────┐
	// │ len <4b>           │ #symbols <4b>       │
	// ├────────────────────┴─────────────────────┤
	// │ ┌──────────────────────┬───────────────┐ │
	// │ │ len(str_1) <uvarint> │ str_1 <bytes> │ │
	// │ ├──────────────────────┴───────────────┤ │
	// │ │                . . .                 │ │
	// │ ├──────────────────────┬───────────────┤ │
	// │ │ len(str_n) <uvarint> │ str_n <bytes> │ │
	// │ └──────────────────────┴───────────────┘ │
	// ├──────────────────────────────────────────┤
	// │ CRC32 <4b>                               │
	// └──────────────────────────────────────────┘

	// Get symbols count; decbuf has already consumed the len field.
	allSymbolsCount = decbuf.Be32int()
	if decbuf.Err() != nil || allSymbolsCount < 0 || allSymbolsCount > decbuf.Len()-crc32.Size {
		return -1, nil, streamencoding.ErrInvalidSize
	}

	seen := 0
	sparseSymbolsOffsets = make([]int, 0, 1+allSymbolsCount/symbolFactor)
	for decbuf.Err() == nil && seen < allSymbolsCount {
		if seen%symbolFactor == 0 {
			sparseSymbolsOffsets = append(sparseSymbolsOffsets, decbuf.Offset())
		}
		decbuf.SkipUvarintBytes() // The symbol.
		seen++
	}

	if decbuf.Err() != nil {
		return -1, nil, decbuf.Err()
	}

	if version == index.FormatV4 && decbuf.Len() != crc32.Size {
		return -1, nil, streamencoding.ErrInvalidSize
	}
	return allSymbolsCount, sparseSymbolsOffsets, nil
}

// SparseSymbolsToProto loads the in-memory sparse symbols data into the protobuf format
func SparseSymbolsToProto(allSymbolsCount int, sparseOffsets []int) *indexheaderpb.Symbols {
	proto := &indexheaderpb.Symbols{}

	offsets := make([]int64, len(sparseOffsets))
	for i, offset := range sparseOffsets {
		offsets[i] = int64(offset)
	}

	proto.Offsets = offsets
	proto.SymbolsCount = int64(allSymbolsCount)

	return proto
}

// SparseSymbolsFromProto loads the protobuf format to in-memory sparse symbols data
func SparseSymbolsFromProto(proto *indexheaderpb.Symbols) (allSymbolsCount int, sparseOffsets []int) {
	allSymbolsCount = int(proto.SymbolsCount)
	sparseOffsets = make([]int, len(proto.Offsets))

	for i, offset := range proto.Offsets {
		sparseOffsets[i] = int(offset)
	}

	return allSymbolsCount, sparseOffsets
}
