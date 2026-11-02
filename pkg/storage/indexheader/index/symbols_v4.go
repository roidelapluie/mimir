// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/mimir-prometheus/blob/f551bcd7d863f7926b90ba9a02340d2ae162d145/tsdb/index/symbol_directory.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"errors"
	"hash/crc32"
	"math"

	"github.com/grafana/dskit/runutil"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

// SparseValuesFromV4SymbolsTable loads the persisted samples without scanning every symbol.
// symbolsEnd is the absolute end of the directory, including its checksum.
// Returned offsets are relative to tableOffset. The directory checksum is always verified;
// doChecksum additionally verifies the full symbol payload checksum.
func SparseValuesFromV4SymbolsTable(ctx context.Context, factory streamencoding.DecbufFactory, tableOffset, symbolsEnd int, doChecksum bool) (count int, offsets []int, err error) {
	if tableOffset < 0 || symbolsEnd < tableOffset || symbolsEnd-tableOffset < 28 {
		return 0, nil, streamencoding.ErrInvalidSize
	}
	symbols := streamencoding.NewDecbufAt64(ctx, factory, tableOffset, nil)
	defer runutil.CloseWithErrCapture(&err, &symbols, "read V4 symbols")
	if symbols.Err() != nil {
		return 0, nil, symbols.Err()
	}
	payloadEnd := symbols.Offset() + symbols.Len() - crc32.Size
	// Reserve the symbol checksum and the minimum directory framing before adding offsets.
	if payloadEnd < 12 || payloadEnd > symbolsEnd-tableOffset-16 {
		return 0, nil, streamencoding.ErrInvalidSize
	}
	if doChecksum {
		symbols.CheckCrc32(castagnoliTable)
		symbols.ResetAt(8)
		if symbols.Err() != nil {
			return 0, nil, symbols.Err()
		}
	}
	count = symbols.Be32int()
	if symbols.Err() != nil || count < 0 || count > payloadEnd-12 {
		return 0, nil, streamencoding.ErrInvalidSize
	}

	directoryOffset := tableOffset + payloadEnd + crc32.Size
	header := factory.NewRawDecbuf(ctx)
	defer runutil.CloseWithErrCapture(&err, &header, "read V4 symbol directory framing")
	if header.Err() != nil {
		return 0, nil, header.Err()
	}
	if symbolsEnd > header.Len() {
		return 0, nil, streamencoding.ErrInvalidSize
	}
	header.ResetAt(directoryOffset)
	directorySize := uint64(header.Be32())
	if header.Err() != nil || directorySize < 4 || directorySize > uint64(math.MaxInt-8) || directorySize+8 != uint64(symbolsEnd-directoryOffset) {
		return 0, nil, streamencoding.ErrInvalidSize
	}

	directory := factory.NewDecbufAtChecked(ctx, directoryOffset, castagnoliTable)
	defer runutil.CloseWithErrCapture(&err, &directory, "read V4 symbol directory")
	if directory.Err() != nil {
		return 0, nil, directory.Err()
	}
	if directory.Be32() != symbolFactor {
		return 0, nil, errors.New("invalid symbol sampling interval")
	}
	samples := (uint64(count) + symbolFactor - 1) / symbolFactor
	if samples > uint64(directory.Len()-crc32.Size) {
		return 0, nil, streamencoding.ErrInvalidSize
	}
	offsets = make([]int, 0, int(samples))
	var offset uint64
	var previous string
	for i := range samples {
		delta := directory.Uvarint64()
		if directory.Err() != nil || directory.Len() < crc32.Size || delta == 0 || offset >= uint64(payloadEnd) || delta >= uint64(payloadEnd)-offset {
			return 0, nil, streamencoding.ErrInvalidSize
		}
		offset += delta
		if i == 0 && offset != 12 {
			return 0, nil, errors.New("invalid first symbol offset")
		}
		symbols.ResetAt(int(offset))
		value, err := readV4SampleSymbol(&symbols, payloadEnd)
		if err != nil {
			return 0, nil, err
		}
		if i > 0 && value <= previous {
			return 0, nil, errors.New("unsorted sampled symbols")
		}
		previous = value
		offsets = append(offsets, int(offset))
	}
	if directory.Err() != nil || directory.Len() != crc32.Size {
		return 0, nil, errors.New("invalid symbol directory length")
	}
	if count == 0 {
		if payloadEnd != 12 {
			return 0, nil, errors.New("invalid symbol count")
		}
		return count, offsets, nil
	}

	// Validate the final group ends exactly at the symbol payload boundary.
	symbols.ResetAt(offsets[len(offsets)-1])
	remaining := count - (len(offsets)-1)*symbolFactor
	for i := range remaining {
		value, err := readV4SampleSymbol(&symbols, payloadEnd)
		if err != nil {
			return 0, nil, err
		}
		if i > 0 && value <= previous {
			return 0, nil, errors.New("unsorted symbols")
		}
		previous = value
	}
	if symbols.Offset() != payloadEnd {
		return 0, nil, errors.New("invalid symbol count")
	}
	return count, offsets, nil
}

func readV4SampleSymbol(d *streamencoding.Decbuf, payloadEnd int) (string, error) {
	length := d.Uvarint64()
	if d.Err() != nil || d.Offset() > payloadEnd || length > uint64(payloadEnd-d.Offset()) {
		return "", streamencoding.ErrInvalidSize
	}
	value := string(d.Bytes(int(length)))
	return value, d.Err()
}
