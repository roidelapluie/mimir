// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"hash/crc32"
	"math"
)

// NewDecbufAt64 opens a section framed by an eight-byte payload length and a CRC32 checksum.
// Offsets in the returned buffer are relative to the beginning of the length field.
func NewDecbufAt64(ctx context.Context, factory DecbufFactory, offset int, table *crc32.Table) Decbuf {
	if offset < 0 {
		return Decbuf{E: ErrInvalidSize}
	}
	d := factory.NewRawDecbuf(ctx)
	if d.Err() != nil {
		return d
	}
	d.ResetAt(offset)
	length := d.Be64()
	if d.Err() != nil {
		return d
	}
	if length > uint64(math.MaxInt-12) || d.Len() < crc32.Size || length > uint64(d.Len()-crc32.Size) {
		d.E = ErrInvalidSize
		return d
	}
	d.r = &sectionReader{BufReader: d.r, start: offset, size: int(length) + 12}
	if table != nil {
		d.CheckCrc32(table)
		d.ResetAt(8)
	}
	return d
}

type sectionReader struct {
	BufReader
	start, size int
}

func (r *sectionReader) Offset() int   { return r.BufReader.Offset() - r.start }
func (r *sectionReader) Len() int      { return r.size - r.Offset() }
func (r *sectionReader) Buffered() int { return min(r.BufReader.Buffered(), r.Len()) }
func (r *sectionReader) Reset() error  { return r.ResetAt(0) }
func (r *sectionReader) ResetAt(off int) error {
	if off < 0 || off > r.size {
		return ErrInvalidSize
	}
	return r.BufReader.ResetAt(r.start + off)
}
func (r *sectionReader) Skip(n int) error {
	if n < 0 || n > r.Len() {
		return ErrInvalidSize
	}
	return r.BufReader.Skip(n)
}
func (r *sectionReader) Peek(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrInvalidSize
	}
	return r.BufReader.Peek(min(n, r.Len()))
}
func (r *sectionReader) Read(n int) ([]byte, error) {
	if n < 0 || n > r.Len() {
		_ = r.BufReader.Skip(r.Len())
		return nil, ErrInvalidSize
	}
	return r.BufReader.Read(n)
}
func (r *sectionReader) ReadInto(dst []byte) error {
	if len(dst) > r.Len() {
		_ = r.BufReader.Skip(r.Len())
		return ErrInvalidSize
	}
	return r.BufReader.ReadInto(dst)
}
