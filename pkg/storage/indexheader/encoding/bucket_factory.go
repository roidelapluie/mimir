// SPDX-License-Identifier: AGPL-3.0-only

package encoding

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"sync"

	"github.com/thanos-io/objstore"
)

// BucketDecbufFactory creates new bucket-reader-backed Decbuf instances
// for a specific index file in object storage
type BucketDecbufFactory struct {
	bkt             objstore.BucketReader
	objectPath      string // Path to index file in bucket
	sectionLenCache map[int]int
	mu              sync.Mutex
}

// NewBucketDecbufFactory creates a new BucketDecbufFactory for the given object path.
func NewBucketDecbufFactory(bkt objstore.BucketReader, objectPath string) *BucketDecbufFactory {
	return &BucketDecbufFactory{
		bkt:        bkt,
		objectPath: objectPath,
		// Allocate cache to hold the start offsets of Symbols and Postings Offsets tables.
		sectionLenCache: make(map[int]int, 2),
	}
}

func (bf *BucketDecbufFactory) getCachedContentLength(offset int) (int, bool) {
	bf.mu.Lock()
	defer bf.mu.Unlock()
	length, ok := bf.sectionLenCache[offset]
	return length, ok
}

func (bf *BucketDecbufFactory) getContentLength(ctx context.Context, offset int) (int, error) {
	// Check for content length before making a bucket attributes call.
	if length, ok := bf.getCachedContentLength(offset); ok {
		return length, nil
	}
	attrs, err := bf.bkt.Attributes(ctx, bf.objectPath)
	if err != nil {
		return 0, fmt.Errorf("get size from %s: %w", bf.objectPath, err)
	}
	if offset > int(attrs.Size) {
		return 0, fmt.Errorf("offset greater than object size of %s", bf.objectPath)
	}

	var contentLength int
	bf.mu.Lock()
	if cachedContentLength, ok := bf.sectionLenCache[offset]; ok {
		// Section length may have been cached since we last checked
		contentLength = cachedContentLength
	} else {
		// We do not know section length yet;
		// use the lower-level BucketReader to scan the length data
		metaReader := NewBucketReader(
			ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size)-offset,
		)

		lengthBytes := make([]byte, numLenBytes)
		n, err := metaReader.Read(lengthBytes)
		if err != nil {
			bf.mu.Unlock()
			return 0, err
		}
		if n != numLenBytes {
			bf.mu.Unlock()
			return 0, fmt.Errorf("insufficient bytes read for size (got %d, wanted %d): %w", n, numLenBytes, ErrInvalidSize)
		}
		contentLength = int(binary.BigEndian.Uint32(lengthBytes))
		bf.sectionLenCache[offset] = contentLength
	}
	bf.mu.Unlock()
	return contentLength, nil
}

func (bf *BucketDecbufFactory) NewDecbufAtChecked(ctx context.Context, offset int, table *crc32.Table) Decbuf {
	contentLength, err := bf.getContentLength(ctx, offset)
	if err != nil {
		return Decbuf{E: err}
	}

	bufferLength := numLenBytes + contentLength + crc32.Size

	bufReader := NewBucketBufReader(ctx, bf.bkt, bf.objectPath, offset, bufferLength)
	// bufReader is expected start at base offset + 4 after consuming length bytes
	err = bufReader.Skip(numLenBytes)
	if err != nil {
		_ = bufReader.Close()
		return Decbuf{E: err}
	}
	d := Decbuf{r: bufReader}

	if table != nil {
		if d.CheckCrc32(table); d.Err() != nil {
			return d
		}

		// reset to the beginning of the content after reading it all for the CRC.
		d.ResetAt(numLenBytes)
	}

	return d
}

// NewDecbufInSection creates a Decbuf which can only read a section of a table.
// sectionStartOffset and sectionEndOffset are relative offsets from tableOffset (an absolute offset).
// If sectionEndOffset is beyond the end of the table, length is adjusted to only read to the end of the table.
func (bf *BucketDecbufFactory) NewDecbufInSection(ctx context.Context, tableOffset, sectionStartOffset, sectionEndOffset int) Decbuf {
	length, err := bf.getContentLength(ctx, tableOffset)
	if err != nil {
		return Decbuf{E: err}
	}

	tableRelativeEndOffset := numLenBytes + length

	// Force the reader to stop at the end of the table first
	if tableRelativeEndOffset < sectionEndOffset {
		sectionEndOffset = tableRelativeEndOffset
	}
	sectionLength := sectionEndOffset - sectionStartOffset
	if sectionLength <= 0 {
		return Decbuf{E: fmt.Errorf("section length must be greater than 0")}
	}
	bufReader := NewBucketBufReader(
		ctx,
		bf.bkt,
		bf.objectPath,
		tableOffset+sectionStartOffset,
		sectionLength,
	)

	return Decbuf{r: bufReader}
}

func (bf *BucketDecbufFactory) NewDecbufAtUnchecked(ctx context.Context, offset int) Decbuf {
	return bf.NewDecbufAtChecked(ctx, offset, nil)
}

func (bf *BucketDecbufFactory) NewRawDecbuf(ctx context.Context) Decbuf {
	const offset = 0

	attrs, err := bf.bkt.Attributes(ctx, bf.objectPath)
	if err != nil {
		return Decbuf{E: fmt.Errorf("get size from %s: %w", bf.objectPath, err)}
	}
	// Create reader from full file range
	r := NewBucketBufReader(
		ctx, bf.bkt, bf.objectPath, offset, int(attrs.Size),
	)
	d := Decbuf{r: r}
	return d
}

// Close cleans up resources associated with this BucketDecbufFactory.
// For bucket-based implementation, there are no resources to clean up;
// the bucket client lifecycle is managed by parent components.
func (bf *BucketDecbufFactory) Close() error {
	// Nothing to do for bucket-based implementation
	return nil
}

// ReadRange returns an owned copy of exactly size bytes at the absolute offset.
// It does not inspect table framing, issue an attributes request, or read ahead.
func (bf *BucketDecbufFactory) ReadRange(ctx context.Context, offset, size int) (_ []byte, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if offset < 0 || size < 0 || offset > math.MaxInt-size {
		return nil, ErrInvalidSize
	}
	if size == 0 {
		return []byte{}, nil
	}
	reader, err := bf.bkt.GetRange(ctx, bf.objectPath, int64(offset), int64(size))
	if err != nil {
		return nil, err
	}
	defer func() { err = errors.Join(err, reader.Close()) }()
	data := make([]byte, size)
	if _, err := io.ReadFull(reader, data); err != nil {
		return nil, err
	}
	return data, nil
}

// Size returns the object size without allocating a buffered reader.
func (bf *BucketDecbufFactory) Size(ctx context.Context) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	attrs, err := bf.bkt.Attributes(ctx, bf.objectPath)
	if err != nil {
		return 0, fmt.Errorf("get size from %s: %w", bf.objectPath, err)
	}
	if attrs.Size < 0 || attrs.Size > math.MaxInt {
		return 0, ErrInvalidSize
	}
	return int(attrs.Size), nil
}
