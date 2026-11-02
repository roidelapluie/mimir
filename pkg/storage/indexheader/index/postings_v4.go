// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/grafana/mimir-prometheus/blob/ffd628f20fc15cbf9ff1482cfe349c3106666a23/tsdb/index/postings_table.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Prometheus Authors.

package index

import (
	"context"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/prometheus/tsdb/encoding"
	"github.com/prometheus/prometheus/tsdb/index"

	streamencoding "github.com/grafana/mimir/pkg/storage/indexheader/encoding"
)

// pagedSource translates absolute TSDB offsets when the table is copied into an index-header.
type pagedSource struct {
	factory       streamencoding.DecbufFactory
	origin, local uint64
}

func (s *pagedSource) read(ctx context.Context, off, size uint64) (_ []byte, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if off < s.origin || off-s.origin > math.MaxInt-s.local {
		return nil, encoding.ErrInvalidSize
	}
	local := off - s.origin + s.local
	d := s.factory.NewRawDecbuf(ctx)
	defer runutil.CloseWithErrCapture(&err, &d, "read paged postings")
	if d.Err() != nil {
		return nil, d.Err()
	}
	if local > uint64(d.Len()) || size > uint64(d.Len())-local {
		return nil, encoding.ErrInvalidSize
	}
	d.ResetAt(int(local))
	b := d.Bytes(int(size))
	return b, d.Err()
}

type pagedPostingsTable struct {
	source               *pagedSource
	start, postingsStart uint64
	groups               map[string]*postingsGroup
	names                []string
	postingsEnd          uint64
}

// NewPostingsOffsetsTableV4 opens a paged directory and reads value pages on demand.
// origin is the original TSDB table offset; local is its offset in factory.
func NewPostingsOffsetsTableV4(ctx context.Context, factory streamencoding.DecbufFactory, local, origin, postingsStart, postingsEnd uint64) (PostingsOffsetsTable, error) {
	d := factory.NewRawDecbuf(ctx)
	if d.Err() != nil {
		err := d.Err()
		_ = d.Close()
		return nil, err
	}
	size := uint64(d.Len())
	if err := d.Close(); err != nil {
		return nil, err
	}
	if local > size || origin > math.MaxInt || size-local > math.MaxInt-origin || postingsStart > postingsEnd || postingsEnd > origin {
		return nil, encoding.ErrInvalidSize
	}
	source := &pagedSource{factory: factory, origin: origin, local: local}
	r, err := readPagedPostingsTable(ctx, source, origin, size-local+origin, postingsStart)
	if err != nil {
		return nil, err
	}
	r.postingsEnd = postingsEnd
	for name := range r.groups {
		r.names = append(r.names, name)
	}
	slices.Sort(r.names)
	return r, nil
}

func (r *pagedPostingsTable) IsRemote() bool {
	_, ok := r.source.factory.(*streamencoding.BucketDecbufFactory)
	return ok
}
func (r *pagedPostingsTable) PostingsOffsetsInMemSampling() int { return 128 }
func (r *pagedPostingsTable) LabelNames() ([]string, error) {
	names := make([]string, 0, len(r.names))
	for _, name := range r.names {
		if name != "" {
			names = append(names, name)
		}
	}
	return names, nil
}

func (r *pagedPostingsTable) entryRange(ctx context.Context, name string, page, entry int, p *postingsPage) (index.Range, error) {
	start := p.base + p.delta(entry)
	end := r.postingsEnd
	g := r.groups[name]
	if entry+1 < p.count {
		end = p.base + p.delta(entry+1)
	} else if page+1 < len(g.pages) {
		next, err := r.load(ctx, g, page+1)
		if err != nil {
			return index.Range{}, err
		}
		end = next.base + next.delta(0)
	} else {
		ni, _ := slices.BinarySearch(r.names, name)
		if ni+1 < len(r.names) {
			next, err := r.load(ctx, r.groups[r.names[ni+1]], 0)
			if err != nil {
				return index.Range{}, err
			}
			end = next.base + next.delta(0)
		}
	}
	if start > math.MaxInt64-4 || end < start+8 || end > r.postingsEnd {
		return index.Range{}, encoding.ErrInvalidSize
	}
	return index.Range{Start: int64(start) + 4, End: int64(end) - 4}, nil
}

func (r *pagedPostingsTable) PostingsOffset(ctx context.Context, name, value string) (index.Range, bool, error) {
	if err := ctx.Err(); err != nil {
		return index.Range{}, false, err
	}
	g := r.groups[name]
	if g == nil {
		return index.Range{}, false, nil
	}
	pi := 0
	if len(g.pages) > 1 {
		if !strings.HasPrefix(value, g.prefix) {
			return index.Range{}, false, nil
		}
		suffix := value[len(g.prefix):]
		pi = sort.Search(len(g.pages), func(i int) bool { return g.pages[i].end >= suffix })
		if pi == len(g.pages) {
			return index.Range{}, false, nil
		}
	}
	p, err := r.load(ctx, g, pi)
	if err != nil {
		return index.Range{}, false, err
	}
	i := sort.Search(p.count, func(i int) bool { return p.value(i) >= value })
	if i == p.count || p.value(i) != value {
		return index.Range{}, false, nil
	}
	rng, err := r.entryRange(ctx, name, pi, i, p)
	return rng, err == nil, err
}

func (r *pagedPostingsTable) LabelValuesOffsets(ctx context.Context, name, prefix string, filter func(string) bool) ([]PostingListOffset, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	g := r.groups[name]
	if g == nil {
		return nil, nil
	}
	var result []PostingListOffset
	for pi := range g.pages {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		p, err := r.load(ctx, g, pi)
		if err != nil {
			return nil, err
		}
		for i := 0; i < p.count; i++ {
			value := p.value(i)
			if !strings.HasPrefix(value, prefix) || filter != nil && !filter(value) {
				continue
			}
			rng, err := r.entryRange(ctx, name, pi, i, p)
			if err != nil {
				return nil, err
			}
			result = append(result, PostingListOffset{LabelValue: strings.Clone(value), Off: rng})
		}
	}
	return result, nil
}

type postingsGroup struct {
	prefix string
	count  int
	pages  []*postingsPage
}

type postingsPage struct {
	offset               uint64
	size                 uint32
	count                int
	end                  string
	err                  error
	starts, deltas, data []byte
	base                 uint64
	width                int
}

func readPagedPostingsTable(ctx context.Context, source *pagedSource, start, end, postingsStart uint64) (*pagedPostingsTable, error) {
	h, err := source.read(ctx, start, 28)
	if err != nil {
		return nil, err
	}
	if string(h[:8]) != "PPOT0001" || crc32.Checksum(h[:24], castagnoliTable) != binary.BigEndian.Uint32(h[24:]) {
		return nil, errors.New("invalid paged postings header")
	}
	off, size := binary.BigEndian.Uint64(h[8:]), binary.BigEndian.Uint64(h[16:])
	if off < start+28 || off > end || size < 8 || size > end-off {
		return nil, encoding.ErrInvalidSize
	}
	root, err := source.read(ctx, off, size)
	if err != nil {
		return nil, err
	}
	if crc32.Checksum(root[:len(root)-4], castagnoliTable) != binary.BigEndian.Uint32(root[len(root)-4:]) {
		return nil, encoding.ErrInvalidChecksum
	}
	d := encoding.Decbuf{B: root[:len(root)-4]}
	n := d.Be32()
	if uint64(n) > uint64(d.Len())/23 {
		return nil, encoding.ErrInvalidSize
	}
	r := &pagedPostingsTable{source: source, start: start, postingsStart: postingsStart, groups: make(map[string]*postingsGroup)}
	previous := ""
	nextPage := start + 28
	var total uint64
	for i := range n {
		name, prefix := d.UvarintStr(), d.UvarintStr()
		np := d.Be32()
		if d.Err() != nil || len(prefix) > 64<<10 || np == 0 || uint64(np) > uint64(d.Len())/17 || i > 0 && name <= previous {
			return nil, errors.New("invalid postings group")
		}
		previous = name
		g := &postingsGroup{prefix: prefix, pages: make([]*postingsPage, 0, np)}
		lastEnd := ""
		for j := range np {
			po, ps, count := d.Be64(), d.Be32(), d.Be32()
			fence := d.UvarintStr()
			if d.Err() != nil || po != nextPage || po > off || uint64(ps) > off-po || ps < 33 || count == 0 || count > 128 || j > 0 && fence <= lastEnd {
				return nil, errors.New("invalid postings page descriptor")
			}
			if uint64(g.count)+uint64(count) > uint64(math.MaxInt) {
				return nil, encoding.ErrInvalidSize
			}
			g.count += int(count)
			total += uint64(count)
			if total > math.MaxUint32 {
				return nil, errors.New("too many postings entries")
			}
			g.pages = append(g.pages, &postingsPage{offset: po, size: ps, count: int(count), end: fence})
			nextPage = po + uint64(ps)
			lastEnd = fence
		}
		if np == 1 && (prefix != "" || lastEnd != "") {
			return nil, errors.New("invalid single-page routing")
		}
		r.groups[name] = g
	}
	if d.Err() != nil || d.Len() != 0 || nextPage != off {
		return nil, errors.New("invalid postings directory")
	}
	return r, nil
}

func (r *pagedPostingsTable) load(ctx context.Context, g *postingsGroup, page int) (*postingsPage, error) {
	descriptor := *g.pages[page]
	p := &descriptor
	func() {
		b, err := r.source.read(ctx, p.offset, uint64(p.size))
		if err != nil {
			p.err = err
			return
		}
		if crc32.Checksum(b[:len(b)-4], castagnoliTable) != binary.BigEndian.Uint32(b[len(b)-4:]) {
			p.err = encoding.ErrInvalidChecksum
			return
		}
		n, nb := uint64(binary.BigEndian.Uint32(b)), uint64(binary.BigEndian.Uint32(b[4:]))
		width := uint64(b[16])
		if n != uint64(p.count) || width != 1 && width != 2 && width != 4 && width != 8 || b[17] != 0 || b[18] != 0 || b[19] != 0 {
			p.err = errors.New("invalid postings page header")
			return
		}
		end := 20 + 4*(n+1) + width*n
		if end+nb+4 != uint64(len(b)) {
			p.err = encoding.ErrInvalidSize
			return
		}
		p.base = binary.BigEndian.Uint64(b[8:])
		p.width = int(width)
		p.starts = b[20 : 20+4*(n+1)]
		p.deltas = b[20+4*(n+1) : end]
		p.data = b[end : end+nb]
		prev := uint32(0)
		for i := 0; i <= p.count; i++ {
			next := binary.BigEndian.Uint32(p.starts[4*i:])
			if uint64(next) > nb || next < prev || i == 0 && next != 0 || i == p.count && uint64(next) != nb {
				p.err = errors.New("invalid postings value offsets")
				return
			}
			prev = next
		}
		for i := range p.count {
			value := p.value(i)
			if i > 0 && value <= p.value(i-1) || !strings.HasPrefix(value, g.prefix) {
				p.err = errors.New("unsorted postings values")
				return
			}
			delta := p.delta(i)
			if delta > math.MaxUint64-p.base || p.base+delta < r.postingsStart || p.base+delta >= r.start {
				p.err = errors.New("postings offset outside postings section")
				return
			}
		}
		if len(g.pages) > 1 && (p.value(p.count - 1)[len(g.prefix):] != p.end || page > 0 && p.value(0)[len(g.prefix):] <= g.pages[page-1].end) {
			p.err = errors.New("postings page does not match routing")
			return
		}
	}()
	return p, p.err
}

func (p *postingsPage) value(i int) string {
	a, b := binary.BigEndian.Uint32(p.starts[4*i:]), binary.BigEndian.Uint32(p.starts[4*(i+1):])
	return yoloString(p.data[a:b])
}

func (p *postingsPage) delta(i int) uint64 {
	b := p.deltas[i*p.width:]
	switch p.width {
	case 1:
		return uint64(b[0])
	case 2:
		return uint64(binary.BigEndian.Uint16(b))
	case 4:
		return uint64(binary.BigEndian.Uint32(b))
	default:
		return binary.BigEndian.Uint64(b)
	}
}
