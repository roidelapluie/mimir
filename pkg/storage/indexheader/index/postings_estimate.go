// SPDX-License-Identifier: AGPL-3.0-only

package index

import (
	"context"
	"sort"
	"strings"
)

// PostingsEstimate bounds the data covered by candidate postings-offset pages.
// It excludes caching, transport read-ahead and request coalescing. PostingsBytes
// includes list framing and padding, not series data or an intersection estimate.
// Equality and prefix matches can select fewer entries than these page bounds.
type PostingsEstimate struct {
	Pages         uint64
	OffsetBytes   uint64
	PostingsBytes uint64
}

// PostingsEstimator optionally estimates reads from an already loaded directory.
// After directory initialization, estimating performs no page or postings reads.
// A nil values slice selects a prefix range; a non-nil slice selects exact values
// and ignores prefix. The result counts each candidate page once. A false supported
// result means the format cannot provide estimates, not that the match is empty.
type PostingsEstimator interface {
	EstimatePostings(ctx context.Context, name, prefix string, values []string) (estimate PostingsEstimate, supported bool, err error)
}

func (g *postingsGroup) valuePage(value string) int {
	if len(g.pages) == 1 {
		return 0
	}
	if !strings.HasPrefix(value, g.prefix) {
		return len(g.pages)
	}
	suffix := value[len(g.prefix):]
	return sort.Search(len(g.pages), func(i int) bool { return g.fence(i) >= suffix })
}

// prefixPages returns a conservative half-open page interval for a value prefix.
func (g *postingsGroup) prefixPages(prefix string) (int, int) {
	if strings.HasPrefix(g.prefix, prefix) {
		return 0, len(g.pages)
	}
	if !strings.HasPrefix(prefix, g.prefix) {
		return 0, 0
	}
	if len(g.pages) == 1 {
		return 0, 1
	}
	suffix := prefix[len(g.prefix):]
	first := sort.Search(len(g.pages), func(i int) bool { return g.fence(i) >= suffix })
	// Find the smallest string beyond every string with this prefix.
	upper := []byte(suffix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 255 {
			upper[i]++
			bound := string(upper[:i+1])
			last := sort.Search(len(g.pages), func(j int) bool { return g.fence(j) >= bound })
			return first, min(last+1, len(g.pages))
		}
	}
	return first, len(g.pages)
}

// EstimatePostings implements PostingsEstimator using only directory metadata.
func (r *pagedPostingsTable) EstimatePostings(ctx context.Context, name, prefix string, values []string) (PostingsEstimate, bool, error) {
	var estimate PostingsEstimate
	if err := ctx.Err(); err != nil {
		return estimate, true, err
	}
	g := r.groups[name]
	if g == nil {
		return estimate, true, nil
	}
	add := func(i int) {
		p := g.page(i)
		estimate.Pages++
		estimate.OffsetBytes += uint64(p.size)
		estimate.PostingsBytes += p.postingsEnd - p.postingsStart
	}
	if values == nil {
		first, last := g.prefixPages(prefix)
		for i := first; i < last; i++ {
			if err := ctx.Err(); err != nil {
				return PostingsEstimate{}, true, err
			}
			add(i)
		}
	} else {
		seen := make(map[int]struct{}, len(values))
		for _, value := range values {
			if err := ctx.Err(); err != nil {
				return PostingsEstimate{}, true, err
			}
			i := g.valuePage(value)
			if i == len(g.pages) {
				continue
			}
			if _, ok := seen[i]; ok {
				continue
			}
			seen[i] = struct{}{}
			add(i)
		}
	}
	return estimate, true, nil
}
