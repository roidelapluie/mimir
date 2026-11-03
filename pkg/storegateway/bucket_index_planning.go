// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"cmp"
	"context"
	"errors"
	"slices"

	"github.com/grafana/dskit/runutil"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	streamindex "github.com/grafana/mimir/pkg/storage/indexheader/index"
	"github.com/grafana/mimir/pkg/storage/tsdb"
)

// planPostingGroups uses V4 directory bounds before resolving matcher offsets.
// It keeps an intersecting group and the metric-name matcher. Other matchers can
// be deferred to series filtering when reading their offset pages alone would
// exceed the existing series-byte heuristic. Postings bounds order candidates;
// they are not treated as exact matcher selectivity or as a lower bound on cost.
func planPostingGroups(ctx context.Context, ms []*labels.Matcher, reader indexheader.Reader, strategy postingsSelectionStrategy) ([]postingGroup, []*labels.Matcher, error) {
	fallback := func() ([]postingGroup, []*labels.Matcher, error) {
		groups, err := toPostingGroups(ctx, ms, reader)
		return groups, nil, err
	}
	estimator, ok := reader.(streamindex.PostingsEstimator)
	if !ok {
		return fallback()
	}
	// Only the strategy that already permits deferred series filtering opts in.
	var costStrategy worstCaseFetchedDataStrategy
	switch s := strategy.(type) {
	case worstCaseFetchedDataStrategy:
		costStrategy = s
	case *labelValuesPostingsStrategy:
		var supported bool
		costStrategy, supported = s.matchersStrategy.(worstCaseFetchedDataStrategy)
		if !supported {
			return fallback()
		}
	default:
		return fallback()
	}
	type candidate struct {
		group    rawPostingGroup
		estimate streamindex.PostingsEstimate
	}
	candidates := make([]candidate, 0, len(ms))
	hasIntersecting := false
	for _, m := range ms {
		g := toRawPostingGroup(m)
		var values []string
		if !g.isLazy {
			values = make([]string, len(g.keys))
			for i, key := range g.keys {
				values[i] = key.Value
			}
		}
		estimate, supported, err := estimator.EstimatePostings(ctx, g.labelName, g.prefix, values)
		if err != nil {
			return nil, nil, err
		}
		if !supported {
			return fallback()
		}
		candidates = append(candidates, candidate{group: g, estimate: estimate})
		hasIntersecting = hasIntersecting || !g.isSubtract
	}
	// Negative-only queries need the all-postings group and keep the existing path.
	if !hasIntersecting {
		return fallback()
	}
	slices.SortStableFunc(candidates, func(a, b candidate) int {
		if a.group.isSubtract != b.group.isSubtract {
			if !a.group.isSubtract {
				return -1
			}
			return 1
		}
		cost := func(c candidate) float64 {
			return float64(c.estimate.OffsetBytes) + float64(c.estimate.PostingsBytes)*costStrategy.postingListActualSizeFactor
		}
		return cmp.Compare(cost(a), cost(b))
	})
	groups := make([]postingGroup, 0, len(ms))
	var pending []*labels.Matcher
	for _, c := range candidates {
		if err := ctx.Err(); err != nil {
			return nil, nil, err
		}
		maxSeries := numSeriesInSmallestIntersectingPostingGroup(groups)
		if maxSeries > 0 && c.group.labelName != model.MetricNameLabel && c.estimate.OffsetBytes > uint64(maxSeries)*uint64(tsdb.EstimatedSeriesP99Size) {
			pending = append(pending, c.group.matcher)
			continue
		}
		g, err := c.group.toPostingGroup(ctx, reader)
		if err != nil {
			return nil, nil, err
		}
		if !g.isSubtract && len(g.keys) == 0 {
			return nil, nil, nil
		}
		groups = append(groups, g)
	}
	return groups, pending, nil
}

// blockLabelValuesFromDirectory postpones target-label enumeration until matcher
// postings are available. This lets selective queries return values from series
// without first scanning the target label's remote postings-offset pages.
func blockLabelValuesFromDirectory(ctx context.Context, b *bucketBlock, strategy postingsSelectionStrategy, maxSeriesPerBatch int, labelName, prefix string, exact bool, matchers []*labels.Matcher, stats *safeQueryStats) ([]string, bool, error) {
	estimator, ok := b.indexHeaderReader.(streamindex.PostingsEstimator)
	if !ok {
		return nil, false, nil
	}
	var exactValues []string
	if exact {
		exactValues = []string{prefix}
	}
	estimate, supported, err := estimator.EstimatePostings(ctx, labelName, prefix, exactValues)
	if err != nil || !supported {
		return nil, supported, err
	}
	if estimate.Pages == 0 {
		return nil, true, nil
	}
	r := b.indexReader(strategy)
	defer runutil.CloseWithLogOnErr(b.logger, r, "close block index reader")
	refs, pending, err := r.ExpandedPostings(ctx, matchers, stats)
	if err != nil {
		return nil, true, err
	}
	if len(refs) == 0 {
		return nil, true, nil
	}
	seriesBytes := uint64(len(refs)) * uint64(tsdb.EstimatedSeriesP99Size)
	if len(pending) != 0 || seriesBytes < estimate.OffsetBytes+estimate.PostingsBytes {
		values, err := labelValuesFromSeries(ctx, labelName, maxSeriesPerBatch, pending, r, b, refs, stats)
		return values, true, err
	}
	var offsets []streamindex.PostingListOffset
	if exact {
		offset, err := b.indexHeaderReader.PostingsOffset(ctx, labelName, prefix)
		if errors.Is(err, indexheader.NotFoundRangeErr) {
			return nil, true, nil
		}
		if err != nil {
			return nil, true, err
		}
		offsets = []streamindex.PostingListOffset{{LabelValue: prefix, Off: offset}}
	} else {
		offsets, err = b.indexHeaderReader.LabelValuesOffsets(ctx, labelName, prefix, nil)
		if err != nil {
			return nil, true, err
		}
	}
	values, err := labelValuesFromPostings(ctx, labelName, r, offsets, refs, stats)
	return values, true, err
}
