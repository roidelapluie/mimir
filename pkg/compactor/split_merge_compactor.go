// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"
	"fmt"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"

	util_log "github.com/grafana/mimir/pkg/util/log"
	"github.com/grafana/mimir/pkg/util/validation"
)

func splitAndMergeGrouperFactory(_ context.Context, cfg Config, cfgProvider ConfigProvider, userID string, logger log.Logger, _ prometheus.Registerer) Grouper {
	return NewSplitAndMergeGrouper(
		userID,
		cfg.BlockRanges.ToMilliseconds(),
		cfgProvider,
		logger)
}

func splitAndMergeCompactorFactory(ctx context.Context, cfg Config, cfgProvider ConfigProvider, logger log.Logger, reg prometheus.Registerer) (BlocksCompactorProvider, Planner, error) {
	blockRanges := cfg.BlockRanges.ToMilliseconds()

	concurrencyOpts := tsdb.DefaultLeveledCompactorConcurrencyOptions()
	concurrencyOpts.MaxOpeningBlocks = cfg.MaxOpeningBlocksConcurrency
	concurrencyOpts.MaxClosingBlocks = cfg.MaxClosingBlocksConcurrency
	concurrencyOpts.SymbolsFlushersCount = cfg.SymbolsFlushersConcurrency

	// The metrics are built once and shared by every compactor: registering them twice would panic,
	// and they are aggregated across tenants anyway.
	metrics := tsdb.NewCompactorMetrics(reg)

	// Jobs for different tenants compact concurrently, so each combination of chunk encoding
	// and index format has its own compactor. All compactors share the same metrics.
	type compactorKey struct {
		encoding    chunkenc.Encoding
		indexFormat int
	}
	compactors := make(map[compactorKey]Compactor, 2*len(validation.FloatChunkEncodingValues))
	for _, value := range validation.FloatChunkEncodingValues {
		enc := validation.ParseFloatChunkEncoding(value)
		for _, version := range []int{index.FormatV2, index.FormatV4} {
			compactor, err := tsdb.NewLeveledCompactorWithOptions(ctx, nil, util_log.SlogFromGoKit(logger), blockRanges, nil, tsdb.LeveledCompactorOptions{
				Metrics:            metrics,
				FloatChunkEncoding: func() chunkenc.Encoding { return enc },
				IndexFormatVersion: version,
				// Inert here, since Mimir plans compaction itself, but preserves the previous setting.
				EnableOverlappingCompaction: true,
			})
			if err != nil {
				return nil, nil, fmt.Errorf("creating compactor for float chunk encoding %s and index format %d: %w", value, version, err)
			}
			compactor.SetConcurrencyOptions(concurrencyOpts)
			compactors[compactorKey{encoding: enc, indexFormat: version}] = compactor
		}
	}

	provider := func(userID string) Compactor {
		encoding := cfgProvider.FloatChunkEncoding(userID)
		version := cfgProvider.IndexFormatVersion(userID)
		// Downstream providers may return values outside the limits. Fall back independently.
		if version != index.FormatV4 {
			version = index.FormatV2
		}
		key := compactorKey{encoding: encoding, indexFormat: version}
		if compactor, ok := compactors[key]; ok {
			return compactor
		}
		key.encoding = validation.ParseFloatChunkEncoding(validation.DefaultFloatChunkEncodingValue)
		return compactors[key]
	}

	return provider, NewSplitAndMergePlanner(blockRanges), nil
}

// configureSplitAndMergeCompactor updates the provided configuration injecting the split-and-merge compactor.
func configureSplitAndMergeCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = splitAndMergeGrouperFactory
	cfg.BlocksCompactorFactory = splitAndMergeCompactorFactory
}
