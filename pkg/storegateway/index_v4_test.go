// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/services"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/hashcache"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/indexheader"
	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	"github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/storegateway/storepb"
)

func TestBucketStore_IndexV4(t *testing.T) {
	for _, headerVersion := range []int{indexheader.BinaryFormatV1, indexheader.BinaryFormatV2} {
		t.Run(fmt.Sprintf("header-v%d", headerVersion), func(t *testing.T) {
			ctx := t.Context()
			logger := log.NewNopLogger()
			dir := t.TempDir()
			// Enough distinct values to span several postings-offset pages.
			head, expected := createHeadWithSeries(t, 0, headGenOptions{
				TSDBDir: filepath.Join(dir, "head"), Series: 600, SamplesPerSeries: 3,
				Random: rand.New(rand.NewSource(1)),
			})
			t.Cleanup(func() { require.NoError(t, head.Close()) })
			compactor, err := tsdb.NewLeveledCompactorWithOptions(ctx, nil, promslog.NewNopLogger(), []int64{1000000}, nil, tsdb.LeveledCompactorOptions{IndexFormatVersion: index.FormatV4})
			require.NoError(t, err)
			ids, err := compactor.Write(dir, head, head.MinTime(), head.MaxTime()+1, nil)
			require.NoError(t, err)
			require.Len(t, ids, 1)
			blockDir := filepath.Join(dir, ids[0].String())
			_, err = block.InjectThanosMeta(logger, blockDir, block.ThanosMeta{Source: block.TestSource})
			require.NoError(t, err)
			bkt := objstore.NewInMemBucket()
			t.Cleanup(func() { require.NoError(t, bkt.Close()) })
			_, err = block.Upload(ctx, logger, bkt, blockDir, nil)
			require.NoError(t, err)
			instrBkt := objstore.WithNoopInstr(bkt)
			fetcher, err := block.NewMetaFetcher(logger, 1, instrBkt, "", nil, nil, 0)
			require.NoError(t, err)
			cfg := defaultPrepareStoreConfig(t)
			cfg.bucketStoreConfig.IndexHeader.Version = headerVersion
			store, err := NewBucketStore("tenant", instrBkt, newTestBucketIndexMetadataReader(t, bkt, "tenant"), fetcher,
				t.TempDir(), cfg.bucketStoreConfig, selectAllStrategy{}, newStaticChunksLimiterFactory(0), newStaticSeriesLimiterFactory(0),
				newGapBasedPartitionersHelper(mimir_tsdb.DefaultPartitionerMaxGapSize), hashcache.NewSeriesHashCache(1024*1024), NewBucketStoreMetrics(nil), WithLogger(logger),
			)
			require.NoError(t, err)
			require.NoError(t, services.StartAndAwaitRunning(ctx, store))
			t.Cleanup(func() {
				require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
				require.NoError(t, store.RemoveBlocksAndClose())
			})
			require.NoError(t, store.InitialSync(ctx))
			srv := newStoreGatewayTestServer(t, store)
			for _, tc := range []struct {
				name     string
				matchers []storepb.LabelMatcher
				expected []*storeTestSeries
			}{
				{"all", []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "foo", Value: "bar"}}, expected},
				{"last page", []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "i", Value: expected[len(expected)-1].Labels[1].Value}}, expected[len(expected)-1:]},
				{"absent", []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "i", Value: "absent"}}, nil},
			} {
				t.Run(tc.name, func(t *testing.T) {
					actual, warnings, _, _, err := srv.Series(ctx, &storepb.SeriesRequest{MinTime: math.MinInt64, MaxTime: math.MaxInt64, Matchers: tc.matchers, StreamingChunksBatchSize: 17})
					require.NoError(t, err)
					require.Empty(t, warnings)
					require.Equal(t, tc.expected, actual)
					for n, series := range actual {
						require.Len(t, series.Chunks, 1)
						gotChunk, err := chunkenc.FromData(chunkenc.EncXOR, series.Chunks[0].Raw.Data)
						require.NoError(t, err)
						expectedChunk, err := chunkenc.FromData(chunkenc.EncXOR, tc.expected[n].Chunks[0].Raw.Data)
						require.NoError(t, err)
						got, want := gotChunk.Iterator(nil), expectedChunk.Iterator(nil)
						for want.Next() == chunkenc.ValFloat {
							require.Equal(t, chunkenc.ValFloat, got.Next())
							wantT, wantV := want.At()
							gotT, gotV := got.At()
							require.Equal(t, wantT, gotT)
							require.Equal(t, wantV, gotV)
						}
						require.Equal(t, chunkenc.ValNone, got.Next())
						require.NoError(t, got.Err())
						require.NoError(t, want.Err())
					}
				})
			}
			names, err := store.LabelNames(ctx, &storepb.LabelNamesRequest{Start: math.MinInt64, End: math.MaxInt64})
			require.NoError(t, err)
			require.Equal(t, []string{"foo", "i"}, names.Names)
			expectedValues := make([]string, len(expected))
			for n, series := range expected {
				expectedValues[n] = series.Labels[1].Value
			}
			values, err := store.LabelValues(ctx, &storepb.LabelValuesRequest{Start: math.MinInt64, End: math.MaxInt64, Label: "i"})
			require.NoError(t, err)
			require.Equal(t, expectedValues, values.Values)
			values, err = store.LabelValues(ctx, &storepb.LabelValuesRequest{Start: math.MinInt64, End: math.MaxInt64, Label: "i", Matchers: []storepb.LabelMatcher{{Type: storepb.LabelMatcher_EQ, Name: "i", Value: expectedValues[len(expectedValues)-1]}}})
			require.NoError(t, err)
			require.Equal(t, expectedValues[len(expectedValues)-1:], values.Values)
		})
	}
}
