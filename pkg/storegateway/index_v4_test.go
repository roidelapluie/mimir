// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"context"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sync/atomic"
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
	for _, tc := range []struct {
		headerVersion int
		lazy          bool
	}{
		{indexheader.BinaryFormatV1, false},
		{indexheader.BinaryFormatV1, true},
		{indexheader.BinaryFormatV2, false},
		{indexheader.BinaryFormatV2, true},
	} {
		t.Run(fmt.Sprintf("header-v%d/lazy-%v", tc.headerVersion, tc.lazy), func(t *testing.T) {
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
			sparseObject := ids[0].String() + "/" + block.SparseIndexHeaderFilename
			exists, err := bkt.Exists(ctx, sparseObject)
			require.NoError(t, err)
			require.False(t, exists)
			require.NoFileExists(t, filepath.Join(blockDir, block.SparseIndexHeaderFilename))
			trackedBucket := &v4SparseReadTrackingBucket{Bucket: bkt}
			instrBkt := objstore.WithNoopInstr(trackedBucket)
			fetcher, err := block.NewMetaFetcher(logger, 1, instrBkt, "", nil, nil, 0)
			require.NoError(t, err)
			cfg := defaultPrepareStoreConfig(t)
			cfg.bucketStoreConfig.IndexHeader.Version = tc.headerVersion
			cfg.bucketStoreConfig.IndexHeader.LazyLoadingEnabled = tc.lazy
			storeDir := t.TempDir()
			headerFilename := block.IndexHeaderFilename
			if tc.headerVersion == indexheader.BinaryFormatV2 {
				headerFilename = block.IndexHeaderFilenameV2
			}
			headerPath := filepath.Join(storeDir, ids[0].String(), headerFilename)
			for _, state := range []string{"cold", "reopen", "corrupt-prefix"} {
				t.Run(state, func(t *testing.T) {
					if state == "corrupt-prefix" {
						// Previous stores have closed, so this damages only the persisted cache.
						f, err := os.OpenFile(headerPath, os.O_WRONLY, 0)
						require.NoError(t, err)
						n, writeErr := f.WriteAt([]byte{byte(indexheader.MagicIndex>>24) ^ 0xff}, 0)
						closeErr := f.Close()
						require.NoError(t, writeErr)
						require.Equal(t, 1, n)
						require.NoError(t, closeErr)
					}
					store, err := NewBucketStore("tenant", instrBkt, newTestBucketIndexMetadataReader(t, bkt, "tenant"), fetcher,
						storeDir, cfg.bucketStoreConfig, selectAllStrategy{}, newStaticChunksLimiterFactory(0), newStaticSeriesLimiterFactory(0),
						newGapBasedPartitionersHelper(mimir_tsdb.DefaultPartitionerMaxGapSize), hashcache.NewSeriesHashCache(1024*1024), NewBucketStoreMetrics(nil), WithLogger(logger),
					)
					require.NoError(t, err)
					require.NoError(t, services.StartAndAwaitRunning(ctx, store))
					t.Cleanup(func() {
						require.NoError(t, services.StopAndAwaitTerminated(context.Background(), store))
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
					require.Zero(t, trackedBucket.sparseReads.Load(), "V4 must not fetch a sparse sidecar")
					require.NoFileExists(t, filepath.Join(storeDir, ids[0].String(), block.SparseIndexHeaderFilename))
					require.FileExists(t, headerPath)
				})
			}
		})
	}
}

// v4SparseReadTrackingBucket records sparse-sidecar reads, including attempts for missing objects.
type v4SparseReadTrackingBucket struct {
	objstore.Bucket
	sparseReads atomic.Int64
}

func (b *v4SparseReadTrackingBucket) Get(ctx context.Context, name string) (io.ReadCloser, error) {
	if filepath.Base(name) == block.SparseIndexHeaderFilename {
		b.sparseReads.Add(1)
	}
	return b.Bucket.Get(ctx, name)
}

func (b *v4SparseReadTrackingBucket) GetRange(ctx context.Context, name string, offset, length int64) (io.ReadCloser, error) {
	if filepath.Base(name) == block.SparseIndexHeaderFilename {
		b.sparseReads.Add(1)
	}
	return b.Bucket.GetRange(ctx, name, offset, length)
}
