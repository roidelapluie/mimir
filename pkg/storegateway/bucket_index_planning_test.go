// SPDX-License-Identifier: AGPL-3.0-only

package storegateway

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/go-kit/log"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"github.com/thanos-io/objstore"

	"github.com/grafana/mimir/pkg/storage/indexheader"
)

type planningBucket struct {
	objstore.Bucket
	requests atomic.Int64
}

func (b *planningBucket) GetRange(ctx context.Context, name string, off, length int64) (io.ReadCloser, error) {
	b.requests.Add(1)
	return b.Bucket.GetRange(ctx, name, off, length)
}

func newPostingsPlanningFixture(tb testing.TB, version int) (*indexheader.StreamBinaryReader, *planningBucket) {
	tb.Helper()
	ctx := tb.Context()
	filename := filepath.Join(tb.TempDir(), "index")
	w, err := index.NewWriterWithOptions(ctx, filename, index.WriterOptions{FormatVersion: version})
	require.NoError(tb, err)
	symbols := []string{"cluster", "pod", "prod", "test"}
	for i := range 4096 {
		symbols = append(symbols, fmt.Sprintf("pod-%04d", i))
	}
	slices.Sort(symbols)
	for _, s := range symbols {
		require.NoError(tb, w.AddSymbol(s))
	}
	for i := range 4096 {
		cluster := "test"
		if i == 0 {
			cluster = "prod"
		}
		require.NoError(tb, w.AddSeries(storage.SeriesRef(i), labels.FromStrings("cluster", cluster, "pod", fmt.Sprintf("pod-%04d", i))))
	}
	require.NoError(tb, w.Close())
	raw, err := os.ReadFile(filename)
	require.NoError(tb, err)
	bucket := &planningBucket{Bucket: objstore.NewInMemBucket()}
	tb.Cleanup(func() { require.NoError(tb, bucket.Close()) })
	id := ulid.MustNew(1, nil)
	require.NoError(tb, bucket.Upload(ctx, id.String()+"/index", bytes.NewReader(raw)))
	r, err := indexheader.NewStreamBinaryReader(ctx, id, objstore.WithNoopInstr(bucket), tb.TempDir(), indexheader.Config{Version: indexheader.BinaryFormatV2}, 32, log.NewNopLogger(), indexheader.NewStreamBinaryReaderMetrics(nil))
	require.NoError(tb, err)
	tb.Cleanup(func() { require.NoError(tb, r.Close()) })
	bucket.requests.Store(0)
	return r, bucket
}

func TestPlanPostingGroups(t *testing.T) {
	for _, version := range []int{index.FormatV2, index.FormatV4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			r, bucket := newPostingsPlanningFixture(t, version)
			rare := labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod")
			broad := labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*0.*")
			for _, tc := range []struct {
				name     string
				matchers []*labels.Matcher
				deferred bool
			}{
				{"equality-and-regex", []*labels.Matcher{rare, broad}, true},
				{"regex-anchor", []*labels.Matcher{labels.MustNewMatcher(labels.MatchRegexp, "cluster", "pr.*"), broad}, true},
				{"negative", []*labels.Matcher{rare, labels.MustNewMatcher(labels.MatchNotRegexp, "pod", ".*9.*")}, true},
				{"empty-matching", []*labels.Matcher{rare, labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*")}, true},
				{"negative-only", []*labels.Matcher{labels.MustNewMatcher(labels.MatchNotEqual, "cluster", "prod")}, false},
				{"absent", []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "missing", "value"), broad}, false},
			} {
				t.Run(tc.name, func(t *testing.T) {
					bucket.requests.Store(0)
					groups, pending, err := planPostingGroups(t.Context(), tc.matchers, r, worstCaseFetchedDataStrategy{1})
					require.NoError(t, err)
					if version == index.FormatV4 && tc.deferred {
						require.Len(t, groups, 1)
						require.Len(t, pending, 1)
						require.Same(t, tc.matchers[1], pending[0])
						require.Equal(t, int64(1), bucket.requests.Load())
						require.Equal(t, "cluster", groups[0].keys[0].Name)
					} else {
						require.Empty(t, pending)
					}
				})
			}
			// Strategies requiring complete postings retain their existing behavior.
			_, pending, err := planPostingGroups(t.Context(), []*labels.Matcher{rare, broad}, r, selectAllStrategy{})
			require.NoError(t, err)
			require.Empty(t, pending)
		})
	}
}

func BenchmarkPostingsPlanning(b *testing.B) {
	for _, version := range []int{index.FormatV2, index.FormatV4} {
		b.Run(fmt.Sprintf("format=%d", version), func(b *testing.B) {
			for _, planned := range []bool{false, true} {
				b.Run(fmt.Sprintf("planned=%v", planned), func(b *testing.B) {
					r, bucket := newPostingsPlanningFixture(b, version)
					matchers := []*labels.Matcher{labels.MustNewMatcher(labels.MatchEqual, "cluster", "prod"), labels.MustNewMatcher(labels.MatchRegexp, "pod", ".*0.*")}
					b.ReportAllocs()
					for b.Loop() {
						if planned {
							_, _, err := planPostingGroups(b.Context(), matchers, r, worstCaseFetchedDataStrategy{1})
							require.NoError(b, err)
						} else {
							_, err := toPostingGroups(b.Context(), matchers, r)
							require.NoError(b, err)
						}
					}
					b.ReportMetric(float64(bucket.requests.Load())/float64(b.N), "requests/op")
				})
			}
		})
	}
}
