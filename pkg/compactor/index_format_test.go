// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"fmt"
	"path/filepath"
	"testing"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/flagext"
	"github.com/oklog/ulid/v2"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/storage/tsdb/block"
)

func TestSplitAndMergeCompactorFactory_IndexFormat(t *testing.T) {
	for _, shards := range []uint64{1, 2} {
		t.Run(fmt.Sprintf("shards=%d", shards), func(t *testing.T) {
			const tenant = "tenant"
			cfg := Config{}
			flagext.DefaultValues(&cfg)
			cfgProvider := newMockConfigProvider()
			cfgProvider.floatChunkEncodings[tenant] = chunkenc.EncXOR2
			provider, _, err := splitAndMergeCompactorFactory(t.Context(), cfg, cfgProvider, log.NewNopLogger(), prometheus.NewPedanticRegistry())
			require.NoError(t, err)
			dir := t.TempDir()
			series := make([]labels.Labels, 30)
			for n := range series {
				series[n] = labels.FromStrings("series", fmt.Sprintf("%02d", n))
			}
			var inputs []string
			for n := 0; n < 2; n++ {
				id, err := block.CreateBlock(t.Context(), dir, series, 10, int64(n*1000), int64((n+1)*1000), labels.EmptyLabels())
				require.NoError(t, err)
				inputs = append(inputs, filepath.Join(dir, id.String()))
			}
			// Changing the runtime limit changes the next job, including rewriting V4 inputs back to V2.
			for _, version := range []int{index.FormatV4, index.FormatV2} {
				cfgProvider.indexFormatVersions[tenant] = version
				outputDir := t.TempDir()
				ids, err := provider(tenant).CompactWithSplitting(outputDir, inputs, nil, shards)
				require.NoError(t, err)
				require.Len(t, ids, int(shards))
				inputs = nil
				var sampleCount uint64
				for _, id := range ids {
					require.NotEqual(t, ulid.ULID{}, id)
					path := filepath.Join(outputDir, id.String())
					reader, err := index.NewFileReader(filepath.Join(path, "index"), index.DecodePostingsRaw)
					require.NoError(t, err)
					require.Equal(t, version, reader.Version())
					require.NoError(t, reader.Close())
					b, err := tsdb.OpenBlock(nil, path, nil, nil)
					require.NoError(t, err)
					sampleCount += b.Meta().Stats.NumSamples
					require.NoError(t, b.Close())
					inputs = append(inputs, path)
				}
				require.Equal(t, uint64(600), sampleCount)
			}
		})
	}
}
