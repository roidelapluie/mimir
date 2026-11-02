// SPDX-License-Identifier: AGPL-3.0-only

package ingester

import (
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/mimirpb"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestIngesterIndexFormat(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("v4=%v", enabled), func(t *testing.T) {
			const tenant = "tenant"
			limits := defaultLimitsTestConfig()
			limits.TSDBIndexV4Enabled = enabled
			limits.OutOfOrderTimeWindow = model.Duration(time.Hour)
			overrides := validation.NewOverrides(limits, nil)
			cfg := defaultIngesterTestConfig(t)
			cfg.BlocksStorageConfig.TSDB.OutOfOrderCapacityMax = 2
			i, ring, err := prepareIngesterWithBlockStorageAndOverrides(t, cfg, overrides, nil, "", "", prometheus.NewRegistry())
			require.NoError(t, err)
			startAndWaitHealthy(t, i, ring)
			ctx := user.InjectOrgID(t.Context(), tenant)
			// Force overlapping OOO chunks to cover both compaction writers.
			for _, ts := range []int64{10000, 1000, 3000, 2000, 4000} {
				_, err := i.Push(ctx, mimirpb.ToWriteRequest(
					[][]mimirpb.LabelAdapter{{{Name: model.MetricNameLabel, Value: "index_format"}}},
					[]mimirpb.Sample{{TimestampMs: ts, Value: float64(ts)}}, nil, nil, mimirpb.API,
				))
				require.NoError(t, err)
			}
			i.compactBlocks(ctx, true, math.MaxInt64, nil)
			db := i.getTSDB(tenant)
			require.NotEmpty(t, db.Blocks())
			expectedVersion := index.FormatV2
			if enabled {
				expectedVersion = index.FormatV4
			}
			var samples uint64
			for _, block := range db.Blocks() {
				reader, err := index.NewFileReader(filepath.Join(db.db.Dir(), block.Meta().ULID.String(), "index"), index.DecodePostingsRaw)
				require.NoError(t, err)
				require.Equal(t, expectedVersion, reader.Version())
				require.NoError(t, reader.Close())
				samples += block.Meta().Stats.NumSamples
			}
			require.Equal(t, uint64(5), samples)
		})
	}
}
