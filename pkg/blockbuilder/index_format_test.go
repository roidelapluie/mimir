// SPDX-License-Identifier: AGPL-3.0-only

package blockbuilder

import (
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-kit/log"
	"github.com/grafana/dskit/user"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"
	"github.com/prometheus/common/promslog"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"

	mimir_tsdb "github.com/grafana/mimir/pkg/storage/tsdb"
	blockmeta "github.com/grafana/mimir/pkg/storage/tsdb/block"
	"github.com/grafana/mimir/pkg/util/validation"
)

func TestTSDBBuilderIndexFormat(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		t.Run(fmt.Sprintf("v4=%v", enabled), func(t *testing.T) {
			const tenant = "tenant"
			limits := map[string]*validation.Limits{tenant: {TSDBIndexV4Enabled: enabled, OutOfOrderTimeWindow: model.Duration(time.Hour)}}
			config, overrides := blockBuilderConfig(t, "kafka:9092", validation.NewMockTenantLimits(limits))
			config.BlocksStorage.TSDB.OutOfOrderCapacityMax = 2
			config.GenerateSparseIndexHeaders = true
			logger := log.NewNopLogger()
			builder := NewTSDBBuilder(0, config, overrides, logger, newTSDBBuilderMetrics(prometheus.NewPedanticRegistry()), mimir_tsdb.NewTSDBMetrics(prometheus.NewPedanticRegistry(), logger))
			t.Cleanup(func() { require.NoError(t, builder.Close()) })
			ctx := user.InjectOrgID(t.Context(), tenant)
			base := 2 * time.Hour.Milliseconds()
			// Overlapping OOO chunks exercise the re-encoding compaction path as well as the in-order head.
			for _, offset := range []int64{10000, 1000, 3000, 2000, 4000} {
				req := createWriteRequest("", floatSample(base+offset, float64(offset)), nil)
				require.NoError(t, builder.PushToStorageAndReleaseRequest(ctx, &req))
			}
			dir := t.TempDir()
			_, err := builder.CompactAndUpload(ctx, mockUploaderFunc(t, dir))
			require.NoError(t, err)
			db, err := tsdb.Open(dir, promslog.NewNopLogger(), nil, nil, nil)
			require.NoError(t, err)
			t.Cleanup(func() { require.NoError(t, db.Close()) })
			require.NotEmpty(t, db.Blocks())
			expectedVersion := index.FormatV2
			if enabled {
				expectedVersion = index.FormatV4
			}
			for _, block := range db.Blocks() {
				reader, err := index.NewFileReader(filepath.Join(dir, block.Meta().ULID.String(), "index"), index.DecodePostingsRaw)
				require.NoError(t, err)
				require.Equal(t, expectedVersion, reader.Version())
				require.NoError(t, reader.Close())
				sparsePath := filepath.Join(dir, block.Meta().ULID.String(), blockmeta.SparseIndexHeaderFilename)
				if enabled {
					require.NoFileExists(t, sparsePath)
				} else {
					require.FileExists(t, sparsePath)
				}
			}
			_, samples := blockChunks(t, db)
			expected := []sample{}
			for _, offset := range []int64{1000, 2000, 3000, 4000, 10000} {
				expected = append(expected, sample{t: base + offset, v: float64(offset)})
			}
			require.Equal(t, expected, samples)
		})
	}
}
