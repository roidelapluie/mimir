// SPDX-License-Identifier: AGPL-3.0-only

package exporter

import (
	"strings"
	"testing"

	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"

	"github.com/grafana/mimir/pkg/util/validation"
)

func TestOverridesExporter_TSDBIndexV4(t *testing.T) {
	exporter, err := NewOverridesExporter(
		Config{EnabledMetrics: []string{"tsdb_index_v4_enabled"}},
		&validation.Limits{TSDBIndexV4Enabled: true},
		validation.NewMockTenantLimits(map[string]*validation.Limits{
			"v2": {TSDBIndexV4Enabled: false},
			"v4": {TSDBIndexV4Enabled: true},
		}), log.NewNopLogger(), nil,
	)
	require.NoError(t, err)
	require.NoError(t, testutil.CollectAndCompare(exporter, strings.NewReader(`
# HELP cortex_limits_overrides Resource limit overrides applied to tenants
# TYPE cortex_limits_overrides gauge
cortex_limits_overrides{limit_name="tsdb_index_v4_enabled",user="v2"} 0
# HELP cortex_limits_defaults Resource limit defaults for tenants without overrides
# TYPE cortex_limits_defaults gauge
cortex_limits_defaults{limit_name="tsdb_index_v4_enabled"} 1
`), "cortex_limits_overrides", "cortex_limits_defaults"))
}
