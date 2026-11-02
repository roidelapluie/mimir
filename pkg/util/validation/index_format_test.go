// SPDX-License-Identifier: AGPL-3.0-only

package validation

import (
	"encoding/json"
	"testing"

	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
)

func TestLimitsLoading_TSDBIndexV4(t *testing.T) {
	defaults := getDefaultLimits()
	defaults.TSDBIndexV4Enabled = true
	SetDefaultLimitsForYAMLUnmarshalling(defaults)
	t.Cleanup(func() { SetDefaultLimitsForYAMLUnmarshalling(getDefaultLimits()) })
	for name, unmarshal := range map[string]func([]byte, any) error{
		"YAML": yaml.Unmarshal,
		"JSON": json.Unmarshal,
		"map": func(data []byte, out any) error {
			var config map[string]any
			if err := yaml.Unmarshal(data, &config); err != nil {
				return err
			}
			return DecodeLimitsMap(config, out)
		},
	} {
		t.Run(name, func(t *testing.T) {
			for _, tc := range []struct {
				name, input string
				version     int
			}{
				{"omitted inherits enabled global", `{}`, index.FormatV4},
				{"explicit false overrides enabled global", `{"tsdb_index_v4_enabled":false}`, index.FormatV2},
				{"explicit true", `{"tsdb_index_v4_enabled":true}`, index.FormatV4},
			} {
				t.Run(tc.name, func(t *testing.T) {
					var limits Limits
					require.NoError(t, unmarshal([]byte(tc.input), &limits))
					overrides := NewOverrides(defaults, NewMockTenantLimits(map[string]*Limits{"tenant": &limits}))
					require.Equal(t, tc.version, overrides.IndexFormatVersion("tenant"))
					require.Equal(t, index.FormatV4, overrides.IndexFormatVersion("without-override"))
				})
			}
		})
	}
	require.Equal(t, index.FormatV2, NewOverrides(getDefaultLimits(), nil).IndexFormatVersion("tenant"))
}
