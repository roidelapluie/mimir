// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/index"
	"github.com/stretchr/testify/require"
)

func TestPrintLabelValues(t *testing.T) {
	for _, version := range []int{index.FormatV2, index.FormatV4} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			dir := t.TempDir()
			w, err := index.NewWriterWithOptions(t.Context(), filepath.Join(dir, "index"), index.WriterOptions{FormatVersion: version})
			require.NoError(t, err)
			for _, symbol := range []string{"a", "b", "job", "test"} {
				require.NoError(t, w.AddSymbol(symbol))
			}
			require.NoError(t, w.AddSeries(0, labels.FromStrings("a", "b", "job", "test")))
			require.NoError(t, w.Close())

			output, err := os.CreateTemp(t.TempDir(), "stdout")
			require.NoError(t, err)
			original := os.Stdout
			os.Stdout = output
			t.Cleanup(func() { os.Stdout = original; require.NoError(t, output.Close()) })
			require.NoError(t, printLabelValues(dir))
			os.Stdout = original
			_, err = output.Seek(0, io.SeekStart)
			require.NoError(t, err)
			data, err := io.ReadAll(output)
			require.NoError(t, err)
			require.Equal(t, "a 1 [b]\njob 1 [test]\n", string(data))
		})
	}
}
