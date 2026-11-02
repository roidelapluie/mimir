// SPDX-License-Identifier: AGPL-3.0-only

package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/grafana/dskit/flagext"
	"github.com/prometheus/prometheus/tsdb/index"
)

func main() {
	// Parse CLI arguments.
	args, err := flagext.ParseFlagsAndArguments(flag.CommandLine)
	if err != nil {
		fmt.Fprintln(os.Stderr, err.Error())
		os.Exit(1)
	}

	if len(args) == 0 {
		fmt.Println("No block directory specified.")
		return
	}
	for _, blockDir := range args {
		err = printLabelValues(blockDir)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
			os.Exit(1)
		}
	}
}

func printLabelValues(blockDir string) (err error) {
	indexPath := filepath.Join(blockDir, "index")
	r, err := index.NewFileReader(indexPath, index.DecodePostingsRaw)
	if err != nil {
		return fmt.Errorf("opening block index %s: %w", indexPath, err)
	}
	defer func() {
		if closeErr := r.Close(); err == nil {
			err = closeErr
		}
	}()

	ctx := context.Background()
	names, err := r.LabelNames(ctx)
	if err != nil {
		return fmt.Errorf("reading label names: %w", err)
	}
	for _, name := range names {
		values, err := r.SortedLabelValues(ctx, name, nil)
		if err != nil {
			return fmt.Errorf("reading values for label %q: %w", name, err)
		}
		fmt.Println(name, len(values), values)
	}
	return nil
}
