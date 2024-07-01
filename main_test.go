//go:build linux

package main

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

/*
 * The unit tests in this file simulate command line invocation.
 */
func TestMain(test *testing.T) {
	tempDir := test.TempDir()
	inputFile := filepath.Join(tempDir, "move-main-input.jsonl")
	outputFile := filepath.Join(tempDir, "move-main-output.jsonl")
	err := touchFile(inputFile)
	require.NoError(test, err)
	os.Setenv("SENZING_TOOLS_INPUT_URL", fmt.Sprintf("file://%s", inputFile))
	os.Setenv("SENZING_TOOLS_OUTPUT_URL", fmt.Sprintf("file://%s", outputFile))
	main()
}

// ----------------------------------------------------------------------------
// Utiity functions
// ----------------------------------------------------------------------------

func touchFile(name string) error {
	file, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return file.Close()
}
