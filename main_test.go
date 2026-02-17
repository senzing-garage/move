//go:build linux

package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMain(test *testing.T) {
	tempDir := test.TempDir()

	inputFile, err := os.CreateTemp(tempDir, "move-main-input-*.jsonl")
	require.NoError(test, err)

	defer os.Remove(inputFile.Name())

	outputFile, err := os.CreateTemp(tempDir, "move-main-output-*.jsonl")
	require.NoError(test, err)
	err = os.Remove(filepath.Clean(outputFile.Name()))
	require.NoError(test, err)

	test.Setenv("SENZING_TOOLS_INPUT_URL", "file://"+inputFile.Name())
	test.Setenv("SENZING_TOOLS_OUTPUT_URL", "file://"+outputFile.Name())
	main()
}
