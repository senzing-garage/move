//go:build linux

package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_PreRun_Linux(test *testing.T) {
	_ = test
	args := []string{"command-name"}
	PreRun(RootCmd, args)
}

func Test_RunE_Linux(test *testing.T) {
	tempDir := test.TempDir()
	inputFile := filepath.Join(tempDir, "move-cmd-input.jsonl")
	outputFile := filepath.Join(tempDir, "move-cmd-output.jsonl")
	err := touchFile(inputFile)
	require.NoError(test, err)
	os.Setenv("SENZING_TOOLS_INPUT_URL", fmt.Sprintf("file://%s", inputFile))
	os.Setenv("SENZING_TOOLS_OUTPUT_URL", fmt.Sprintf("file://%s", outputFile))
	os.Setenv("SENZING_TOOLS_DELAY_IN_SECONDS", "60")
	err = RunE(RootCmd, []string{})
	require.NoError(test, err)
}
