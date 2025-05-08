//go:build linux

package cmd_test

import (
	"path/filepath"
	"testing"

	"github.com/senzing-garage/move/cmd"
	"github.com/stretchr/testify/require"
)

func Test_PreRun_Linux(test *testing.T) {
	_ = test
	args := []string{"command-name"}
	cmd.PreRun(cmd.RootCmd, args)
}

func Test_RunE_Linux(test *testing.T) {
	tempDir := test.TempDir()
	inputFile := filepath.Join(tempDir, "move-cmd-input.jsonl")
	outputFile := filepath.Join(tempDir, "move-cmd-output.jsonl")
	err := touchFile(inputFile)
	require.NoError(test, err)
	test.Setenv("SENZING_TOOLS_INPUT_URL", "file://"+inputFile)
	test.Setenv("SENZING_TOOLS_OUTPUT_URL", "file://"+outputFile)
	test.Setenv("SENZING_TOOLS_DELAY_IN_SECONDS", "60")
	err = cmd.RunE(cmd.RootCmd, []string{})
	require.NoError(test, err)
}
