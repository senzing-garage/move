//go:build linux

package cmd_test

import (
	"os"
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

	inputFile, err := os.CreateTemp(tempDir, "move-cmd-input-*.jsonl")
	require.NoError(test, err)
	defer os.Remove(inputFile.Name())

	outputFile, err := os.CreateTemp(tempDir, "move-cmd-output-*.jsonl")
	require.NoError(test, err)
	err = os.Remove(outputFile.Name())
	require.NoError(test, err)

	test.Setenv("SENZING_TOOLS_INPUT_URL", "file://"+inputFile.Name())
	test.Setenv("SENZING_TOOLS_OUTPUT_URL", "file://"+outputFile.Name())
	test.Setenv("SENZING_TOOLS_DELAY_IN_SECONDS", "60")

	err = cmd.RunE(cmd.RootCmd, []string{})
	require.NoError(test, err)
}
