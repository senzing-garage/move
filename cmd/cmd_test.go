package cmd

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Test public functions
// ----------------------------------------------------------------------------

/*
 * The unit tests in this file simulate command line invocation.
 */
func Test_Execute(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "--help"}
	Execute()
}

func Test_Execute_completion(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "completion"}
	Execute()
}

func Test_Execute_docs(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "docs"}
	Execute()
}

func Test_PreRun(test *testing.T) {
	_ = test
	args := []string{"command-name"}
	PreRun(RootCmd, args)
}

func Test_RunE(test *testing.T) {
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

func Test_ExecuteCommand_Help(test *testing.T) {
	cmd := RootCmd
	outbuf := bytes.NewBufferString("")
	errbuf := bytes.NewBufferString("")
	cmd.SetOut(outbuf)
	cmd.SetErr(errbuf)
	cmd.SetArgs([]string{"--help"})
	err := RootCmd.Execute()
	require.NoError(test, err)

	stdout, err := io.ReadAll(outbuf)
	if err != nil {
		test.Fatal(err)
	}
	// fmt.Println("stdout:", string(stdout))
	if !strings.Contains(string(stdout), "Available Commands") {
		test.Fatalf("expected help text")
	}
}

// Test that the version is output, this is a bit diffcult given, the
// number changes, so just make sure it has a couple of '.' chars.
func TestVersion(t *testing.T) {
	result := Version()
	require.Equal(t, 2, strings.Count(result, "."))
}

// ----------------------------------------------------------------------------
// Test private functions
// ----------------------------------------------------------------------------

func Test_completionAction(test *testing.T) {
	var buffer bytes.Buffer
	err := completionAction(&buffer)
	require.NoError(test, err)
}

func Test_docsAction_badDir(test *testing.T) {
	var buffer bytes.Buffer
	badDir := "/tmp/no/directory/exists"
	err := docsAction(&buffer, badDir)
	require.Error(test, err)
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
