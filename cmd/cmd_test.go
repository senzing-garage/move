package cmd_test

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/senzing-garage/move/cmd"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// Test public functions
// ----------------------------------------------------------------------------

func Test_Execute(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "--help"}

	cmd.Execute()
}

func Test_Execute_completion(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "completion"}

	cmd.Execute()
}

func Test_Execute_docs(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "docs"}

	cmd.Execute()
}

func Test_Execute_help(test *testing.T) {
	_ = test
	os.Args = []string{"command-name", "--help"}

	cmd.Execute()
}

func Test_PreRun(test *testing.T) {
	_ = test
	args := []string{"command-name", "--help"}
	cmd.PreRun(cmd.RootCmd, args)
}

// func Test_RunE(test *testing.T) {
// 	test.Setenv("SENZING_TOOLS_AVOID_SERVING", "true")
// 	err := RunE(RootCmd, []string{})
// 	require.NoError(test, err)
// }

// func Test_RootCmd(test *testing.T) {
// 	_ = test
// 	err := RootCmd.Execute()
// 	require.NoError(test, err)
// 	err = RootCmd.RunE(RootCmd, []string{})
// 	require.NoError(test, err)
// }

func Test_CompletionCmd(test *testing.T) {
	_ = test
	err := cmd.CompletionCmd.Execute()
	require.NoError(test, err)
	err = cmd.CompletionCmd.RunE(cmd.CompletionCmd, []string{})
	require.NoError(test, err)
}

func Test_docsCmd(test *testing.T) {
	_ = test
	err := cmd.DocsCmd.Execute()
	require.NoError(test, err)
	err = cmd.DocsCmd.RunE(cmd.DocsCmd, []string{})
	require.NoError(test, err)
}

func Test_ExecuteCommand_Help(test *testing.T) {
	outbuf := bytes.NewBufferString("")
	errbuf := bytes.NewBufferString("")

	cmd.RootCmd.SetOut(outbuf)
	cmd.RootCmd.SetErr(errbuf)
	cmd.RootCmd.SetArgs([]string{"--help"})
	err := cmd.RootCmd.Execute()
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
	result := cmd.Version()
	require.Equal(t, 2, strings.Count(result, "."))
}
