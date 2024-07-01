package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

/*
 * The unit tests in this file simulate command line invocation.
 */

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

	assert.Equal(t, 2, strings.Count(result, "."))
}
