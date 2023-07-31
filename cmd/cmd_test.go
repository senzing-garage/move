package cmd

import (
	"bytes"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
 * The unit tests in this file simulate command line invocation.
 */
func TestExecute_Command_NoInputUrl(t *testing.T) {
	cmd := RootCmd
	outbuf := bytes.NewBufferString("")
	errbuf := bytes.NewBufferString("")
	cmd.SetOut(outbuf)
	cmd.SetErr(errbuf)
	cmd.SetArgs([]string{"--input-url", "none"})
	exError := RootCmd.Execute()
	if exError == nil {
		t.Fatalf("expected Execute() to generated an error")
	}
	stderr, err := io.ReadAll(errbuf)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(stderr), "validation failed") {
		t.Fatalf("expected input-url parameter error")
	}
}

// test that Help is output correctly
func TestExecute_Command_Help(t *testing.T) {
	cmd := RootCmd
	outbuf := bytes.NewBufferString("")
	errbuf := bytes.NewBufferString("")
	cmd.SetOut(outbuf)
	cmd.SetErr(errbuf)
	cmd.SetArgs([]string{"--help"})
	RootCmd.Execute()

	stdout, err := io.ReadAll(outbuf)
	if err != nil {
		t.Fatal(err)
	}
	// fmt.Println("stdout:", string(stdout))
	if !strings.Contains(string(stdout), "Available Commands") {
		t.Fatalf("expected help text")
	}
}

// Test that the version is output, this is a bit diffcult given, the
// number changes, so just make sure it has a couple of '.' chars.
func TestVersion(t *testing.T) {

	result := Version()

	assert.Equal(t, 2, strings.Count(result, "."))
}
