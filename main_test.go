package main

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

/*
 * The unit tests in this file simulate command line invocation.
 */
func TestMain(test *testing.T) {
	tempDir := test.TempDir()
	inputFile := fmt.Sprintf("%s/move-main-input.jsonl", tempDir)
	outputFile := fmt.Sprintf("%s/move-main-output.jsonl", tempDir)
	err := touchFile(inputFile)
	assert.NoError(test, err)
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
