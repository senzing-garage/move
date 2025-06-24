//go:build linux

package main

import (
	"os"
	"testing"
)

func TestMain(test *testing.T) {

	os.Args = []string{"command-name", "--help"}

	main()
}
