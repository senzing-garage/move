//go:build !windows
// +build !windows

package move_test

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/senzing-garage/move/move"
	"github.com/stretchr/testify/require"
)

const (
	testdataGZIPBadData      = "gzip/bad-data.jsonl.gz"
	testdataGZIPGoodData     = "gzip/good-data.jsonl.gz"
	testdataJSONLBadData     = "jsonl/bad-data.jsonl"
	testdataJSONLGoodData    = "jsonl/good-data.jsonl"
	testdataTxtGZIPBadData   = "txt/bad-data.jsonl.gz.txt"
	testdataTxtGZIPGoodData  = "txt/good-data.jsonl.gz.txt"
	testdataTxtJSONLBadData  = "txt/bad-data.jsonl.txt"
	testdataTxtJSONLGoodData = "txt/good-data.jsonl.txt"
)

// ----------------------------------------------------------------------------
// test Move method
// ----------------------------------------------------------------------------

func TestBasicMove_Move_Input_BadURL(test *testing.T) {

	testCases := []struct {
		name       string
		testObject move.Move
		expectErr  bool
	}{
		{
			name:      "Bad InputURL - bad schema",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   "bad://" + testFilename(test, testdataGZIPGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad InputURL - no schema",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   "://",
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad InputURL - bad URL",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   "{}http://" + testFilename(test, testdataGZIPGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
	}

	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			err := testCase.testObject.Move(test.Context())
			if testCase.expectErr {
				require.Error(test, err)
			} else {
				require.NoError(test, err)
			}
		})
	}
}

func TestBasicMove_Move_Input_File(test *testing.T) {

	testCases := []struct {
		name       string
		testObject move.Move
		expectErr  bool
	}{
		{
			name: "Read JSONL file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataJSONLGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read Txt file of JSONL",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataTxtJSONLGoodData),
				FileType:  "JSONL",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad JSONL file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataJSONLBadData),
				JSONOutput: true,
				LogLevel:   "ERROR",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad TXT file of JSONL",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataTxtJSONLBadData),
				FileType:  "JSONL",
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name: "Read GZIP file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataGZIPGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read TXT file of GZIP",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataTxtGZIPGoodData),
				FileType:   "GZ",
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad GZIP file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataGZIPBadData),
				JSONOutput: true,
				LogLevel:   "ERROR",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad TXT file of GZIP",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testFilename(test, testdataTxtGZIPBadData),
				FileType:   "GZ",
				JSONOutput: true,
				LogLevel:   "ERROR",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad inputURL - JSONL filename",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   "file:///bad.jsonl",
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad inputURL - GZIP filename",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   "file:///bad.jsonl.gz",
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
	}

	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			err := testCase.testObject.Move(test.Context())
			if testCase.expectErr {
				require.Error(test, err)
			} else {
				require.NoError(test, err)
			}
		})
	}
}

func TestBasicMove_Move_Input_HTTP(test *testing.T) {

	// Serve HTTP.

	server, listener, port := serveHTTP(test, testDataDir(test))

	go func() {
		err := server.Serve(*listener)
		if err != nil {
			if !errors.Is(err, http.ErrServerClosed) {
				log.Fatalf("server.Serve(): %v", err)
			}
		}
	}()

	// Test cases.

	testCases := []struct {
		name       string
		testObject move.Move
		expectErr  bool
	}{
		{
			name: "Read JSONL HTTP",
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/%s", port, testdataJSONLGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad JSONL HTTP",
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/%s", port, testdataJSONLBadData),
				JSONOutput: true,
				LogLevel:   "ERROR",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read GZIP HTTP",
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/%s", port, testdataGZIPGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad GZIP HTTP",
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/%s", port, testdataGZIPBadData),
				JSONOutput: true,
				LogLevel:   "ERROR",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad InputURL JSONL HTTP",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/bad.jsonl", port),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name:      "Bad InputURL GZIP HTTP",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:   fmt.Sprintf("http://localhost:%d/bad.jsonl.gz", port),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
	}

	for _, testCase := range testCases {
		test.Run(testCase.name, func(test *testing.T) {
			err := testCase.testObject.Move(test.Context())
			if testCase.expectErr {
				require.Error(test, err)
			} else {
				require.NoError(test, err)
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------------

func testDataDir(test *testing.T) string {
	absoluteFilePath, err := filepath.Abs("../testdata/")
	require.NoError(test, err)
	_, err = os.Stat(absoluteFilePath)
	require.NoError(test, err)
	return absoluteFilePath
}

func testFilename(test *testing.T, partialFilePath string) string {
	absoluteFilePath, err := filepath.Abs("../testdata/" + partialFilePath)
	require.NoError(test, err)
	_, err = os.Stat(absoluteFilePath)
	require.NoError(test, err)
	return absoluteFilePath
}

// Serve the requested resource on a random port.
func serveHTTP(t *testing.T, directory string) (*http.Server, *net.Listener, int) {
	t.Helper()

	var port int

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	listenerAddr, isOK := listener.Addr().(*net.TCPAddr)
	if isOK {
		port = listenerAddr.Port
	}

	fileServer := http.FileServer(http.Dir(directory))
	server := http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           fileServer,
		ReadHeaderTimeout: 2 * time.Second,
	}

	return &server, &listener, port
}
