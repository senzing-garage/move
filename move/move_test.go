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
	"strconv"
	"testing"
	"time"

	"github.com/senzing-garage/move/move"
	"github.com/senzing-garage/move/szrecord"
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

func TestBasicMove_Move_Input_Bad(test *testing.T) {
	testCases := []struct {
		name       string
		testObject move.Move
		expectErr  bool
	}{
		{
			name:      "Bad inputURL - JSONL filename",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file:///bad.jsonl",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad inputURL - GZIP filename",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file:///bad.jsonl.gz",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad inputURL - unknown file extension",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file:///extension.bad",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL - bad schema",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "bad://" + testFilename(test, testdataGZIPGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL - no schema",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "://",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL - bad URL",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "{}http://" + testFilename(test, testdataGZIPGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL - URL too short",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "/",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		// {
		// 	name:      "Bad output - short URL",
		// 	expectErr: true,
		// 	testObject: &move.BasicMove{
		// 		InputURL:   "file://" + testFilename(test, testdataJSONLGoodData),
		// 		LogLevel:   "WARN",
		// 		OutputURL:  "/",
		// 	},
		// },
		{
			name:      "Bad LogLevel",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "BAD_LOG_LEVEL",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL - bad file extension",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file://extension.bad",
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad RecordMin/RecordMax",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
				RecordMin: 5,
				RecordMax: 2,
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
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read JSONL file - monitoring every 3",
			testObject: &move.BasicMove{
				InputURL:      "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:      "WARN",
				RecordMonitor: 3,
				OutputURL:     "null://",
			},
		},
		{
			name: "Read JSONL file - write JSONL to file",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "file://" + test.TempDir() + "/output.jsonl",
			},
		},
		// {
		// 	name: "Read JSONL file - write to bad file extension",
		// 	testObject: &move.BasicMove{
		// 		InputURL:   "file://" + testFilename(test, testdataJSONLGoodData),
		// 		LogLevel:   "DEBUG",
		// 		OutputURL:  "file://" + test.TempDir() + "/output.bad",
		// 	},
		// },
		{
			name: "Read JSONL file - write GZIP to file",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "file://" + test.TempDir() + "/output.jsonl.gz",
			},
		},
		{
			name: "Read JSONL file - write to stdout",
			testObject: &move.BasicMove{
				InputURL: "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel: "WARN",
			},
		},
		{
			name: "Read JSONL file - RecordMin/RecordMax",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
				RecordMin: 2,
				RecordMax: 5,
			},
		},
		{
			name: "Read Txt file of JSONL",
			testObject: &move.BasicMove{
				FileType:  "JSONL",
				InputURL:  "file://" + testFilename(test, testdataTxtJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad JSONL file",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataJSONLBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad TXT file of JSONL",
			testObject: &move.BasicMove{
				FileType:  "JSONL",
				InputURL:  "file://" + testFilename(test, testdataTxtJSONLBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name: "Read GZIP file",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataGZIPGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read TXT file of GZIP",
			testObject: &move.BasicMove{
				FileType:  "GZ",
				InputURL:  "file://" + testFilename(test, testdataTxtGZIPGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad GZIP file",
			testObject: &move.BasicMove{
				InputURL:  "file://" + testFilename(test, testdataGZIPBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad TXT file of GZIP",
			testObject: &move.BasicMove{
				FileType:  "GZ",
				InputURL:  "file://" + testFilename(test, testdataTxtGZIPBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
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
				InputURL:  fmt.Sprintf("http://localhost:%d/%s", port, testdataJSONLGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad JSONL HTTP",
			testObject: &move.BasicMove{
				InputURL:  fmt.Sprintf("http://localhost:%d/%s", port, testdataJSONLBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name: "Read GZIP HTTP",
			testObject: &move.BasicMove{
				InputURL:  fmt.Sprintf("http://localhost:%d/%s", port, testdataGZIPGoodData),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name: "Read bad GZIP HTTP",
			testObject: &move.BasicMove{
				InputURL:  fmt.Sprintf("http://localhost:%d/%s", port, testdataGZIPBadData),
				LogLevel:  "ERROR",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL JSONL HTTP",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  fmt.Sprintf("http://localhost:%d/bad.jsonl", port),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad InputURL GZIP HTTP",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  fmt.Sprintf("http://localhost:%d/bad.jsonl.gz", port),
				LogLevel:  "WARN",
				OutputURL: "null://",
			},
		},
		{
			name:      "Bad inputURL - unknown file extension",
			expectErr: true,
			testObject: &move.BasicMove{
				InputURL:  "http:///extension.bad",
				LogLevel:  "WARN",
				OutputURL: "null://",
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

// func TestBasicMove_Move_Xxx(test *testing.T) {

// 	var err error

// 	inputFile, err := os.Open(testFilename(test, testdataJSONLGoodData))
// 	require.NoError(test, err)

// 	defer inputFile.Close()

// 	// Swap STDIN

// 	oldStdin := os.Stdin
// 	defer func() { os.Stdin = oldStdin }() // Restore original Stdin
// 	os.Stdin = inputFile

// 	testObject := &move.BasicMove{
// 		LogLevel:   "WARN",
// 		// OutputURL:  "null://",
// 	}

// 	err = testObject.Move(test.Context())
// 	require.NoError(test, err)

// }

func TestBasicMove_Move_Compare_Files(test *testing.T) {
	inputFile := testFilename(test, testdataJSONLGoodData)
	outputFile := test.TempDir() + "/output.jsonl"
	testObject := &move.BasicMove{
		InputURL:  "file://" + inputFile,
		LogLevel:  "WARN",
		OutputURL: "file://" + outputFile,
	}

	err := testObject.Move(test.Context())
	require.NoError(test, err)

	expected, err := os.ReadFile(inputFile)
	require.NoError(test, err)

	actual, err := os.ReadFile(outputFile)
	require.NoError(test, err)

	require.Equal(test, expected, actual)
}

func TestBasicMove_SzRecord(test *testing.T) {
	const (
		body     = "This is the body"
		RecordID = 999
		source   = "This is the source"
	)

	testObject := szrecord.SzRecord{
		Body:   body,
		ID:     RecordID,
		Source: source,
	}

	require.Equal(test, body, testObject.GetMessage())

	expected := source + "-" + strconv.Itoa(RecordID)
	require.Equal(test, expected, testObject.GetMessageID())
}

// ----------------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------------

func testDataDir(t *testing.T) string {
	t.Helper()

	absoluteFilePath, err := filepath.Abs("../testdata/")
	require.NoError(t, err)
	_, err = os.Stat(absoluteFilePath)
	require.NoError(t, err)

	return absoluteFilePath
}

func testFilename(t *testing.T, partialFilePath string) string {
	t.Helper()

	absoluteFilePath, err := filepath.Abs("../testdata/" + partialFilePath)
	require.NoError(t, err)
	_, err = os.Stat(absoluteFilePath)
	require.NoError(t, err)

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
