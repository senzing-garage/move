//go:build !windows
// +build !windows

package move_test

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/senzing-garage/move/move"
	"github.com/stretchr/testify/require"
)

const (
	testdataJSONLGoodData = "jsonl/good-data.jsonl"
	testdataJSONLBadData  = "jsonl/bad-data.jsonl"
	testdataGZIPGoodData  = "gzip/good-data.jsonl.gz"
	testdataGZIPBadData   = "gzip/bad-data.jsonl.gz"
)

// ----------------------------------------------------------------------------
// test Move method
// ----------------------------------------------------------------------------

func TestBasicMove_Move(test *testing.T) {

	testCases := []struct {
		name       string
		testObject move.Move
		expectErr  bool
	}{
		{
			name: "Read JSONL file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testDataPath(test, testdataJSONLGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad JSONL file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testDataPath(test, testdataJSONLBadData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read GZIP file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testDataPath(test, testdataGZIPGoodData),
				JSONOutput: true,
				LogLevel:   "WARN",
				OutputURL:  "null://",
			},
		},
		{
			name: "Read bad GZIP file",
			testObject: &move.BasicMove{
				InputURL:   "file://" + testDataPath(test, testdataGZIPBadData),
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

// Test the move method using a table of test data.
// func TestBasicMove_Move_table(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	test.Cleanup(cleanUp)

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	test.Cleanup(cleanUpTempFile)

// 	// serve jsonl file
// 	server, listener, port := serveResource(test, filename)
// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	idx := strings.LastIndex(filename, "/")

// 	// create a temporary gzip file of good test data
// 	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(test, testGoodData)
// 	test.Cleanup(cleanUpTempGZIPFile)

// 	// serve gzip file
// 	gzipServer, gzipListener, gzipPort := serveResource(test, gzipFileName)
// 	go func() {
// 		if err := gzipServer.Serve(*gzipListener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	gzipIdx := strings.LastIndex(gzipFileName, "/")

// 	type fields struct {
// 		FileType                  string
// 		InputURL                  string
// 		JSONOutput                bool
// 		LogLevel                  string
// 		MonitoringPeriodInSeconds int
// 		OutputURL                 string
// 		RecordMax                 int
// 		RecordMin                 int
// 		RecordMonitor             int
// 	}

// 	testCases := []struct {
// 		name        string
// 		fields      fields
// 		expectedErr bool
// 	}{
// 		{
// 			name:        "test read jsonl file, bad file name",
// 			fields:      fields{InputURL: "file:///bad.jsonl"},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read gzip file, bad file name",
// 			fields:      fields{InputURL: "file:///bad.gz"},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl resource",
// 			fields:      fields{InputURL: fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):])},
// 			expectedErr: false,
// 		},
// 		{
// 			name: "test read gzip resource",
// 			fields: fields{
// 				InputURL: fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):]),
// 			},
// 			expectedErr: false,
// 		},
// 		{
// 			name:        "test read jsonl resource, bad resource name",
// 			fields:      fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.jsonl", port)},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read gzip resource, bad resource name",
// 			fields:      fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort)},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url schema",
// 			fields:      fields{InputURL: "bad://" + filename},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url",
// 			fields:      fields{InputURL: "{}http://" + filename},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url",
// 			fields:      fields{InputURL: "://"},
// 			expectedErr: true,
// 		},
// 	}
// 	for _, testCase := range testCases {
// 		test.Run(testCase.name, func(test *testing.T) {
// 			basicMove := &move.BasicMove{
// 				InputURL: testCase.fields.InputURL,
// 			}
// 			if err := basicMove.Move(ctx); (err != nil) != testCase.expectedErr {
// 				test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, testCase.expectedErr)
// 			}
// 		})
// 	}

// 	writer.Close()

// 	// shutdown servers
// 	err := server.Shutdown(ctx)
// 	require.NoError(test, err)

// 	err = gzipServer.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// Test the move method using a table of test data.
// func TestBasicMove_Move_json_output_table(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStderr(test)
// 	test.Cleanup(cleanUp)

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	// serve jsonl file
// 	server, listener, port := serveResource(test, filename)
// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	idx := strings.LastIndex(filename, "/")

// 	// create a temporary gzip file of good test data
// 	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(test, testGoodData)
// 	defer cleanUpTempGZIPFile()

// 	// serve gzip file
// 	gzipServer, gzipListener, gzipPort := serveResource(test, gzipFileName)
// 	go func() {
// 		if err := gzipServer.Serve(*gzipListener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	gzipIdx := strings.LastIndex(gzipFileName, "/")

// 	type fields struct {
// 		FileType                  string
// 		InputURL                  string
// 		JSONOutput                bool
// 		LogLevel                  string
// 		MonitoringPeriodInSeconds int
// 		OutputURL                 string
// 		RecordMax                 int
// 		RecordMin                 int
// 		RecordMonitor             int
// 	}

// 	testCases := []struct {
// 		name        string
// 		fields      fields
// 		expectedErr bool
// 	}{
// 		{
// 			name:        "test read jsonl file",
// 			fields:      fields{InputURL: "file://" + filename, JSONOutput: true},
// 			expectedErr: false,
// 		},
// 		{
// 			name:        "test read gzip file",
// 			fields:      fields{InputURL: "file://" + gzipFileName, JSONOutput: true},
// 			expectedErr: false,
// 		},
// 		{
// 			name:        "test read jsonl file, bad file name",
// 			fields:      fields{InputURL: "file:///bad.jsonl", JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read gzip file, bad file name",
// 			fields:      fields{InputURL: "file:///bad.gz", JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name: "test read jsonl resource",
// 			fields: fields{
// 				InputURL:   fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]),
// 				JSONOutput: true,
// 			},
// 			expectedErr: false,
// 		},
// 		{
// 			name: "test read gzip resource",
// 			fields: fields{
// 				InputURL:   fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):]),
// 				JSONOutput: true,
// 			},
// 			expectedErr: false,
// 		},
// 		{
// 			name:        "test read jsonl resource, bad resource name",
// 			fields:      fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.jsonl", port), JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read gzip resource, bad resource name",
// 			fields:      fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort), JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url schema",
// 			fields:      fields{InputURL: "bad://" + filename, JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url",
// 			fields:      fields{InputURL: "{}http://" + filename, JSONOutput: true},
// 			expectedErr: true,
// 		},
// 		{
// 			name:        "test read jsonl file, bad url",
// 			fields:      fields{InputURL: "://", JSONOutput: true},
// 			expectedErr: true,
// 		},
// 	}
// 	for _, testCase := range testCases {
// 		ctx := test.Context()
// 		test.Run(testCase.name, func(test *testing.T) {
// 			basicMove := &move.BasicMove{
// 				InputURL:   testCase.fields.InputURL,
// 				JSONOutput: testCase.fields.JSONOutput,
// 			}
// 			if err := basicMove.Move(ctx); (err != nil) != testCase.expectedErr {
// 				test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, testCase.expectedErr)
// 			}
// 		})
// 	}

// 	writer.Close()

// 	// shutdown servers
// 	err := server.Shutdown(ctx)
// 	require.NoError(test, err)

// 	err = gzipServer.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// Test the move method, with a single jsonl file.
// func TestBasicMove_Move(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	mover := &move.BasicMove{
// 		InputURL: "file://" + filename,
// 	}
// 	expectedErr := false

// 	if err := mover.Move(ctx); (err != nil) != expectedErr {
// 		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, expectedErr)
// 	}

// 	writer.Close()
// }

// Test the move method, with a single unknown file type.
// func TestBasicMove_Move_unknown_file_type(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "txt")
// 	defer cleanUpTempFile()

// 	mover := &move.BasicMove{
// 		InputURL: "file://" + filename,
// 	}
// 	expectedErr := true

// 	if err := mover.Move(ctx); (err != nil) != expectedErr {
// 		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, expectedErr)
// 	}

// 	writer.Close()
// }

// Test the move method, with a single unknown resource type.
// func TestBasicMove_Move_unknown_resource_type(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "txt")
// 	defer cleanUpTempFile()

// 	mover := &move.BasicMove{
// 		InputURL: "http://" + filename,
// 	}
// 	expectedErr := true

// 	if err := mover.Move(ctx); (err != nil) != expectedErr {
// 		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, expectedErr)
// 	}

// 	writer.Close()
// }

// Test the move method, with a single jsonl file.
// func TestBasicMove_Move_wait_for_logStats(test *testing.T) {
// 	ctx := test.Context()

// 	reader, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	// create a temporary jsonl file of good test data
// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	mover := &move.BasicMove{
// 		InputURL:                  "file://" + filename,
// 		MonitoringPeriodInSeconds: 1,
// 	}
// 	expectedErr := false

// 	if err := mover.Move(ctx); (err != nil) != expectedErr {
// 		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, expectedErr)
// 	}

// 	time.Sleep(2 * time.Second)

// 	writer.Close()

// 	out, _ := io.ReadAll(reader)
// 	actual := string(out)

// 	expected := "CPUs"
// 	if !strings.Contains(actual, expected) {
// 		test.Errorf("MoveImpl.Move() = %v, want %v", actual, expected)
// 	}
// }

// ----------------------------------------------------------------------------
// test processJSONL method
// ----------------------------------------------------------------------------

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_processJSONL(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	file, err := os.Open(filename)
// 	if err != nil {
// 		test.Fatal(err)
// 	}

// 	defer file.Close()

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL:      "file://" + filename,
// 		RecordMax:     11,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	mover.ProcessJSONL(filename, file, recordchan)

// 	writer.Close()

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 10
// 	if actual != expected {
// 		test.Errorf("MoveImpl.processJSONL() error = %v, want %v", err, expected)
// 	}
// }

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_processJSONL_bad_records(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, cleanUpTempFile := createTempDataFile(test, testBadData, "jsonl")
// 	defer cleanUpTempFile()

// 	file, err := os.Open(filename)
// 	if err != nil {
// 		test.Fatal(err)
// 	}

// 	defer file.Close()

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL:      "file://" + filename,
// 		RecordMax:     14,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	mover.ProcessJSONL(filename, file, recordchan)

// 	writer.Close()

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 9
// 	require.Equal(test, expected, actual)
// }

// ----------------------------------------------------------------------------
// test file read methods
// ----------------------------------------------------------------------------

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_readJSONLFile(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL:      "file://" + filename,
// 		RecordMax:     11,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	err := mover.ReadJSONLFile(filename, recordchan)

// 	writer.Close()
// 	require.NoError(test, err)

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 10
// 	require.Equal(test, expected, actual)
// }

// Attempt to read jsonl file that doesn't exist.
// func TestBasicMove_readJSONLFile_file_does_not_exist(test *testing.T) {
// 	filename := "bad.jsonl"

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL: "file://" + filename,
// 	}

// 	err := mover.ReadJSONLFile(filename, recordchan)
// 	require.Error(test, err)
// }

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_readGZIPFile(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, cleanUpTempFile := createTempGZIPDataFile(test, testGoodData)
// 	defer cleanUpTempFile()

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL:      "file://" + filename,
// 		RecordMax:     11,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	err := mover.ReadGZIPFile(filename, recordchan)

// 	writer.Close()

// 	require.NoError(test, err)

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 10
// 	require.Equal(test, expected, actual)
// }

// Attempt to read jsonl file that doesn't exist.
// func TestBasicMove_readGZIPFile_file_does_not_exist(test *testing.T) {
// 	filename := "bad.gz"

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		InputURL: "file://" + filename,
// 	}

// 	err := mover.ReadGZIPFile(filename, recordchan)
// 	require.Error(test, err)
// }

// ----------------------------------------------------------------------------
// test resource read methods
// ----------------------------------------------------------------------------

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_readJSONLResource(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
// 	defer cleanUpTempFile()

// 	server, listener, port := serveResource(test, filename)
// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	recordchan := make(chan queues.Record, 15)
// 	idx := strings.LastIndex(filename, "/")
// 	mover := &move.BasicMove{
// 		RecordMax:     11,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	err := mover.ReadJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

// 	writer.Close()

// 	require.NoError(test, err)

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 10
// 	require.Equal(test, expected, actual)

// 	err = server.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// Attempt to read jsonl file that doesn't exist.
// func TestBasicMove_readJSONLResource_file_does_not_exist(test *testing.T) {
// 	ctx := test.Context()
// 	filename := "/bad.jsonl"

// 	server, listener, port := serveResource(test, filename)
// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	recordchan := make(chan queues.Record, 15)

// 	idx := strings.LastIndex(filename, "/")
// 	mover := &move.BasicMove{}
// 	err := mover.ReadJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
// 	require.Error(test, err)

// 	err = server.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// Read jsonl file successfully, no record validation errors.
// func TestBasicMove_readGZIPResource(test *testing.T) {
// 	ctx := test.Context()

// 	_, writer, cleanUp := mockStdout(test)
// 	test.Cleanup(cleanUp)

// 	filename, moreCleanUp := createTempGZIPDataFile(test, testGoodData)
// 	test.Cleanup(moreCleanUp)

// 	server, listener, port := serveResource(test, filename)

// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	recordchan := make(chan queues.Record, 15)
// 	idx := strings.LastIndex(filename, "/")
// 	mover := &move.BasicMove{
// 		RecordMax:     11,
// 		RecordMin:     2,
// 		RecordMonitor: 5,
// 	}
// 	err := mover.ReadGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

// 	writer.Close()

// 	require.NoError(test, err)

// 	actual := 0
// 	for range recordchan {
// 		actual++
// 	}

// 	expected := 10
// 	require.Equal(test, expected, actual)

// 	err = server.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// Attempt to read jsonl file that doesn't exist.
// func TestBasicMove_readGZIPResource_file_does_not_exist(test *testing.T) {
// 	ctx := test.Context()
// 	filename := "/bad.gz"

// 	server, listener, port := serveResource(test, filename)
// 	go func() {
// 		if err := server.Serve(*listener); !errors.Is(err, http.ErrServerClosed) {
// 			log.Fatalf("server.Serve(): %v", err)
// 		}
// 	}()

// 	recordchan := make(chan queues.Record, 15)
// 	idx := strings.LastIndex(filename, "/")

// 	mover := &move.BasicMove{}
// 	err := mover.ReadGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
// 	require.Error(test, err)

// 	err = server.Shutdown(ctx)
// 	require.NoError(test, err)
// }

// ----------------------------------------------------------------------------
// test write methods
// ----------------------------------------------------------------------------

// func TestBasicMove_writeStdout(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	defer cleanUp()

// 	filename, moreCleanUp := createTempDataFile(test, testGoodData, "jsonl")
// 	defer moreCleanUp()

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		// FileType:                  tt.fields.FileType,
// 		InputURL: "file://" + filename,
// 		// LogLevel:                  tt.fields.LogLevel,
// 		// MonitoringPeriodInSeconds: tt.fields.MonitoringPeriodInSeconds,
// 		// OutputUrl:                 tt.fields.OutputUrl,
// 		// RecordMax:                 tt.fields.RecordMax,
// 		// RecordMin:                 tt.fields.RecordMin,
// 		// RecordMonitor:             tt.fields.RecordMonitor,
// 	}

// 	err := mover.ReadJSONLFile(filename, recordchan)
// 	require.NoError(test, err)

// 	err = mover.WriteStdout(recordchan)
// 	require.NoError(test, err, "MoveImpl.writeStdout() = %v, want %v")
// 	writer.Close()
// }

// func TestBasicMove_writeStdout_no_stdout(test *testing.T) {
// 	_, writer, cleanUp := mockStdout(test)
// 	test.Cleanup(cleanUp)

// 	filename, moreCleanUp := createTempDataFile(test, testGoodData, "jsonl")
// 	test.Cleanup(moreCleanUp)

// 	recordchan := make(chan queues.Record, 15)

// 	mover := &move.BasicMove{
// 		// FileType:                  tt.fields.FileType,
// 		InputURL: "file://" + filename,
// 		// LogLevel:                  tt.fields.LogLevel,
// 		// MonitoringPeriodInSeconds: tt.fields.MonitoringPeriodInSeconds,
// 		// OutputUrl:                 tt.fields.OutputUrl,
// 		// RecordMax:                 tt.fields.RecordMax,
// 		// RecordMin:                 tt.fields.RecordMin,
// 		// RecordMonitor:             tt.fields.RecordMonitor,
// 	}

// 	err := mover.ReadJSONLFile(filename, recordchan)
// 	require.NoError(test, err)

// 	output := os.Stdout
// 	/os.Stdout = nil
// 	err = mover.WriteStdout(recordchan)
// 	require.Error(test, err, "MoveImpl.writeStdout()")

// 	os.Stdout = output

// 	writer.Close()
// }

// func TestBasicMove_SetLogLevel(test *testing.T) {
// 	type fields struct {
// 		FileType                  string
// 		InputURL                  string
// 		JSONOutput                bool
// 		LogLevel                  string
// 		MonitoringPeriodInSeconds int
// 		OutputURL                 string
// 		RecordMax                 int
// 		RecordMin                 int
// 		RecordMonitor             int
// 	}

// 	testCases := []struct {
// 		name         string
// 		fields       fields
// 		logLevelName string
// 		expectedErr  bool
// 	}{
// 		{
// 			name:         "Test SetLogLevel",
// 			fields:       fields{LogLevel: "info"},
// 			logLevelName: "DEBUG",
// 			expectedErr:  false,
// 		},
// 		{
// 			name:         "Test SetLogLevel",
// 			fields:       fields{LogLevel: "info"},
// 			logLevelName: "bad",
// 			expectedErr:  true,
// 		},
// 		{
// 			name:         "Test SetLogLevel",
// 			fields:       fields{JSONOutput: true, LogLevel: "info"},
// 			logLevelName: "DEBUG",
// 			expectedErr:  false,
// 		},
// 		{
// 			name:         "Test SetLogLevel",
// 			fields:       fields{JSONOutput: true, LogLevel: "info"},
// 			logLevelName: "bad",
// 			expectedErr:  true,
// 		},
// 	}
// 	for _, testCase := range testCases {
// 		ctx := test.Context()
// 		test.Run(testCase.name, func(test *testing.T) {
// 			basicMove := &move.BasicMove{
// 				LogLevel: testCase.fields.LogLevel,
// 			}
// 			if err := basicMove.SetLogLevel(ctx, testCase.logLevelName); (err != nil) != testCase.expectedErr {
// 				test.Errorf("MoveImpl.SetLogLevel() error = %v, wantErr %v", err, testCase.expectedErr)
// 			}

// 			if !testCase.expectedErr {
// 				actual := basicMove.Logger().GetLogLevel()
// 				if actual != testCase.logLevelName {
// 					test.Errorf("MoveImpl.SetLogLevel() got = %v, want %v", actual, testCase.logLevelName)
// 				}
// 			}
// 		})
// 	}
// }

// ----------------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------------

func testDataPath(test *testing.T, partialFilePath string) string {
	absoluteFilePath, err := filepath.Abs("../testdata/" + partialFilePath)
	require.NoError(test, err)
	_, err = os.Stat(absoluteFilePath)
	require.NoError(test, err)
	return absoluteFilePath
}

// Create a tempdata file with the given content and extension.
// func createTempDataFile(t *testing.T, content string, fileextension string) (string, func()) {
// 	t.Helper()

// 	var filename string

// 	tmpfile, err := os.CreateTemp(t.TempDir(), "test.*."+fileextension)
// 	require.NoError(t, err)

// 	_, err = tmpfile.WriteString(content)
// 	require.NoError(t, err)

// 	filename = tmpfile.Name()

// 	err = tmpfile.Close()
// 	require.NoError(t, err)

// 	return filename,
// 		func() {
// 			os.Remove(filename)
// 		}
// }

// Serve the requested resource on a random port.
// func serveResource(t *testing.T, filename string) (*http.Server, *net.Listener, int) {
// 	t.Helper()

// 	var port int

// 	listener, err := net.Listen("tcp", "127.0.0.1:0")
// 	require.NoError(t, err)

// 	listenerAddr, isOK := listener.Addr().(*net.TCPAddr)
// 	if isOK {
// 		port = listenerAddr.Port
// 	}

// 	idx := strings.LastIndex(filename, string(os.PathSeparator))
// 	fileServer := http.FileServer(http.Dir(filename[:idx]))
// 	server := http.Server{
// 		Addr:              fmt.Sprintf(":%d", port),
// 		Handler:           fileServer,
// 		ReadHeaderTimeout: 2 * time.Second,
// 	}

// 	return &server, &listener, port
// }

// Capture stdout for testing.
// func mockStdout(t *testing.T) (*os.File, *os.File, func()) {
// 	t.Helper()

// 	var (
// 		reader *os.File
// 		writer *os.File
// 	)

// 	origStdout := os.Stdout
// 	reader, writer, err := os.Pipe()
// 	require.NoError(t, err)

// 	os.Stdout = writer

// 	return reader,
// 		writer,
// 		func() {
// 			// clean-up
// 			os.Stdout = origStdout
// 		}
// }

// Capture stderr for testing.
// func mockStderr(t *testing.T) (*os.File, *os.File, func()) {
// 	t.Helper()

// 	var (
// 		reader *os.File
// 		writer *os.File
// 	)

// 	origStderr := os.Stderr
// 	reader, writer, err := os.Pipe()
// 	require.NoError(t, err)

// 	os.Stderr = writer

// 	return reader,
// 		writer,
// 		func() {
// 			// clean-up
// 			os.Stderr = origStderr
// 		}
// }
