//go:build !windows
// +build !windows

package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/senzing-garage/go-queueing/queues"
	"github.com/stretchr/testify/require"
)

// ----------------------------------------------------------------------------
// test Move method
// ----------------------------------------------------------------------------

// test the move method using a table of test data
func TestBasicMove_Move_table(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	// serve jsonl file
	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	idx := strings.LastIndex(filename, "/")

	// create a temporary gzip file of good test data
	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(test, testGoodData)
	defer cleanUpTempGZIPFile()

	// serve gzip file
	gzipServer, gzipListener, gzipPort := serveResource(test, gzipFileName)
	go func() {
		if err := gzipServer.Serve(*gzipListener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	gzipIdx := strings.LastIndex(gzipFileName, "/")

	type fields struct {
		FileType                  string
		InputURL                  string
		JSONOutput                bool
		LogLevel                  string
		MonitoringPeriodInSeconds int
		OutputURL                 string
		RecordMax                 int
		RecordMin                 int
		RecordMonitor             int
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{name: "test read jsonl file", fields: fields{InputURL: fmt.Sprintf("file://%s", filename)}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip file", fields: fields{InputURL: fmt.Sprintf("file://%s", gzipFileName)}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl file, bad file name", fields: fields{InputURL: "file:///bad.jsonl"}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip file, bad file name", fields: fields{InputURL: "file:///bad.gz"}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl resource", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):])}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip resource", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):])}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl resource, bad resource name", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.jsonl", port)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip resource, bad resource name", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url schema", fields: fields{InputURL: fmt.Sprintf("bad://%s", filename)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputURL: fmt.Sprintf("{}http://%s", filename)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputURL: "://"}, args: args{ctx: context.Background()}, wantErr: true},
	}
	for _, tt := range tests {
		test.Run(tt.name, func(test *testing.T) {
			m := &BasicMove{
				InputURL: tt.fields.InputURL,
			}
			if err := m.Move(tt.args.ctx); (err != nil) != tt.wantErr {
				test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	w.Close()

	// shutdown servers
	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
	if err := gzipServer.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
}

// test the move method using a table of test data
func TestBasicMove_Move_json_output_table(test *testing.T) {

	_, w, cleanUp := mockStderr(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	// serve jsonl file
	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	idx := strings.LastIndex(filename, "/")

	// create a temporary gzip file of good test data
	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(test, testGoodData)
	defer cleanUpTempGZIPFile()

	// serve gzip file
	gzipServer, gzipListener, gzipPort := serveResource(test, gzipFileName)
	go func() {
		if err := gzipServer.Serve(*gzipListener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	gzipIdx := strings.LastIndex(gzipFileName, "/")

	type fields struct {
		FileType                  string
		InputURL                  string
		JSONOutput                bool
		LogLevel                  string
		MonitoringPeriodInSeconds int
		OutputURL                 string
		RecordMax                 int
		RecordMin                 int
		RecordMonitor             int
	}
	type args struct {
		ctx context.Context
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{name: "test read jsonl file", fields: fields{InputURL: fmt.Sprintf("file://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip file", fields: fields{InputURL: fmt.Sprintf("file://%s", gzipFileName), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl file, bad file name", fields: fields{InputURL: "file:///bad.jsonl", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip file, bad file name", fields: fields{InputURL: "file:///bad.gz", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl resource", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip resource", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):]), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl resource, bad resource name", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.jsonl", port), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip resource, bad resource name", fields: fields{InputURL: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url schema", fields: fields{InputURL: fmt.Sprintf("bad://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputURL: fmt.Sprintf("{}http://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputURL: "://", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
	}
	for _, tt := range tests {
		test.Run(tt.name, func(test *testing.T) {
			m := &BasicMove{
				InputURL:   tt.fields.InputURL,
				JSONOutput: tt.fields.JSONOutput,
			}
			if err := m.Move(tt.args.ctx); (err != nil) != tt.wantErr {
				test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
	w.Close()

	// shutdown servers
	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
	if err := gzipServer.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
}

// test the move method, with a single jsonl file
func TestBasicMove_Move(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	m := &BasicMove{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	wantErr := false
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
	w.Close()
}

// test the move method, with a single unknown file type
func TestBasicMove_Move_unknown_file_type(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "txt")
	defer cleanUpTempFile()

	m := &BasicMove{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	wantErr := true
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
	w.Close()
}

// test the move method, with a single unknown resource type
func TestBasicMove_Move_unknown_resource_type(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "txt")
	defer cleanUpTempFile()

	m := &BasicMove{
		InputURL: fmt.Sprintf("http://%s", filename),
	}
	wantErr := true
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
	w.Close()
}

// test the move method, with a single jsonl file
func TestBasicMove_Move_wait_for_logStats(test *testing.T) {

	r, w, cleanUp := mockStdout(test)
	defer cleanUp()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	m := &BasicMove{
		InputURL:                  fmt.Sprintf("file://%s", filename),
		MonitoringPeriodInSeconds: 1,
	}
	wantErr := false
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		test.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
	time.Sleep(2 * time.Second)

	w.Close()
	out, _ := io.ReadAll(r)
	got := string(out)

	want := "CPUs"
	if !strings.Contains(got, want) {
		test.Errorf("MoveImpl.Move() = %v, want %v", got, want)
	}
}

// ----------------------------------------------------------------------------
// test processJSONL method
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestBasicMove_processJSONL(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		test.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJSONL(filename, file, recordchan)

	w.Close()

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		test.Errorf("MoveImpl.processJSONL() error = %v, want %v", err, want)
	}
}

// read jsonl file successfully, no record validation errors
func TestBasicMove_processJSONL_bad_records(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, cleanUpTempFile := createTempDataFile(test, testBadData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		test.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     14,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJSONL(filename, file, recordchan)

	w.Close()

	got := 0
	for range recordchan {
		got++
	}
	want := 9
	if got != want {
		test.Errorf("MoveImpl.processJSONL() error = %v, want %v", err, want)
	}
}

// ----------------------------------------------------------------------------
// test file read methods
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestBasicMove_readJSONLFile(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readJSONLFile(filename, recordchan)
	w.Close()

	if err != nil {
		test.Errorf("MoveImpl.processJSONL() error = %v, want no error", err)
	}

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		test.Errorf("MoveImpl.processJSONL() error = %v, want %v", err, want)
	}
}

// attempt to read jsonl file that doesn't exist
func TestBasicMove_readJSONLFile_file_does_not_exist(test *testing.T) {

	filename := "bad.jsonl"

	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readJSONLFile(filename, recordchan)
	if err == nil {
		test.Errorf("MoveImpl.processJSONL() error = %v, want error", err)
	}
}

// read jsonl file successfully, no record validation errors
func TestBasicMove_readGZIPFile(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, cleanUpTempFile := createTempGZIPDataFile(test, testGoodData)
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readGZIPFile(filename, recordchan)

	w.Close()

	if err != nil {
		test.Errorf("MoveImpl.readGZIPFile() error = %v, want no error", err)
	}

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		test.Errorf("MoveImpl.readGZIPFile() error = %v, want %v", err, want)
	}
}

// attempt to read jsonl file that doesn't exist
func TestBasicMove_readGZIPFile_file_does_not_exist(test *testing.T) {

	filename := "bad.gz"

	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readGZIPFile(filename, recordchan)
	if err == nil {
		test.Errorf("MoveImpl.readGZIPFile() error = %v, want error", err)
	}
}

// ----------------------------------------------------------------------------
// test resource read methods
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestBasicMove_readJSONLResource(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, cleanUpTempFile := createTempDataFile(test, testGoodData, "jsonl")
	defer cleanUpTempFile()

	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")
	mover := &BasicMove{
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

	w.Close()

	if err != nil {
		test.Errorf("MoveImpl.readJSONLResource() error = %v, want no error", err)
	}

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		test.Errorf("MoveImpl.readJSONLResource() error = %v, want %v", err, want)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
}

// attempt to read jsonl file that doesn't exist
func TestBasicMove_readJSONLResource_file_does_not_exist(test *testing.T) {

	filename := "/bad.jsonl"

	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)

	idx := strings.LastIndex(filename, "/")
	mover := &BasicMove{}
	err := mover.readJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

	if err == nil {
		test.Errorf("MoveImpl.readJSONLResource() error = %v, want error", err)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}

}

// read jsonl file successfully, no record validation errors
func TestBasicMove_readGZIPResource(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, moreCleanUp := createTempGZIPDataFile(test, testGoodData)
	defer moreCleanUp()
	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")
	mover := &BasicMove{
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

	w.Close()

	if err != nil {
		test.Errorf("MoveImpl.readJSONLResource() error = %v, want no error", err)
	}

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		test.Errorf("MoveImpl.readJSONLResource() error = %v, want %v", err, want)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
}

// attempt to read jsonl file that doesn't exist
func TestBasicMove_readGZIPResource_file_does_not_exist(test *testing.T) {

	filename := "/bad.gz"

	server, listener, port := serveResource(test, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")

	mover := &BasicMove{}
	err := mover.readGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)

	if err == nil {
		test.Errorf("MoveImpl.readGZIPResource() error = %v, want error", err)
	}

	if err := server.Shutdown(context.Background()); err != nil {
		test.Error(err)
	}
}

// ----------------------------------------------------------------------------
// test write methods
// ----------------------------------------------------------------------------

func TestBasicMove_writeStdout(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, moreCleanUp := createTempDataFile(test, testGoodData, "jsonl")
	defer moreCleanUp()
	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		// FileType:                  tt.fields.FileType,
		InputURL: fmt.Sprintf("file://%s", filename),
		// LogLevel:                  tt.fields.LogLevel,
		// MonitoringPeriodInSeconds: tt.fields.MonitoringPeriodInSeconds,
		// OutputUrl:                 tt.fields.OutputUrl,
		// RecordMax:                 tt.fields.RecordMax,
		// RecordMin:                 tt.fields.RecordMin,
		// RecordMonitor:             tt.fields.RecordMonitor,
	}

	err := mover.readJSONLFile(filename, recordchan)
	require.NoError(test, err)

	err = mover.writeStdout(recordchan)
	require.NoError(test, err, "MoveImpl.writeStdout() = %v, want %v")
	w.Close()
}

func TestBasicMove_writeStdout_no_stdout(test *testing.T) {

	_, w, cleanUp := mockStdout(test)
	defer cleanUp()

	filename, moreCleanUp := createTempDataFile(test, testGoodData, "jsonl")
	defer moreCleanUp()
	recordchan := make(chan queues.Record, 15)

	mover := &BasicMove{
		// FileType:                  tt.fields.FileType,
		InputURL: fmt.Sprintf("file://%s", filename),
		// LogLevel:                  tt.fields.LogLevel,
		// MonitoringPeriodInSeconds: tt.fields.MonitoringPeriodInSeconds,
		// OutputUrl:                 tt.fields.OutputUrl,
		// RecordMax:                 tt.fields.RecordMax,
		// RecordMin:                 tt.fields.RecordMin,
		// RecordMonitor:             tt.fields.RecordMonitor,
	}

	err := mover.readJSONLFile(filename, recordchan)
	require.NoError(test, err)

	o := os.Stdout
	os.Stdout = nil
	err = mover.writeStdout(recordchan)
	require.Error(test, err, "MoveImpl.writeStdout()")

	os.Stdout = o
	w.Close()
}

func TestBasicMove_SetLogLevel(test *testing.T) {
	type fields struct {
		FileType                  string
		InputURL                  string
		JSONOutput                bool
		LogLevel                  string
		MonitoringPeriodInSeconds int
		OutputURL                 string
		RecordMax                 int
		RecordMin                 int
		RecordMonitor             int
	}
	type args struct {
		ctx          context.Context
		logLevelName string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"Test SetLogLevel", fields{LogLevel: "info"}, args{ctx: context.Background(), logLevelName: "DEBUG"}, false},
		{"Test SetLogLevel", fields{LogLevel: "info"}, args{ctx: context.Background(), logLevelName: "bad"}, true},
		{"Test SetLogLevel", fields{JSONOutput: true, LogLevel: "info"}, args{ctx: context.Background(), logLevelName: "DEBUG"}, false},
		{"Test SetLogLevel", fields{JSONOutput: true, LogLevel: "info"}, args{ctx: context.Background(), logLevelName: "bad"}, true},
	}
	for _, tt := range tests {
		test.Run(tt.name, func(test *testing.T) {
			v := &BasicMove{
				LogLevel: tt.fields.LogLevel,
			}
			if err := v.SetLogLevel(tt.args.ctx, tt.args.logLevelName); (err != nil) != tt.wantErr {
				test.Errorf("MoveImpl.SetLogLevel() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				got := v.logger.GetLogLevel()
				if got != tt.args.logLevelName {
					test.Errorf("MoveImpl.SetLogLevel() got = %v, want %v", got, tt.args.logLevelName)
				}
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------------

// create a tempdata file with the given content and extension
func createTempDataFile(test *testing.T, content string, fileextension string) (filename string, cleanUp func()) {
	test.Helper()
	tmpfile, err := os.CreateTemp(test.TempDir(), "test.*."+fileextension)
	if err != nil {
		test.Fatal(err)
	}

	if _, err := tmpfile.WriteString(content); err != nil {
		test.Fatal(err)
	}

	filename = tmpfile.Name()

	if err := tmpfile.Close(); err != nil {
		test.Fatal(err)
	}
	return filename,
		func() {
			os.Remove(filename)
		}
}

// create a temp gzipped datafile with the given content
func createTempGZIPDataFile(test *testing.T, content string) (filename string, cleanUp func()) {
	test.Helper()

	tmpfile, err := os.CreateTemp("", "test.*.jsonl.gz")
	if err != nil {
		test.Fatal(err)
	}
	defer tmpfile.Close()
	gf := gzip.NewWriter(tmpfile)
	defer gf.Close()
	fw := bufio.NewWriter(gf)
	if _, err := fw.WriteString(content); err != nil {
		test.Fatal(err)
	}
	fw.Flush()
	filename = tmpfile.Name()
	return filename,
		func() {
			os.Remove(filename)
		}
}

// serve the requested resource on a random port
func serveResource(test *testing.T, filename string) (*http.Server, *net.Listener, int) {
	test.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		test.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	idx := strings.LastIndex(filename, string(os.PathSeparator))
	fs := http.FileServer(http.Dir(filename[:idx]))
	server := http.Server{
		Addr:              fmt.Sprintf(":%d", port),
		Handler:           fs,
		ReadHeaderTimeout: 2 * time.Second,
	}
	return &server, &listener, port

}

// capture stdout for testing
func mockStdout(test *testing.T) (reader *os.File, writer *os.File, cleanUp func()) {
	test.Helper()

	origStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		test.Fatalf("couldn't get os Pipe: %v", err)
	}
	os.Stdout = writer

	return reader,
		writer,
		func() {
			// clean-up
			os.Stdout = origStdout
		}
}

// capture stderr for testing
func mockStderr(test *testing.T) (reader *os.File, writer *os.File, cleanUp func()) {
	test.Helper()
	origStderr := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		test.Fatalf("couldn't get os Pipe: %v", err)
	}
	os.Stderr = writer

	return reader,
		writer,
		func() {
			// clean-up
			os.Stderr = origStderr
		}
}

var testGoodData = `{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000001", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000002", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "SUITE E-2,UNION COURT BUILDING, P.O. BOX N-8188, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000002"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000003", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "LYFORD CAY HOUSE, LYFORD CAY, P.O. BOX N-7785, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000003"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000004", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "P.O. BOX N-3708 BAHAMAS FINANCIAL CENTRE, P.O. BOX N-3708 SHIRLEY & CHARLOTTE STS, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000004"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000005", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "LYFORD CAY HOUSE, 3RD FLOOR, LYFORD CAY, P.O. BOX N-3024, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000005"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000006", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "303 SHIRLEY STREET, P.O. BOX N-492, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000006"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000007", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "OCEAN CENTRE, MONTAGU FORESHORE, P.O. BOX SS-19084 EAST BAY STREET, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000007"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000008", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "PROVIDENCE HOUSE, EAST WING EAST HILL ST, P.O. BOX CB-12399, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000008"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000009", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "BAYSIDE EXECUTIVE PARK, WEST BAY & BLAKE, P.O. BOX N-4875, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000009"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000010", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "GROUND FLOOR, GOODMAN'S BAY CORPORATE CE, P.O. BOX N 3933, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000010"}
{"SOCIAL_HANDLE": "shuddersv", "DATE_OF_BIRTH": "16/7/1974", "ADDR_STATE": "NC", "ADDR_POSTAL_CODE": "257609", "ENTITY_TYPE": "TEST", "GENDER": "F", "srccode": "MDMPER", "RECORD_ID": "151110080", "DSRC_ACTION": "A", "ADDR_CITY": "Raleigh", "DRIVERS_LICENSE_NUMBER": "95", "PHONE_NUMBER": "984-881-8384", "NAME_LAST": "OBERMOELLER", "entityid": "151110080", "ADDR_LINE1": "3802 eBllevue RD", "DATA_SOURCE": "TEST"}
{"SOCIAL_HANDLE": "battlesa", "ADDR_STATE": "LA", "ADDR_POSTAL_CODE": "70706", "NAME_FIRST": "DEVIN", "ENTITY_TYPE": "TEST", "GENDER": "M", "srccode": "MDMPER", "CC_ACCOUNT_NUMBER": "5018608175414044187", "RECORD_ID": "151267101", "DSRC_ACTION": "A", "ADDR_CITY": "Denham Springs", "DRIVERS_LICENSE_NUMBER": "614557601", "PHONE_NUMBER": "318-398-0649", "NAME_LAST": "LOVELL", "entityid": "151267101", "ADDR_LINE1": "8487 Ashley ", "DATA_SOURCE": "TEST"}
`
var testBadData = `{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000001", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
{"DATA_SOURCE": "ICIJ", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
{"RECORD_ID": "24000001", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000002", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "SUITE E-2,UNION COURT BUILDING, P.O. BOX N-8188, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000002"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000003", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "LYFORD CAY HOUSE, LYFORD CAY, P.O. BOX N-7785, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000003"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000004", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "P.O. BOX N-3708 BAHAMAS FINANCIAL CENTRE, P.O. BOX N-3708 SHIRLEY & CHARLOTTE STS, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000004"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000005", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "LYFORD CAY HOUSE, 3RD FLOOR, LYFORD CAY, P.O. BOX N-3024, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000005"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000005B" "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "LYFORD CAY HOUSE, 3RD FLOOR, LYFORD CAY, P.O. BOX N-3024, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000005"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000006", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "303 SHIRLEY STREET, P.O. BOX N-492, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000006"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000007", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "OCEAN CENTRE, MONTAGU FORESHORE, P.O. BOX SS-19084 EAST BAY STREET, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000007"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000008", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "PROVIDENCE HOUSE, EAST WING EAST HILL ST, P.O. BOX CB-12399, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000008"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000009", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "BAYSIDE EXECUTIVE PARK, WEST BAY & BLAKE, P.O. BOX N-4875, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000009"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000010", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "GROUND FLOOR, GOODMAN'S BAY CORPORATE CE, P.O. BOX N 3933, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000010"}
{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000010B" "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "GROUND FLOOR, GOODMAN'S BAY CORPORATE CE, P.O. BOX N 3933, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000010"}
{"SOCIAL_HANDLE": "shuddersv", "DATE_OF_BIRTH": "16/7/1974", "ADDR_STATE": "NC", "ADDR_POSTAL_CODE": "257609", "ENTITY_TYPE": "TEST", "GENDER": "F", "srccode": "MDMPER", "RECORD_ID": "151110080", "DSRC_ACTION": "A", "ADDR_CITY": "Raleigh", "DRIVERS_LICENSE_NUMBER": "95", "PHONE_NUMBER": "984-881-8384", "NAME_LAST": "OBERMOELLER", "entityid": "151110080", "ADDR_LINE1": "3802 eBllevue RD", "DATA_SOURCE": "TEST"}
{"SOCIAL_HANDLE": "battlesa", "ADDR_STATE": "LA", "ADDR_POSTAL_CODE": "70706", "NAME_FIRST": "DEVIN", "ENTITY_TYPE": "TEST", "GENDER": "M", "srccode": "MDMPER", "CC_ACCOUNT_NUMBER": "5018608175414044187", "RECORD_ID": "151267101", "DSRC_ACTION": "A", "ADDR_CITY": "Denham Springs", "DRIVERS_LICENSE_NUMBER": "614557601", "PHONE_NUMBER": "318-398-0649", "NAME_LAST": "LOVELL", "entityid": "151267101", "ADDR_LINE1": "8487 Ashley ", "DATA_SOURCE": "TEST"}
`
