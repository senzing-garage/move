//lint:file-ignore U1000 Ignore all unused code, this is a test file.

package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/senzing/go-logging/logging"
	"github.com/senzing/go-queueing/queues"
	"github.com/stretchr/testify/assert"
)

// ----------------------------------------------------------------------------
// test Move method
// ----------------------------------------------------------------------------

// test the move method using a table of test data
func TestMoveImpl_Move_table(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	// serve jsonl file
	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	idx := strings.LastIndex(filename, "/")

	// create a temporary gzip file of good test data
	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(t, testGoodData)
	defer cleanUpTempGZIPFile()

	// serve gzip file
	gzipServer, gzipListener, gzipPort := serveResource(t, gzipFileName)
	go func() {
		if err := gzipServer.Serve(*gzipListener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	gzipIdx := strings.LastIndex(gzipFileName, "/")

	type fields struct {
		FileType                  string
		InputUrl                  string
		JSONOutput                bool
		LogLevel                  string
		MonitoringPeriodInSeconds int
		OutputUrl                 string
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
		{name: "test read jsonl file", fields: fields{InputUrl: fmt.Sprintf("file://%s", filename)}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip file", fields: fields{InputUrl: fmt.Sprintf("file://%s", gzipFileName)}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl file, bad file name", fields: fields{InputUrl: "file:///bad.jsonl"}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip file, bad file name", fields: fields{InputUrl: "file:///bad.gz"}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl resource", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):])}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip resource", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):])}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl resource, bad resource name", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/bad.jsonl", port)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip resource, bad resource name", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url schema", fields: fields{InputUrl: fmt.Sprintf("bad://%s", filename)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputUrl: fmt.Sprintf("{}http://%s", filename)}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputUrl: "://"}, args: args{ctx: context.Background()}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MoveImpl{
				InputURL: tt.fields.InputUrl,
			}
			if err := m.Move(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// shutdown servers
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	if err := gzipServer.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

// test the move method using a table of test data
func TestMoveImpl_Move_json_output_table(t *testing.T) {

	_, cleanUpStderr := mockStderr(t)
	defer cleanUpStderr()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	// serve jsonl file
	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	idx := strings.LastIndex(filename, "/")

	// create a temporary gzip file of good test data
	gzipFileName, cleanUpTempGZIPFile := createTempGZIPDataFile(t, testGoodData)
	defer cleanUpTempGZIPFile()

	// serve gzip file
	gzipServer, gzipListener, gzipPort := serveResource(t, gzipFileName)
	go func() {
		if err := gzipServer.Serve(*gzipListener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	gzipIdx := strings.LastIndex(gzipFileName, "/")

	type fields struct {
		FileType                  string
		InputUrl                  string
		JSONOutput                bool
		LogLevel                  string
		MonitoringPeriodInSeconds int
		OutputUrl                 string
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
		{name: "test read jsonl file", fields: fields{InputUrl: fmt.Sprintf("file://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip file", fields: fields{InputUrl: fmt.Sprintf("file://%s", gzipFileName), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl file, bad file name", fields: fields{InputUrl: "file:///bad.jsonl", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip file, bad file name", fields: fields{InputUrl: "file:///bad.gz", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl resource", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read gzip resource", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/%s", gzipPort, gzipFileName[(gzipIdx+1):]), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: false},
		{name: "test read jsonl resource, bad resource name", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/bad.jsonl", port), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read gzip resource, bad resource name", fields: fields{InputUrl: fmt.Sprintf("http://localhost:%d/bad.gz", gzipPort), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url schema", fields: fields{InputUrl: fmt.Sprintf("bad://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputUrl: fmt.Sprintf("{}http://%s", filename), JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
		{name: "test read jsonl file, bad url", fields: fields{InputUrl: "://", JSONOutput: true}, args: args{ctx: context.Background()}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &MoveImpl{
				InputURL:   tt.fields.InputUrl,
				JSONOutput: tt.fields.JSONOutput,
			}
			if err := m.Move(tt.args.ctx); (err != nil) != tt.wantErr {
				t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}

	// shutdown servers
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	if err := gzipServer.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

// test the move method, with a single jsonl file
func TestMoveImpl_Move(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	m := &MoveImpl{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	wantErr := false
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
}

// test the move method, with a single unknown file type
func TestMoveImpl_Move_unknown_file_type(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "txt")
	defer cleanUpTempFile()

	m := &MoveImpl{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	wantErr := true
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
}

// test the move method, with a single unknown resource type
func TestMoveImpl_Move_unknown_resource_type(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "txt")
	defer cleanUpTempFile()

	m := &MoveImpl{
		InputURL: fmt.Sprintf("http://%s", filename),
	}
	wantErr := true
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
}

// test the move method, with a single jsonl file
func TestMoveImpl_Move_wait_for_logStats(t *testing.T) {

	scanner, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	// create a temporary jsonl file of good test data
	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	m := &MoveImpl{
		InputURL:                  fmt.Sprintf("file://%s", filename),
		MonitoringPeriodInSeconds: 1,
	}
	wantErr := false
	if err := m.Move(context.Background()); (err != nil) != wantErr {
		t.Errorf("MoveImpl.Move() error = %v, wantErr %v", err, wantErr)
	}
	time.Sleep(2 * time.Second)
	var got string = ""
	for i := 0; i < 8; i++ {
		scanner.Scan()
		got += scanner.Text()
		got += "\n"
	}
	want := "CPUs"
	if !strings.Contains(got, want) {
		t.Errorf("MoveImpl.Move() = %v, want %v", got, want)
	}
}

// ----------------------------------------------------------------------------
// test processJSONL method
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestMoveImpl_processJSONL(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJSONL(filename, file, recordchan)

	got := 0
	for range recordchan {
		got++
	}
	want := 10
	if got != want {
		t.Errorf("MoveImpl.processJSONL() error = %v, want %v", err, want)
	}
}

// read jsonl file successfully, no record validation errors
func TestMoveImpl_processJSONL_bad_records(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, cleanUpTempFile := createTempDataFile(t, testBadData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     14,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJSONL(filename, file, recordchan)

	count := 0
	for range recordchan {
		count++
	}
	assert.Equal(t, 9, count)
}

// ----------------------------------------------------------------------------
// test file read methods
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestMoveImpl_readJSONLFile(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readJSONLFile(filename, recordchan)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range recordchan {
		count++
	}
	assert.Equal(t, 10, count)
}

// attempt to read jsonl file that doesn't exist
func TestMoveImpl_readJSONLFile_file_does_not_exist(t *testing.T) {

	filename := "bad.jsonl"

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readJSONLFile(filename, recordchan)
	assert.Error(t, err)
}

// read jsonl file successfully, no record validation errors
func TestMoveImpl_readGZIPFile(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, cleanUpTempFile := createTempGZIPDataFile(t, testGoodData)
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readGZIPFile(filename, recordchan)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range recordchan {
		count++
	}
	assert.Equal(t, 10, count)
}

// attempt to read jsonl file that doesn't exist
func TestMoveImpl_readGZIPFile_file_does_not_exist(t *testing.T) {

	filename := "bad.gz"

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputURL: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readGZIPFile(filename, recordchan)
	assert.Error(t, err)

}

// ----------------------------------------------------------------------------
// test resource read methods
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestMoveImpl_readJSONLResource(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")
	mover := &MoveImpl{
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range recordchan {
		count++
	}

	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	assert.Equal(t, 10, count)
}

// attempt to read jsonl file that doesn't exist
func TestMoveImpl_readJSONLResource_file_does_not_exist(t *testing.T) {

	filename := "/bad.jsonl"

	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)

	idx := strings.LastIndex(filename, "/")
	mover := &MoveImpl{}
	err := mover.readJSONLResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	assert.Error(t, err)

	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}

}

// read jsonl file successfully, no record validation errors
func TestMoveImpl_readGZIPResource(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, moreCleanUp := createTempGZIPDataFile(t, testGoodData)
	defer moreCleanUp()
	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")
	mover := &MoveImpl{
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	if err != nil {
		t.Fatal(err)
	}

	count := 0
	for range recordchan {
		count++
	}
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
	assert.Equal(t, 10, count)
}

// attempt to read jsonl file that doesn't exist
func TestMoveImpl_readGZIPResource_file_does_not_exist(t *testing.T) {

	filename := "/bad.gz"

	server, listener, port := serveResource(t, filename)
	go func() {
		if err := server.Serve(*listener); err != http.ErrServerClosed {
			log.Fatalf("server.Serve(): %v", err)
		}
	}()
	recordchan := make(chan queues.Record, 15)
	idx := strings.LastIndex(filename, "/")

	mover := &MoveImpl{}
	err := mover.readGZIPResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	assert.Error(t, err)
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}
}

// ----------------------------------------------------------------------------
// test write methods
// ----------------------------------------------------------------------------

func TestMoveImpl_writeStdout(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, moreCleanUp := createTempDataFile(t, testGoodData, "jsonl")
	defer moreCleanUp()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
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
	if err != nil {
		t.Error(err)
	}
	var want error = nil
	if got := mover.writeStdout(recordchan); got != want {
		t.Errorf("MoveImpl.writeStdout() = %v, want %v", got, want)
	}
}

func TestMoveImpl_writeStdout_no_stdout(t *testing.T) {

	_, cleanUpStdout := mockStdout(t)
	defer cleanUpStdout()

	filename, moreCleanUp := createTempDataFile(t, testGoodData, "jsonl")
	defer moreCleanUp()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
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
	if err != nil {
		t.Error(err)
	}
	o := os.Stdout
	os.Stdout = nil
	var want error = nil
	if got := mover.writeStdout(recordchan); got == want {
		t.Errorf("MoveImpl.writeStdout() = %v, want %v", got, want)
	}
	os.Stdout = o

}

func TestMoveImpl_SetLogLevel(t *testing.T) {
	type fields struct {
		FileType                  string
		InputURL                  string
		JSONOutput                bool
		logger                    logging.LoggingInterface
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
		t.Run(tt.name, func(t *testing.T) {
			v := &MoveImpl{
				LogLevel: tt.fields.LogLevel,
			}
			if err := v.SetLogLevel(tt.args.ctx, tt.args.logLevelName); (err != nil) != tt.wantErr {
				t.Errorf("MoveImpl.SetLogLevel() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !tt.wantErr {
				got := v.logger.GetLogLevel()
				if got != tt.args.logLevelName {
					t.Errorf("MoveImpl.SetLogLevel() got = %v, want %v", got, tt.args.logLevelName)
				}
			}
		})
	}
}

// ----------------------------------------------------------------------------
// Helper functions
// ----------------------------------------------------------------------------

// create a tempdata file with the given content and extension
func createTempDataFile(t *testing.T, content string, fileextension string) (filename string, cleanUp func()) {
	t.Helper()
	tmpfile, err := os.CreateTemp(t.TempDir(), "test.*."+fileextension)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := tmpfile.WriteString(content); err != nil {
		t.Fatal(err)
	}

	filename = tmpfile.Name()

	if err := tmpfile.Close(); err != nil {
		t.Fatal(err)
	}
	return filename,
		func() {
			os.Remove(filename)
		}
}

// create a temp gzipped datafile with the given content
func createTempGZIPDataFile(t *testing.T, content string) (filename string, cleanUp func()) {
	t.Helper()

	tmpfile, err := os.CreateTemp("", "test.*.jsonl.gz")
	if err != nil {
		t.Fatal(err)
	}
	defer tmpfile.Close()
	gf := gzip.NewWriter(tmpfile)
	defer gf.Close()
	fw := bufio.NewWriter(gf)
	if _, err := fw.WriteString(content); err != nil {
		t.Fatal(err)
	}
	fw.Flush()
	filename = tmpfile.Name()
	return filename,
		func() {
			os.Remove(filename)
		}
}

// serve the requested resource on a random port
func serveResource(t *testing.T, filename string) (*http.Server, *net.Listener, int) {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	idx := strings.LastIndex(filename, string(os.PathSeparator))
	fs := http.FileServer(http.Dir(filename[:idx]))
	server := http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: fs,
	}
	return &server, &listener, port

}

// capture stdout for testing
func mockStdout(t *testing.T) (buffer *bufio.Scanner, cleanUp func()) {
	t.Helper()
	origStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("couldn't get os Pipe: %v", err)
	}
	os.Stdout = writer

	return bufio.NewScanner(reader),
		func() {
			//clean-up
			os.Stdout = origStdout
		}
}

// capture stderr for testing
func mockStderr(t *testing.T) (buffer *bufio.Scanner, cleanUp func()) {
	t.Helper()
	origStderr := os.Stderr
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatalf("couldn't get os Pipe: %v", err)
	}
	os.Stderr = writer

	return bufio.NewScanner(reader),
		func() {
			//clean-up
			os.Stderr = origStderr
		}
}

var testGoodData string = `{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000001", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
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
var testBadData string = `{"DATA_SOURCE": "ICIJ", "RECORD_ID": "24000001", "ENTITY_TYPE": "ADDRESS", "RECORD_TYPE": "ADDRESS", "icij_source": "BAHAMAS", "icij_type": "ADDRESS", "COUNTRIES": [{"COUNTRY_OF_ASSOCIATION": "BHS"}], "ADDR_FULL": "ANNEX FREDERICK & SHIRLEY STS, P.O. BOX N-4805, NASSAU, BAHAMAS", "REL_ANCHOR_DOMAIN": "ICIJ_ID", "REL_ANCHOR_KEY": "24000001"}
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
