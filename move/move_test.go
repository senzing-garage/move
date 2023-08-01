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

	"github.com/senzing/go-queuing/queues"
	"github.com/stretchr/testify/assert"
)

// ----------------------------------------------------------------------------
// test processJsonl method
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestProcessJsonl(t *testing.T) {

	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJsonl(filename, file, recordchan)

	count := 0
	for range recordchan {
		count++
	}
	assert.Equal(t, 10, count)
}

// read jsonl file successfully, no record validation errors
func TestProcessJsonl_bad_records(t *testing.T) {

	filename, cleanUpTempFile := createTempDataFile(t, testBadData, "jsonl")
	defer cleanUpTempFile()

	file, err := os.Open(filename)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl:      fmt.Sprintf("file://%s", filename),
		RecordMax:     14,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	mover.processJsonl(filename, file, recordchan)

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
func TestReadJsonlFile(t *testing.T) {

	filename, cleanUpTempFile := createTempDataFile(t, testGoodData, "jsonl")
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readJsonlFile(filename, recordchan)
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
func TestReadJsonlFile_file_does_not_exist(t *testing.T) {

	filename := "bad.jsonl"

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readJsonlFile(filename, recordchan)
	assert.Error(t, err)
}

// read jsonl file successfully, no record validation errors
func TestReadGzipFile(t *testing.T) {

	filename, cleanUpTempFile := createTempGzDataFile(t, testGoodData)
	defer cleanUpTempFile()

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl:      fmt.Sprintf("file://%s", filename),
		RecordMax:     11,
		RecordMin:     2,
		RecordMonitor: 5,
	}
	err := mover.readGzipFile(filename, recordchan)
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
func TestReadGzipFile_file_does_not_exist(t *testing.T) {

	filename := "bad.gz"

	recordchan := make(chan queues.Record, 15)

	mover := &MoveImpl{
		InputUrl: fmt.Sprintf("file://%s", filename),
	}
	err := mover.readGzipFile(filename, recordchan)
	assert.Error(t, err)

}

// ----------------------------------------------------------------------------
// test resource read methods
// ----------------------------------------------------------------------------

// read jsonl file successfully, no record validation errors
func TestReadJsonlResource(t *testing.T) {

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
	err := mover.readJsonlResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
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
func TestReadJsonlResource_file_does_not_exist(t *testing.T) {

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
	err := mover.readJsonlResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	assert.Error(t, err)

	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
	}

}

// read jsonl file successfully, no record validation errors
func TestReadGzipResource(t *testing.T) {

	filename, moreCleanUp := createTempGzDataFile(t, testGoodData)
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
	err := mover.readGzipResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
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
func TestReadGzipResource_file_does_not_exist(t *testing.T) {

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
	err := mover.readGzipResource(fmt.Sprintf("http://localhost:%d/%s", port, filename[(idx+1):]), recordchan)
	assert.Error(t, err)
	if err := server.Shutdown(context.Background()); err != nil {
		t.Error(err)
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
func createTempGzDataFile(t *testing.T, content string) (filename string, cleanUp func()) {
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

// serve the requested resource on the requested port
func serveResource(t *testing.T, filename string) (*http.Server, *net.Listener, int) {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	idx := strings.LastIndex(filename, "/")
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
		assert.Fail(t, "couldn't get os Pipe: %v", err)
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
		assert.Fail(t, "couldn't get os Pipe: %v", err)
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
