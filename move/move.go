package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/senzing-garage/go-helpers/record"
	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-logging/logging"
	"github.com/senzing-garage/go-observing/notifier"
	"github.com/senzing-garage/go-observing/observer"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
	"github.com/senzing-garage/go-queueing/queues/rabbitmq"
	"github.com/senzing-garage/go-queueing/queues/sqs"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Error struct {
	error
}

type BasicMove struct {
	FileType                  string
	InputURL                  string
	lineNumber                int
	logger                    logging.Logging
	LogLevel                  string
	MonitoringPeriodInSeconds int
	mutexLineNumber           sync.Mutex
	mutexLogStats             sync.Mutex
	observerOrigin            string
	observers                 subject.Subject
	OutputURL                 string
	PlainText                 bool
	RecordMax                 int
	RecordMin                 int
	RecordMonitor             int
	Validate                  bool
	waitGroup                 sync.WaitGroup
}

const (
	callerSkip    = 4
	FiletypeGZ    = "GZ"
	FiletypeJSONL = "JSONL"
	numChannels   = 10
)

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Move = (*BasicMove)(nil)

// ----------------------------------------------------------------------------
// -- Public methods
// ----------------------------------------------------------------------------

func (move *BasicMove) IsLoggable(messageNumber int) bool {
	logThreshold, ok := messageThresholds[move.getLogger().GetLogLevel()]
	if ok {
		if messageNumber >= logThreshold {
			return true
		}
	}

	return false
}

// Method Move moves records from one place to another.
//
// Optionally validates each record as they are read and only moves valid records.
// Typically used to move records from a file to a queue for processing.
func (move *BasicMove) Move(ctx context.Context) error {
	var (
		readErr  error
		writeErr error
		err      error
	)

	if len(move.LogLevel) > 0 {
		err = move.SetLogLevel(ctx, move.LogLevel)
		if err != nil {
			return wraperror.Errorf(err, "SetLogLevel")
		}
	}

	if move.RecordMin > move.RecordMax {
		move.log(5040, move.RecordMin, move.RecordMax)

		return wraperror.Errorf(errForPackage, "RecordMin (%d) > RecordMax (%d)", move.RecordMin, move.RecordMax)
	}

	move.logEntry()

	if move.MonitoringPeriodInSeconds <= 0 {
		move.MonitoringPeriodInSeconds = 60
	}

	ticker := time.NewTicker(time.Duration(move.MonitoringPeriodInSeconds) * time.Second)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				move.logStats()
			}
		}
	}()

	// var waitGroup sync.WaitGroup

	recordchan := make(chan queues.Record, numChannels)

	move.waitGroup.Add(1)

	go func() {
		defer move.waitGroup.Done()

		readErr = move.read(ctx, recordchan)
		if readErr != nil {
			select {
			case <-recordchan:
				// channel already closed
			default:
				close(recordchan)
			}
		}
	}()

	move.waitGroup.Add(1)

	go func() {
		defer move.waitGroup.Done()

		writeErr = move.write(ctx, recordchan)
	}()

	move.waitGroup.Wait()

	if readErr != nil {
		return wraperror.Errorf(readErr, "Read error")
	}

	if writeErr != nil {
		return wraperror.Errorf(writeErr, "Write error")
	}

	move.logExit()

	return wraperror.Errorf(err, wraperror.NoMessage)
}

/*
Method RegisterObserver adds the observer to the list of observers notified.

Input
  - ctx: A context to control lifecycle.
  - observer: The observer to be added.
*/
func (move *BasicMove) RegisterObserver(ctx context.Context, observer observer.Observer) error {
	var err error

	if move.observers == nil {
		move.observers = &subject.SimpleSubject{}
	}

	err = move.observers.RegisterObserver(ctx, observer)

	if move.observers != nil {
		go func() {
			details := map[string]string{
				"observerID": observer.GetObserverID(ctx),
			}
			notifier.Notify(ctx, move.observers, move.observerOrigin, ComponentID, 8000, err, details)
		}()
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

/*
The SetLogLevel method sets the level of logging.

Input
  - ctx: A context to control lifecycle.
  - logLevel: The desired log level. TRACE, DEBUG, INFO, WARN, ERROR, FATAL or PANIC.
*/
func (move *BasicMove) SetLogLevel(ctx context.Context, logLevelName string) error {
	_ = ctx

	var err error

	// Verify value of logLevelName.

	if !logging.IsValidLogLevelName(logLevelName) {
		move.log(5060, logLevelName)

		return wraperror.Errorf(errForPackage, "invalid error level: %s", logLevelName)
	}

	// Set ValidateImpl log level.

	err = move.getLogger().SetLogLevel(logLevelName)
	if err == nil {
		move.LogLevel = logLevelName
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

/*
Method UnregisterObserver removes the observer to the list of observers notified.

Input
  - ctx: A context to control lifecycle.
  - observer: The observer to be added.
*/
func (move *BasicMove) UnregisterObserver(ctx context.Context, observer observer.Observer) error {
	var err error

	if move.observers != nil {
		// Tricky code:
		// client.notify is called synchronously before client.observers is set to nil.
		// In client.notify, each observer will get notified in a goroutine.
		// Then client.observers may be set to nil, but observer goroutines will be OK.
		details := map[string]string{
			"observerID": observer.GetObserverID(ctx),
		}
		notifier.Notify(ctx, move.observers, move.observerOrigin, ComponentID, 8704, err, details)
		err = move.observers.UnregisterObserver(ctx, observer)

		if !move.observers.HasObservers(ctx) {
			move.observers = nil
		}
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

func (move *BasicMove) GetTotalLines() int {
	return move.lineNumber
}

// ----------------------------------------------------------------------------
// Reader routing
// ----------------------------------------------------------------------------

// Method read attempts to determine the source of records.
// It then parses the source and puts the records into the record channel.
func (move *BasicMove) read(ctx context.Context, recordchan chan queues.Record) error {
	_ = ctx

	var err error

	inputURL := move.InputURL
	inputURLLen := len(inputURL)

	if inputURLLen == 0 {
		// assume stdin
		return move.readStdin(ctx, recordchan)
	}

	// This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	// if len(inputURL) < len("s://p") {
	// 	move.log(5000, inputURL)

	// 	return wraperror.Errorf(errForPackage, "invalid SENZING_TOOLS_INPUT_URL: %s", inputURL)
	// }

	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		move.log(5001, inputURL)

		return wraperror.Errorf(err, "url.Parse")
	}

	switch parsedURL.Scheme {
	case "file":
		err = move.readFile(ctx, parsedURL, recordchan)
	case "http", "https":
		err = move.readHTTP(ctx, parsedURL, recordchan)
	default:
		move.log(5002, inputURL, parsedURL.Scheme)

		return wraperror.Errorf(errForPackage, "cannot handle input URL: %s", parsedURL.Scheme)
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

func (move *BasicMove) readFile(ctx context.Context, parsedURL *url.URL, recordchan chan queues.Record) error {
	_ = ctx

	var err error

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == FiletypeJSONL:
		err = move.readFileOfJSONL(ctx, parsedURL.Path, recordchan)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == FiletypeGZ:
		err = move.readFileOfGZIP(ctx, parsedURL.Path, recordchan)
	default:
		// IMPROVE: process JSON file?
		close(recordchan)
		move.log(5003, "file://"+parsedURL.Path)

		err = wraperror.Errorf(errForPackage, "unable to process file://%s", parsedURL.Path)
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

func (move *BasicMove) readHTTP(ctx context.Context, parsedURL *url.URL, recordchan chan queues.Record) error {
	_ = ctx

	var err error

	inputURL := parsedURL.String()

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == FiletypeJSONL:
		err = move.readHTTPofJSONL(ctx, inputURL, recordchan)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == FiletypeGZ:
		err = move.readHTTPofGZIP(ctx, inputURL, recordchan)
	default:
		move.log(5004, "http://"+parsedURL.Path)

		return wraperror.Errorf(errForPackage, "unable to process http://%s", parsedURL.Path)
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Readers
// ----------------------------------------------------------------------------

// Opens and reads a JSONL file that has been GZIPped.
func (move *BasicMove) readFileOfGZIP(ctx context.Context, gzipFileName string, recordchan chan queues.Record) error {
	cleanGzipFileName := filepath.Clean(gzipFileName)

	gzipfile, err := os.Open(cleanGzipFileName)
	if err != nil {
		move.log(5005, cleanGzipFileName)

		return wraperror.Errorf(err, "os.Open")
	}

	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return wraperror.Errorf(err, "gzip.NewReader")
	}
	defer reader.Close()

	move.processJSONL(ctx, gzipFileName, reader, recordchan)

	return nil
}

// Opens and reads a JSONL file.
func (move *BasicMove) readFileOfJSONL(ctx context.Context, jsonFile string, recordchan chan queues.Record) error {
	cleanJSONFile := filepath.Clean(jsonFile)

	file, err := os.Open(cleanJSONFile)
	if err != nil {
		move.log(5006, cleanJSONFile)

		return wraperror.Errorf(err, "os.Open")
	}

	defer file.Close()

	move.processJSONL(ctx, jsonFile, file, recordchan)

	return nil
}

func (move *BasicMove) readHTTPofGZIP(ctx context.Context, gzipURL string, recordchan chan queues.Record) error {
	//nolint:noctx
	response, err := http.Get(gzipURL) //nolint:gosec
	if err != nil {
		move.log(5007, gzipURL)

		return wraperror.Errorf(err, "error retrieving inputURL: %s", gzipURL)
	}

	if response.StatusCode != http.StatusOK {
		move.log(5008, gzipURL, response.StatusCode)

		return wraperror.Errorf(errForPackage, "unable to retrieve: %s, return code: %d", gzipURL, response.StatusCode)
	}

	defer response.Body.Close()

	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		return wraperror.Errorf(err, "gzip.NewReader")
	}

	defer reader.Close()

	move.processJSONL(ctx, gzipURL, reader, recordchan)

	return nil
}

// Opens and reads a JSONL http resource.
func (move *BasicMove) readHTTPofJSONL(ctx context.Context, jsonURL string, recordchan chan queues.Record) error {
	//nolint:noctx
	response, err := http.Get(jsonURL) //nolint:gosec
	if err != nil {
		move.log(5009, jsonURL)

		return wraperror.Errorf(err, "http.Get")
	}

	if response.StatusCode != http.StatusOK {
		move.log(5010, jsonURL, response.StatusCode)

		return wraperror.Errorf(errForPackage, "unable to retrieve: %s, return code: %d", jsonURL, response.StatusCode)
	}

	defer response.Body.Close()

	move.processJSONL(ctx, jsonURL, response.Body, recordchan)

	return nil
}

func (move *BasicMove) readStdin(ctx context.Context, recordchan chan queues.Record) error {
	info, err := os.Stdin.Stat()
	if err != nil {
		move.log(5050)

		return wraperror.Errorf(err, "error reading stdin")
	}
	// printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		reader := bufio.NewReader(os.Stdin)
		move.processJSONL(ctx, "stdin", reader, recordchan)

		return nil
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Writer routing
// ----------------------------------------------------------------------------

// This function implements writing to RabbitMQ.
func (move *BasicMove) write(ctx context.Context, recordchan chan queues.Record) error {
	var err error

	outputURL := move.OutputURL

	outputURLLen := len(outputURL)
	if outputURLLen == 0 {
		// assume stdout
		return move.writeStdout(ctx, recordchan)
	}

	// This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < len("s://p") {
		move.log(5030, outputURL)

		return wraperror.Errorf(errForPackage, "invalid outputURL: %s", outputURL)
	}

	parsedURL, err := url.Parse(outputURL)
	if err != nil {
		move.log(5031, outputURL)

		return wraperror.Errorf(err, "invalid outputURL: %s", outputURL)
	}

	switch parsedURL.Scheme {
	case "amqp":
		rabbitmq.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.PlainText)
	case "file":
		err = move.writeFile(ctx, parsedURL, recordchan)
	case "https":
		// uses actual AWS SQS URL  IMPROVE: detect sqs/amazonaws url?
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.PlainText)
	case "null":
		move.writeNull(ctx, recordchan)
	case "sqs":
		// allows for using a dummy URL with just a queue-name
		// eg  sqs://lookup?queue-name=myqueue
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.PlainText)
	default:
		return wraperror.Errorf(errForPackage, "unknown scheme, unable to write to: %s", outputURL)
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

func (move *BasicMove) writeFile(ctx context.Context, parsedURL *url.URL, recordchan chan queues.Record) error {
	_ = ctx

	var err error

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == FiletypeJSONL:
		err = move.writeFileOfJSONL(ctx, parsedURL.Path, recordchan)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
		err = move.writeFileOfGZIP(ctx, parsedURL.Path, recordchan)
	default:
		// IMPROVE: process JSON file?
		err = wraperror.Errorf(errForPackage, "only able to process JSON-Lines files at this time")
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Writers
// ----------------------------------------------------------------------------

func (move *BasicMove) writeFileOfGZIP(ctx context.Context, fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { // file exists
		return wraperror.Errorf(errForPackage, "output file %s already exists", fileName)
	}

	fileName = filepath.Clean(fileName)

	file, err := os.Create(fileName)
	if err != nil {
		move.log(5032, fileName)

		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			move.log(3001, fileName, err)
		}
	}()

	_, err = file.Stat()
	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	gzfile := gzip.NewWriter(file)
	defer gzfile.Close()

	writer := bufio.NewWriter(gzfile)

	for record := range recordchan {
		recordDefinition := record.GetMessage()
		move.observeWrite(ctx, recordDefinition)

		_, err := writer.WriteString(recordDefinition + "\n")
		if err != nil {
			return wraperror.Errorf(err, "error writing to stdout")
		}
	}

	err = writer.Flush()
	if err != nil {
		return wraperror.Errorf(err, "error flushing %s", fileName)
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

func (move *BasicMove) writeFileOfJSONL(ctx context.Context, fileName string, recordchan chan queues.Record) error {
	_ = ctx

	_, err := os.Stat(fileName)
	if err == nil { // file exists
		move.log(5032, fileName)

		return wraperror.Errorf(errForPackage, "output file %s already exists", fileName)
	}

	fileName = filepath.Clean(fileName)

	file, err := os.Create(fileName)
	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	defer func() {
		err := file.Close()
		if err != nil {
			move.log(3001, fileName, err)
		}
	}()

	_, err = file.Stat()
	if err != nil {
		move.log(5033, fileName)

		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	writer := bufio.NewWriter(file)

	for record := range recordchan {
		recordDefinition := record.GetMessage()
		move.observeWrite(ctx, recordDefinition)

		_, err := writer.WriteString(recordDefinition + "\n")
		if err != nil {
			return wraperror.Errorf(err, "error writing to stdout")
		}
	}

	err = writer.Flush()
	if err != nil {
		return wraperror.Errorf(err, "error flushing %s", fileName)
	}

	return nil
}

func (move *BasicMove) writeNull(ctx context.Context, recordchan chan queues.Record) {
	_ = ctx

	for record := range recordchan {
		recordDefinition := record.GetMessage()
		move.observeWrite(ctx, recordDefinition)
	}
}

func (move *BasicMove) writeStdout(ctx context.Context, recordchan chan queues.Record) error {
	_ = ctx

	_, err := os.Stdout.Stat()
	if err != nil {
		move.log(5051)

		return wraperror.Errorf(err, "error opening stdout")
	}

	writer := bufio.NewWriter(os.Stdout)

	for record := range recordchan {
		recordDefinition := record.GetMessage()
		move.observeWrite(ctx, recordDefinition)

		_, err := writer.WriteString(recordDefinition + "\n")
		if err != nil {
			return wraperror.Errorf(err, "error writing to stdout")
		}
	}

	err = writer.Flush()
	if err != nil {
		return wraperror.Errorf(err, "error flushing stdout")
	}

	return nil
}

// ----------------------------------------------------------------------------
// Utility methods
// ----------------------------------------------------------------------------

func (move *BasicMove) increaseTotalLines(increase int) int {
	move.mutexLineNumber.Lock()
	defer move.mutexLineNumber.Unlock()

	move.lineNumber += increase
	result := move.lineNumber

	return result
}

// Process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel.
func (move *BasicMove) processJSONL(
	ctx context.Context,
	fileName string,
	reader io.Reader,
	recordchan chan queues.Record,
) {
	_ = ctx
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	move.resetTotalLines()

	for scanner.Scan() {
		lineNumber := move.increaseTotalLines(1)
		if lineNumber < move.RecordMin {
			continue
		}

		recordDefinition := strings.TrimSpace(scanner.Text())

		if len(recordDefinition) > 0 { // ignore blank lines
			valid := true
			if move.Validate {
				valid = move.isRecordDefinitionValid(ctx, recordDefinition, lineNumber)
			}

			if valid {
				move.observeRead(ctx, recordDefinition)
				recordchan <- &SzRecord{recordDefinition, lineNumber, fileName}
			}
		}

		if (move.RecordMonitor > 0) && (lineNumber%move.RecordMonitor == 0) {
			move.log(2001, lineNumber)
		}

		if move.RecordMax > 0 && lineNumber >= (move.RecordMax) {
			break
		}
	}

	close(recordchan)
}

func (move *BasicMove) resetTotalLines() {
	move.mutexLineNumber.Lock()
	defer move.mutexLineNumber.Unlock()
	move.lineNumber = 0
}

func (move *BasicMove) isRecordDefinitionValid(ctx context.Context, recordDefinition string, lineNumber int) bool {
	result, err := record.Validate(recordDefinition)
	if err != nil || !result {
		result = false

		move.log(3010, lineNumber, err)

		if move.observers != nil {
			var (
				aRecord        record.Record
				dataSourceCode string
				recordID       string
			)

			validUnmarshal := json.Unmarshal([]byte(recordDefinition), &aRecord) == nil
			if validUnmarshal {
				dataSourceCode = aRecord.DataSource
				recordID = aRecord.ID
			}

			move.waitGroup.Add(1)

			go func() {
				defer move.waitGroup.Done()

				details := map[string]string{
					"dataSourceCode": dataSourceCode,
					"lineNumber":     strconv.Itoa(lineNumber),
					"recordId":       recordID,
				}
				notifier.Notify(ctx, move.observers, move.observerOrigin, ComponentID, 8003, err, details)
			}()
		}
	}

	return result
}

// ----------------------------------------------------------------------------
// Observing
// ----------------------------------------------------------------------------

func (move *BasicMove) observeRead(ctx context.Context, recordDefinition string) {
	if move.observers != nil {
		move.waitGroup.Add(1)

		go func() {
			defer move.waitGroup.Done()

			var (
				aRecord        record.Record
				dataSourceCode string
				recordID       string
			)

			valid := json.Unmarshal([]byte(recordDefinition), &aRecord) == nil
			if valid {
				dataSourceCode = aRecord.DataSource
				recordID = aRecord.ID
			}

			details := map[string]string{
				"dataSourceCode": dataSourceCode,
				"recordId":       recordID,
			}
			notifier.Notify(ctx, move.observers, move.observerOrigin, ComponentID, 8001, nil, details)
		}()
	}
}

func (move *BasicMove) observeWrite(ctx context.Context, recordDefinition string) {
	if move.observers != nil {
		move.waitGroup.Add(1)

		go func() {
			defer move.waitGroup.Done()

			var (
				aRecord        record.Record
				dataSourceCode string
				recordID       string
			)

			valid := json.Unmarshal([]byte(recordDefinition), &aRecord) == nil
			if valid {
				dataSourceCode = aRecord.DataSource
				recordID = aRecord.ID
			}

			details := map[string]string{
				"dataSourceCode": dataSourceCode,
				"recordId":       recordID,
			}
			notifier.Notify(ctx, move.observers, move.observerOrigin, ComponentID, 8002, nil, details)
		}()
	}
}

// ----------------------------------------------------------------------------
// Logging
// ----------------------------------------------------------------------------

// Get the Logger singleton.
func (move *BasicMove) getLogger() logging.Logging {
	var err error

	if move.logger == nil {
		options := []interface{}{
			logging.OptionCallerSkip{Value: callerSkip},
			logging.OptionMessageFields{Value: []string{"id", "text", "reason"}},
		}

		move.logger, err = logging.NewSenzingLogger(ComponentID, IDMessages, options...)
		if err != nil {
			panic(err)
		}
	}

	return move.logger
}

// Log message.
func (move *BasicMove) log(messageNumber int, details ...interface{}) {
	if move.PlainText {
		if move.IsLoggable(messageNumber) {
			outputln(fmt.Sprintf(IDMessages[messageNumber], details...))
		}
	} else {
		move.getLogger().Log(messageNumber, details...)
	}
}

func (move *BasicMove) logBuildInfo() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		move.log(1002, buildInfo.GoVersion, buildInfo.Path, buildInfo.Main.Path, buildInfo.Main.Version)
	} else {
		move.log(3011)
	}
}

func (move *BasicMove) logEntry() {
	move.log(
		1001,
		move.InputURL,
		move.OutputURL,
		move.FileType,
		move.RecordMin,
		move.RecordMax,
		move.RecordMonitor,
		move.MonitoringPeriodInSeconds,
		move.LogLevel,
	)
	move.logBuildInfo()
	move.logStats()
}

func (move *BasicMove) logExit() {}

func (move *BasicMove) logStats() {
	var (
		memStats runtime.MemStats
		gcStats  debug.GCStats
	)

	move.mutexLogStats.Lock()
	defer move.mutexLogStats.Unlock()

	runtime.ReadMemStats(&memStats)
	debug.ReadGCStats(&gcStats)
	move.log(
		1003,
		runtime.NumCPU(),
		runtime.NumGoroutine(),
		runtime.NumCgoCall(),
		memStats.NumGC,
		gcStats.PauseTotal,
		gcStats.LastGC,
		memStats.TotalAlloc,
		memStats.HeapAlloc,
		memStats.NextGC,
		memStats.GCSys,
		memStats.HeapSys,
		memStats.StackSys,
		memStats.Sys,
		memStats.GCCPUFraction,
	)
}

// ----------------------------------------------------------------------------
// Private functions
// ----------------------------------------------------------------------------

func outputln(message ...any) {
	fmt.Println(message...) //nolint
}
