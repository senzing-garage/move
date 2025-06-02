package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/senzing-garage/go-helpers/record"
	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-logging/logging"
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
	JSONOutput                bool
	logger                    logging.Logging
	LogLevel                  string
	MonitoringPeriodInSeconds int
	OutputURL                 string
	RecordMax                 int
	RecordMin                 int
	RecordMonitor             int
}

const (
	JSONL       = "JSONL"
	numChannels = 10
	callerSkip  = 4
)

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Move = (*BasicMove)(nil)

// ----------------------------------------------------------------------------
// -- Public methods
// ----------------------------------------------------------------------------

func (move *BasicMove) Logger() logging.Logging {
	return move.logger
}

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (move *BasicMove) Move(ctx context.Context) error {
	var (
		readErr  error
		writeErr error
		err      error
	)

	move.logBuildInfo()
	move.logStats()

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

	var waitGroup sync.WaitGroup

	recordchan := make(chan queues.Record, numChannels)

	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()

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

	waitGroup.Add(1)

	go func() {
		defer waitGroup.Done()

		writeErr = move.write(ctx, recordchan)
	}()

	waitGroup.Wait()

	if readErr != nil {
		return readErr
	} else if writeErr != nil {
		return writeErr
	}

	return err
}

// ----------------------------------------------------------------------------

// Process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel.
func (move *BasicMove) ProcessJSONL(fileName string, reader io.Reader, recordchan chan queues.Record) {
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	iteration := 0
	for scanner.Scan() {
		iteration++
		if iteration < move.RecordMin {
			continue
		}

		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := record.Validate(str)
			if valid {
				recordchan <- &SzRecord{str, iteration, fileName}
			} else {
				move.log(3010, iteration, err)
			}
		}

		if (move.RecordMonitor > 0) && (iteration%move.RecordMonitor == 0) {
			move.log(2001, iteration)
		}

		if move.RecordMax > 0 && iteration >= (move.RecordMax) {
			break
		}
	}

	close(recordchan)
}

// ----------------------------------------------------------------------------

// Opens and reads a JSONL file.
func (move *BasicMove) ReadJSONLFile(jsonFile string, recordchan chan queues.Record) error {
	cleanJSONFile := filepath.Clean(jsonFile)

	file, err := os.Open(cleanJSONFile)
	if err != nil {
		return wraperror.Errorf(err, "move.ReadJSONLFile.os.Open error: %w", err)
	}

	defer file.Close()

	move.ProcessJSONL(jsonFile, file, recordchan)

	return nil
}

// ----------------------------------------------------------------------------

// Opens and reads a JSONL file that has been GZIPped.
func (move *BasicMove) ReadGZIPFile(gzipFileName string, recordchan chan queues.Record) error {
	cleanGzipFileName := filepath.Clean(gzipFileName)

	gzipfile, err := os.Open(cleanGzipFileName)
	if err != nil {
		return wraperror.Errorf(err, "move.ReadGZIPFile.os.Open error: %w", err)
	}

	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return wraperror.Errorf(err, "move.ReadGZIPFile.gzip.NewReader error: %w", err)
	}
	defer reader.Close()

	move.ProcessJSONL(gzipFileName, reader, recordchan)

	return nil
}

// ----------------------------------------------------------------------------

// Opens and reads a JSONL http resource.
func (move *BasicMove) ReadJSONLResource(jsonURL string, recordchan chan queues.Record) error {
	//nolint:noctx
	response, err := http.Get(jsonURL) //nolint:gosec
	if err != nil {
		return wraperror.Errorf(err, "move.ReadJSONLResource.http.Get error: %w", err)
	}

	if response.StatusCode != http.StatusOK {
		return wraperror.Errorf(errForPackage, "unable to retrieve %s, return code %d", jsonURL, response.StatusCode)
	}

	defer response.Body.Close()

	move.ProcessJSONL(jsonURL, response.Body, recordchan)

	return nil
}

func (move *BasicMove) ReadGZIPResource(gzipURL string, recordchan chan queues.Record) error {
	//nolint:noctx
	response, err := http.Get(gzipURL) //nolint:gosec
	if err != nil {
		return wraperror.Errorf(err, "fatal error retrieving inputURL %s", gzipURL)
	}

	if response.StatusCode != http.StatusOK {
		return wraperror.Errorf(errForPackage, "unable to retrieve %s, return code %d", gzipURL, response.StatusCode)
	}

	defer response.Body.Close()

	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		return wraperror.Errorf(err, "gzip.NewReader error")
	}

	defer reader.Close()

	move.ProcessJSONL(gzipURL, reader, recordchan)

	return nil
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
		return wraperror.Errorf(errForPackage, "invalid error level: %s", logLevelName)
	}

	// Set ValidateImpl log level.

	err = move.getLogger().SetLogLevel(logLevelName)

	return wraperror.Errorf(err, "move.SetLogLevel error: %w", err)
}

// ----------------------------------------------------------------------------

func (move *BasicMove) WriteStdout(recordchan chan queues.Record) error {
	_, err := os.Stdout.Stat()
	if err != nil {
		return wraperror.Errorf(err, "fatal error opening stdout")
	}

	writer := bufio.NewWriter(os.Stdout)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
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
// -- Private methods
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// -- Write implementation: writes records in the record channel to the output
// ----------------------------------------------------------------------------

// This function implements writing to RabbitMQ.
func (move *BasicMove) write(ctx context.Context, recordchan chan queues.Record) error {
	outputURL := move.OutputURL

	outputURLLen := len(outputURL)
	if outputURLLen == 0 {
		// assume stdout
		return move.WriteStdout(recordchan)
	}

	// This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < len("s://p") {
		return wraperror.Errorf(errForPackage, "invalid outputURL: %s", outputURL)
	}

	parsedURL, err := url.Parse(outputURL)
	if err != nil {
		return wraperror.Errorf(err, "invalid outputURL: %s", outputURL)
	}

	switch parsedURL.Scheme {
	case "amqp":
		rabbitmq.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	case "file":
		switch {
		case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == JSONL:
			return move.writeJSONLFile(parsedURL.Path, recordchan)
		case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.writeGZIPFile(parsedURL.Path, recordchan)
		default:
			// IMPROVE: process JSON file?
			return wraperror.Errorf(errForPackage, "only able to process JSON-Lines files at this time")
		}
	case "sqs":
		// allows for using a dummy URL with just a queue-name
		// eg  sqs://lookup?queue-name=myqueue
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	case "https":
		// uses actual AWS SQS URL  IMPROVE: detect sqs/amazonaws url?
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	default:
		return wraperror.Errorf(errForPackage, "unknow scheme, unable to write to: %s", outputURL)
	}

	move.log(2000)

	return nil
}

// ----------------------------------------------------------------------------

func (move *BasicMove) writeJSONLFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { // file exists
		return wraperror.Errorf(errForPackage, "error output file %s exists. error: %w", fileName, err)
	}

	fileName = filepath.Clean(fileName)
	file, err := os.Create(fileName)

	defer func() {
		err := file.Close()
		move.log(3001, fileName, err)
	}()

	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	_, err = file.Stat()
	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	writer := bufio.NewWriter(file)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
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

// ----------------------------------------------------------------------------

func (move *BasicMove) writeGZIPFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { // file exists
		return wraperror.Errorf(errForPackage, "error output file %s exists", fileName)
	}

	fileName = filepath.Clean(fileName)
	file, err := os.Create(fileName)

	defer func() {
		err := file.Close()
		move.log(3001, fileName, err)
	}()

	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	_, err = file.Stat()
	if err != nil {
		return wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	gzfile := gzip.NewWriter(file)
	defer gzfile.Close()

	writer := bufio.NewWriter(gzfile)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
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

// ----------------------------------------------------------------------------
// -- Read implementation: reads records from the input to the record channel
// ----------------------------------------------------------------------------

// this function attempts to determine the source of records.
// it then parses the source and puts the records into the record channel.
func (move *BasicMove) read(ctx context.Context, recordchan chan queues.Record) error {
	_ = ctx

	inputURL := move.InputURL
	inputURLLen := len(inputURL)

	if inputURLLen == 0 {
		// assume stdin
		return move.readStdin(recordchan)
	}

	// This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < len("s://p") {
		move.log(5000, inputURL)

		return wraperror.Errorf(errForPackage, "check the inputURL parameter: %s", inputURL)
	}

	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		return wraperror.Errorf(err, "move.read.url.Parse error: %w", err)
	}

	switch parsedURL.Scheme {
	case "file":
		switch {
		case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == JSONL:
			return move.ReadJSONLFile(parsedURL.Path, recordchan)
		case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.ReadGZIPFile(parsedURL.Path, recordchan)
		default:
			// IMPROVE: process JSON file?
			close(recordchan)
			move.log(5011)

			return wraperror.Errorf(errForPackage, "unable to process file://%s", parsedURL.Path)
		}
	case "http", "https":
		switch {
		case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(move.FileType) == JSONL:
			return move.ReadJSONLResource(inputURL, recordchan)
		case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.ReadGZIPResource(inputURL, recordchan)
		default:
			move.log(5012)

			return wraperror.Errorf(errForPackage, "unable to process http://%s", parsedURL.Path)
		}
	default:
		return wraperror.Errorf(errForPackage, "we don't handle %s input URLs", parsedURL.Scheme)
	}
}

// ----------------------------------------------------------------------------

func (move *BasicMove) readStdin(recordchan chan queues.Record) error {
	info, err := os.Stdin.Stat()
	if err != nil {
		return wraperror.Errorf(err, "fatal error reading stdin")
	}
	// printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		reader := bufio.NewReader(os.Stdin)
		move.ProcessJSONL("stdin", reader, recordchan)

		return nil
	}

	return wraperror.Errorf(errForPackage, "fatal error stdin not piped")
}

// ----------------------------------------------------------------------------
// Logging --------------------------------------------------------------------
// ----------------------------------------------------------------------------

// Get the Logger singleton.
func (move *BasicMove) getLogger() logging.Logging {
	var err error

	if move.logger == nil {
		options := []interface{}{
			&logging.OptionCallerSkip{Value: callerSkip},
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
	if move.JSONOutput {
		move.getLogger().Log(messageNumber, details...)
	} else {
		outputln(fmt.Sprintf(IDMessages[messageNumber], details...))
	}
}

// ----------------------------------------------------------------------------

func (move *BasicMove) logBuildInfo() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		move.log(2002, buildInfo.GoVersion, buildInfo.Path, buildInfo.Main.Path, buildInfo.Main.Version)
	} else {
		move.log(3011)
	}
}

// ----------------------------------------------------------------------------

var lock sync.Mutex

func (move *BasicMove) logStats() {
	lock.Lock()
	defer lock.Unlock()

	cpus := runtime.NumCPU()
	goRoutines := runtime.NumGoroutine()
	cgoCalls := runtime.NumCgoCall()

	var memStats runtime.MemStats

	runtime.ReadMemStats(&memStats)

	var gcStats debug.GCStats

	debug.ReadGCStats(&gcStats)
	move.log(
		2003,
		cpus,
		goRoutines,
		cgoCalls,
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
