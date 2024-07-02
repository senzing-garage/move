package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"errors"
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

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Move = (*BasicMove)(nil)

var (
// waitGroup sync.WaitGroup
)

// ----------------------------------------------------------------------------
// -- Public methods
// ----------------------------------------------------------------------------

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (move *BasicMove) Move(ctx context.Context) (err error) {

	var readErr error
	var writeErr error

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
	waitGroup.Add(2)
	recordchan := make(chan queues.Record, 10)

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
	return nil
}

// ----------------------------------------------------------------------------
// -- Private methods
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// -- Write implementation: writes records in the record channel to the output
// ----------------------------------------------------------------------------

// this function implements writing to RabbitMQ
func (move *BasicMove) write(ctx context.Context, recordchan chan queues.Record) error {

	outputURL := move.OutputURL
	outputURLLen := len(outputURL)
	if outputURLLen == 0 {
		// assume stdout
		return move.writeStdout(recordchan)
	}

	// This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < 5 {
		return fmt.Errorf("invalid outputURL: %s", outputURL)
	}

	u, err := url.Parse(outputURL)
	if err != nil {
		return fmt.Errorf("invalid outputURL: %s %w", outputURL, err)
	}

	switch u.Scheme {
	case "amqp":
		rabbitmq.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	case "file":
		switch {
		case strings.HasSuffix(u.Path, "jsonl"), strings.ToUpper(move.FileType) == "JSONL":
			return move.writeJSONLFile(u.Path, recordchan)
		case strings.HasSuffix(u.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.writeGZIPFile(u.Path, recordchan)
		default:
			//TODO: process JSON file?
			return errors.New("only able to process JSON-Lines files at this time")
		}
	case "sqs":
		// allows for using a dummy URL with just a queue-name
		// eg  sqs://lookup?queue-name=myqueue
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	case "https":
		// uses actual AWS SQS URL  TODO: detect sqs/amazonaws url?
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan, move.LogLevel, move.JSONOutput)
	default:
		return fmt.Errorf("unknow scheme, unable to write to: %s", outputURL)
	}
	move.log(2000)
	return nil
}

// ----------------------------------------------------------------------------

func (move *BasicMove) writeStdout(recordchan chan queues.Record) error {

	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening stdout %w", err)
	}

	writer := bufio.NewWriter(os.Stdout)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %w", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing stdout %w", err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func (move *BasicMove) writeJSONLFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { // file exists
		return fmt.Errorf("error output file %s exists", fileName)
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer func() {
		err := f.Close()
		move.log(3001, fileName, err)
	}()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %w", fileName, err)
	}
	_, err = f.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %w", fileName, err)
	}

	writer := bufio.NewWriter(f)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %w", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing %s %w", fileName, err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func (move *BasicMove) writeGZIPFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { // file exists
		return fmt.Errorf("error output file %s exists", fileName)
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer func() {
		err := f.Close()
		move.log(3001, fileName, err)
	}()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %w", fileName, err)
	}
	_, err = f.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %w", fileName, err)
	}

	gzfile := gzip.NewWriter(f)
	defer gzfile.Close()
	writer := bufio.NewWriter(gzfile)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %w", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing %s %w", fileName, err)
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
	if len(inputURL) < 5 {
		move.log(5000, inputURL)
		return fmt.Errorf("check the inputURL parameter: %s", inputURL)
	}

	u, err := url.Parse(inputURL)
	if err != nil {
		return err
	}

	switch u.Scheme {
	case "file":
		switch {
		case strings.HasSuffix(u.Path, "jsonl"), strings.ToUpper(move.FileType) == "JSONL":
			return move.readJSONLFile(u.Path, recordchan)
		case strings.HasSuffix(u.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.readGZIPFile(u.Path, recordchan)
		default:
			//TODO: process JSON file?
			close(recordchan)
			move.log(5011)
			return errors.New("unable to process file")
		}
	case "http", "https":
		switch {
		case strings.HasSuffix(u.Path, "jsonl"), strings.ToUpper(move.FileType) == "JSONL":
			return move.readJSONLResource(inputURL, recordchan)
		case strings.HasSuffix(u.Path, "gz"), strings.ToUpper(move.FileType) == "GZ":
			return move.readGZIPResource(inputURL, recordchan)
		default:
			move.log(5012)
			return errors.New("unable to process file")
		}
	default:
		return fmt.Errorf("we don't handle %s input URLs", u.Scheme)
	}

}

// ----------------------------------------------------------------------------

// process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel
func (move *BasicMove) processJSONL(fileName string, reader io.Reader, recordchan chan queues.Record) {

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	i := 0
	for scanner.Scan() {
		i++
		if i < move.RecordMin {
			continue
		}
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := record.Validate(str)
			if valid {
				recordchan <- &szRecord{str, i, fileName}
			} else {
				move.log(3010, i, err)
			}
		}
		if (move.RecordMonitor > 0) && (i%move.RecordMonitor == 0) {
			move.log(2001, i)
		}
		if move.RecordMax > 0 && i >= (move.RecordMax) {
			break
		}
	}
	close(recordchan)
}

// ----------------------------------------------------------------------------

func (move *BasicMove) readStdin(recordchan chan queues.Record) error {
	info, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("fatal error reading stdin %w", err)
	}
	// printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {

		reader := bufio.NewReader(os.Stdin)
		move.processJSONL("stdin", reader, recordchan)
		return nil
	}
	return fmt.Errorf("fatal error stdin not piped")
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL http resource
func (move *BasicMove) readJSONLResource(jsonURL string, recordchan chan queues.Record) error {

	//nolint:noctx
	response, err := http.Get(jsonURL) //nolint:gosec
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("unable to retrieve %s, return code %d", jsonURL, response.StatusCode)
	}
	defer response.Body.Close()

	move.processJSONL(jsonURL, response.Body, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file
func (move *BasicMove) readJSONLFile(jsonFile string, recordchan chan queues.Record) error {
	jsonFile = filepath.Clean(jsonFile)
	file, err := os.Open(jsonFile)
	if err != nil {
		return err
	}
	defer file.Close()

	move.processJSONL(jsonFile, file, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file that has been GZIPped
func (move *BasicMove) readGZIPFile(gzipFileName string, recordchan chan queues.Record) error {
	gzipFileName = filepath.Clean(gzipFileName)
	gzipfile, err := os.Open(gzipFileName)
	if err != nil {
		return err
	}
	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return err
	}
	defer reader.Close()

	move.processJSONL(gzipFileName, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------
func (move *BasicMove) readGZIPResource(gzipURL string, recordchan chan queues.Record) error {

	//nolint:noctx
	response, err := http.Get(gzipURL) //nolint:gosec
	if err != nil {
		return fmt.Errorf("fatal error retrieving inputURL %w", err)
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("unable to retrieve %s, return code %d", gzipURL, response.StatusCode)
	}
	defer response.Body.Close()
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		return fmt.Errorf("fatal error reading inputURL %w", err)
	}
	defer reader.Close()

	move.processJSONL(gzipURL, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------
// Logging --------------------------------------------------------------------
// ----------------------------------------------------------------------------

// Get the Logger singleton.
func (move *BasicMove) getLogger() logging.Logging {
	var err error
	if move.logger == nil {
		options := []interface{}{
			&logging.OptionCallerSkip{Value: 4},
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
		fmt.Println(fmt.Sprintf(IDMessages[messageNumber], details...))
	}
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
		return fmt.Errorf("invalid error level: %s", logLevelName)
	}

	// Set ValidateImpl log level.

	err = move.getLogger().SetLogLevel(logLevelName)
	return err
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
	move.log(2003, cpus, goRoutines, cgoCalls, memStats.NumGC, gcStats.PauseTotal, gcStats.LastGC, memStats.TotalAlloc, memStats.HeapAlloc, memStats.NextGC, memStats.GCSys, memStats.HeapSys, memStats.StackSys, memStats.Sys, memStats.GCCPUFraction)

}
