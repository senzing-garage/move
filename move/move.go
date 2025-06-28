package move

import (
	"context"
	"fmt"
	"net/url"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-logging/logging"
	"github.com/senzing-garage/go-observing/notifier"
	"github.com/senzing-garage/go-observing/observer"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
	"github.com/senzing-garage/move/recordreader"
	"github.com/senzing-garage/move/recordwriter"
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
	linesRead                 int
	linesWritten              int
	logger                    logging.Logging
	LogLevel                  string
	MonitoringPeriodInSeconds int
	mutexLogStats             sync.Mutex
	observerOrigin            string
	observers                 subject.Subject
	OutputURL                 string
	PlainText                 bool
	reader                    recordreader.RecordReader
	RecordMax                 int
	RecordMin                 int
	RecordMonitor             int
	Validate                  bool
	waitGroup                 *sync.WaitGroup
	writer                    recordwriter.RecordWriter
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

func (mover *BasicMove) GetLinesRead() int {
	return mover.linesRead
}

func (mover *BasicMove) GetLinesWritten() int {
	return mover.linesWritten
}

func (mover *BasicMove) IsLoggable(messageNumber int) bool {
	logThreshold, ok := messageThresholds[mover.getLogger().GetLogLevel()]
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
func (mover *BasicMove) Move(ctx context.Context) error {
	var (
		readErr   error
		writeErr  error
		err       error
		waitGroup sync.WaitGroup
	)

	// Prolog.

	mover.waitGroup = &waitGroup

	if len(mover.LogLevel) > 0 {
		err = mover.SetLogLevel(ctx, mover.LogLevel)
		if err != nil {
			return wraperror.Errorf(err, "SetLogLevel")
		}
	}

	if mover.RecordMin > mover.RecordMax {
		return wraperror.Errorf(errForPackage, "RecordMin (%d) > RecordMax (%d)", mover.RecordMin, mover.RecordMax)
	}

	mover.logEntry()
	mover.startMonitoring(ctx)

	// Create channel, reader, and writer.

	recordChannel := make(chan queues.Record, numChannels)

	mover.reader, err = mover.createReader(ctx, recordChannel)
	if err != nil {
		return wraperror.Errorf(err, "could not create a reader from %s", mover.InputURL)
	}

	mover.writer, err = mover.createWriter(ctx, recordChannel)
	if err != nil {
		return wraperror.Errorf(err, "could not create a writer from %s", mover.OutputURL)
	}

	// Run reader and writer.

	mover.waitGroup.Add(1)

	go func() {
		defer mover.waitGroup.Done()

		mover.linesRead, readErr = mover.reader.Read(ctx)
		if readErr != nil {
			select {
			case <-recordChannel:
				// channel already closed
			default:
				close(recordChannel)
			}
		}
	}()

	mover.waitGroup.Add(1)

	go func() {
		defer mover.waitGroup.Done()

		mover.linesWritten, writeErr = mover.writer.Write(ctx)
	}()

	mover.waitGroup.Wait()

	// Epilog.

	if readErr != nil {
		return wraperror.Errorf(readErr, "Read error")
	}

	if writeErr != nil {
		return wraperror.Errorf(writeErr, "Write error")
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

/*
Method RegisterObserver adds the observer to the list of observers notified.

Input
  - ctx: A context to control lifecycle.
  - observer: The observer to be added.
*/
func (mover *BasicMove) RegisterObserver(ctx context.Context, observer observer.Observer) error {
	var err error

	if mover.observers == nil {
		mover.observers = &subject.SimpleSubject{}
	}

	err = mover.observers.RegisterObserver(ctx, observer)

	if mover.observers != nil {
		go func() {
			details := map[string]string{
				"observerID": observer.GetObserverID(ctx),
			}
			notifier.Notify(ctx, mover.observers, mover.observerOrigin, ComponentID, 8000, err, details)
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
func (mover *BasicMove) SetLogLevel(ctx context.Context, logLevelName string) error {
	_ = ctx

	var err error

	// Verify value of logLevelName.

	if !logging.IsValidLogLevelName(logLevelName) {
		mover.log(5060, logLevelName)

		return wraperror.Errorf(errForPackage, "invalid error level: %s", logLevelName)
	}

	// Set ValidateImpl log level.

	err = mover.getLogger().SetLogLevel(logLevelName)
	if err == nil {
		mover.LogLevel = logLevelName
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

/*
Method UnregisterObserver removes the observer to the list of observers notified.

Input
  - ctx: A context to control lifecycle.
  - observer: The observer to be added.
*/
func (mover *BasicMove) UnregisterObserver(ctx context.Context, observer observer.Observer) error {
	var err error

	if mover.observers != nil {
		// Tricky code:
		// client.notify is called synchronously before client.observers is set to nil.
		// In client.notify, each observer will get notified in a goroutine.
		// Then client.observers may be set to nil, but observer goroutines will be OK.
		details := map[string]string{
			"observerID": observer.GetObserverID(ctx),
		}
		notifier.Notify(ctx, mover.observers, mover.observerOrigin, ComponentID, 8704, err, details)
		err = mover.observers.UnregisterObserver(ctx, observer)

		if !mover.observers.HasObservers(ctx) {
			mover.observers = nil
		}
	}

	return wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Reader factory
// ----------------------------------------------------------------------------

func (mover *BasicMove) createReader(
	ctx context.Context,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	inputURL := mover.InputURL
	inputURLLen := len(inputURL)

	if inputURLLen == 0 { // assume stdin for zero-length URL.
		reader, err := mover.createStdinJSONReader(ctx, recordChannel)

		return reader, wraperror.Errorf(err, "createStdinJsonReader")
	}

	parsedURL, err := url.Parse(inputURL)
	if err != nil {
		return nil, wraperror.Errorf(err, "url.Parse: %s", inputURL)
	}

	switch parsedURL.Scheme {
	case "file":
		return mover.createFileReader(ctx, parsedURL, recordChannel)
	case "http", "https":
		return mover.createHTTPReader(ctx, parsedURL, recordChannel)
	default:
		return nil, wraperror.Errorf(errForPackage, "invalid protocol %s in input URL: %s", parsedURL.Scheme, inputURL)
	}
}

func (mover *BasicMove) createFileReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	_ = ctx

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(mover.FileType) == FiletypeJSONL:
		return mover.createFileJSONReader(ctx, parsedURL, recordChannel)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(mover.FileType) == FiletypeGZ:
		return mover.createFileGzipReader(ctx, parsedURL, recordChannel)
	default:
		return nil, wraperror.Errorf(errForPackage, "cannot create reader for file://%s", parsedURL.Path)
	}
}

func (mover *BasicMove) createHTTPReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	_ = ctx

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(mover.FileType) == FiletypeJSONL:
		return mover.createHTTPJSONReader(ctx, parsedURL, recordChannel)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(mover.FileType) == FiletypeGZ:
		return mover.createHTTPGzipReader(ctx, parsedURL, recordChannel)
	default:
		return nil, wraperror.Errorf(errForPackage, "cannot create reader for %s", parsedURL.String())
	}
}

func (mover *BasicMove) createFileGzipReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	_ = ctx
	result := &recordreader.FileGzipReader{
		FilePath:       parsedURL.Path,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		RecordMax:      mover.RecordMax,
		RecordMin:      mover.RecordMin,
		RecordMonitor:  mover.RecordMonitor,
		Validate:       mover.Validate,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createFileJSONReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	_ = ctx
	result := &recordreader.FileJsonlReader{
		FilePath:       parsedURL.Path,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		RecordMax:      mover.RecordMax,
		RecordMin:      mover.RecordMin,
		RecordMonitor:  mover.RecordMonitor,
		Validate:       mover.Validate,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createHTTPGzipReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	_ = ctx
	_ = parsedURL
	result := &recordreader.HTTPGzipReader{
		InputURL:       mover.InputURL,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		RecordMax:      mover.RecordMax,
		RecordMin:      mover.RecordMin,
		RecordMonitor:  mover.RecordMonitor,
		Validate:       mover.Validate,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createHTTPJSONReader(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	_ = ctx
	_ = parsedURL
	result := &recordreader.HTTPJsonlReader{
		InputURL:       mover.InputURL,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		RecordMax:      mover.RecordMax,
		RecordMin:      mover.RecordMin,
		RecordMonitor:  mover.RecordMonitor,
		Validate:       mover.Validate,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createStdinJSONReader(
	ctx context.Context,
	recordChannel chan queues.Record,
) (recordreader.RecordReader, error) {
	var err error

	_ = ctx
	result := &recordreader.StdinJsonlReader{
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		RecordMax:      mover.RecordMax,
		RecordMin:      mover.RecordMin,
		RecordMonitor:  mover.RecordMonitor,
		Validate:       mover.Validate,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Writer factory
// ----------------------------------------------------------------------------

func (mover *BasicMove) createWriter(
	ctx context.Context,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	var err error

	outputURL := mover.OutputURL
	outputURLLen := len(outputURL)

	if outputURLLen == 0 { // assume stdin for zero-length URL.
		reader, err := mover.createStdoutWriter(ctx, recordChannel)

		return reader, wraperror.Errorf(err, "createStdinJsonReader")
	}

	parsedURL, err := url.Parse(outputURL)
	if err != nil {
		return nil, wraperror.Errorf(err, "invalid outputURL: %s", outputURL)
	}

	switch parsedURL.Scheme {
	case "file":
		return mover.createFileWriter(ctx, parsedURL, recordChannel)
	case "null":
		return mover.createNullWriter(ctx, recordChannel)
	default:
		return nil, wraperror.Errorf(
			errForPackage,
			"invalid protocol %s in output URL: %s",
			parsedURL.Scheme,
			outputURL,
		)
	}
}

func (mover *BasicMove) createFileWriter(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	_ = ctx

	switch {
	case strings.HasSuffix(parsedURL.Path, "jsonl"), strings.ToUpper(mover.FileType) == FiletypeJSONL:
		return mover.createFileJSONWriter(ctx, parsedURL, recordChannel)
	case strings.HasSuffix(parsedURL.Path, "gz"), strings.ToUpper(mover.FileType) == FiletypeGZ:
		return mover.createFileGzipWriter(ctx, parsedURL, recordChannel)
	default:
		return nil, wraperror.Errorf(errForPackage, "cannot create reader for file://%s", parsedURL.Path)
	}
}

func (mover *BasicMove) createFileGzipWriter(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	var err error

	_ = ctx
	result := &recordwriter.FileGzipWriter{
		FilePath:       parsedURL.Path,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createFileJSONWriter(
	ctx context.Context,
	parsedURL *url.URL,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	var err error

	_ = ctx
	result := &recordwriter.FileJSONWriter{
		FilePath:       parsedURL.Path,
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createNullWriter(
	ctx context.Context,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	var err error

	_ = ctx
	result := &recordwriter.NullWriter{
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

func (mover *BasicMove) createStdoutWriter(
	ctx context.Context,
	recordChannel chan queues.Record,
) (recordwriter.RecordWriter, error) {
	var err error

	_ = ctx
	result := &recordwriter.StdoutWriter{
		ObserverOrigin: mover.observerOrigin,
		Observers:      mover.observers,
		RecordChannel:  recordChannel,
		WaitGroup:      mover.waitGroup,
	}

	return result, wraperror.Errorf(err, wraperror.NoMessage)
}

// ----------------------------------------------------------------------------
// Utility methods
// ----------------------------------------------------------------------------

func (mover *BasicMove) startMonitoring(ctx context.Context) {
	if mover.MonitoringPeriodInSeconds <= 0 {
		mover.MonitoringPeriodInSeconds = 60
	}

	ticker := time.NewTicker(time.Duration(mover.MonitoringPeriodInSeconds) * time.Second)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				mover.logStats()
			}
		}
	}()
}

// ----------------------------------------------------------------------------
// Logging
// ----------------------------------------------------------------------------

// Get the Logger singleton.
func (mover *BasicMove) getLogger() logging.Logging {
	var err error

	if mover.logger == nil {
		options := []interface{}{
			logging.OptionCallerSkip{Value: callerSkip},
			logging.OptionMessageFields{Value: []string{"id", "text", "reason"}},
		}

		mover.logger, err = logging.NewSenzingLogger(ComponentID, IDMessages, options...)
		if err != nil {
			panic(err)
		}
	}

	return mover.logger
}

// Log message.
func (mover *BasicMove) log(messageNumber int, details ...interface{}) {
	if mover.PlainText {
		if mover.IsLoggable(messageNumber) {
			outputln(fmt.Sprintf(IDMessages[messageNumber], details...))
		}
	} else {
		mover.getLogger().Log(messageNumber, details...)
	}
}

func (mover *BasicMove) logBuildInfo() {
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		mover.log(1002, buildInfo.GoVersion, buildInfo.Path, buildInfo.Main.Path, buildInfo.Main.Version)
	} else {
		mover.log(3011)
	}
}

func (mover *BasicMove) logEntry() {
	mover.log(
		1001,
		mover.InputURL,
		mover.OutputURL,
		mover.FileType,
		mover.RecordMin,
		mover.RecordMax,
		mover.RecordMonitor,
		mover.MonitoringPeriodInSeconds,
		mover.LogLevel,
	)
	mover.logBuildInfo()
	mover.logStats()
}

func (mover *BasicMove) logStats() {
	var (
		memStats runtime.MemStats
		gcStats  debug.GCStats
	)

	mover.mutexLogStats.Lock()
	defer mover.mutexLogStats.Unlock()

	runtime.ReadMemStats(&memStats)
	debug.ReadGCStats(&gcStats)
	mover.log(
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
