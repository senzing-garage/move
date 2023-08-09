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

	"github.com/senzing/go-common/record"
	"github.com/senzing/go-queueing/queues"
	"github.com/senzing/go-queueing/queues/rabbitmq"
	"github.com/senzing/go-queueing/queues/sqs"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type MoveError struct {
	error
}

type MoveImpl struct {
	FileType                  string
	InputURL                  string
	LogLevel                  string
	MonitoringPeriodInSeconds int
	OutputURL                 string
	RecordMax                 int
	RecordMin                 int
	RecordMonitor             int
}

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Move = (*MoveImpl)(nil)

var (
// waitGroup sync.WaitGroup
)

// ----------------------------------------------------------------------------
// -- Public methods
// ----------------------------------------------------------------------------

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (m *MoveImpl) Move(ctx context.Context) (err error) {

	var readErr error = nil
	var writeErr error = nil

	logBuildInfo()
	logStats()

	if m.MonitoringPeriodInSeconds <= 0 {
		m.MonitoringPeriodInSeconds = 60
	}
	ticker := time.NewTicker(time.Duration(m.MonitoringPeriodInSeconds) * time.Second)
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				logStats()
			}
		}
	}()

	var waitGroup sync.WaitGroup
	waitGroup.Add(2)
	recordchan := make(chan queues.Record, 10)

	go func() {
		defer waitGroup.Done()
		readErr = m.read(ctx, recordchan)
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
		writeErr = m.write(ctx, recordchan)
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
func (m *MoveImpl) write(ctx context.Context, recordchan chan queues.Record) error {

	outputURL := m.OutputURL
	outputURLLen := len(outputURL)
	if outputURLLen == 0 {
		//assume stdout
		return m.writeStdout(recordchan)
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < 5 {
		return fmt.Errorf("invalid outputURL: %s", outputURL)
	}

	u, err := url.Parse(outputURL)
	if err != nil {
		return fmt.Errorf("invalid outputURL: %s %v", outputURL, err)
	}

	switch u.Scheme {
	case "amqp":
		rabbitmq.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	case "file":
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			return m.writeJSONLFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			return m.writeGZIPFile(u.Path, recordchan)
		} else {
			// valid := m.validate(u.Path)
			// fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			return errors.New("only able to process JSON-Lines files at this time")
		}
	case "sqs":
		//allows for using a dummy URL with just a queue-name
		// eg  sqs://lookup?queue-name=myqueue
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	case "https":
		//uses actual AWS SQS URL  TODO: detect sqs/amazonaws url?
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	default:
		return fmt.Errorf("unknow scheme, unable to write to: %s", outputURL)
	}
	fmt.Println("So long and thanks for all the fish.")
	return nil
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeStdout(recordchan chan queues.Record) error {

	_, err := os.Stdout.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening stdout %v", err)
	}

	writer := bufio.NewWriter(os.Stdout)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %v", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing stdout %v", err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeJSONLFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		return fmt.Errorf("error output file %s exists", fileName)
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer func() {
		err := f.Close()
		fmt.Printf("Error closing file %s: %v\n", fileName, err)
	}()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %v", fileName, err)
	}
	_, err = f.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %v", fileName, err)
	}

	writer := bufio.NewWriter(f)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %v", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing %s %v", fileName, err)
	}
	return nil
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeGZIPFile(fileName string, recordchan chan queues.Record) error {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		return fmt.Errorf("error output file %s exists", fileName)
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer func() {
		err := f.Close()
		fmt.Printf("Error closing file: %v", err)
	}()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %v", fileName, err)
	}
	_, err = f.Stat()
	if err != nil {
		return fmt.Errorf("fatal error opening %s %v", fileName, err)
	}

	gzfile := gzip.NewWriter(f)
	defer gzfile.Close()
	writer := bufio.NewWriter(gzfile)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			return fmt.Errorf("error writing to stdout %v", err)
		}
	}
	err = writer.Flush()
	if err != nil {
		return fmt.Errorf("error flushing %s %v", fileName, err)
	}
	return nil
}

// ----------------------------------------------------------------------------
// -- Read implementation: reads records from the input to the record channel
// ----------------------------------------------------------------------------

// this function attempts to determine the source of records.
// it then parses the source and puts the records into the record channel.
func (m *MoveImpl) read(ctx context.Context, recordchan chan queues.Record) error {

	inputURL := m.InputURL
	inputURLLen := len(inputURL)

	if inputURLLen == 0 {
		//assume stdin
		return m.readStdin(recordchan)
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", inputURL)
		return fmt.Errorf("ERROR: check the inputURL parameter: %s", inputURL)
	}

	u, err := url.Parse(inputURL)
	if err != nil {
		return err
	}

	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			return m.readJSONLFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			return m.readGZIPFile(u.Path, recordchan)
		} else {
			// valid := m.validate(u.Path)
			// fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			close(recordchan)
			return errors.New("unable to process file")
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			return m.readJSONLResource(inputURL, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			return m.readGZIPResource(inputURL, recordchan)
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
			return errors.New("unable to process file")
		}
	} else {
		return fmt.Errorf("we don't handle %s input URLs", u.Scheme)
	}
}

// ----------------------------------------------------------------------------

// process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel
func (m *MoveImpl) processJSONL(fileName string, reader io.Reader, recordchan chan queues.Record) {

	fmt.Println(time.Now(), "Start file read", fileName)

	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	i := 0
	for scanner.Scan() {
		i++
		if i < m.RecordMin {
			continue
		}
		str := strings.TrimSpace(scanner.Text())
		// ignore blank lines
		if len(str) > 0 {
			valid, err := record.Validate(str)
			if valid {
				recordchan <- &szRecord{str, i, fileName}
			} else {
				fmt.Println("Line", i, err)
			}
		}
		if (m.RecordMonitor > 0) && (i%m.RecordMonitor == 0) {
			fmt.Println(time.Now(), "Records sent to queue:", i)
		}
		if m.RecordMax > 0 && i >= (m.RecordMax) {
			break
		}
	}
	close(recordchan)

	fmt.Println(time.Now(), "Record channel close for file", fileName)
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) readStdin(recordchan chan queues.Record) error {
	info, err := os.Stdin.Stat()
	if err != nil {
		return fmt.Errorf("fatal error reading stdin %v", err)
	}
	//printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {

		reader := bufio.NewReader(os.Stdin)
		m.processJSONL("stdin", reader, recordchan)
		return nil
	}
	return fmt.Errorf("fatal error stdin not piped")
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL http resource
func (m *MoveImpl) readJSONLResource(jsonURL string, recordchan chan queues.Record) error {
	// #nosec G107
	response, err := http.Get(jsonURL)
	if err != nil {
		return err
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("unable to retrieve %s, return code %d", jsonURL, response.StatusCode)
	}
	defer response.Body.Close()

	m.processJSONL(jsonURL, response.Body, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file
func (m *MoveImpl) readJSONLFile(jsonFile string, recordchan chan queues.Record) error {
	jsonFile = filepath.Clean(jsonFile)
	file, err := os.Open(jsonFile)
	if err != nil {
		return err
	}
	defer file.Close()

	m.processJSONL(jsonFile, file, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL file that has been GZIPped
func (m *MoveImpl) readGZIPFile(gzipFileName string, recordchan chan queues.Record) error {
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

	m.processJSONL(gzipFileName, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------
func (m *MoveImpl) readGZIPResource(gzipURL string, recordchan chan queues.Record) error {
	// #nosec G107
	response, err := http.Get(gzipURL)
	if err != nil {
		return fmt.Errorf("fatal error retrieving inputURL %v", err)
	}
	if response.StatusCode != 200 {
		return fmt.Errorf("unable to retrieve %s, return code %d", gzipURL, response.StatusCode)
	}
	defer response.Body.Close()
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		return fmt.Errorf("fatal error reading inputURL %v", err)
	}
	defer reader.Close()

	m.processJSONL(gzipURL, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// validates that a file is valid JSON
// TODO:  What is a valid JSON file?
// func (m *MoveImpl) validate(jsonFile string) bool {

// 	var file *os.File = os.Stdin

// 	if jsonFile != "" {
// 		var err error
// 		jsonFile = filepath.Clean(jsonFile)
// 		file, err = os.Open(jsonFile)
// 		if err != nil {
// 			log.Fatal(err)
// 		}
// 	}
// 	info, err := file.Stat()
// 	if err != nil {
// 		panic(err)
// 	}
// 	if info.Size() <= 0 {
// 		log.Fatal("No file found to validate.")
// 	}
// 	m.printFileInfo(info)

// 	bytes := m.getBytes(file)
// 	if err := file.Close(); err != nil {
// 		log.Fatal(err)
// 	}

// 	valid := json.Valid(bytes)
// 	return valid
// }

// ----------------------------------------------------------------------------

// used for validating a JSON file
// TODO:  this seems like a naive implementation.  What if the file is very large?
// func (m *MoveImpl) getBytes(file *os.File) []byte {

// 	reader := bufio.NewReader(file)
// 	var output []byte

// 	for {
// 		input, err := reader.ReadByte()
// 		if err != nil && err == io.EOF {
// 			break
// 		}
// 		output = append(output, input)
// 	}
// 	return output
// }

// ----------------------------------------------------------------------------

// print basic file information.
// TODO:  should this info be logged?  DELETE ME?
// func (m *MoveImpl) printFileInfo(info os.FileInfo) {
// 	fmt.Println("name: ", info.Name())
// 	fmt.Println("size: ", info.Size())
// 	fmt.Println("mode: ", info.Mode())
// 	fmt.Println("mod time: ", info.ModTime())
// 	fmt.Println("is dir: ", info.IsDir())
// 	if info.Mode()&os.ModeDevice == os.ModeDevice {
// 		fmt.Println("detected device: ", os.ModeDevice)
// 	}
// 	if info.Mode()&os.ModeCharDevice == os.ModeCharDevice {
// 		fmt.Println("detected char device: ", os.ModeCharDevice)
// 	}
// 	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
// 		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
// 	}
// }

// ----------------------------------------------------------------------------

// print out basic URL information.
// TODO:  should this info be logged?  DELETE ME?
// func (m *MoveImpl) printURL(u *url.URL) {

// 	fmt.Println("\tScheme: ", u.Scheme)
// 	fmt.Println("\tUser full: ", u.User)
// 	fmt.Println("\tUser name: ", u.User.Username())
// 	p, _ := u.User.Password()
// 	fmt.Println("\tPassword: ", p)

// 	fmt.Println("\tHost full: ", u.Host)
// 	host, port, _ := net.SplitHostPort(u.Host)
// 	fmt.Println("\tHost: ", host)
// 	fmt.Println("\tPort: ", port)

// 	fmt.Println("\tPath: ", u.Path)
// 	fmt.Println("\tFragment: ", u.Fragment)

// 	fmt.Println("\tQuery string: ", u.RawQuery)
// 	raw, _ := url.ParseQuery(u.RawQuery)
// 	fmt.Println("\tParsed query string: ", raw)
// 	for key, value := range raw {
// 		fmt.Println("Key:", key, "=>", "Value:", value[0])
// 	}

// }

// ----------------------------------------------------------------------------

func logBuildInfo() {
	buildInfo, ok := debug.ReadBuildInfo()
	fmt.Println("---------------------------------------------------------------")
	if ok {
		fmt.Println("GoVersion:", buildInfo.GoVersion)
		fmt.Println("Path:", buildInfo.Path)
		fmt.Println("Main.Path:", buildInfo.Main.Path)
		fmt.Println("Main.Version:", buildInfo.Main.Version)
	} else {
		fmt.Println("Unable to read build info.")
	}
}

// ----------------------------------------------------------------------------

func logStats() {
	cpus := runtime.NumCPU()
	goRoutines := runtime.NumGoroutine()
	cgoCalls := runtime.NumCgoCall()
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	var gcStats debug.GCStats
	debug.ReadGCStats(&gcStats)

	// fmt.Println("---------------------------------------------------------------")
	// fmt.Println("Time:", time.Now())
	// fmt.Println("CPUs:", cpus)
	// fmt.Println("Go routines:", goRoutines)
	// fmt.Println("CGO calls:", cgoCalls)
	// fmt.Println("Num GC:", memStats.NumGC)
	// fmt.Println("GCSys:", memStats.GCSys)
	// fmt.Println("GC pause total:", gcStats.PauseTotal)
	// fmt.Println("LastGC:", gcStats.LastGC)
	// fmt.Println("HeapAlloc:", memStats.HeapAlloc)
	// fmt.Println("NextGC:", memStats.NextGC)
	// fmt.Println("CPU fraction used by GC:", memStats.GCCPUFraction)

	fmt.Println("---------------------------------------------------------------")
	printCSV(">>>", "Time", "CPUs", "Go routines", "CGO calls", "Num GC", "GC pause total", "LastGC", "TotalAlloc", "HeapAlloc", "NextGC", "GCSys", "HeapSys", "StackSys", "Sys - total OS bytes", "CPU fraction used by GC")
	printCSV(">>>", time.Now(), cpus, goRoutines, cgoCalls, memStats.NumGC, gcStats.PauseTotal, gcStats.LastGC, memStats.TotalAlloc, memStats.HeapAlloc, memStats.NextGC, memStats.GCSys, memStats.HeapSys, memStats.StackSys, memStats.Sys, memStats.GCCPUFraction)
}

// ----------------------------------------------------------------------------

func printCSV(fields ...any) {
	for _, field := range fields {
		fmt.Print(field, ",")
	}
	fmt.Println("")
}
