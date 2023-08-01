package move

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
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
	"github.com/senzing/go-queuing/queues"
	"github.com/senzing/go-queuing/queues/rabbitmq"
	"github.com/senzing/go-queuing/queues/sqs"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type MoveError struct {
	error
}

type MoveImpl struct {
	FileType                  string
	InputUrl                  string
	LogLevel                  string
	MonitoringPeriodInSeconds int
	OutputUrl                 string
	RecordMax                 int
	RecordMin                 int
	RecordMonitor             int
}

// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ Move = (*MoveImpl)(nil)

var (
	waitGroup sync.WaitGroup
)

// ----------------------------------------------------------------------------
// -- Public methods
// ----------------------------------------------------------------------------

// move records from one place to another.  validates each record as they are
// read and only moves valid records.  typically used to move records from
// a file to a queue for processing.
func (m *MoveImpl) Move(ctx context.Context) {

	logBuildInfo()
	logStats()

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

	waitGroup.Add(2)
	recordchan := make(chan queues.Record, 10)
	go m.read(ctx, recordchan)
	go m.write(ctx, recordchan)
	waitGroup.Wait()
}

// ----------------------------------------------------------------------------
// -- Private methods
// ----------------------------------------------------------------------------

// ----------------------------------------------------------------------------
// -- Write implementation: writes records in the record channel to the output
// ----------------------------------------------------------------------------

// this function implements writing to RabbitMQ
func (m *MoveImpl) write(ctx context.Context, recordchan chan queues.Record) {

	defer waitGroup.Done()

	outputURL := m.OutputUrl
	outputURLLen := len(outputURL)

	if outputURLLen == 0 {
		//assume stdout
		m.writeStdout(recordchan)
		return
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(outputURL) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", outputURL)
		return
	}

	fmt.Println("outputURL: ", outputURL)
	u, err := url.Parse(outputURL)
	if err != nil {
		panic(err)
	}
	m.printURL(u)
	switch u.Scheme {
	case "amqp":
		rabbitmq.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	case "file":
		success := true
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			success = m.writeJSONLFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ file.")
			success = m.writeGZFile(u.Path, recordchan)
		} else {
			valid := m.validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			fmt.Println("Only able to process JSON-Lines files at this time.")
			success = false
		}
		if !success {
			panic("Unable to continue.")
		}
	case "sqs":
		//allows for using a dummy URL with just a queue-name
		// eg  sqs://lookup?queue-name=myqueue
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	case "https":
		//uses actual AWS SQS URL  TODO: detect sqs/amazonaws url?
		sqs.StartManagedProducer(ctx, outputURL, runtime.GOMAXPROCS(0), recordchan)
	default:
		fmt.Println("Unknown URL Scheme.  Unable to write to:", outputURL)
	}
	fmt.Println("So long and thanks for all the fish.")
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeStdout(recordchan chan queues.Record) bool {
	_, err := os.Stdout.Stat()
	if err != nil {
		fmt.Println("Fatal error opening stdout.", err)
		return false
	}
	// printFileInfo(info)

	writer := bufio.NewWriter(os.Stdout)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			fmt.Println("Error writing to stdout")
			return false
		}
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing stdout", err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeJSONLFile(fileName string, recordchan chan queues.Record) bool {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		fmt.Println("Error output file", fileName, "exists.")
		return false
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer f.Close()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	info, err := f.Stat()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	m.printFileInfo(info)

	writer := bufio.NewWriter(f)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			fmt.Println("Error writing to stdout")
			return false
		}
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing", fileName, err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------

func (m *MoveImpl) writeGZFile(fileName string, recordchan chan queues.Record) bool {
	_, err := os.Stat(fileName)
	if err == nil { //file exists
		fmt.Println("Error output file", fileName, "exists.")
		return false
	}
	fileName = filepath.Clean(fileName)
	f, err := os.Create(fileName)
	defer f.Close()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	info, err := f.Stat()
	if err != nil {
		fmt.Println("Fatal error opening", fileName, err)
		return false
	}
	m.printFileInfo(info)
	gzfile := gzip.NewWriter(f)
	defer gzfile.Close()
	writer := bufio.NewWriter(gzfile)
	for record := range recordchan {
		_, err := writer.WriteString(record.GetMessage() + "\n")
		if err != nil {
			fmt.Println("Error writing to stdout")
			return false
		}
	}
	err = writer.Flush()
	if err != nil {
		fmt.Println("Error flushing", fileName, err)
		return false
	}
	return true
}

// ----------------------------------------------------------------------------
// -- Read implementation: reads records from the input to the record channel
// ----------------------------------------------------------------------------

// this function attempts to determine the source of records.
// it then parses the source and puts the records into the record channel.
func (m *MoveImpl) read(ctx context.Context, recordchan chan queues.Record) error {

	defer waitGroup.Done()

	inputUrl := m.InputUrl
	inputUrlLen := len(inputUrl)

	if inputUrlLen == 0 {
		//assume stdin
		success := m.readStdin(recordchan)
		if !success {
			return errors.New("Unable to read stdin")
		}
		return nil
	}

	//This assumes the URL includes a schema and path so, minimally:
	//  "s://p" where the schema is 's' and 'p' is the complete path
	if len(inputUrl) < 5 {
		fmt.Printf("ERROR: check the inputURL parameter: %s\n", inputUrl)
		return errors.New(fmt.Sprintf("ERROR: check the inputURL parameter: %s\n", inputUrl))
	}

	fmt.Println("inputURL: ", inputUrl)
	u, err := url.Parse(inputUrl)
	if err != nil {
		panic(err)
	}
	//printURL(u)
	if u.Scheme == "file" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL file.")
			return m.readJsonlFile(u.Path, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ file.")
			return m.readGzipFile(u.Path, recordchan)
		} else {
			valid := m.validate(u.Path)
			fmt.Println("Is valid JSON?", valid)
			//TODO: process JSON file?
			close(recordchan)
			return errors.New("Unable to process file")
		}
	} else if u.Scheme == "http" || u.Scheme == "https" {
		if strings.HasSuffix(u.Path, "jsonl") || strings.ToUpper(m.FileType) == "JSONL" {
			fmt.Println("Reading as a JSONL resource.")
			return m.readJsonlResource(inputUrl, recordchan)
		} else if strings.HasSuffix(u.Path, "gz") || strings.ToUpper(m.FileType) == "GZ" {
			fmt.Println("Reading as a GZ resource.")
			return m.readGzipResource(inputUrl, recordchan)
		} else {
			fmt.Println("If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--fileType).")
			return errors.New("Unable to process file")
		}
	} else {
		msg := fmt.Sprintf("We don't handle %s input URLs.", u.Scheme)
		return errors.New(msg)
	}
}

// ----------------------------------------------------------------------------

// process records in the JSONL format; reading one record per line from
// the given reader and placing the records into the record channel
func (m *MoveImpl) processJsonl(fileName string, reader io.Reader, recordchan chan queues.Record) {

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

func (m *MoveImpl) readStdin(recordchan chan queues.Record) bool {
	info, err := os.Stdin.Stat()
	if err != nil {
		fmt.Println("Fatal error opening stdin.", err)
		return false
	}
	//printFileInfo(info)

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {

		reader := bufio.NewReader(os.Stdin)
		m.processJsonl("stdin", reader, recordchan)
		return true
	}
	fmt.Println("Fatal error stdin not piped.")
	return false
}

// ----------------------------------------------------------------------------

// opens and reads a JSONL http resource
func (m *MoveImpl) readJsonlResource(jsonURL string, recordchan chan queues.Record) error {
	// #nosec G107
	response, err := http.Get(jsonURL)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	m.processJsonl(jsonURL, response.Body, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a Jsonl file
func (m *MoveImpl) readJsonlFile(jsonFile string, recordchan chan queues.Record) error {
	jsonFile = filepath.Clean(jsonFile)
	file, err := os.Open(jsonFile)
	if err != nil {
		return err
	}
	defer file.Close()

	m.processJsonl(jsonFile, file, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// opens and reads a Jsonl file that has been Gzipped
func (m *MoveImpl) readGzipFile(gzFileName string, recordchan chan queues.Record) error {
	gzFileName = filepath.Clean(gzFileName)
	gzipfile, err := os.Open(gzFileName)
	if err != nil {
		return err
	}
	defer gzipfile.Close()

	reader, err := gzip.NewReader(gzipfile)
	if err != nil {
		return err
	}
	defer reader.Close()

	m.processJsonl(gzFileName, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------
func (m *MoveImpl) readGzipResource(gzURL string, recordchan chan queues.Record) error {
	// #nosec G107
	response, err := http.Get(gzURL)
	if err != nil {
		fmt.Println("Fatal error retrieving inputURL.", err)
		return err
	}
	defer response.Body.Close()
	reader, err := gzip.NewReader(response.Body)
	if err != nil {
		fmt.Println("Fatal error reading inputURL.", err)
		return err
	}
	defer reader.Close()

	m.processJsonl(gzURL, reader, recordchan)
	return nil
}

// ----------------------------------------------------------------------------

// validates that a file is valid JSON
func (m *MoveImpl) validate(jsonFile string) bool {

	var file *os.File = os.Stdin

	if jsonFile != "" {
		var err error
		jsonFile = filepath.Clean(jsonFile)
		file, err = os.Open(jsonFile)
		if err != nil {
			log.Fatal(err)
		}
	}
	info, err := file.Stat()
	if err != nil {
		panic(err)
	}
	if info.Size() <= 0 {
		log.Fatal("No file found to validate.")
	}
	m.printFileInfo(info)

	bytes := m.getBytes(file)
	if err := file.Close(); err != nil {
		log.Fatal(err)
	}

	valid := json.Valid(bytes)
	return valid
}

// ----------------------------------------------------------------------------

// used for validating a JSON file
// TODO:  this seems like a naive implementation.  What if the file is very large?
func (m *MoveImpl) getBytes(file *os.File) []byte {

	reader := bufio.NewReader(file)
	var output []byte

	for {
		input, err := reader.ReadByte()
		if err != nil && err == io.EOF {
			break
		}
		output = append(output, input)
	}
	return output
}

// ----------------------------------------------------------------------------

// print basic file information.
// TODO:  should this info be logged?  DELETE ME?
func (m *MoveImpl) printFileInfo(info os.FileInfo) {
	fmt.Println("name: ", info.Name())
	fmt.Println("size: ", info.Size())
	fmt.Println("mode: ", info.Mode())
	fmt.Println("mod time: ", info.ModTime())
	fmt.Println("is dir: ", info.IsDir())
	if info.Mode()&os.ModeDevice == os.ModeDevice {
		fmt.Println("detected device: ", os.ModeDevice)
	}
	if info.Mode()&os.ModeCharDevice == os.ModeCharDevice {
		fmt.Println("detected char device: ", os.ModeCharDevice)
	}
	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		fmt.Println("detected named pipe: ", os.ModeNamedPipe)
	}
}

// ----------------------------------------------------------------------------

// print out basic URL information.
// TODO:  should this info be logged?  DELETE ME?
func (m *MoveImpl) printURL(u *url.URL) {

	fmt.Println("\tScheme: ", u.Scheme)
	fmt.Println("\tUser full: ", u.User)
	fmt.Println("\tUser name: ", u.User.Username())
	p, _ := u.User.Password()
	fmt.Println("\tPassword: ", p)

	fmt.Println("\tHost full: ", u.Host)
	host, port, _ := net.SplitHostPort(u.Host)
	fmt.Println("\tHost: ", host)
	fmt.Println("\tPort: ", port)

	fmt.Println("\tPath: ", u.Path)
	fmt.Println("\tFragment: ", u.Fragment)

	fmt.Println("\tQuery string: ", u.RawQuery)
	raw, _ := url.ParseQuery(u.RawQuery)
	fmt.Println("\tParsed query string: ", raw)
	for key, value := range raw {
		fmt.Println("Key:", key, "=>", "Value:", value[0])
	}

}

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

// ----------------------------------------------------------------------------
// record implementation: provides a raw data record implementation
// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ queues.Record = (*szRecord)(nil)

type szRecord struct {
	body   string
	id     int
	source string
}

func (r *szRecord) GetMessage() string {
	return r.body
}

func (r *szRecord) GetMessageId() string {
	//TODO: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", r.source, r.id)
}
