package recordwriter

import (
	"bufio"
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type FileJSONWriter struct {
	FilePath       string
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	WaitGroup      *sync.WaitGroup
}

func (writer *FileJSONWriter) Write(ctx context.Context) (int, error) {
	var (
		err          error
		linesWritten int
	)

	fileName := filepath.Clean(writer.FilePath)

	_, err = os.Stat(writer.FilePath)
	if err == nil { // file exists
		return linesWritten, wraperror.Errorf(errForPackage, "output file %s already exists", fileName)
	}

	file, err := os.Create(fileName)
	if err != nil {
		return linesWritten, wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	defer file.Close()

	_, err = file.Stat()
	if err != nil {
		return linesWritten, wraperror.Errorf(err, "fatal error opening %s", fileName)
	}

	fileWriter := bufio.NewWriter(file)

	for record := range writer.RecordChannel {
		linesWritten++
		recordDefinition := record.GetMessage()
		notifyWrite(ctx, writer.ObserverOrigin, writer.Observers, writer.WaitGroup, recordDefinition)

		_, err := fileWriter.WriteString(recordDefinition + "\n")
		if err != nil {
			return linesWritten, wraperror.Errorf(err, "error writing to stdout")
		}
	}

	err = fileWriter.Flush()
	if err != nil {
		return linesWritten, wraperror.Errorf(err, "error flushing %s", fileName)
	}

	return linesWritten, wraperror.Errorf(err, wraperror.NoMessage)
}
