package recordwriter

import (
	"bufio"
	"context"
	"os"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type StdoutWriter struct {
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	WaitGroup      *sync.WaitGroup
}

func (writer *StdoutWriter) Write(ctx context.Context) (int, error) {
	var (
		err          error
		linesWritten int
	)

	_, err = os.Stdout.Stat()
	if err != nil {
		return linesWritten, wraperror.Errorf(err, "error opening stdout")
	}

	fileWriter := bufio.NewWriter(os.Stdout)

	for record := range writer.RecordChannel {
		recordDefinition := record.GetMessage()
		notifyWrite(ctx, writer.ObserverOrigin, writer.Observers, writer.WaitGroup, recordDefinition)

		_, err := fileWriter.WriteString(recordDefinition + "\n")
		if err != nil {
			return linesWritten, wraperror.Errorf(err, "error writing to stdout")
		}
	}

	err = fileWriter.Flush()
	if err != nil {
		return linesWritten, wraperror.Errorf(err, "error flushing stdout")
	}

	return linesWritten, wraperror.Errorf(err, wraperror.NoMessage)
}
