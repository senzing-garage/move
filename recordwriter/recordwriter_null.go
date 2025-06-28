package recordwriter

import (
	"context"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type NullWriter struct {
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	WaitGroup      *sync.WaitGroup
}

func (writer *NullWriter) Write(ctx context.Context) (int, error) {
	var (
		err          error
		linesWritten int
	)

	for record := range writer.RecordChannel {
		linesWritten++
		recordDefinition := record.GetMessage()
		notifyWrite(ctx, writer.ObserverOrigin, writer.Observers, writer.WaitGroup, recordDefinition)
	}

	return linesWritten, wraperror.Errorf(err, wraperror.NoMessage)
}
