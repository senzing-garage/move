package recordreader

import (
	"context"
	"net/http"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type HTTPJsonlReader struct {
	InputURL       string
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	RecordMax      int
	RecordMin      int
	RecordMonitor  int
	Validate       bool
	WaitGroup      *sync.WaitGroup
}

func (reader *HTTPJsonlReader) Read(ctx context.Context) (int, error) {
	var (
		err       error
		linesRead int
	)

	//nolint:noctx
	response, err := http.Get(reader.InputURL)
	if err != nil {
		return linesRead, wraperror.Errorf(err, "http.Get %s", reader.InputURL)
	}

	if response.StatusCode != http.StatusOK {
		return linesRead, wraperror.Errorf(
			errForPackage,
			"unable to retrieve: %s, return code: %d",
			reader.InputURL,
			response.StatusCode,
		)
	}

	defer response.Body.Close()

	linesRead, err = processJSONL(ctx,
		reader.InputURL,
		reader.RecordMin,
		reader.RecordMax,
		response.Body,
		reader.Validate,
		reader.RecordMonitor,
		reader.ObserverOrigin,
		reader.Observers,
		reader.WaitGroup,
		reader.RecordChannel)

	return linesRead, wraperror.Errorf(err, wraperror.NoMessage)
}
