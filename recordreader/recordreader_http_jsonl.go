package recordreader

import (
	"context"
	"net/http"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type HttpJsonlReader struct {
	InputURL       string
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	RecordMax      int
	RecordMin      int
	RecordMonitor  int
	Validate       bool
	waitGroup      sync.WaitGroup
}

func (reader *HttpJsonlReader) Read(ctx context.Context) error {

	//nolint:noctx
	response, err := http.Get(reader.InputURL) //nolint:gosec
	if err != nil {
		return wraperror.Errorf(err, "http.Get %s", reader.InputURL)
	}

	if response.StatusCode != http.StatusOK {
		return wraperror.Errorf(
			errForPackage,
			"unable to retrieve: %s, return code: %d",
			reader.InputURL,
			response.StatusCode,
		)
	}

	defer response.Body.Close()

	processJSONL(ctx,
		reader.InputURL,
		reader.RecordMin,
		reader.RecordMax,
		response.Body,
		reader.Validate,
		reader.RecordMonitor,
		reader.ObserverOrigin,
		reader.Observers,
		&reader.waitGroup,
		reader.RecordChannel)

	return wraperror.Errorf(err, wraperror.NoMessage)
}
