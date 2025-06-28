package recordreader

import (
	"compress/gzip"
	"context"
	"net/http"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type HTTPGzipReader struct {
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

func (reader *HTTPGzipReader) Read(ctx context.Context) (int, error) {
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

	gzipReader, err := gzip.NewReader(response.Body)
	if err != nil {
		return linesRead, wraperror.Errorf(err, "gzip.NewReader")
	}

	defer gzipReader.Close()

	linesRead, err = processJSONL(ctx,
		reader.InputURL,
		reader.RecordMin,
		reader.RecordMax,
		gzipReader,
		reader.Validate,
		reader.RecordMonitor,
		reader.ObserverOrigin,
		reader.Observers,
		reader.WaitGroup,
		reader.RecordChannel)

	return linesRead, wraperror.Errorf(err, wraperror.NoMessage)
}
