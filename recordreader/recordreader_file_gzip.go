package recordreader

import (
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type FileGzipReader struct {
	FilePath       string
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	RecordMax      int
	RecordMin      int
	RecordMonitor  int
	Validate       bool
	WaitGroup      *sync.WaitGroup
}

func (reader *FileGzipReader) Read(ctx context.Context) (int, error) {
	var (
		err       error
		linesRead int
	)

	cleanFilePath := filepath.Clean(reader.FilePath)

	file, err := os.Open(cleanFilePath)
	if err != nil {
		return linesRead, wraperror.Errorf(err, "os.Open: %s", cleanFilePath)
	}

	defer file.Close()

	gzipFile, err := gzip.NewReader(file)
	if err != nil {
		return linesRead, wraperror.Errorf(err, "gzip.NewReader: %s", cleanFilePath)
	}
	defer gzipFile.Close()

	linesRead, err = processJSONL(ctx,
		reader.FilePath,
		reader.RecordMin,
		reader.RecordMax,
		gzipFile,
		reader.Validate,
		reader.RecordMonitor,
		reader.ObserverOrigin,
		reader.Observers,
		reader.WaitGroup,
		reader.RecordChannel)

	return linesRead, wraperror.Errorf(err, wraperror.NoMessage)
}
