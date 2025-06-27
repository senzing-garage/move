package recordreader

import (
	"context"
	"os"
	"path/filepath"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type FileJsonlReader struct {
	FilePath       string
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	RecordMax      int
	RecordMin      int
	RecordMonitor  int
	Validate       bool
	waitGroup      sync.WaitGroup
}

func (reader *FileJsonlReader) Read(ctx context.Context) error {
	cleanFilePath := filepath.Clean(reader.FilePath)

	file, err := os.Open(cleanFilePath)
	if err != nil {
		return wraperror.Errorf(err, "os.Open: %s", cleanFilePath)
	}

	defer file.Close()

	processJSONL(ctx,
		reader.FilePath,
		reader.RecordMin,
		reader.RecordMax,
		file,
		reader.Validate,
		reader.RecordMonitor,
		reader.ObserverOrigin,
		reader.Observers,
		&reader.waitGroup,
		reader.RecordChannel)

	return wraperror.Errorf(err, wraperror.NoMessage)
}
