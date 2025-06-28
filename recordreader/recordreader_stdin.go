package recordreader

import (
	"bufio"
	"context"
	"os"
	"sync"

	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
)

type StdinJsonlReader struct {
	ObserverOrigin string
	Observers      subject.Subject
	RecordChannel  chan queues.Record
	RecordMax      int
	RecordMin      int
	RecordMonitor  int
	Validate       bool
	WaitGroup      *sync.WaitGroup
}

func (reader *StdinJsonlReader) Read(ctx context.Context) (int, error) {
	var (
		err       error
		linesRead int
	)

	info, err := os.Stdin.Stat()
	if err != nil {
		return linesRead, wraperror.Errorf(err, "error reading stdin")
	}

	if info.Mode()&os.ModeNamedPipe == os.ModeNamedPipe {
		stdinReader := bufio.NewReader(os.Stdin)
		linesRead, err = processJSONL(ctx,
			"stdin",
			reader.RecordMin,
			reader.RecordMax,
			stdinReader,
			reader.Validate,
			reader.RecordMonitor,
			reader.ObserverOrigin,
			reader.Observers,
			reader.WaitGroup,
			reader.RecordChannel,
		)
	}

	return linesRead, wraperror.Errorf(err, wraperror.NoMessage)
}
