package recordreader

import (
	"bufio"
	"context"
	"io"
	"strings"
	"sync"

	"github.com/senzing-garage/go-helpers/record"
	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
	"github.com/senzing-garage/move/szrecord"
)

func processJSONL(
	ctx context.Context,
	inputName string,
	minLine int,
	maxLine int,
	reader io.Reader,
	validate bool,
	recordMonitor int,
	observerOrigin string,
	observers subject.Subject,
	waitGroup *sync.WaitGroup,
	recordChannel chan queues.Record,
) (int, error) {
	var (
		lineNumber int
		err        error
	)

	_ = ctx
	scanner := bufio.NewScanner(reader)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		lineNumber++
		if lineNumber < minLine {
			continue
		}

		recordDefinition := strings.TrimSpace(scanner.Text())

		if len(recordDefinition) > 0 { // ignore blank lines
			valid := true
			if validate {
				valid = isRecordDefinitionValid(ctx, observerOrigin, observers, waitGroup, recordDefinition, lineNumber)
			}

			if valid {
				notifyRead(ctx, observerOrigin, observers, waitGroup, recordDefinition)
				recordChannel <- &szrecord.SzRecord{
					Body:   recordDefinition,
					ID:     lineNumber,
					Source: inputName,
				}
			}
		}

		if (recordMonitor > 0) && (lineNumber%recordMonitor == 0) {
			notifyRecordMonitor(ctx, observerOrigin, observers, waitGroup, lineNumber)
		}

		if maxLine > 0 && (lineNumber >= maxLine) {
			break
		}
	}

	close(recordChannel)

	return lineNumber, wraperror.Errorf(err, wraperror.NoMessage)
}

func isRecordDefinitionValid(
	ctx context.Context,
	observerOrigin string,
	observers subject.Subject,
	waitGroup *sync.WaitGroup,
	recordDefinition string,
	lineNumber int,
) bool {
	result, err := record.Validate(recordDefinition)
	if err != nil || !result {
		result = false

		notifyRecordDefinitionInvalid(
			ctx,
			observerOrigin,
			observers,
			waitGroup,
			lineNumber,
			recordDefinition)
	}

	return result
}
