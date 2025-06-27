package recordreader

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"strconv"
	"strings"
	"sync"

	"github.com/senzing-garage/go-common/record"
	"github.com/senzing-garage/go-observing/notifier"
	"github.com/senzing-garage/go-observing/subject"
	"github.com/senzing-garage/go-queueing/queues"
	"github.com/senzing-garage/move/move"
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
				recordChannel <- &move.SzRecord{recordDefinition, lineNumber, inputName}
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
	return lineNumber, err
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

func notifyRead(
	ctx context.Context,
	observerOrigin string,
	observers subject.Subject,
	waitGroup *sync.WaitGroup,
	recordDefinition string,
) {
	if observers != nil {
		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			var (
				aRecord        record.Record
				dataSourceCode string
				recordID       string
			)

			valid := json.Unmarshal([]byte(recordDefinition), &aRecord) == nil
			if valid {
				dataSourceCode = aRecord.DataSource
				recordID = aRecord.ID
			}

			details := map[string]string{
				"dataSourceCode": dataSourceCode,
				"recordId":       recordID,
			}
			notifier.Notify(ctx, observers, observerOrigin, ComponentID, 8001, nil, details)
		}()
	}
}

func notifyRecordDefinitionInvalid(
	ctx context.Context,
	observerOrigin string,
	observers subject.Subject,
	waitGroup *sync.WaitGroup,
	lineNumber int,
	recordDefinition string,
) {
	if observers != nil {
		var (
			aRecord        record.Record
			dataSourceCode string
			err            error
			recordID       string
		)

		validUnmarshal := json.Unmarshal([]byte(recordDefinition), &aRecord) == nil
		if validUnmarshal {
			dataSourceCode = aRecord.DataSource
			recordID = aRecord.ID
		}

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			details := map[string]string{
				"dataSourceCode": dataSourceCode,
				"lineNumber":     strconv.Itoa(lineNumber),
				"recordId":       recordID,
			}
			notifier.Notify(ctx, observers, observerOrigin, ComponentID, 8003, err, details)
		}()
	}
}

func notifyRecordMonitor(
	ctx context.Context,
	observerOrigin string,
	observers subject.Subject,
	waitGroup *sync.WaitGroup,
	lineNumber int,
) {
	if observers != nil {
		var err error

		waitGroup.Add(1)

		go func() {
			defer waitGroup.Done()

			details := map[string]string{
				"lineNumber": strconv.Itoa(lineNumber),
			}
			notifier.Notify(ctx, observers, observerOrigin, ComponentID, 8006, err, details)
		}()
	}
}
