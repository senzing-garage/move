package recordreader

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"

	"github.com/senzing-garage/go-helpers/record"
	"github.com/senzing-garage/go-observing/notifier"
	"github.com/senzing-garage/go-observing/subject"
)

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
