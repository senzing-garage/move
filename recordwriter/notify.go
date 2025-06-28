package recordwriter

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/senzing-garage/go-helpers/record"
	"github.com/senzing-garage/go-observing/notifier"
	"github.com/senzing-garage/go-observing/subject"
)

func notifyWrite(
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
			notifier.Notify(ctx, observers, observerOrigin, ComponentID, 8002, nil, details)
		}()
	}
}
