package cmdobserver

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

// CmdObserver is a simple example of the Subject interface.
// It is mainly used for testing.
type CmdObserver struct {
	ID                        string
	IsSilent                  bool
	dataSourceCodes           map[string]int64
	invalidRecordDefinitions  []int64
	totalRead                 int64
	mutex62028001             sync.Mutex
	mutex62028002             sync.Mutex
	mutex62028003             sync.Mutex
	mutexUpdateLastUpdateTime sync.Mutex
	// mutexPrint      sync.Mutex
	lastUpdateTime time.Time
}

// ----------------------------------------------------------------------------
// Observer interface methods
// ----------------------------------------------------------------------------

/*
The GetObserverID method returns the unique identifier of the observer.
Use by the subject to manage the list of Observers.

Input
  - ctx: A context to control lifecycle.
*/
func (observer *CmdObserver) GetObserverID(ctx context.Context) string {
	_ = ctx

	return observer.ID
}

/*
The UpdateObserver method processes the message sent by the Subject.
The subject invokes UpdateObserver as a goroutine.

Input
  - ctx: A context to control lifecycle.
  - message: The string to propagate to all registered Observers.
*/
func (observer *CmdObserver) UpdateObserver(ctx context.Context, message string) {
	_ = ctx

	var observerMessage ObserverMessage

	valid := json.Unmarshal([]byte(message), &observerMessage) == nil
	if !valid {
		panic("Invalid observer message: " + message)
	}

	observer.handleObserverMessage(ctx, observerMessage.SubjectID+observerMessage.MessageID, message)
}

// ----------------------------------------------------------------------------
// Interface methods
// ----------------------------------------------------------------------------

func (observer *CmdObserver) GetDataSourceCodes() map[string]int64 {
	if observer.dataSourceCodes == nil {
		observer.dataSourceCodes = map[string]int64{}
	}

	return observer.dataSourceCodes
}

func (observer *CmdObserver) GetInvalidRecordDefinitions() []int64 {
	if observer.invalidRecordDefinitions == nil {
		observer.invalidRecordDefinitions = []int64{}
	}

	return observer.invalidRecordDefinitions
}

func (observer *CmdObserver) GetLastUpdateTime() time.Time {
	return observer.lastUpdateTime
}

func (observer *CmdObserver) GetTotalRead() int64 {
	return observer.totalRead
}

// ----------------------------------------------------------------------------
// Private methods
// ----------------------------------------------------------------------------

func (observer *CmdObserver) handleObserverMessage(
	ctx context.Context,
	uniqueMessageID string,
	message string,
) {
	_ = ctx

	switch uniqueMessageID {
	case "62028001":
		observer.handleObserverMessage62028001(ctx, message)
	case "62028002":
		observer.handleObserverMessage62028002(ctx, message)
	case "62028003":
		observer.handleObserverMessage62028003(ctx, message)
	}
}

func (observer *CmdObserver) handleObserverMessage62028001(ctx context.Context, message string) {
	_ = ctx
	_ = message

	observer.mutex62028001.Lock()
	defer observer.mutex62028001.Unlock()

	observer.totalRead++
}

func (observer *CmdObserver) handleObserverMessage62028002(ctx context.Context, message string) {
	var parsedMessage ObserverMessage62028002

	_ = ctx

	valid := json.Unmarshal([]byte(message), &parsedMessage) == nil
	if !valid {
		panic("move.cmdobserver.62028002: Invalid observer message: " + message)
	}

	observer.updateLastUpdateTime(ctx, parsedMessage.MessageTime)

	observer.mutex62028002.Lock()
	defer observer.mutex62028002.Unlock()

	// If needed, initialize dataSourcesCodes.

	if observer.dataSourceCodes == nil {
		observer.dataSourceCodes = map[string]int64{}
	}

	// Update data source counter.

	value, ok := observer.dataSourceCodes[parsedMessage.DataSourceCode]
	if !ok {
		observer.dataSourceCodes[parsedMessage.DataSourceCode] = 1
	} else {
		observer.dataSourceCodes[parsedMessage.DataSourceCode] = value + 1
	}
}

func (observer *CmdObserver) handleObserverMessage62028003(ctx context.Context, message string) {
	var parsedMessage ObserverMessage62028003

	_ = ctx

	valid := json.Unmarshal([]byte(message), &parsedMessage) == nil
	if !valid {
		panic("move.cmdobserver.62028003: Invalid observer message: " + message)
	}

	observer.updateLastUpdateTime(ctx, parsedMessage.MessageTime)

	observer.mutex62028003.Lock()
	defer observer.mutex62028003.Unlock()

	// If needed, initialize dataSourcesCodes.

	if observer.invalidRecordDefinitions == nil {
		observer.invalidRecordDefinitions = []int64{}
	}

	// Update data source counter.

	lineNumberInt64, err := strconv.ParseInt(parsedMessage.LineNumber, 10, 64)
	if err != nil {
		panic("move.cmdobserver.62028003: Cannot convert the following to an integer: " + parsedMessage.LineNumber)
	}

	observer.invalidRecordDefinitions = append(observer.invalidRecordDefinitions, lineNumberInt64)
}

func (observer *CmdObserver) updateLastUpdateTime(ctx context.Context, timeString string) {
	_ = ctx

	messageTime, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return
	}

	observer.mutexUpdateLastUpdateTime.Lock()
	defer observer.mutexUpdateLastUpdateTime.Unlock()

	if messageTime.After(observer.lastUpdateTime) {
		observer.lastUpdateTime = messageTime
	}
}
