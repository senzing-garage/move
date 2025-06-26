package cmdobserver

import (
	"context"
	"encoding/json"
	"sync"
	"time"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

// CmdObserver is a simple example of the Subject interface.
// It is mainly used for testing.
type CmdObserver struct {
	ID              string
	IsSilent        bool
	dataSourceCodes map[string]int64
	totalRead       int64
	mutex62028001   sync.Mutex
	mutex62028002   sync.Mutex
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
	return observer.dataSourceCodes
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
	}
}

func (observer *CmdObserver) handleObserverMessage62028001(ctx context.Context, message string) {
	_ = ctx
	_ = message

	observer.mutex62028001.Lock()
	observer.totalRead++
	observer.mutex62028001.Unlock()
}

func (observer *CmdObserver) handleObserverMessage62028002(ctx context.Context, message string) {
	var parsedMessage ObserverMessage62028002

	_ = ctx

	valid := json.Unmarshal([]byte(message), &parsedMessage) == nil
	if !valid {
		panic("Invalid observer message: " + message)
	}

	observer.mutex62028002.Lock()

	// Update last time.

	messageTime, err := time.Parse(time.RFC3339, parsedMessage.MessageTime)
	if err == nil {
		if messageTime.After(observer.lastUpdateTime) {
			observer.lastUpdateTime = messageTime
		}
	}

	// Update data source counter.

	if observer.dataSourceCodes == nil {
		observer.dataSourceCodes = map[string]int64{}
	}

	value, ok := observer.dataSourceCodes[parsedMessage.DataSourceCode]
	if !ok {
		observer.dataSourceCodes[parsedMessage.DataSourceCode] = 1
	} else {
		observer.dataSourceCodes[parsedMessage.DataSourceCode] = value + 1
	}
	//	if !observer.IsSilent {
	//		observer.mutexPrint.Lock()
	//		fmt.Printf("\n>>>>>> %s\n", message)                          //nolint
	//		fmt.Printf(">>>>>>     total read: %d\n", observer.totalRead) //nolint
	//		fmt.Printf(">>>>>>     DS: %+v\n", observer.dataSourceCodes)  //nolint
	//		observer.mutexPrint.Unlock()
	//	}
	observer.mutex62028002.Unlock()
}
