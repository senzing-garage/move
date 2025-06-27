package recordreader

import (
	"context"
	"errors"

	"github.com/senzing-garage/observe/observer"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type RecordReader interface {
	Read(ctx context.Context) error
	SetLogLevel(ctx context.Context, logLevelName string) error
	RegisterObserver(ctx context.Context, observer observer.Observer) error
	UnregisterObserver(ctx context.Context, observer observer.Observer) error
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// move is 6202:  https://github.com/senzing-garage/knowledge-base/blob/main/lists/senzing-component-ids.md
const ComponentID = 6202

// Log message prefix.
const Prefix = "move: "

var errForPackage = errors.New("recordreader")
