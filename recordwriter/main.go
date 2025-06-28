package recordwriter

import (
	"context"
	"errors"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type RecordWriter interface {
	Write(ctx context.Context) (int, error)
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// move is 6202:  https://github.com/senzing-garage/knowledge-base/blob/main/lists/senzing-component-ids.md
const ComponentID = 6202

// Log message prefix.
const Prefix = "move: "

var errForPackage = errors.New("recordwriter")
