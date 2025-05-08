package move

import (
	"fmt"

	"github.com/senzing-garage/go-queueing/queues"
)

// ----------------------------------------------------------------------------
// record implementation: provides a raw data record implementation
// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ queues.Record = (*SzRecord)(nil)

type SzRecord struct {
	Body   string
	ID     int
	Source string
}

func (r *SzRecord) GetMessage() string {
	return r.Body
}

func (r *SzRecord) GetMessageID() string {
	// IMPROVE: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", r.Source, r.ID)
}
