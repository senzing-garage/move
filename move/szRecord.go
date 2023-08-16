package move

import (
	"fmt"

	"github.com/senzing/go-queueing/queues"
)

// ----------------------------------------------------------------------------
// record implementation: provides a raw data record implementation
// ----------------------------------------------------------------------------

// Check at compile time that the implementation adheres to the interface.
var _ queues.Record = (*szRecord)(nil)

type szRecord struct {
	body   string
	id     int
	source string
}

func (r *szRecord) GetMessage() string {
	return r.body
}

func (r *szRecord) GetMessageID() string {
	//TODO: meaningful or random MessageId?
	return fmt.Sprintf("%s-%d", r.source, r.id)
}
