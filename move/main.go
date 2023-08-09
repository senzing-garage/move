package move

import "context"

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Move interface {
	Move(context.Context) error
}

// move is 6602:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIDFormat = "senzing-6202%04d"
