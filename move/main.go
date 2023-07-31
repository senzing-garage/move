package move

import "context"

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Mover interface {
	Move(context.Context)
}

// mover is 6602:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const MessageIdFormat = "senzing-6202%04d"
