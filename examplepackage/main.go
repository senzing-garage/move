package examplepackage

import (
	"context"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

// The ExamplePackage interface is an example interface.
type ExamplePackage interface {
	SaySomething(ctx context.Context) error
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// An example constant.
const ExampleConstant = 1

// ----------------------------------------------------------------------------
// Variables
// ----------------------------------------------------------------------------

// An example variable.
var ExampleVariable = map[int]string{
	1: "Just a string",
}
