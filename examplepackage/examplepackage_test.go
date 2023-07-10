package examplepackage

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

// ----------------------------------------------------------------------------
// Test harness
// ----------------------------------------------------------------------------

func TestMain(m *testing.M) {
	err := setup()
	if err != nil {
		fmt.Print(err)
		os.Exit(1)
	}
	code := m.Run()
	err = teardown()
	if err != nil {
		fmt.Print(err)
	}
	os.Exit(code)
}

func setup() error {
	var err error = nil
	return err
}

func teardown() error {
	var err error = nil
	return err
}

// ----------------------------------------------------------------------------
// Test interface functions
// ----------------------------------------------------------------------------

func TestExamplePackageImpl_SaySomething(test *testing.T) {
	ctx := context.TODO()
	testObject := &ExamplePackageImpl{
		Something: "I'm here",
	}
	err := testObject.SaySomething(ctx)
	assert.Nil(test, err)
}

// ----------------------------------------------------------------------------
// Examples for godoc documentation
// ----------------------------------------------------------------------------

func ExampleExamplePackageImpl_SaySomething() {
	// For more information, visit https://github.com/Senzing/move/blob/main/examplepackage/examplepackage_test.go
	ctx := context.TODO()
	examplePackage := &ExamplePackageImpl{
		Something: "I'm here",
	}
	examplePackage.SaySomething(ctx)
	//Output:
	//examplePackage: I'm here
}
