package move

import "context"

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Move interface {
	Move(context.Context) error
	SetLogLevel(ctx context.Context, logLevelName string) error
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// move is 6202:  https://github.com/Senzing/knowledge-base/blob/main/lists/senzing-product-ids.md
const ComponentID = 6202

// Log message prefix.
const Prefix = "move: "

// ----------------------------------------------------------------------------
// Variables
// ----------------------------------------------------------------------------

// Message templates for g2config implementations.
var IDMessages = map[int]string{
	3001: Prefix + "Error closing file %s: %+v",
	3010: Prefix + "Error validating line %d %+v",
	3011: Prefix + "Unable to read build info.",
	5000: Prefix + "Fatal error, Check the input-url parameter: %s",
	5011: Prefix + "If this is a valid JSONL file, please rename with the .jsonl extension or use the file type override (--file-type).",
	5012: Prefix + "If this is a valid JSONL resource, please rename with the .jsonl extension or use the file type override (--file-type).",
	9000: Prefix + "So long and thanks for all the fish.",
	9001: Prefix + "Records sent to queue: %d",
	9002: Prefix + "GoVersion: %s, Path: %s, Main.Path: %s, Main.Version: %s",
	9003: Prefix + "CPUs: %d, Go routines: %d, CGO calls: %d, Num GC: %d, GC pause total: %v, LastGC: %v, TotalAlloc: %d, HeapAlloc: %d, NextGC: %d, GCSys: %d, HeapSys: %d, StackSys: %d, Sys - total OS bytes: %d, CPU fraction used by GC: %f",
}

// Error level ranges and usage:
// Level 	Range 		Use 							Comments
// TRACE 	0000-0999 	Entry/Exit tracing 				May contain sensitive data.
// DEBUG 	1000-1999 	Values seen during processing 	May contain sensitive data.
// INFO 	2000-2999 	Process steps achieved
// WARN 	3000-3999 	Unexpected situations, but processing was successful
// ERROR 	4000-4999 	Unexpected situations, processing was not successful
// FATAL 	5000-5999 	The process needs to shutdown
// PANIC 	6000-6999 	The underlying system is at issue
// 			8000-8999 	Reserved for observer messages

// Status strings for specific messages.
var IDStatuses = map[int]string{}
