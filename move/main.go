package move

import (
	"context"
	"errors"
)

// ----------------------------------------------------------------------------
// Types
// ----------------------------------------------------------------------------

type Move interface {
	Move(ctx context.Context) error
	SetLogLevel(ctx context.Context, logLevelName string) error
}

// ----------------------------------------------------------------------------
// Constants
// ----------------------------------------------------------------------------

// move is 6202:  https://github.com/senzing-garage/knowledge-base/blob/main/lists/senzing-product-ids.md
const ComponentID = 6202

// Log message prefix.
const Prefix = "move: "

// ----------------------------------------------------------------------------
// Variables
// ----------------------------------------------------------------------------

// Error level ranges and usage.
var IDMessages = map[int]string{
	// Level 	Range 		Use 							Comments.
	// TRACE 	0000-0999 	Entry/Exit tracing 				May contain sensitive data.

	// DEBUG 	1000-1999 	Values seen during processing 	May contain sensitive data.

	1000: Prefix + "Drained %v",
	1001: Prefix + "InputURL: %s; OutputURL: %s; FileType: %s; RecordMin: %d; RecordMax: %d",
	1002: Prefix + "GoVersion: %s, Path: %s, Main.Path: %s, Main.Version: %s",
	1003: Prefix + "CPUs: %d, Go routines: %d, CGO calls: %d, Num GC: %d, GC pause total: %v, LastGC: %v, TotalAlloc: %d, HeapAlloc: %d, NextGC: %d, GCSys: %d, HeapSys: %d, StackSys: %d, Sys - total OS bytes: %d, CPU fraction used by GC: %f",

	// INFO 	2000-2999 	Process steps achieved.

	2000: Prefix + "So long and thanks for all the fish.",
	2001: Prefix + "Records sent to queue: %d",

	// WARN 	3000-3999 	Unexpected situations, but processing was successful.

	3001: Prefix + "Error closing file %s: %+v",
	3010: Prefix + "Error validating line %d %+v",
	3011: Prefix + "Unable to read build info.",

	// ERROR 	4000-4999 	Unexpected situations, processing was not successful.

	// FATAL 	5000-5999 	The process needs to shutdown.

	5000: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s.",
	5001: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s.",
	5002: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Bad protocol: %s. Only file, http, and https protocols supported.",
	5003: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Only .jsonl and .gz file extensions supported, unless specified by the file type override (SENZING_TOOLS_INPUT_FILE_TYPE).",
	5004: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Only .jsonl and .gz file extensions supported, unless specified by the file type override (SENZING_TOOLS_INPUT_FILE_TYPE).",
	5005: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Unable to open gzip file.",
	5006: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Unable to open jsonl file.",
	5007: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Unable to HTTP GET for gzip.",
	5008: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Status code: %d for HTTP GET of gzip file.",
	5009: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Unable to HTTP GET for jsonl.",
	5010: Prefix + "Invalid SENZING_TOOLS_INPUT_URL: %s. Status code: %d for HTTP GET of jsonl file.",
	5030: Prefix + "Invalid SENZING_TOOLS_OUTPUT_URL: %s.",
	5031: Prefix + "Invalid SENZING_TOOLS_OUTPUT_URL: %s.",
	5032: Prefix + "Invalid SENZING_TOOLS_OUTPUT_URL: %s. Unable to create file.",
	5033: Prefix + "Invalid SENZING_TOOLS_OUTPUT_URL. %s. File already exists.",
	5040: Prefix + "SENZING_TOOLS_RECORD_MIN (%d) was larger than SENZING_TOOLS_RECORD_MAX (%d).",
	5050: Prefix + "Unable to read from STDIN.",
	5051: Prefix + "Unable to write to STDOUT.",
	5060: Prefix + "Invalid SENZING_TOOLS_LOG_LEVEL: %s. Valid values: TRACE, DEBUG, INFO, WARN, ERROR, FATAL, and PANIC.",

	// PANIC 	6000-6999 	The underlying system is at issue.
	//			8000-8999 	Reserved for observer messages.
}

// Status strings for specific messages.
var IDStatuses = map[int]string{}

var errForPackage = errors.New("move")
