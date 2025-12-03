# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

`move` is a Go CLI tool that moves Senzing records between files and queues. It's part of the senzing-tools suite. The tool validates each record as JSON with required `RECORD_ID` and `DATA_SOURCE` fields before moving.

**Supported inputs:** Local files (`file://`), HTTP/HTTPS URLs, stdin
**Supported outputs:** RabbitMQ (`amqp://`), AWS SQS (`sqs://` or `https://`), local files, stdout
**File formats:** JSONL (`.jsonl`), GZIP-compressed JSONL (`.gz`)

## Build & Development Commands

```bash
# Build
make clean build            # Build binary for current platform
make build-all              # Build for all platforms (darwin/linux/windows, amd64/arm64)

# Test
make clean setup test       # Run tests with gotestfmt output
go test -v ./...            # Run tests directly

# Run single test
go test -v -run TestName ./path/to/package

# Lint
make lint                   # Run golangci-lint, govulncheck, cspell

# Coverage
make clean setup coverage   # Generate coverage report (opens in browser)
make check-coverage         # Run coverage with threshold check

# Run locally
make run                    # Run via go run
./target/linux-amd64/move   # Run built binary

# Dependencies
make dependencies-for-development   # Install dev tools (golangci-lint, gotestfmt, etc.)
make dependencies                   # Update Go module dependencies
```

## Architecture

### Package Structure

- `main.go` - Entry point, calls `cmd.Execute()`
- `cmd/` - CLI layer using Cobra/Viper
  - `root.go` - Command definition, flag bindings, creates `BasicMove` and calls `Move()`
  - `context_*.go` - OS-specific context variables
- `move/` - Core business logic
  - `main.go` - `Move` interface definition, component ID (6202), log message catalog
  - `move.go` - `BasicMove` implementation with read/write goroutines
  - `szRecord.go` - `SzRecord` implements `queues.Record` interface

### Data Flow

1. `BasicMove.Move()` spawns two goroutines connected by a buffered channel (capacity 10)
2. Read goroutine: Parses input (file/URL/stdin), validates JSON records, sends to channel
3. Write goroutine: Consumes from channel, writes to output (queue/file/stdout)
4. Record validation uses `github.com/senzing-garage/go-helpers/record.Validate()`

### Key Dependencies

- `go-cmdhelping` - CLI flag/option handling
- `go-queueing` - RabbitMQ and SQS producers (`rabbitmq.StartManagedProducer`, `sqs.StartManagedProducer`)
- `go-logging` - Structured logging with Senzing message IDs
- `cobra/viper` - CLI framework

### Configuration

All options can be set via CLI flags or `SENZING_TOOLS_*` environment variables:
- `--input-url` / `SENZING_TOOLS_INPUT_URL` - Source URL
- `--output-url` / `SENZING_TOOLS_OUTPUT_URL` - Destination URL
- `--input-file-type` / `SENZING_TOOLS_INPUT_FILE_TYPE` - Override file type detection (JSONL)
- `--record-min`, `--record-max` - Record range filtering
- `--json-output` - Enable JSON-formatted log output
