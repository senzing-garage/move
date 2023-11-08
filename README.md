# move

## Synopsis

`move` is a command in the
[senzing-tools](https://github.com/Senzing/senzing-tools)
suite of tools.
This command moves records between files and queues.

[![Go Reference](https://pkg.go.dev/badge/github.com/senzing/move.svg)](https://pkg.go.dev/github.com/senzing/move)
[![Go Report Card](https://goreportcard.com/badge/github.com/senzing/move)](https://goreportcard.com/report/github.com/senzing/move)
[![go-test.yaml](https://github.com/Senzing/move/actions/workflows/go-test.yaml/badge.svg)](https://github.com/Senzing/move/actions/workflows/go-test.yaml)
[![License](https://img.shields.io/badge/License-Apache2-brightgreen.svg)](https://github.com/Senzing/move/blob/main/LICENSE)

## Overview

`move` moves records from a file to a queue or another file.  In other words,
it is a general tool for moving records around.  When it does this is validates
each record to ensure that it is valid JSON and contains two necessary key-value
pairs:  `RECORD_ID` and `DATA_SOURCE`.

A file is given to `move` with the command-line parameter `input-url` or
as the environment variable `SENZING_TOOLS_INPUT_URL`.  Note this is a URL so
local files will need `file://` and remote files `http://` or `https://`. If
the given file has the `.gz` extension, it will be treated as a compressed file
JSONL file.  If the file has a `.jsonl` extension it will be treated
accordingly. If the file has another extension it will be rejected, unless the
`input-file-type` or `SENZING_TOOLS_INPUT_FILE_TYPE` is set to `JSONL`.

Using the command-line parameter `output-url` or the environment variable
`SENZING_TOOLS_OUTPUT_URL` a queue can also be specified to put records into.
URLs starting with `amqp://` are interpreted as RabbitMQ queues.  URLs
starting with `sqs://` are interpreted as SQS queues.  Files can also be
specified as an output URL.

## Install

1. The `move` command is installed with the
   [senzing-tools](https://github.com/Senzing/senzing-tools)
   suite of tools.
   See senzing-tools [install](https://github.com/Senzing/senzing-tools#install).

## Use

```console
senzing-tools move [flags]
```

1. For options and flags:
    1. [Online documentation](https://hub.senzing.com/senzing-tools/senzing-tools_move.html)
    1. Runtime documentation:

        ```console
        senzing-tools move --help
        ```

1. In addition to the following simple usage examples, there are additional [Examples](docs/examples.md).

### Using command line options

1. :pencil2: Specify file URL using command line option.
   Example:

    ```console
    senzing-tools move \
        --input-url https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl
    ```

1. See [Parameters](#parameters) for additional parameters.

### Using environment variables

1. :pencil2: Specify file URL using environment variable.
   Example:

    ```console
    export SENZING_TOOLS_INPUT_URL=https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl
    senzing-tools move
    ```

1. See [Parameters](#parameters) for additional parameters.

### Using Docker

This usage shows how to move a file with a Docker container.

1. :pencil2: Run `senzing/senzing-tools`.
   Example:

    ```console
    docker run \
        --env SENZING_TOOLS_COMMAND=move \
        --env SENZING_TOOLS_INPUT_URL=https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl \
        --rm \
        senzing/senzing-tools
    ```

1. See [Parameters](#parameters) for additional parameters.

### Parameters

- **[SENZING_TOOLS_INPUT_FILE_TYPE](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_input_file_type)**
- **[SENZING_TOOLS_INPUT_URL](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_input_url)**
- **[SENZING_TOOLS_JSON_OUTPUT](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_json_output)**
- **[SENZING_TOOLS_LOG_LEVEL](https://github.com/Senzing/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_log_level)**

## References

- [Command reference](https://hub.senzing.com/senzing-tools/senzing-tools_move.html)
- [Development](docs/development.md)
- [Errors](docs/errors.md)
- [Examples](docs/examples.md)
