# move

If you are beginning your journey with [Senzing],
please start with [Senzing Quick Start guides].

You are in the [Senzing Garage] where projects are "tinkered" on.
Although this GitHub repository may help you understand an approach to using Senzing,
it's not considered to be "production ready" and is not considered to be part of the Senzing product.
Heck, it may not even be appropriate for your application of Senzing!

## :warning: WARNING: move is still in development :warning: _

At the moment, this is "work-in-progress" with Semantic Versions of `0.n.x`.
Although it can be reviewed and commented on,
the recommendation is not to use it yet.

## Synopsis

`move` is a command in the
[senzing-tools](https://github.com/senzing-garage/senzing-tools)
suite of tools.
This command moves records between files and queues.

[![Go Reference Badge]][Package reference]
[![Go Report Card Badge]][Go Report Card]
[![License Badge]][License]
[![go-test-linux.yaml Badge]][go-test-linux.yaml]
[![go-test-darwin.yaml Badge]][go-test-darwin.yaml]
[![go-test-windows.yaml Badge]][go-test-windows.yaml]

[![golangci-lint.yaml Badge]][golangci-lint.yaml]

## Overview

`move` moves records from a file to a queue or another file.  In other words,
it is a general tool for moving records around.  When it does this it validates
each record to ensure that it is valid JSON and contains two necessary key-value
pairs:  `RECORD_ID` and `DATA_SOURCE`.

A file is given to `move` with the command-line parameter `input-url` or
as the environment variable `SENZING_TOOLS_INPUT_URL`.  Note this is a URL so
local files will need `file://` and remote files `http://` or `https://`. If
the given file has the `.gz` extension, it will be treated as a compressed file
JSONL file.  If the file has a `.jsonl` extension it will be treated
accordingly. If the file has another extension it will be rejected, unless the
`input-file-type` or `SENZING_TOOLS_INPUT_FILE_TYPE` is set to `JSONL`.  For example,
if you have a JSONL formatted file with the URL `file:///tmp/data.json`, it will
be rejected unless `--input-file-type=JSONL` parameter or the equivalent environment
variable is set.

Using the command-line parameter `output-url` or the environment variable
`SENZING_TOOLS_OUTPUT_URL` a queue can also be specified to put records into.
URLs starting with `amqp://` are interpreted as RabbitMQ queues.  URLs
starting with `sqs://` are interpreted as SQS queue look-ups.  URLs starting with
`https://` are interpreted as SQS queue URLs.  See [Parameters](#parameters) for
additional information on SQS.  Files can also be specified as an output URL.

## Install

1. The `move` command is installed with the
   [senzing-tools](https://github.com/senzing-garage/senzing-tools)
   suite of tools.
   See senzing-tools [install](https://github.com/senzing-garage/senzing-tools#install).

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
        --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" \
        --output-url "amqp://guest:guest@192.168.6.128:5672"
    ```

1. See [Parameters](#parameters) for additional parameters.

### Using environment variables

1. :pencil2: Specify file URL using environment variable.
   Example:

    ```console
    export SENZING_TOOLS_INPUT_URL=https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl
    export SENZING_TOOLS_OUTPUT_URL=amqp://guest:guest@192.168.6.128:5672
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

- **[SENZING_TOOLS_INPUT_FILE_TYPE](https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_input_file_type)**
- **[SENZING_TOOLS_INPUT_URL](https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_input_url)**
- **[SENZING_TOOLS_JSON_OUTPUT](https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_json_output)**
- **[SENZING_TOOLS_LOG_LEVEL](https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_log_level)**
- **[SENZING_TOOLS_OUTPUT_URL](https://github.com/senzing-garage/knowledge-base/blob/main/lists/environment-variables.md#senzing_tools_output_url)**
- Notes about the `output-url`:
  - At present SQS URLs can be formed in two ways
        1. If your environment is set up such that SQS queue names can be queried, then
        an `sqs://` URL can be used when formed as such `sqs://lookup?queue-name=myqueue`
        1. Alternatively, use the direct HTTPS URL of the SQS queue, which is found
        in the AWS SQS console.

## References

1. [API documentation]
1. [Development]
1. [Errors]
1. [Examples]
1. [Package reference]

[API documentation]: https://pkg.go.dev/github.com/senzing-garage/move
[Development]: docs/development.md
[Errors]: docs/errors.md
[Examples]: docs/examples.md
[Go Reference Badge]: https://pkg.go.dev/badge/github.com/senzing-garage/move.svg
[Go Report Card Badge]: https://goreportcard.com/badge/github.com/senzing-garage/move
[Go Report Card]: https://goreportcard.com/report/github.com/senzing-garage/move
[go-test-darwin.yaml Badge]: https://github.com/senzing-garage/move/actions/workflows/go-test-darwin.yaml/badge.svg
[go-test-darwin.yaml]: https://github.com/senzing-garage/move/actions/workflows/go-test-darwin.yaml
[go-test-linux.yaml Badge]: https://github.com/senzing-garage/move/actions/workflows/go-test-linux.yaml/badge.svg
[go-test-linux.yaml]: https://github.com/senzing-garage/move/actions/workflows/go-test-linux.yaml
[go-test-windows.yaml Badge]: https://github.com/senzing-garage/move/actions/workflows/go-test-windows.yaml/badge.svg
[go-test-windows.yaml]: https://github.com/senzing-garage/move/actions/workflows/go-test-windows.yaml
[golangci-lint.yaml Badge]: https://github.com/senzing-garage/move/actions/workflows/golangci-lint.yaml/badge.svg
[golangci-lint.yaml]: https://github.com/senzing-garage/move/actions/workflows/golangci-lint.yaml
[License Badge]: https://img.shields.io/badge/License-Apache2-brightgreen.svg
[License]: https://github.com/senzing-garage/move/blob/main/LICENSE
[Package reference]: https://pkg.go.dev/github.com/senzing-garage/move
[Senzing Garage]: https://github.com/senzing-garage
[Senzing Quick Start guides]: https://docs.senzing.com/quickstart/
[Senzing]: https://senzing.com/
