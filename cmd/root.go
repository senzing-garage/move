/*
 */
package cmd

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/senzing-garage/go-cmdhelping/cmdhelper"
	"github.com/senzing-garage/go-cmdhelping/option"
	"github.com/senzing-garage/go-cmdhelping/option/optiontype"
	"github.com/senzing-garage/go-helpers/wraperror"
	"github.com/senzing-garage/move/cmdobserver"
	"github.com/senzing-garage/move/move"
	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

const (
	Short string = "Move records from one location to another."
	Use   string = "move"
	Long  string = `
    Welcome to move!
    This tool will move records from one place to another. It validates the records conform to the Generic Entity Specification.

    For example:

    move --input-url "file:///path/to/json/lines/file.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
    move --input-url "https://public-read-access.s3.amazonaws.com/TestDataSets/SenzingTruthSet/truth-set-3.0.0.jsonl" --output-url "amqp://guest:guest@192.168.6.96:5672"
`
)

const (
	secondsPerMinute int64 = 60
	secondsPerHour   int64 = secondsPerMinute * 60
	inHalf                 = 2
)

// ----------------------------------------------------------------------------
// Context variables
// ----------------------------------------------------------------------------

var validate = option.ContextVariable{
	Arg:     "validate",
	Default: option.OsLookupEnvBool("SENZING_TOOLS_VALIDATE", false),
	Envar:   "SENZING_TOOLS_VALIDATE",
	Help:    "Validate records prior to moving [%s]",
	Type:    optiontype.Bool,
}

var ContextVariablesForMultiPlatform = []option.ContextVariable{
	option.DelayInSeconds,
	option.EngineInstanceName.SetDefault(fmt.Sprintf("move-%d", time.Now().Unix())),
	option.InputFileType,
	option.InputURL,
	option.JSONOutput,
	option.LogLevel,
	option.MonitoringPeriodInSeconds,
	option.OutputURL,
	option.RecordMax,
	option.RecordMin,
	option.RecordMonitor,
	validate,
}

var ContextVariables = append(ContextVariablesForMultiPlatform, ContextVariablesForOsArch...)

// ----------------------------------------------------------------------------
// Command
// ----------------------------------------------------------------------------

// RootCmd represents the command.
var RootCmd = &cobra.Command{
	Use:     Use,
	Short:   Short,
	Long:    Long,
	PreRun:  PreRun,
	RunE:    RunE,
	Version: Version(),
}

// ----------------------------------------------------------------------------
// Public functions
// ----------------------------------------------------------------------------

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the RootCmd.
func Execute() {
	err := RootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

// Used in construction of cobra.Command.
func PreRun(cobraCommand *cobra.Command, args []string) {
	cmdhelper.PreRun(cobraCommand, args, Use, ContextVariables)
}

// Used in construction of cobra.Command.
func RunE(_ *cobra.Command, _ []string) error {
	var err error

	ctx := context.Background()

	delay()

	mover := &move.BasicMove{
		FileType:                  viper.GetString(option.InputFileType.Arg),
		InputURL:                  viper.GetString(option.InputURL.Arg),
		LogLevel:                  viper.GetString(option.LogLevel.Arg),
		MonitoringPeriodInSeconds: viper.GetInt(option.MonitoringPeriodInSeconds.Arg),
		OutputURL:                 viper.GetString(option.OutputURL.Arg),
		PlainText:                 true,
		RecordMax:                 viper.GetInt(option.RecordMax.Arg),
		RecordMin:                 viper.GetInt(option.RecordMin.Arg),
		RecordMonitor:             viper.GetInt(option.RecordMonitor.Arg),
		Validate:                  viper.GetBool(validate.Arg),
	}

	anObserver := cmdobserver.CmdObserver{
		ID: "move",
	}

	err = mover.RegisterObserver(ctx, &anObserver)
	if err != nil {
		return wraperror.Errorf(err, "RegisterObserver")
	}

	startTime := time.Now()

	err = mover.Move(ctx)
	if err != nil {
		return wraperror.Errorf(err, "Move")
	}

	printExit(startTime, mover, &anObserver)

	return wraperror.Errorf(err, wraperror.NoMessage)
}

// Used in construction of cobra.Command.
func Version() string {
	return cmdhelper.Version(githubVersion, githubIteration)
}

// ----------------------------------------------------------------------------
// Private functions
// ----------------------------------------------------------------------------

func delay() {
	jsonOutput := viper.GetBool(option.JSONOutput.Arg)
	delayInSeconds := viper.GetInt(option.DelayInSeconds.Arg)

	if delayInSeconds > 0 {
		if !jsonOutput {
			outputln(
				time.Now(),
				"Sleep for",
				delayInSeconds,
				"seconds to let queues and databases settle down and come up.",
			)
		}

		time.Sleep(time.Duration(delayInSeconds) * time.Second)
	}
}

// Since init() is always invoked, define command line parameters.
func init() {
	cmdhelper.Init(RootCmd, ContextVariables)
}

func printExit(startTime time.Time, moveit *move.BasicMove, anObserver *cmdobserver.CmdObserver) {
	var totalWrite int64

	outputf("\nMove complete.\n")
	outputf("%16d lines read\n", moveit.GetTotalLines())
	outputf("%16d records moved\n", anObserver.GetTotalRead())

	if viper.GetBool(validate.Arg) {
		printInvalidRecordDefinitionCount(anObserver.GetInvalidRecordDefinitions())
	}

	outputf("         Target: %s\n", viper.GetString(option.OutputURL.Arg))
	printTime(startTime, anObserver.GetLastUpdateTime())

	if len(anObserver.GetDataSourceCodes()) > 0 {
		dataSourceNameLength := 11
		for dataSourceCode := range anObserver.GetDataSourceCodes() {
			if len(dataSourceCode) > dataSourceNameLength {
				dataSourceNameLength = len(dataSourceCode)
			}
		}

		outputf("\n      Count%sData source\n", strings.Repeat(" ", dataSourceNameLength/inHalf))
		outputln("---------------- " + strings.Repeat("-", dataSourceNameLength))

		for x, y := range anObserver.GetDataSourceCodes() {
			totalWrite += y
			outputf("%16d %s\n", y, x)
		}
	}

	// Errors.

	if totalWrite != anObserver.GetTotalRead() {
		outputf(
			"Error: Discrepency between read and write counts. (%d vs. %d)\n",
			anObserver.GetTotalRead(),
			totalWrite,
		)
	}

	if viper.GetBool(validate.Arg) && slices.Contains([]string{"DEBUG", "TRACE"}, moveit.LogLevel) {
		printInvalidRecordDefinitions(anObserver.GetInvalidRecordDefinitions())
	}
}

func printTime(startTime time.Time, stopTime time.Time) {
	duration := stopTime.Sub(startTime)
	outputf("       Duration: %f seconds", duration.Seconds())

	seconds := int64(duration.Seconds())
	if seconds > 0 {
		hours := seconds / secondsPerHour
		seconds %= secondsPerHour
		minutes := seconds / secondsPerMinute
		seconds %= secondsPerMinute

		outputf(" (")

		if hours > 0 {
			outputf("Hours: %d ", hours)
		}

		if minutes > 0 {
			outputf("Minutes: %d ", minutes)
		}

		outputf("Seconds: %d)", seconds)
	}

	outputf("\n")
}

func printInvalidRecordDefinitionCount(invalidRecordDefinitions []int64) {
	outputf("%16d invalid records\n", len(invalidRecordDefinitions))
}

func printInvalidRecordDefinitions(invalidRecordDefinitions []int64) {
	outputf("  Invalid lines: ")

	for _, value := range invalidRecordDefinitions {
		outputf("%d ", value)
	}

	outputf("\n")
}

func outputf(format string, message ...any) {
	fmt.Printf(format, message...) //nolint
}

func outputln(message ...any) {
	fmt.Println(message...) //nolint
}
