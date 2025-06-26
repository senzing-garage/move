/*
 */
package cmd

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/senzing-garage/go-cmdhelping/cmdhelper"
	"github.com/senzing-garage/go-cmdhelping/option"
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
)

// ----------------------------------------------------------------------------
// Context variables
// ----------------------------------------------------------------------------

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

	printIntroduction()
	delay()

	mover := &move.BasicMove{
		FileType:                  viper.GetString(option.InputFileType.Arg),
		InputURL:                  viper.GetString(option.InputURL.Arg),
		JSONOutput:                viper.GetBool(option.JSONOutput.Arg),
		LogLevel:                  viper.GetString(option.LogLevel.Arg),
		MonitoringPeriodInSeconds: viper.GetInt(option.MonitoringPeriodInSeconds.Arg),
		OutputURL:                 viper.GetString(option.OutputURL.Arg),
		RecordMax:                 viper.GetInt(option.RecordMax.Arg),
		RecordMin:                 viper.GetInt(option.RecordMin.Arg),
		RecordMonitor:             viper.GetInt(option.RecordMonitor.Arg),
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

	printSummary(startTime, &anObserver)

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

func printIntroduction() {
	jsonOutput := viper.GetBool(option.JSONOutput.Arg)
	if !jsonOutput {
		outputln("Run with the following parameters:")

		for _, key := range viper.AllKeys() {
			outputln("  - ", key, " = ", viper.Get(key))
		}
	}
}

func printSummary(startTime time.Time, anObserver *cmdobserver.CmdObserver) {
	var totalWrite int64

	outputln("Data sources:")

	for x, y := range anObserver.GetDataSourceCodes() {
		totalWrite += y
		outputf("%12d: %s\n", y, x)
	}

	outputln("-------------------")
	outputf("%12d: Total\n", anObserver.GetTotalRead())

	if totalWrite != anObserver.GetTotalRead() {
		outputf("Error in read vs. write counts. (%d vs. %d)\n", anObserver.GetTotalRead(), totalWrite)
	}

	// Calculate duration.

	printTime(startTime, anObserver.GetLastUpdateTime())
}

func printTime(startTime time.Time, stopTime time.Time) {
	duration := stopTime.Sub(startTime)
	outputf("Duration: %f seconds", duration.Seconds())

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

func outputf(format string, message ...any) {
	fmt.Printf(format, message...) //nolint
}

func outputln(message ...any) {
	fmt.Println(message...) //nolint
}
