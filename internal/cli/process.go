package cli

import (
	"errors"
	"fmt"
	"io"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type processStartOptions struct {
	name             string
	command          string
	arguments        []string
	workingDirectory string
	ioMode           codev1.ProcessIOMode
	environment      map[string]string
}

type processSignalOptions struct {
	reference *codev1.ProcessReference
	signal    codev1.ProcessSignal
	wait      bool
}

type processLogOptions struct {
	processID string
	streams   []codev1.ProcessLogStream
	offset    *uint64
	tail      *uint64
	follow    bool
}

func (r *REPL) startProcess(arguments []string) error {
	options, err := parseProcessStartOptions(arguments)
	if err != nil {
		return err
	}
	workingDirectory, err := resolveRemotePath(r.cwd, options.workingDirectory)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.StartProcessWithOptions(ctx, remoteclient.ProcessStartOptions{
		Name: options.name, Command: options.command, Arguments: options.arguments,
		WorkingDirectory: workingDirectory, IOMode: options.ioMode, Environment: options.environment,
	})
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "started %s (%s), pid %d, %s mode, cwd %s\n", info.GetName(), info.GetId(), info.GetPid(), processIOModeName(info.GetIoMode()), info.GetWorkingDirectory())
	return nil
}

func (r *REPL) listProcesses(arguments []string) error {
	all := false
	for _, argument := range arguments {
		if argument == "-a" || argument == "--all" {
			all = true
			continue
		}
		return fmt.Errorf("unknown ps option %q; %s", argument, commandUsage["ps"])
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	processes, err := r.client.ListProcesses(ctx, all)
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "ID\tNAME\tPID\tMODE\tSTATE\tEXIT\tSTARTED\tCWD\tCOMMAND")
	for _, process := range processes {
		fmt.Fprintf(writer, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\n",
			process.GetId(), process.GetName(), process.GetPid(), processIOModeName(process.GetIoMode()),
			processStateName(process.GetState()), processExitName(process), formatProcessTime(process.GetStartedAt()),
			process.GetWorkingDirectory(), displayProcessCommand(process))
	}
	return writer.Flush()
}

func (r *REPL) signalProcess(arguments []string) error {
	options, err := parseProcessSignalOptions(arguments)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.SignalProcess(ctx, options.reference, options.signal, options.wait)
	if err != nil {
		return err
	}
	if options.wait {
		fmt.Fprintf(r.stdout, "%s (%s) is %s (%s)\n", info.GetName(), info.GetId(), processStateName(info.GetState()), processExitName(info))
	} else {
		fmt.Fprintf(r.stdout, "sent %s to %s (%s), pid %d\n", processSignalName(options.signal), info.GetName(), info.GetId(), info.GetPid())
	}
	return nil
}

func (r *REPL) forgetProcess(arguments []string) error {
	if len(arguments) != 1 {
		return errors.New(commandUsage["forget"])
	}
	reference, err := parseProcessReference(arguments[0])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.DeleteProcess(ctx, reference)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "forgot %s (%s)\n", info.GetName(), info.GetId())
	return nil
}

func (r *REPL) observeProcessLogs(arguments []string) error {
	options, err := parseProcessLogOptions(arguments)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	stream, err := r.client.ObserveProcessLogs(ctx, options.processID, remoteclient.ProcessLogOptions{
		Streams: options.streams, Offset: options.offset, TailLines: options.tail, Follow: options.follow,
	})
	if err != nil {
		return err
	}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		chunk := response.GetChunk()
		if chunk == nil || len(chunk.GetData()) == 0 {
			continue
		}
		writer := r.stdout
		if chunk.GetStream() == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
			writer = r.stderr
		}
		n, err := writer.Write(chunk.GetData())
		if err != nil {
			return fmt.Errorf("write process log output: %w", err)
		}
		if n != len(chunk.GetData()) {
			return io.ErrShortWrite
		}
	}
}

func parseProcessLogOptions(arguments []string) (processLogOptions, error) {
	var options processLogOptions
	values := make([]string, 0, 1)
	stdoutSet := false
	stderrSet := false
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "-f", "--follow":
			options.follow = true
		case "-n", "--tail":
			if index+1 >= len(arguments) || options.tail != nil || options.offset != nil {
				return processLogOptions{}, errors.New(commandUsage["logs"])
			}
			index++
			value, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return processLogOptions{}, errors.New("log tail lines must be a non-negative integer")
			}
			options.tail = &value
		case "--offset":
			if index+1 >= len(arguments) || options.offset != nil || options.tail != nil {
				return processLogOptions{}, errors.New(commandUsage["logs"])
			}
			index++
			value, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return processLogOptions{}, errors.New("log offset must be a non-negative integer")
			}
			options.offset = &value
		case "--stdout":
			if stdoutSet {
				return processLogOptions{}, errors.New("--stdout is specified more than once")
			}
			stdoutSet = true
		case "--stderr":
			if stderrSet {
				return processLogOptions{}, errors.New("--stderr is specified more than once")
			}
			stderrSet = true
		case "--":
			values = append(values, arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(arguments[index], "-") {
				return processLogOptions{}, fmt.Errorf("unknown logs option %q; %s", arguments[index], commandUsage["logs"])
			}
			values = append(values, arguments[index])
		}
	}
	if len(values) != 1 || !uuidPattern.MatchString(values[0]) {
		return processLogOptions{}, errors.New(commandUsage["logs"])
	}
	options.processID = strings.ToLower(values[0])
	if stdoutSet {
		options.streams = append(options.streams, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT)
	}
	if stderrSet {
		options.streams = append(options.streams, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR)
	}
	return options, nil
}

func parseProcessStartOptions(arguments []string) (processStartOptions, error) {
	options := processStartOptions{ioMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, environment: make(map[string]string)}
	modeSet := false
	nameSet := false
	cwdSet := false
	for index := 0; index < len(arguments); {
		argument := arguments[index]
		switch argument {
		case "--name":
			if index+1 >= len(arguments) || arguments[index+1] == "" || nameSet {
				return processStartOptions{}, errors.New(commandUsage["exec"])
			}
			options.name = arguments[index+1]
			nameSet = true
			index += 2
		case "--cwd":
			if index+1 >= len(arguments) || cwdSet {
				return processStartOptions{}, errors.New(commandUsage["exec"])
			}
			options.workingDirectory = arguments[index+1]
			cwdSet = true
			index += 2
		case "-e", "--env":
			if index+1 >= len(arguments) {
				return processStartOptions{}, errors.New(commandUsage["exec"])
			}
			key, value, ok := strings.Cut(arguments[index+1], "=")
			if !ok || key == "" {
				return processStartOptions{}, errors.New("environment override must use KEY=VALUE")
			}
			if _, exists := options.environment[key]; exists {
				return processStartOptions{}, fmt.Errorf("environment key %q is specified more than once", key)
			}
			options.environment[key] = value
			index += 2
		case "--pipe":
			if modeSet {
				return processStartOptions{}, errors.New("only one of --pipe and --pty may be specified")
			}
			options.ioMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE
			modeSet = true
			index++
		case "--pty":
			if modeSet {
				return processStartOptions{}, errors.New("only one of --pipe and --pty may be specified")
			}
			options.ioMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PTY
			modeSet = true
			index++
		case "--":
			index++
			if index >= len(arguments) {
				return processStartOptions{}, errors.New(commandUsage["exec"])
			}
			options.command = arguments[index]
			options.arguments = append([]string(nil), arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(argument, "-") {
				return processStartOptions{}, fmt.Errorf("unknown exec option %q; %s", argument, commandUsage["exec"])
			}
			options.command = argument
			options.arguments = append([]string(nil), arguments[index+1:]...)
			index = len(arguments)
		}
	}
	if options.command == "" {
		return processStartOptions{}, errors.New(commandUsage["exec"])
	}
	return options, nil
}

func parseProcessSignalOptions(arguments []string) (processSignalOptions, error) {
	options := processSignalOptions{signal: codev1.ProcessSignal_PROCESS_SIGNAL_TERM}
	values := make([]string, 0, 1)
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "-s", "--signal":
			if index+1 >= len(arguments) {
				return processSignalOptions{}, errors.New(commandUsage["kill"])
			}
			index++
			signal, err := parseProcessSignal(arguments[index])
			if err != nil {
				return processSignalOptions{}, err
			}
			options.signal = signal
		case "-w", "--wait":
			options.wait = true
		case "--":
			values = append(values, arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(arguments[index], "-") {
				return processSignalOptions{}, fmt.Errorf("unknown kill option %q; %s", arguments[index], commandUsage["kill"])
			}
			values = append(values, arguments[index])
		}
	}
	if len(values) != 1 {
		return processSignalOptions{}, errors.New(commandUsage["kill"])
	}
	reference, err := parseProcessReference(values[0])
	if err != nil {
		return processSignalOptions{}, err
	}
	options.reference = reference
	return options, nil
}

func parseProcessReference(value string) (*codev1.ProcessReference, error) {
	if value == "" {
		return nil, errors.New("process reference is required")
	}
	if kind, explicit, ok := strings.Cut(value, ":"); ok {
		if explicit == "" {
			return nil, errors.New("process reference value is required")
		}
		switch strings.ToLower(kind) {
		case "id":
			return &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: explicit}}, nil
		case "name":
			return &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: explicit}}, nil
		case "pid":
			pid, err := strconv.ParseInt(explicit, 10, 64)
			if err != nil || pid <= 0 {
				return nil, errors.New("process pid must be a positive integer")
			}
			return &codev1.ProcessReference{Value: &codev1.ProcessReference_Pid{Pid: pid}}, nil
		default:
			return nil, fmt.Errorf("unknown process reference prefix %q", kind)
		}
	}
	if pid, err := strconv.ParseInt(value, 10, 64); err == nil && pid > 0 {
		return &codev1.ProcessReference{Value: &codev1.ProcessReference_Pid{Pid: pid}}, nil
	}
	if uuidPattern.MatchString(value) {
		return &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: value}}, nil
	}
	return &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: value}}, nil
}

func parseProcessSignal(value string) (codev1.ProcessSignal, error) {
	value = strings.TrimPrefix(strings.ToUpper(strings.TrimSpace(value)), "SIG")
	signals := map[string]string{
		"1": "HUP", "2": "INT", "3": "QUIT", "9": "KILL", "10": "USR1", "12": "USR2", "15": "TERM", "18": "CONT", "19": "STOP",
	}
	if name, ok := signals[value]; ok {
		value = name
	}
	for signal := codev1.ProcessSignal_PROCESS_SIGNAL_HUP; signal <= codev1.ProcessSignal_PROCESS_SIGNAL_CONT; signal++ {
		if processSignalName(signal) == value {
			return signal, nil
		}
	}
	return 0, fmt.Errorf("unsupported signal %q", value)
}

func processSignalName(signal codev1.ProcessSignal) string {
	return strings.TrimPrefix(signal.String(), "PROCESS_SIGNAL_")
}

func processIOModeName(mode codev1.ProcessIOMode) string {
	return strings.ToLower(strings.TrimPrefix(mode.String(), "PROCESS_IO_MODE_"))
}

func processStateName(state codev1.ProcessState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "PROCESS_STATE_"))
}

func processExitName(process *codev1.ProcessInfo) string {
	if process.ExitSignal != nil {
		return "signal " + strconv.FormatInt(int64(process.GetExitSignal()), 10)
	}
	if process.ExitCode != nil {
		return "code " + strconv.FormatInt(int64(process.GetExitCode()), 10)
	}
	return "-"
}

func displayProcessCommand(process *codev1.ProcessInfo) string {
	parts := []string{process.GetCommand()}
	for _, argument := range process.GetArguments() {
		parts = append(parts, strconv.Quote(argument))
	}
	return strings.Join(parts, " ")
}

func formatProcessTime(value *timestamppb.Timestamp) string {
	if value == nil || !value.IsValid() {
		return "-"
	}
	return value.AsTime().Local().Format(time.RFC3339)
}
