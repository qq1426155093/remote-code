package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/chzyer/readline"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

var uuidPattern = regexp.MustCompile(`^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$`)

type processStartOptions struct {
	name             string
	command          string
	arguments        []string
	workingDirectory string
	ioMode           codev1.ProcessIOMode
	inputMode        codev1.ProcessInputMode
	environment      map[string]string
	attach           bool
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
	var terminalSize *codev1.TerminalSize
	if options.attach {
		if r.terminal == nil || !r.terminal.available() {
			return errors.New("exec --attach requires a supported interactive local terminal")
		}
		rows, columns, sizeErr := r.terminal.size()
		if sizeErr != nil {
			return fmt.Errorf("get local terminal size: %w", sizeErr)
		}
		terminalSize = &codev1.TerminalSize{Rows: rows, Columns: columns}
	}
	ctx, cancel := r.commandContext()
	info, err := r.client.StartProcessWithOptions(ctx, remoteclient.ProcessStartOptions{
		Name: options.name, Command: options.command, Arguments: options.arguments,
		WorkingDirectory: workingDirectory, IOMode: options.ioMode,
		InputMode: options.inputMode, TerminalSize: terminalSize, Environment: options.environment,
	})
	cancel()
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "started %s (%s), pid %d, %s mode, input %s, cwd %s\n",
		info.GetName(), info.GetId(), info.GetPid(), processIOModeName(info.GetIoMode()), processInputStateName(info.GetInputState()), info.GetWorkingDirectory())
	if options.attach {
		return r.attach(&codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: info.GetId()}})
	}
	return nil
}

func (r *REPL) listProcesses(arguments []string) error {
	all := false
	for _, argument := range arguments {
		if argument == "-a" || argument == "--all" {
			all = true
			continue
		}
		return usageErrorf("unknown ps option %q", argument)
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	processes, err := r.client.ListProcesses(ctx, all)
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "ID\tNAME\tPID\tMODE\tINPUT\tSTATE\tEXIT\tSTARTED\tCWD\tCOMMAND")
	for _, process := range processes {
		fmt.Fprintf(writer, "%s\t%s\t%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n",
			process.GetId(), process.GetName(), process.GetPid(), processIOModeName(process.GetIoMode()),
			processInputStateName(process.GetInputState()), processStateName(process.GetState()), processExitName(process), formatProcessTime(process.GetStartedAt()),
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
	if len(arguments) == 0 {
		return usageError()
	}
	selectors := make([]*codev1.ProcessSelector, 0, len(arguments))
	for _, argument := range arguments {
		selector, err := parseProcessSelector(argument)
		if err != nil {
			return fmt.Errorf("invalid process selector %q: %w", argument, err)
		}
		selectors = append(selectors, selector)
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	response, err := r.client.BatchDeleteProcesses(ctx, selectors)
	if err != nil {
		return err
	}
	failed := 0
	deleted := 0
	for _, result := range response.GetSelectors() {
		code := codes.Code(result.GetStatus().GetCode())
		if code == codes.OK {
			continue
		}
		failed++
		index := int(result.GetSelectorIndex())
		value := "<invalid>"
		if index >= 0 && index < len(arguments) {
			value = arguments[index]
		}
		fmt.Fprintf(r.stderr, "selector %q: %s (%s)\n", value, batchDeleteMessage(result.GetStatus().GetMessage(), code), code)
	}
	for _, result := range response.GetProcesses() {
		process := result.GetProcess()
		code := codes.Code(result.GetStatus().GetCode())
		if code == codes.OK {
			deleted++
			fmt.Fprintf(r.stdout, "forgot %s (%s)\n", process.GetName(), process.GetId())
			continue
		}
		failed++
		fmt.Fprintf(r.stderr, "skipped %s (%s): %s (%s)\n", process.GetName(), process.GetId(), batchDeleteMessage(result.GetStatus().GetMessage(), code), code)
	}
	if failed > 0 {
		processNoun := "processes"
		if deleted == 1 {
			processNoun = "process"
		}
		operationNoun := "operations"
		if failed == 1 {
			operationNoun = "operation"
		}
		return fmt.Errorf("forgot %d %s; %d %s failed", deleted, processNoun, failed, operationNoun)
	}
	return nil
}

func batchDeleteMessage(message string, code codes.Code) string {
	if message != "" {
		return message
	}
	return code.String()
}

func (r *REPL) observeProcessLogs(arguments []string) error {
	options, err := parseProcessLogOptions(arguments)
	if err != nil {
		return err
	}
	commandContext, cancelCommand := r.commandContext()
	defer cancelCommand()
	streamContext := commandContext
	stopInterrupt := func() {}
	if options.follow {
		streamContext, stopInterrupt = r.interruptContext(commandContext)
	}
	defer stopInterrupt()
	interrupted := func() bool {
		return options.follow && streamContext.Err() != nil && commandContext.Err() == nil
	}
	stream, err := r.client.ObserveProcessLogs(streamContext, options.processID, remoteclient.ProcessLogOptions{
		Streams: options.streams, Offset: options.offset, TailLines: options.tail, Follow: options.follow,
	})
	if err != nil {
		if interrupted() {
			return nil
		}
		return err
	}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if interrupted() {
				return nil
			}
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

func (r *REPL) writeProcessInput(arguments []string) error {
	if len(arguments) != 1 {
		return usageError()
	}
	if r.line == nil {
		return errors.New("stdin submode requires an interactive terminal")
	}
	reference, err := parseProcessReference(arguments[0])
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	session, err := r.client.OpenProcessInput(ctx, reference)
	if err != nil {
		return err
	}
	defer func() { _ = session.Close() }()
	process := session.Process()
	fmt.Fprintf(r.stdout, "attached to stdin of %s (%s); .detach leaves input open, .eof closes PIPE input, .eot sends Ctrl-D\n", process.GetName(), process.GetId())

	var previousCompleter readline.AutoCompleter
	if r.line.Config != nil {
		previousCompleter = r.line.Config.AutoComplete
		r.line.Config.AutoComplete = nil
		defer func() { r.line.Config.AutoComplete = previousCompleter }()
	}
	for {
		r.line.SetPrompt("stdin:" + process.GetName() + "> ")
		line, readErr := r.line.Readline()
		switch {
		case errors.Is(readErr, readline.ErrInterrupt), errors.Is(readErr, io.EOF):
			_, err := session.Detach()
			return err
		case readErr != nil:
			return fmt.Errorf("read process input: %w", readErr)
		}
		switch line {
		case ".detach":
			_, err := session.Detach()
			return err
		case ".eof":
			_, err := session.CloseInput()
			return err
		case ".eot":
			if _, err := session.Write([]byte{4}); err != nil {
				return err
			}
		default:
			if _, err := session.Write([]byte(line + "\n")); err != nil {
				return err
			}
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
				return processLogOptions{}, usageError()
			}
			index++
			value, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return processLogOptions{}, errors.New("log tail lines must be a non-negative integer")
			}
			options.tail = &value
		case "--offset":
			if index+1 >= len(arguments) || options.offset != nil || options.tail != nil {
				return processLogOptions{}, usageError()
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
				return processLogOptions{}, usageErrorf("unknown logs option %q", arguments[index])
			}
			values = append(values, arguments[index])
		}
	}
	if len(values) != 1 || !uuidPattern.MatchString(values[0]) {
		return processLogOptions{}, usageError()
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
	options := processStartOptions{
		ioMode:      codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
		inputMode:   codev1.ProcessInputMode_PROCESS_INPUT_MODE_DISABLED,
		environment: make(map[string]string),
	}
	modeSet := false
	inputSet := false
	attachSet := false
	nameSet := false
	cwdSet := false
	for index := 0; index < len(arguments); {
		argument := arguments[index]
		switch argument {
		case "--name":
			if index+1 >= len(arguments) || arguments[index+1] == "" || nameSet {
				return processStartOptions{}, usageError()
			}
			options.name = arguments[index+1]
			nameSet = true
			index += 2
		case "--cwd":
			if index+1 >= len(arguments) || cwdSet {
				return processStartOptions{}, usageError()
			}
			options.workingDirectory = arguments[index+1]
			cwdSet = true
			index += 2
		case "-e", "--env":
			if index+1 >= len(arguments) {
				return processStartOptions{}, usageError()
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
		case "--stdin":
			if inputSet {
				return processStartOptions{}, errors.New("--stdin is specified more than once")
			}
			options.inputMode = codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED
			inputSet = true
			index++
		case "--attach":
			if attachSet {
				return processStartOptions{}, errors.New("--attach is specified more than once")
			}
			options.attach = true
			attachSet = true
			index++
		case "--":
			index++
			if index >= len(arguments) {
				return processStartOptions{}, usageError()
			}
			options.command = arguments[index]
			options.arguments = append([]string(nil), arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(argument, "-") {
				return processStartOptions{}, usageErrorf("unknown exec option %q", argument)
			}
			options.command = argument
			options.arguments = append([]string(nil), arguments[index+1:]...)
			index = len(arguments)
		}
	}
	if options.command == "" {
		return processStartOptions{}, usageError()
	}
	if options.attach {
		if modeSet && options.ioMode == codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE {
			return processStartOptions{}, errors.New("--attach cannot be combined with --pipe")
		}
		options.ioMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PTY
		options.inputMode = codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED
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
				return processSignalOptions{}, usageError()
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
				return processSignalOptions{}, usageErrorf("unknown kill option %q", arguments[index])
			}
			values = append(values, arguments[index])
		}
	}
	if len(values) != 1 {
		return processSignalOptions{}, usageError()
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

func parseProcessSelector(value string) (*codev1.ProcessSelector, error) {
	if value == "" {
		return nil, errors.New("process selector is required")
	}
	if kind, explicit, ok := strings.Cut(value, ":"); ok {
		if strings.EqualFold(kind, "glob") {
			return parseProcessNameGlob(explicit)
		}
		reference, err := parseProcessReference(value)
		if err != nil {
			return nil, err
		}
		return remoteclient.ExactProcessSelector(reference), nil
	}
	if strings.ContainsAny(value, "*?[") {
		return parseProcessNameGlob(value)
	}
	reference, err := parseProcessReference(value)
	if err != nil {
		return nil, err
	}
	return remoteclient.ExactProcessSelector(reference), nil
}

func parseProcessNameGlob(pattern string) (*codev1.ProcessSelector, error) {
	if pattern == "" {
		return nil, errors.New("process name glob is required")
	}
	if _, err := path.Match(pattern, ""); err != nil {
		return nil, errors.New("process name glob is invalid")
	}
	return remoteclient.ProcessNameGlobSelector(pattern), nil
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

func processInputStateName(state codev1.ProcessInputState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "PROCESS_INPUT_STATE_"))
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
