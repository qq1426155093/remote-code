package process

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxProcesses = 16
	maxTrackedProcesses = 4096
	maxProcessArguments = 256
	maxArgumentBytes    = 4096
	maxArgumentsBytes   = 64 << 10
	maxWorkingPathBytes = 4096
	forcedShutdownWait  = 5 * time.Second
)

var (
	identifierPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	errUnsupportedPlatform = errors.New("process management is not supported on this platform")
)

// Config controls one in-memory process registry.
type Config struct {
	Workspace    string
	Commands     map[string]string
	MaxProcesses int
}

// Service manages process lifecycle and implements the gRPC process API.
type Service struct {
	codev1.UnimplementedProcessServiceServer

	root         *os.Root
	commands     map[string]string
	commandNames []string
	maxProcesses int

	mu        sync.Mutex
	closing   bool
	active    int
	processes map[string]*managedProcess
	byName    map[string]string
	byPID     map[int64]string
	order     []string
}

type managedProcess struct {
	info    *codev1.ProcessInfo
	command *runningCommand
	done    chan struct{}
}

// New validates the workspace and configured command allowlist.
func New(config Config) (*Service, error) {
	if config.Workspace == "" {
		return nil, errors.New("workspace is required")
	}
	abs, err := filepath.Abs(config.Workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace: %w", err)
	}
	workspaceInfo, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("stat workspace: %w", err)
	}
	if !workspaceInfo.IsDir() {
		return nil, fmt.Errorf("workspace %q is not a directory", abs)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("open workspace: %w", err)
	}

	commands := make(map[string]string, len(config.Commands))
	commandNames := make([]string, 0, len(config.Commands))
	for name, executable := range config.Commands {
		if !identifierPattern.MatchString(name) {
			_ = root.Close()
			return nil, fmt.Errorf("process command name %q must match %s", name, identifierPattern)
		}
		resolved, err := resolveExecutable(executable)
		if err != nil {
			_ = root.Close()
			return nil, fmt.Errorf("resolve process command %q: %w", name, err)
		}
		commands[name] = resolved
		commandNames = append(commandNames, name)
	}
	sort.Strings(commandNames)
	maxProcesses := config.MaxProcesses
	if maxProcesses == 0 {
		maxProcesses = defaultMaxProcesses
	}
	if maxProcesses < 0 || maxProcesses > maxTrackedProcesses {
		_ = root.Close()
		return nil, fmt.Errorf("max processes must be between 1 and %d", maxTrackedProcesses)
	}
	return &Service{
		root: root, commands: commands, commandNames: commandNames, maxProcesses: maxProcesses,
		processes: make(map[string]*managedProcess), byName: make(map[string]string), byPID: make(map[int64]string),
	}, nil
}

// AllowedCommands returns a sorted copy of configured command aliases.
func (s *Service) AllowedCommands() []string {
	return append([]string(nil), s.commandNames...)
}

// MaxProcesses returns the active process limit.
func (s *Service) MaxProcesses() int {
	return s.maxProcesses
}

// StartProcess launches one command in its own process group.
func (s *Service) StartProcess(ctx context.Context, request *codev1.StartProcessRequest) (*codev1.StartProcessResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	name, commandName, arguments, workingDirectory, ioMode, err := s.validateStartRequest(request)
	if err != nil {
		return nil, err
	}
	directory, err := s.openWorkingDirectory(workingDirectory)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	id, err := newUUID()
	if err != nil {
		return nil, status.Error(codes.Internal, "allocate process id failed")
	}
	record := &managedProcess{
		info: &codev1.ProcessInfo{
			Id: id, Name: name, IoMode: ioMode, State: codev1.ProcessState_PROCESS_STATE_STARTING,
			Command: commandName, Arguments: append([]string(nil), arguments...), WorkingDirectory: displayWorkingDirectory(workingDirectory),
		},
		done: make(chan struct{}),
	}

	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "process service is shutting down")
	}
	if s.active >= s.maxProcesses {
		s.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted, "active process limit of %d reached", s.maxProcesses)
	}
	if _, exists := s.byName[name]; exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "process name %q already exists", name)
	}
	if err := s.makeHistorySpaceLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.processes[id] = record
	s.byName[name] = id
	s.order = append(s.order, id)
	s.active++

	running, startErr := startCommand(directory, s.commands[commandName], arguments, ioMode)
	if startErr != nil {
		s.removeLocked(record)
		s.active--
		s.mu.Unlock()
		if errors.Is(startErr, errUnsupportedPlatform) {
			return nil, status.Error(codes.Unimplemented, startErr.Error())
		}
		return nil, status.Errorf(codes.Internal, "start process %q failed", name)
	}
	record.command = running
	record.info.Pid = int64(running.cmd.Process.Pid)
	record.info.State = codev1.ProcessState_PROCESS_STATE_RUNNING
	record.info.StartedAt = timestamppb.Now()
	s.byPID[record.info.Pid] = id
	response := &codev1.StartProcessResponse{Process: cloneProcessInfo(record.info)}
	go s.reap(record)
	s.mu.Unlock()
	return response, nil
}

// ListProcesses returns successful starts in stable creation order, including
// exited processes retained in the bounded in-memory history.
func (s *Service) ListProcesses(ctx context.Context, _ *codev1.ListProcessesRequest) (*codev1.ListProcessesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	processes := make([]*codev1.ProcessInfo, 0, len(s.order))
	for _, id := range s.order {
		if record := s.processes[id]; record != nil {
			processes = append(processes, cloneProcessInfo(record.info))
		}
	}
	return &codev1.ListProcessesResponse{Processes: processes}, nil
}

// SignalProcess sends an allowed signal to the entire managed process group.
// When wait is true, the RPC waits for the process leader to be reaped.
func (s *Service) SignalProcess(ctx context.Context, request *codev1.SignalProcessRequest) (*codev1.SignalProcessResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	signal := request.GetSignal()
	if signal == codev1.ProcessSignal_PROCESS_SIGNAL_UNSPECIFIED {
		signal = codev1.ProcessSignal_PROCESS_SIGNAL_TERM
	}
	if _, err := nativeSignal(signal); err != nil {
		return nil, status.Errorf(codes.InvalidArgument, "unsupported process signal %q", signal)
	}

	s.mu.Lock()
	record, err := s.lookupLocked(request.GetProcess())
	if err != nil {
		s.mu.Unlock()
		return nil, err
	}
	if record.info.GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING {
		info := cloneProcessInfo(record.info)
		s.mu.Unlock()
		return nil, status.Errorf(codes.FailedPrecondition, "process %q is %s", info.GetName(), processStateName(info.GetState()))
	}
	pid := int(record.info.GetPid())
	done := record.done
	s.mu.Unlock()

	if err := signalProcessGroup(pid, signal); err != nil {
		select {
		case <-done:
			return &codev1.SignalProcessResponse{Process: s.snapshot(record)}, nil
		default:
		}
		return nil, processSignalError(err)
	}
	if request.GetWait() {
		select {
		case <-done:
		case <-ctx.Done():
			return nil, status.FromContextError(ctx.Err()).Err()
		}
	}
	return &codev1.SignalProcessResponse{Process: s.snapshot(record)}, nil
}

// Shutdown rejects new starts, asks every running group to terminate, then
// force-kills remaining groups when ctx expires. Direct children are always
// waited by their reaper goroutines.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	records := s.runningLocked()
	s.mu.Unlock()
	for _, record := range records {
		_ = signalProcessGroup(int(record.info.GetPid()), codev1.ProcessSignal_PROCESS_SIGNAL_TERM)
	}
	if waitForRecords(ctx, records) {
		return nil
	}

	s.mu.Lock()
	remaining := s.runningLocked()
	s.mu.Unlock()
	for _, record := range remaining {
		_ = signalProcessGroup(int(record.info.GetPid()), codev1.ProcessSignal_PROCESS_SIGNAL_KILL)
	}
	forceContext, cancel := context.WithTimeout(context.Background(), forcedShutdownWait)
	defer cancel()
	_ = waitForRecords(forceContext, remaining)
	return ctx.Err()
}

// Close releases the pinned workspace handle after Shutdown has completed.
func (s *Service) Close() error {
	return s.root.Close()
}

func (s *Service) validateStartRequest(request *codev1.StartProcessRequest) (string, string, []string, string, codev1.ProcessIOMode, error) {
	name := request.GetName()
	if !identifierPattern.MatchString(name) {
		return "", "", nil, "", 0, status.Errorf(codes.InvalidArgument, "process name must match %s", identifierPattern)
	}
	commandName := request.GetCommand()
	if !identifierPattern.MatchString(commandName) {
		return "", "", nil, "", 0, status.Error(codes.InvalidArgument, "process command alias is invalid")
	}
	if _, ok := s.commands[commandName]; !ok {
		return "", "", nil, "", 0, status.Errorf(codes.FailedPrecondition, "process command %q is not configured", commandName)
	}
	arguments := request.GetArguments()
	if len(arguments) > maxProcessArguments {
		return "", "", nil, "", 0, status.Errorf(codes.InvalidArgument, "process accepts at most %d arguments", maxProcessArguments)
	}
	totalBytes := 0
	for _, argument := range arguments {
		if strings.IndexByte(argument, 0) >= 0 {
			return "", "", nil, "", 0, status.Error(codes.InvalidArgument, "process argument contains a NUL byte")
		}
		if len(argument) > maxArgumentBytes {
			return "", "", nil, "", 0, status.Errorf(codes.InvalidArgument, "process argument exceeds %d bytes", maxArgumentBytes)
		}
		totalBytes += len(argument)
	}
	if totalBytes > maxArgumentsBytes {
		return "", "", nil, "", 0, status.Errorf(codes.InvalidArgument, "process arguments exceed %d bytes", maxArgumentsBytes)
	}
	workingDirectory, err := cleanWorkingDirectory(request.GetWorkingDirectory())
	if err != nil {
		return "", "", nil, "", 0, err
	}
	ioMode := request.GetIoMode()
	if ioMode == codev1.ProcessIOMode_PROCESS_IO_MODE_UNSPECIFIED {
		ioMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE
	}
	if ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE && ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
		return "", "", nil, "", 0, status.Errorf(codes.InvalidArgument, "unsupported process I/O mode %q", ioMode)
	}
	return name, commandName, append([]string(nil), arguments...), workingDirectory, ioMode, nil
}

func (s *Service) openWorkingDirectory(rel string) (*os.File, error) {
	directory, err := s.root.Open(rel)
	if err != nil {
		return nil, processPathError(rel, err)
	}
	info, err := directory.Stat()
	if err != nil {
		_ = directory.Close()
		return nil, processPathError(rel, err)
	}
	if !info.IsDir() {
		_ = directory.Close()
		return nil, status.Errorf(codes.FailedPrecondition, "working directory %q is not a directory", displayWorkingDirectory(rel))
	}
	return directory, nil
}

func (s *Service) reap(record *managedProcess) {
	waitErr := record.command.cmd.Wait()
	record.command.close()
	exitCode, exitSignal := processExit(record.command.cmd.ProcessState, waitErr)

	s.mu.Lock()
	record.info.State = codev1.ProcessState_PROCESS_STATE_EXITED
	record.info.ExitedAt = timestamppb.Now()
	record.info.ExitCode = exitCode
	record.info.ExitSignal = exitSignal
	if s.active > 0 {
		s.active--
	}
	close(record.done)
	s.mu.Unlock()
}

func (s *Service) snapshot(record *managedProcess) *codev1.ProcessInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return cloneProcessInfo(record.info)
}

func (s *Service) lookupLocked(reference *codev1.ProcessReference) (*managedProcess, error) {
	if reference == nil || reference.GetValue() == nil {
		return nil, status.Error(codes.InvalidArgument, "process reference is required")
	}
	var id string
	switch value := reference.GetValue().(type) {
	case *codev1.ProcessReference_Id:
		if value.Id == "" {
			return nil, status.Error(codes.InvalidArgument, "process id is required")
		}
		id = value.Id
	case *codev1.ProcessReference_Name:
		if value.Name == "" {
			return nil, status.Error(codes.InvalidArgument, "process name is required")
		}
		id = s.byName[value.Name]
	case *codev1.ProcessReference_Pid:
		if value.Pid <= 0 {
			return nil, status.Error(codes.InvalidArgument, "process pid must be positive")
		}
		id = s.byPID[value.Pid]
	default:
		return nil, status.Error(codes.InvalidArgument, "process reference is invalid")
	}
	record := s.processes[id]
	if record == nil {
		return nil, status.Error(codes.NotFound, "process not found")
	}
	return record, nil
}

func (s *Service) runningLocked() []*managedProcess {
	records := make([]*managedProcess, 0, s.active)
	for _, id := range s.order {
		record := s.processes[id]
		if record != nil && record.info.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING {
			records = append(records, record)
		}
	}
	return records
}

func (s *Service) makeHistorySpaceLocked() error {
	for len(s.order) >= maxTrackedProcesses {
		removed := false
		for _, id := range s.order {
			record := s.processes[id]
			if record != nil && record.info.GetState() == codev1.ProcessState_PROCESS_STATE_EXITED {
				s.removeLocked(record)
				removed = true
				break
			}
		}
		if !removed {
			return status.Errorf(codes.ResourceExhausted, "process history limit of %d reached", maxTrackedProcesses)
		}
	}
	return nil
}

func (s *Service) removeLocked(record *managedProcess) {
	id := record.info.GetId()
	delete(s.processes, id)
	if s.byName[record.info.GetName()] == id {
		delete(s.byName, record.info.GetName())
	}
	if s.byPID[record.info.GetPid()] == id {
		delete(s.byPID, record.info.GetPid())
	}
	for index, orderedID := range s.order {
		if orderedID == id {
			s.order = append(s.order[:index], s.order[index+1:]...)
			break
		}
	}
}

func resolveExecutable(executable string) (string, error) {
	if executable == "" {
		return "", errors.New("executable is required")
	}
	resolved, err := exec.LookPath(executable)
	if err != nil {
		return "", err
	}
	resolved, err = filepath.Abs(resolved)
	if err != nil {
		return "", err
	}
	info, err := os.Stat(resolved)
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return "", errors.New("executable must be an executable regular file")
	}
	return resolved, nil
}

func cleanWorkingDirectory(raw string) (string, error) {
	if len(raw) > maxWorkingPathBytes {
		return "", status.Errorf(codes.InvalidArgument, "working directory exceeds %d bytes", maxWorkingPathBytes)
	}
	if strings.IndexByte(raw, 0) >= 0 {
		return "", status.Error(codes.InvalidArgument, "working directory contains a NUL byte")
	}
	if raw == "" {
		return ".", nil
	}
	if path.IsAbs(raw) {
		return "", status.Error(codes.InvalidArgument, "absolute working directories are not allowed")
	}
	for _, component := range strings.Split(raw, "/") {
		if component == ".." {
			return "", status.Error(codes.InvalidArgument, "parent working directory components are not allowed")
		}
	}
	clean := path.Clean(raw)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", status.Error(codes.InvalidArgument, "working directory escapes the workspace")
	}
	return clean, nil
}

func displayWorkingDirectory(rel string) string {
	if rel == "." {
		return "/"
	}
	return "/" + rel
}

func processPathError(rel string, err error) error {
	code := codes.Internal
	switch {
	case errors.Is(err, fs.ErrNotExist):
		code = codes.NotFound
	case errors.Is(err, fs.ErrPermission), strings.Contains(err.Error(), "path escapes from parent"):
		code = codes.PermissionDenied
	case errors.Is(err, syscall.ENOTDIR):
		code = codes.FailedPrecondition
	}
	return status.Errorf(code, "open working directory %q failed", displayWorkingDirectory(rel))
}

func processSignalError(err error) error {
	switch {
	case errors.Is(err, os.ErrProcessDone), errors.Is(err, syscall.ESRCH):
		return status.Error(codes.FailedPrecondition, "process has already exited")
	case errors.Is(err, syscall.EPERM):
		return status.Error(codes.PermissionDenied, "signal process failed")
	default:
		return status.Error(codes.Internal, "signal process failed")
	}
}

func processStateName(state codev1.ProcessState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "PROCESS_STATE_"))
}

func cloneProcessInfo(info *codev1.ProcessInfo) *codev1.ProcessInfo {
	if info == nil {
		return nil
	}
	return proto.Clone(info).(*codev1.ProcessInfo)
}

func waitForRecords(ctx context.Context, records []*managedProcess) bool {
	for _, record := range records {
		select {
		case <-record.done:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func newUUID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
