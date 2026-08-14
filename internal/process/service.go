package process

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
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
	defaultMaxProcesses      = 16
	maxTrackedProcesses      = 4096
	maxProcessArguments      = 256
	maxArgumentBytes         = 4096
	maxArgumentsBytes        = 64 << 10
	maxWorkingPathBytes      = 4096
	maxCommandBytes          = 4096
	maxEnvironmentVariables  = 256
	maxEnvironmentEntryBytes = 4096
	maxEnvironmentBytes      = 64 << 10
	forcedShutdownWait       = 5 * time.Second
)

var (
	identifierPattern      = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)
	uuidPattern            = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	environmentKeyPattern  = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	nonIdentifierCharacter = regexp.MustCompile(`[^A-Za-z0-9._-]+`)
	errUnsupportedPlatform = errors.New("process management is not supported on this platform")
)

// Config controls the persistent process registry.
type Config struct {
	Workspace        string
	RuntimeDirectory string
	MaxProcesses     int
}

// Service manages process lifecycle and implements the gRPC process API.
type Service struct {
	codev1.UnimplementedProcessServiceServer

	root         *os.Root
	store        *recordStore
	maxProcesses int

	mu          sync.Mutex
	starts      sync.WaitGroup
	closing     bool
	active      int
	processes   map[string]*managedProcess
	activeNames map[string]string
	byName      map[string]string
	byPID       map[int64]string
	order       []string
}

type managedProcess struct {
	info    *codev1.ProcessInfo
	command *runningCommand
	output  *recordOutput
	done    chan struct{}
}

type validatedStart struct {
	name             string
	command          string
	arguments        []string
	workingDirectory string
	ioMode           codev1.ProcessIOMode
	environment      []string
	environmentKeys  []string
}

// New validates the workspace/runtime roots and recovers persistent history.
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
	store, err := openRecordStore(config.RuntimeDirectory)
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	maxProcesses := config.MaxProcesses
	if maxProcesses == 0 {
		maxProcesses = defaultMaxProcesses
	}
	if maxProcesses < 0 || maxProcesses > maxTrackedProcesses {
		_ = root.Close()
		return nil, fmt.Errorf("max processes must be between 1 and %d", maxTrackedProcesses)
	}
	service := &Service{
		root: root, store: store, maxProcesses: maxProcesses,
		processes: make(map[string]*managedProcess), activeNames: make(map[string]string),
		byName: make(map[string]string), byPID: make(map[int64]string),
	}
	loaded, err := store.load()
	if err != nil {
		_ = root.Close()
		return nil, err
	}
	for _, info := range loaded {
		record := &managedProcess{info: info, done: closedChannel()}
		service.processes[info.GetId()] = record
		service.byName[info.GetName()] = info.GetId()
		service.order = append(service.order, info.GetId())
	}
	return service, nil
}

// MaxProcesses returns the active process limit.
func (s *Service) MaxProcesses() int {
	return s.maxProcesses
}

// StartProcess launches a concrete command in its own process group.
func (s *Service) StartProcess(ctx context.Context, request *codev1.StartProcessRequest) (*codev1.StartProcessResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	start, err := s.validateStartRequest(request)
	if err != nil {
		return nil, err
	}
	directory, err := s.openWorkingDirectory(start.workingDirectory)
	if err != nil {
		return nil, err
	}
	defer directory.Close()
	id, err := newUUID()
	if err != nil {
		return nil, status.Error(codes.Internal, "allocate process id failed")
	}
	if start.name == "" {
		start.name = automaticProcessName(start.command, id)
	}
	record := &managedProcess{
		info: &codev1.ProcessInfo{
			Id: id, Name: start.name, IoMode: start.ioMode, State: codev1.ProcessState_PROCESS_STATE_STARTING,
			Command: start.command, Arguments: append([]string(nil), start.arguments...),
			WorkingDirectory: displayWorkingDirectory(start.workingDirectory),
			CreatedAt:        timestamppb.Now(), EnvironmentKeys: append([]string(nil), start.environmentKeys...),
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
	if _, exists := s.activeNames[start.name]; exists {
		s.mu.Unlock()
		return nil, status.Errorf(codes.AlreadyExists, "active process name %q already exists", start.name)
	}
	if err := s.makeHistorySpaceLocked(); err != nil {
		s.mu.Unlock()
		return nil, err
	}
	s.processes[id] = record
	s.activeNames[start.name] = id
	s.byName[start.name] = id
	s.order = append(s.order, id)
	s.active++
	s.starts.Add(1)
	s.mu.Unlock()
	defer s.starts.Done()

	output, err := s.store.create(record.info)
	if err != nil {
		s.mu.Lock()
		s.removeLocked(record)
		s.active--
		s.mu.Unlock()
		return nil, status.Error(codes.Internal, "create persistent process record failed")
	}
	record.output = output
	running, startErr := startCommand(directory, start.command, start.arguments, start.environment, start.ioMode, output)
	if startErr != nil {
		output.close()
		s.mu.Lock()
		failed := cloneProcessInfo(record.info)
		s.mu.Unlock()
		failed.State = codev1.ProcessState_PROCESS_STATE_FAILED
		failed.ExitedAt = timestamppb.Now()
		_ = s.store.writeStatus(failed, "executable could not be started")

		s.mu.Lock()
		record.info = failed
		if s.active > 0 {
			s.active--
		}
		if s.activeNames[start.name] == id {
			delete(s.activeNames, start.name)
		}
		close(record.done)
		s.mu.Unlock()
		if errors.Is(startErr, errUnsupportedPlatform) {
			return nil, status.Errorf(codes.Unimplemented, "process %s: %s", id, startErr)
		}
		return nil, status.Errorf(codes.Internal, "start process failed; record id %s", id)
	}

	s.mu.Lock()
	record.command = running
	runningInfo := cloneProcessInfo(record.info)
	s.mu.Unlock()
	runningInfo.Pid = int64(running.cmd.Process.Pid)
	runningInfo.State = codev1.ProcessState_PROCESS_STATE_RUNNING
	runningInfo.StartedAt = timestamppb.Now()
	if err := s.store.writeStatus(runningInfo, ""); err != nil {
		s.mu.Lock()
		record.info = runningInfo
		s.byPID[runningInfo.GetPid()] = id
		s.mu.Unlock()
		_ = signalProcessGroup(int(runningInfo.GetPid()), codev1.ProcessSignal_PROCESS_SIGNAL_KILL)
		go s.reap(record)
		return nil, status.Errorf(codes.Internal, "persist running process %s failed", id)
	}
	s.mu.Lock()
	record.info = runningInfo
	s.byPID[runningInfo.GetPid()] = id
	s.mu.Unlock()
	go s.reap(record)
	return &codev1.StartProcessResponse{Process: runningInfo}, nil
}

// ListProcesses returns active processes unless all history was requested.
func (s *Service) ListProcesses(ctx context.Context, request *codev1.ListProcessesRequest) (*codev1.ListProcessesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	all := request.GetAll()
	s.mu.Lock()
	defer s.mu.Unlock()
	processes := make([]*codev1.ProcessInfo, 0, len(s.order))
	for _, id := range s.order {
		record := s.processes[id]
		if record == nil || (!all && !isActiveState(record.info.GetState())) {
			continue
		}
		processes = append(processes, cloneProcessInfo(record.info))
	}
	return &codev1.ListProcessesResponse{Processes: processes}, nil
}

// SignalProcess sends an allowed signal to the entire managed process group.
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
	if record.info.GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING || record.command == nil {
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

// Shutdown rejects starts, waits for starts already in progress, terminates
// running groups, and force-kills groups remaining when ctx expires.
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
	s.starts.Wait()

	s.mu.Lock()
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

func (s *Service) validateStartRequest(request *codev1.StartProcessRequest) (validatedStart, error) {
	if request == nil {
		return validatedStart{}, status.Error(codes.InvalidArgument, "start process request is required")
	}
	name := request.GetName()
	if name != "" && !identifierPattern.MatchString(name) {
		return validatedStart{}, status.Errorf(codes.InvalidArgument, "process name must match %s", identifierPattern)
	}
	command := request.GetCommand()
	if command == "" {
		return validatedStart{}, status.Error(codes.InvalidArgument, "process command is required")
	}
	if len(command) > maxCommandBytes {
		return validatedStart{}, status.Errorf(codes.InvalidArgument, "process command exceeds %d bytes", maxCommandBytes)
	}
	if strings.IndexByte(command, 0) >= 0 {
		return validatedStart{}, status.Error(codes.InvalidArgument, "process command contains a NUL byte")
	}
	arguments := request.GetArguments()
	if len(arguments) > maxProcessArguments {
		return validatedStart{}, status.Errorf(codes.InvalidArgument, "process accepts at most %d arguments", maxProcessArguments)
	}
	totalBytes := 0
	for _, argument := range arguments {
		if strings.IndexByte(argument, 0) >= 0 {
			return validatedStart{}, status.Error(codes.InvalidArgument, "process argument contains a NUL byte")
		}
		if len(argument) > maxArgumentBytes {
			return validatedStart{}, status.Errorf(codes.InvalidArgument, "process argument exceeds %d bytes", maxArgumentBytes)
		}
		totalBytes += len(argument)
	}
	if totalBytes > maxArgumentsBytes {
		return validatedStart{}, status.Errorf(codes.InvalidArgument, "process arguments exceed %d bytes", maxArgumentsBytes)
	}
	workingDirectory, err := cleanWorkingDirectory(request.GetWorkingDirectory())
	if err != nil {
		return validatedStart{}, err
	}
	ioMode := request.GetIoMode()
	if ioMode == codev1.ProcessIOMode_PROCESS_IO_MODE_UNSPECIFIED {
		ioMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE
	}
	if ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE && ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
		return validatedStart{}, status.Errorf(codes.InvalidArgument, "unsupported process I/O mode %q", ioMode)
	}
	environment, environmentKeys, err := buildEnvironment(request.GetEnvironment(), ioMode)
	if err != nil {
		return validatedStart{}, err
	}
	return validatedStart{
		name: name, command: command, arguments: append([]string(nil), arguments...), workingDirectory: workingDirectory,
		ioMode: ioMode, environment: environment, environmentKeys: environmentKeys,
	}, nil
}

func buildEnvironment(overrides map[string]string, ioMode codev1.ProcessIOMode) ([]string, []string, error) {
	if len(overrides) > maxEnvironmentVariables {
		return nil, nil, status.Errorf(codes.InvalidArgument, "process accepts at most %d environment overrides", maxEnvironmentVariables)
	}
	total := 0
	keys := make([]string, 0, len(overrides))
	for key, value := range overrides {
		if !environmentKeyPattern.MatchString(key) {
			return nil, nil, status.Errorf(codes.InvalidArgument, "environment key %q is invalid", key)
		}
		if strings.IndexByte(value, 0) >= 0 {
			return nil, nil, status.Errorf(codes.InvalidArgument, "environment value for %q contains a NUL byte", key)
		}
		if len(key)+len(value) > maxEnvironmentEntryBytes {
			return nil, nil, status.Errorf(codes.InvalidArgument, "environment entry %q exceeds %d bytes", key, maxEnvironmentEntryBytes)
		}
		total += len(key) + len(value)
		keys = append(keys, key)
	}
	if total > maxEnvironmentBytes {
		return nil, nil, status.Errorf(codes.InvalidArgument, "environment overrides exceed %d bytes", maxEnvironmentBytes)
	}
	sort.Strings(keys)
	values := make(map[string]string)
	for _, entry := range os.Environ() {
		key, value, ok := strings.Cut(entry, "=")
		if ok {
			values[key] = value
		}
	}
	for key, value := range overrides {
		values[key] = value
	}
	if ioMode == codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
		if _, ok := values["TERM"]; !ok {
			values["TERM"] = "xterm-256color"
		}
	}
	allKeys := make([]string, 0, len(values))
	for key := range values {
		allKeys = append(allKeys, key)
	}
	sort.Strings(allKeys)
	environment := make([]string, 0, len(allKeys))
	for _, key := range allKeys {
		environment = append(environment, key+"="+values[key])
	}
	return environment, keys, nil
}

func automaticProcessName(command, id string) string {
	base := nonIdentifierCharacter.ReplaceAllString(filepath.Base(command), "-")
	base = strings.Trim(base, ".-_")
	if base == "" || base[0] < '0' || (base[0] > '9' && base[0] < 'A') || (base[0] > 'Z' && base[0] < 'a') || base[0] > 'z' {
		base = "process"
	}
	if len(base) > 54 {
		base = base[:54]
	}
	return base + "-" + id[:8]
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
	waitErr := record.command.wait()
	record.command.close()
	record.output.close()
	exitCode, exitSignal := processExit(record.command.cmd.ProcessState, waitErr)

	s.mu.Lock()
	exited := cloneProcessInfo(record.info)
	s.mu.Unlock()
	exited.State = codev1.ProcessState_PROCESS_STATE_EXITED
	exited.ExitedAt = timestamppb.Now()
	exited.ExitCode = exitCode
	exited.ExitSignal = exitSignal
	_ = s.store.writeStatus(exited, "")

	s.mu.Lock()
	record.info = exited
	if s.active > 0 {
		s.active--
	}
	if s.activeNames[record.info.GetName()] == record.info.GetId() {
		delete(s.activeNames, record.info.GetName())
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
		if record != nil && record.info.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING && record.command != nil {
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
			if record != nil && !isActiveState(record.info.GetState()) {
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
	if s.activeNames[record.info.GetName()] == id {
		delete(s.activeNames, record.info.GetName())
	}
	if s.byName[record.info.GetName()] == id {
		delete(s.byName, record.info.GetName())
		for index := len(s.order) - 1; index >= 0; index-- {
			candidate := s.processes[s.order[index]]
			if candidate != nil && candidate.info.GetName() == record.info.GetName() {
				s.byName[record.info.GetName()] = candidate.info.GetId()
				break
			}
		}
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

func isActiveState(state codev1.ProcessState) bool {
	return state == codev1.ProcessState_PROCESS_STATE_STARTING || state == codev1.ProcessState_PROCESS_STATE_RUNNING
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

func closedChannel() chan struct{} {
	result := make(chan struct{})
	close(result)
	return result
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
