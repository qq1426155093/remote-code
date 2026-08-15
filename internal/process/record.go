package process

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	recordSchemaVersion = 1
	metadataFileName    = "metadata.json"
	statusFileName      = "status.json"
	stdoutFileName      = "stdout.log"
	stderrFileName      = "stderr.log"
	maxRecordJSONBytes  = 1 << 20
)

type recordMetadata struct {
	SchemaVersion    int       `json:"schema_version"`
	ID               string    `json:"id"`
	Name             string    `json:"name"`
	IOMode           string    `json:"io_mode"`
	Command          string    `json:"command"`
	Arguments        []string  `json:"arguments,omitempty"`
	WorkingDirectory string    `json:"working_directory"`
	EnvironmentKeys  []string  `json:"environment_keys,omitempty"`
	CreatedAt        time.Time `json:"created_at"`
}

type recordStatus struct {
	State      string     `json:"state"`
	PID        int64      `json:"pid,omitempty"`
	StartedAt  *time.Time `json:"started_at,omitempty"`
	ExitedAt   *time.Time `json:"exited_at,omitempty"`
	ExitCode   *int32     `json:"exit_code,omitempty"`
	ExitSignal *int32     `json:"exit_signal,omitempty"`
	Error      string     `json:"error,omitempty"`
}

type recordStore struct {
	directory string
	logConfig LogConfig
}

type recordOutput struct {
	log    *processLog
	stdout io.Writer
	stderr io.Writer
}

func openRecordStore(directory string, configurations ...LogConfig) (*recordStore, error) {
	if directory == "" {
		return nil, errors.New("runtime directory is required")
	}
	abs, err := filepath.Abs(directory)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime directory: %w", err)
	}
	if err := os.MkdirAll(abs, 0o700); err != nil {
		return nil, fmt.Errorf("create runtime directory: %w", err)
	}
	info, err := os.Lstat(abs)
	if err != nil {
		return nil, fmt.Errorf("stat runtime directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("runtime directory must be a directory, not a symbolic link")
	}
	if err := os.Chmod(abs, 0o700); err != nil {
		return nil, fmt.Errorf("secure runtime directory: %w", err)
	}
	var logConfig LogConfig
	if len(configurations) > 0 {
		logConfig = configurations[0]
	}
	logConfig, err = normalizeLogConfig(logConfig)
	if err != nil {
		return nil, err
	}
	return &recordStore{directory: abs, logConfig: logConfig}, nil
}

func (s *recordStore) create(info *codev1.ProcessInfo) (*recordOutput, error) {
	directory := s.processDirectory(info.GetId())
	if err := os.Mkdir(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create process record directory: %w", err)
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(directory)
		}
	}()
	metadata := metadataFromInfo(info)
	if err := writeExclusiveJSON(filepath.Join(directory, metadataFileName), metadata); err != nil {
		return nil, err
	}
	if err := atomicWriteJSON(directory, statusFileName, statusFromInfo(info, "")); err != nil {
		return nil, err
	}
	processLog, err := newProcessLog(filepath.Join(directory, processLogDirectoryName), s.logConfig)
	if err != nil {
		return nil, err
	}
	if err := syncDirectory(directory); err != nil {
		_ = processLog.finalize()
		return nil, err
	}
	cleanup = false
	return &recordOutput{
		log:    processLog,
		stdout: processLog.stdoutWriter(),
		stderr: processLog.stderrWriter(),
	}, nil
}

func (o *recordOutput) close() {
	if o == nil {
		return
	}
	_ = o.log.finalize()
}

func (s *recordStore) openLog(info *codev1.ProcessInfo) (*processLog, error) {
	directory := s.processDirectory(info.GetId())
	logDirectory := filepath.Join(directory, processLogDirectoryName)
	if _, err := os.Lstat(logDirectory); err == nil {
		logs, openErr := openProcessLog(logDirectory, s.logConfig, !isActiveState(info.GetState()))
		if openErr == nil {
			removeLegacyLogFiles(directory)
		}
		return logs, openErr
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	return s.migrateLegacyLog(directory)
}

func (s *recordStore) migrateLegacyLog(directory string) (*processLog, error) {
	type legacyFrame struct {
		stream codev1.ProcessLogStream
		frame  LogFrame
		order  int
	}
	frames := make([]legacyFrame, 0)
	legacyComplete := true
	for _, source := range []struct {
		name   string
		stream codev1.ProcessLogStream
		order  int
	}{
		{name: stdoutFileName, stream: codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, order: 0},
		{name: stderrFileName, stream: codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR, order: 1},
	} {
		file, err := openExistingProcessLogFile(filepath.Join(directory, source.name), os.O_RDONLY)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("open legacy process log: %w", err)
		}
		decoded, readErr := ReadLogFrames(file)
		info, statErr := file.Stat()
		_ = file.Close()
		if readErr != nil {
			return nil, readErr
		}
		if statErr != nil {
			return nil, statErr
		}
		var decodedBytes int64
		for _, frame := range decoded {
			decodedBytes += logFrameHeaderBytes + int64(len(frame.Payload))
		}
		if decodedBytes != info.Size() {
			legacyComplete = false
		}
		for _, frame := range decoded {
			frames = append(frames, legacyFrame{stream: source.stream, frame: frame, order: source.order})
		}
	}
	sort.SliceStable(frames, func(i, j int) bool {
		if frames[i].frame.Timestamp.Equal(frames[j].frame.Timestamp) {
			return frames[i].order < frames[j].order
		}
		return frames[i].frame.Timestamp.Before(frames[j].frame.Timestamp)
	})
	temporaryRoot, err := os.MkdirTemp(directory, ".logs-migrate-")
	if err != nil {
		return nil, fmt.Errorf("create legacy log migration directory: %w", err)
	}
	defer os.RemoveAll(temporaryRoot)
	temporaryLogDirectory := filepath.Join(temporaryRoot, processLogDirectoryName)
	log, err := newProcessLog(temporaryLogDirectory, s.logConfig)
	if err != nil {
		return nil, err
	}
	current := time.Unix(0, 0)
	log.now = func() time.Time { return current }
	for _, value := range frames {
		current = value.frame.Timestamp
		if _, err := log.write(value.stream, value.frame.Payload); err != nil {
			return nil, err
		}
	}
	log.now = time.Now
	log.complete = legacyComplete
	if err := log.finalize(); err != nil {
		return nil, err
	}
	logDirectory := filepath.Join(directory, processLogDirectoryName)
	if err := os.Rename(temporaryLogDirectory, logDirectory); err != nil {
		return nil, fmt.Errorf("install migrated process log: %w", err)
	}
	if err := syncDirectory(directory); err != nil {
		return nil, err
	}
	migrated, err := openProcessLog(logDirectory, s.logConfig, true)
	if err != nil {
		return nil, fmt.Errorf("verify migrated process log: %w", err)
	}
	removeLegacyLogFiles(directory)
	return migrated, nil
}

func removeLegacyLogFiles(directory string) {
	for _, name := range []string{stdoutFileName, stderrFileName} {
		_ = os.Remove(filepath.Join(directory, name))
	}
}

func (s *recordStore) writeStatus(info *codev1.ProcessInfo, message string) error {
	return atomicWriteJSON(s.processDirectory(info.GetId()), statusFileName, statusFromInfo(info, message))
}

func (s *recordStore) load() ([]*codev1.ProcessInfo, error) {
	entries, err := os.ReadDir(s.directory)
	if err != nil {
		return nil, fmt.Errorf("read runtime directory: %w", err)
	}
	loaded := make([]*codev1.ProcessInfo, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || !uuidPattern.MatchString(entry.Name()) {
			continue
		}
		directory := s.processDirectory(entry.Name())
		info, err := loadRecord(directory)
		if err != nil {
			log.Printf("remote-code-controller: skip invalid process record %s", entry.Name())
			continue
		}
		if info.GetState() == codev1.ProcessState_PROCESS_STATE_STARTING || info.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING {
			info.State = codev1.ProcessState_PROCESS_STATE_LOST
			info.ExitedAt = timestamppb.Now()
			info.ExitCode = nil
			info.ExitSignal = nil
			if err := s.writeStatus(info, "controller restarted before process exit was recorded"); err != nil {
				return nil, fmt.Errorf("persist lost process %s: %w", info.GetId(), err)
			}
		}
		loaded = append(loaded, info)
	}
	sort.Slice(loaded, func(i, j int) bool {
		left, right := loaded[i].GetCreatedAt().AsTime(), loaded[j].GetCreatedAt().AsTime()
		if left.Equal(right) {
			return loaded[i].GetId() < loaded[j].GetId()
		}
		return left.Before(right)
	})
	if len(loaded) > maxTrackedProcesses {
		loaded = loaded[len(loaded)-maxTrackedProcesses:]
	}
	return loaded, nil
}

func loadRecord(directory string) (*codev1.ProcessInfo, error) {
	info, err := os.Lstat(directory)
	if err != nil || !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("invalid process record directory")
	}
	var metadata recordMetadata
	if err := readJSON(filepath.Join(directory, metadataFileName), &metadata); err != nil {
		return nil, err
	}
	var state recordStatus
	if err := readJSON(filepath.Join(directory, statusFileName), &state); err != nil {
		return nil, err
	}
	if metadata.SchemaVersion != recordSchemaVersion || !uuidPattern.MatchString(metadata.ID) || metadata.ID != filepath.Base(directory) {
		return nil, errors.New("invalid process record metadata")
	}
	ioMode, ok := parseStoredIOMode(metadata.IOMode)
	if !ok {
		return nil, errors.New("invalid process record I/O mode")
	}
	processState, ok := parseStoredState(state.State)
	if !ok {
		return nil, errors.New("invalid process record state")
	}
	result := &codev1.ProcessInfo{
		Id: metadata.ID, Name: metadata.Name, Pid: state.PID, IoMode: ioMode,
		State: processState, Command: metadata.Command, Arguments: append([]string(nil), metadata.Arguments...),
		WorkingDirectory: metadata.WorkingDirectory, EnvironmentKeys: append([]string(nil), metadata.EnvironmentKeys...),
		CreatedAt: timestamppb.New(metadata.CreatedAt), ExitCode: state.ExitCode, ExitSignal: state.ExitSignal,
	}
	if state.StartedAt != nil {
		result.StartedAt = timestamppb.New(*state.StartedAt)
	}
	if state.ExitedAt != nil {
		result.ExitedAt = timestamppb.New(*state.ExitedAt)
	}
	if !identifierPattern.MatchString(result.GetName()) || result.GetCommand() == "" ||
		result.GetCreatedAt() == nil || !result.GetCreatedAt().IsValid() || !validStoredState(result.GetState()) {
		return nil, errors.New("incomplete process record")
	}
	for _, key := range result.GetEnvironmentKeys() {
		if !environmentKeyPattern.MatchString(key) {
			return nil, errors.New("invalid process record environment key")
		}
	}
	return result, nil
}

func validStoredState(state codev1.ProcessState) bool {
	return state >= codev1.ProcessState_PROCESS_STATE_STARTING && state <= codev1.ProcessState_PROCESS_STATE_LOST
}

func metadataFromInfo(info *codev1.ProcessInfo) recordMetadata {
	return recordMetadata{
		SchemaVersion: recordSchemaVersion, ID: info.GetId(), Name: info.GetName(), IOMode: storedIOMode(info.GetIoMode()),
		Command: info.GetCommand(), Arguments: append([]string(nil), info.GetArguments()...),
		WorkingDirectory: info.GetWorkingDirectory(), EnvironmentKeys: append([]string(nil), info.GetEnvironmentKeys()...),
		CreatedAt: info.GetCreatedAt().AsTime(),
	}
}

func statusFromInfo(info *codev1.ProcessInfo, message string) recordStatus {
	result := recordStatus{
		State: storedState(info.GetState()), PID: info.GetPid(), ExitCode: info.ExitCode, ExitSignal: info.ExitSignal, Error: message,
	}
	if value := info.GetStartedAt(); value != nil && value.IsValid() {
		timestamp := value.AsTime()
		result.StartedAt = &timestamp
	}
	if value := info.GetExitedAt(); value != nil && value.IsValid() {
		timestamp := value.AsTime()
		result.ExitedAt = &timestamp
	}
	return result
}

func storedIOMode(mode codev1.ProcessIOMode) string {
	return strings.TrimPrefix(mode.String(), "PROCESS_IO_MODE_")
}

func parseStoredIOMode(value string) (codev1.ProcessIOMode, bool) {
	mode, ok := codev1.ProcessIOMode_value["PROCESS_IO_MODE_"+value]
	return codev1.ProcessIOMode(mode), ok
}

func storedState(state codev1.ProcessState) string {
	return strings.TrimPrefix(state.String(), "PROCESS_STATE_")
}

func parseStoredState(value string) (codev1.ProcessState, bool) {
	state, ok := codev1.ProcessState_value["PROCESS_STATE_"+value]
	return codev1.ProcessState(state), ok
}

func writeExclusiveJSON(name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode process record: %w", err)
	}
	data = append(data, '\n')
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create process record: %w", err)
	}
	if err := writeFull(file, data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write process record: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync process record: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close process record: %w", err)
	}
	return nil
}

func atomicWriteJSON(directory, name string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode process status: %w", err)
	}
	data = append(data, '\n')
	temporary, err := os.CreateTemp(directory, ".status-*")
	if err != nil {
		return fmt.Errorf("create process status temporary file: %w", err)
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("secure process status: %w", err)
	}
	if err := writeFull(temporary, data); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write process status: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync process status: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close process status: %w", err)
	}
	if err := os.Rename(temporaryName, filepath.Join(directory, name)); err != nil {
		return fmt.Errorf("replace process status: %w", err)
	}
	return syncDirectory(directory)
}

func openOutputFile(name string) (*os.File, error) {
	file, err := os.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create process output log: %w", err)
	}
	return file, nil
}

func readJSON(name string, destination any) error {
	info, err := os.Lstat(name)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("process record is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > maxRecordJSONBytes {
		return errors.New("process record size is invalid")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(data, destination); err != nil {
		return err
	}
	return nil
}

func syncDirectory(directory string) error {
	file, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer file.Close()
	return file.Sync()
}

func (s *recordStore) processDirectory(id string) string {
	return filepath.Join(s.directory, id)
}
