package process

import (
	"encoding/json"
	"errors"
	"fmt"
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
}

type recordOutput struct {
	stdoutFile *os.File
	stderrFile *os.File
	stdout     *frameWriter
	stderr     *frameWriter
}

func openRecordStore(directory string) (*recordStore, error) {
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
	return &recordStore{directory: abs}, nil
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
	stdoutFile, err := openOutputFile(filepath.Join(directory, stdoutFileName))
	if err != nil {
		return nil, err
	}
	stderrFile, err := openOutputFile(filepath.Join(directory, stderrFileName))
	if err != nil {
		_ = stdoutFile.Close()
		return nil, err
	}
	if err := syncDirectory(directory); err != nil {
		_ = stdoutFile.Close()
		_ = stderrFile.Close()
		return nil, err
	}
	cleanup = false
	return &recordOutput{
		stdoutFile: stdoutFile,
		stderrFile: stderrFile,
		stdout:     newFrameWriter(stdoutFile),
		stderr:     newFrameWriter(stderrFile),
	}, nil
}

func (o *recordOutput) close() {
	if o == nil {
		return
	}
	_ = o.stdoutFile.Sync()
	_ = o.stderrFile.Sync()
	_ = o.stdoutFile.Close()
	_ = o.stderrFile.Close()
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
