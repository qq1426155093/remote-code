package process

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestServicePersistsPipeOutputMetadataAndStatus(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	service := newProcessServiceAt(t, workspace, runtimeDirectory, 2)
	secret := "value-that-must-not-be-persisted"
	request := helperStartRequest("logged-pipe", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "output")
	request.Environment = map[string]string{"REMOTE_CODE_TEST_SECRET": secret}
	started, err := service.StartProcess(context.Background(), request)
	if err != nil {
		t.Fatalf("StartProcess() error = %v", err)
	}
	exited := waitForProcessExit(t, service, started.GetProcess().GetId())
	if exited.GetExitCode() != 0 || !reflect.DeepEqual(exited.GetEnvironmentKeys(), []string{"REMOTE_CODE_TEST_SECRET"}) || exited.GetCreatedAt() == nil {
		t.Fatalf("exited process = %+v", exited)
	}
	directory := filepath.Join(runtimeDirectory, exited.GetId())
	assertMode(t, directory, 0o700)
	for _, name := range []string{metadataFileName, statusFileName} {
		assertMode(t, filepath.Join(directory, name), 0o600)
	}
	logDirectory := filepath.Join(directory, processLogDirectoryName)
	assertMode(t, logDirectory, 0o700)
	logFiles, err := os.ReadDir(logDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range logFiles {
		assertMode(t, filepath.Join(logDirectory, entry.Name()), 0o600)
	}
	metadata := readTestFile(t, filepath.Join(directory, metadataFileName))
	if bytes.Contains(metadata, []byte(secret)) || !bytes.Contains(metadata, []byte("REMOTE_CODE_TEST_SECRET")) {
		t.Fatalf("metadata secret handling is incorrect: %s", metadata)
	}
	var persistedStatus recordStatus
	if err := json.Unmarshal(readTestFile(t, filepath.Join(directory, statusFileName)), &persistedStatus); err != nil {
		t.Fatal(err)
	}
	if persistedStatus.State != "EXITED" || persistedStatus.ExitCode == nil || *persistedStatus.ExitCode != 0 || persistedStatus.ExitedAt == nil {
		t.Fatalf("status = %+v, want exited code 0", persistedStatus)
	}
	stdout := readTestProcessLog(t, service, exited.GetId(), codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT)
	stderr := readTestProcessLog(t, service, exited.GetId(), codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR)
	if string(stdout) != "stdout:\x00"+secret || string(stderr) != "stderr:\xff"+secret {
		t.Fatalf("stdout=%q stderr=%q", stdout, stderr)
	}

	active, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{})
	if err != nil || len(active.GetProcesses()) != 0 {
		t.Fatalf("ListProcesses(active) = %+v, %v", active, err)
	}
	all, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if err != nil || len(all.GetProcesses()) != 1 {
		t.Fatalf("ListProcesses(all) = %+v, %v", all, err)
	}
}

func TestServicePersistsPTYMergedOutput(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	runtimeDirectory := t.TempDir()
	service := newProcessServiceAt(t, t.TempDir(), runtimeDirectory, 1)
	started, err := service.StartProcess(context.Background(), helperStartRequest(
		"logged-pty", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, "pty-output",
	))
	if err != nil {
		t.Fatal(err)
	}
	info := waitForProcessExit(t, service, started.GetProcess().GetId())
	combined := string(readTestProcessLog(t, service, info.GetId(), codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT))
	if !strings.Contains(combined, "pty-stdout") || !strings.Contains(combined, "pty-stderr") {
		t.Fatalf("PTY stdout log = %q, want both streams", combined)
	}
	stderr := readTestProcessLog(t, service, info.GetId(), codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR)
	if len(stderr) != 0 {
		t.Fatalf("PTY stderr log = %x; want empty", stderr)
	}
}

func TestServicePersistsFailedStartAndGeneratesName(t *testing.T) {
	runtimeDirectory := t.TempDir()
	service := newProcessServiceAt(t, t.TempDir(), runtimeDirectory, 1)
	_, err := service.StartProcess(context.Background(), &codev1.StartProcessRequest{
		Command: filepath.Join(t.TempDir(), "missing-command"),
		IoMode:  codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
	})
	if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), "record id") {
		t.Fatalf("StartProcess(missing) error = %v", err)
	}
	all, listErr := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if listErr != nil || len(all.GetProcesses()) != 1 {
		t.Fatalf("ListProcesses(all) = %+v, %v", all, listErr)
	}
	failed := all.GetProcesses()[0]
	if failed.GetState() != codev1.ProcessState_PROCESS_STATE_FAILED || failed.GetName() == "" || failed.GetPid() != 0 {
		t.Fatalf("failed record = %+v", failed)
	}
	if _, statErr := os.Stat(filepath.Join(runtimeDirectory, failed.GetId(), statusFileName)); statErr != nil {
		t.Fatalf("failed status missing: %v", statErr)
	}
}

func TestServiceRecoversExitedAndMarksActiveRecordLost(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	store, err := openRecordStore(runtimeDirectory)
	if err != nil {
		t.Fatal(err)
	}
	created := time.Now().Add(-time.Minute)
	for _, test := range []struct {
		id    string
		name  string
		state codev1.ProcessState
	}{
		{id: "11111111-1111-4111-8111-111111111111", name: "old-exited", state: codev1.ProcessState_PROCESS_STATE_EXITED},
		{id: "22222222-2222-4222-8222-222222222222", name: "old-running", state: codev1.ProcessState_PROCESS_STATE_RUNNING},
	} {
		info := &codev1.ProcessInfo{
			Id: test.id, Name: test.name, Pid: 12345, IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
			State: test.state, Command: "/bin/true", WorkingDirectory: "/", CreatedAt: timestamppb.New(created),
			StartedAt: timestamppb.New(created.Add(time.Second)),
		}
		if test.state == codev1.ProcessState_PROCESS_STATE_EXITED {
			code := int32(0)
			info.ExitCode = &code
			info.ExitedAt = timestamppb.New(created.Add(2 * time.Second))
		}
		output, createErr := store.create(info)
		if createErr != nil {
			t.Fatal(createErr)
		}
		output.close()
		if writeErr := store.writeStatus(info, ""); writeErr != nil {
			t.Fatal(writeErr)
		}
		created = created.Add(time.Second)
	}
	service := newProcessServiceAt(t, workspace, runtimeDirectory, 1)
	all, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if err != nil || len(all.GetProcesses()) != 2 {
		t.Fatalf("ListProcesses(all) = %+v, %v", all, err)
	}
	if all.GetProcesses()[0].GetState() != codev1.ProcessState_PROCESS_STATE_EXITED || all.GetProcesses()[1].GetState() != codev1.ProcessState_PROCESS_STATE_LOST {
		t.Fatalf("recovered states = %s, %s", all.GetProcesses()[0].GetState(), all.GetProcesses()[1].GetState())
	}
	_, err = service.SignalProcess(context.Background(), &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: all.GetProcesses()[1].GetId()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_KILL,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("SignalProcess(lost) code = %s", status.Code(err))
	}
	var persisted recordStatus
	if err := json.Unmarshal(readTestFile(t, filepath.Join(runtimeDirectory, all.GetProcesses()[1].GetId(), statusFileName)), &persisted); err != nil {
		t.Fatal(err)
	}
	if persisted.State != "LOST" || persisted.ExitedAt == nil {
		t.Fatalf("persisted recovered status = %+v", persisted)
	}
}

func TestServiceReloadsExitedProcessAfterRestart(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	first, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxProcesses: 1})
	if err != nil {
		t.Fatal(err)
	}
	started, err := first.StartProcess(context.Background(), helperStartRequest(
		"restart-history", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "7",
	))
	if err != nil {
		t.Fatal(err)
	}
	exited := waitForProcessExit(t, first, started.GetProcess().GetId())
	if exited.GetExitCode() != 7 {
		t.Fatalf("exit code = %d, want 7", exited.GetExitCode())
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxProcesses: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = second.Shutdown(context.Background())
		_ = second.Close()
	}()
	all, err := second.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if err != nil || len(all.GetProcesses()) != 1 {
		t.Fatalf("ListProcesses(restarted) = %+v, %v", all, err)
	}
	got := all.GetProcesses()[0]
	if got.GetId() != exited.GetId() || got.GetState() != codev1.ProcessState_PROCESS_STATE_EXITED || got.GetExitCode() != 7 {
		t.Fatalf("reloaded process = %+v", got)
	}
}

func newProcessServiceAt(t *testing.T, workspace, runtimeDirectory string, maxProcesses int) *Service {
	t.Helper()
	service, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxProcesses: maxProcesses})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
		_ = service.Close()
	})
	return service
}

func readTestFile(t *testing.T, name string) []byte {
	t.Helper()
	value, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func readTestFrames(t *testing.T, name string) []LogFrame {
	t.Helper()
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	frames, err := ReadLogFrames(file)
	if err != nil {
		t.Fatal(err)
	}
	return frames
}

func joinPayloads(frames []LogFrame) []byte {
	var output []byte
	for _, frame := range frames {
		output = append(output, frame.Payload...)
	}
	return output
}

func readTestProcessLog(t *testing.T, service *Service, id string, selected codev1.ProcessLogStream) []byte {
	t.Helper()
	service.mu.Lock()
	record := service.processes[id]
	service.mu.Unlock()
	if record == nil || record.logs == nil {
		t.Fatalf("process %s has no log", id)
	}
	prepared, err := record.logs.prepareOffset(0)
	if err != nil {
		t.Fatal(err)
	}
	var output []byte
	err = record.logs.readRange(prepared.start, prepared.end, prepared.earliest, map[codev1.ProcessLogStream]bool{selected: true}, nil, func(record storedProcessLogRecord, _ bool) error {
		output = append(output, record.payload...)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return output
}

func assertMode(t *testing.T, name string, want os.FileMode) {
	t.Helper()
	info, err := os.Stat(name)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != want {
		t.Fatalf("mode(%s) = %04o, want %04o", name, info.Mode().Perm(), want)
	}
}
