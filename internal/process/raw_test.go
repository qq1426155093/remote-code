package process

import (
	"bytes"
	"context"
	"io"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// rawHelperArguments builds a command line that re-enters the test binary as
// the process helper from a raw start.
func rawHelperArguments(arguments ...string) []string {
	helperArguments := []string{"-test.run=^TestProcessHelper$", "--"}
	return append(helperArguments, arguments...)
}

func TestStartRawProcess_EchoRoundTrip(t *testing.T) {
	service := newTestProcessService(t, t.TempDir(), 4)
	t.Setenv(helperEnvironment, "1")
	raw, err := service.StartRawProcess(context.Background(), RawProcessSpec{
		Name:      "raw-echo",
		Command:   os.Args[0],
		Arguments: rawHelperArguments("echo"),
	})
	if err != nil {
		t.Fatalf("StartRawProcess() error = %v", err)
	}
	if raw.Process.GetId() == "" || raw.Process.GetPid() <= 0 {
		t.Fatalf("raw process info = %+v, want id and pid", raw.Process)
	}
	if raw.Process.GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("raw process state = %s, want RUNNING", raw.Process.GetState())
	}
	if raw.Process.GetInputMode() != codev1.ProcessInputMode_PROCESS_INPUT_MODE_DISABLED {
		t.Fatalf("raw process input mode = %s, want DISABLED on the gRPC surface", raw.Process.GetInputMode())
	}
	if _, err := raw.Stdin.Write([]byte("ping\npong\n")); err != nil {
		t.Fatalf("Write(stdin) error = %v", err)
	}
	if err := raw.Stdin.Close(); err != nil {
		t.Fatalf("Close(stdin) error = %v", err)
	}
	data, err := io.ReadAll(raw.Stdout)
	if err != nil {
		t.Fatalf("ReadAll(stdout) error = %v", err)
	}
	if string(data) != "ping\npong\n" {
		t.Fatalf("echo round trip = %q, want the written lines back", truncateForTest(string(data)))
	}
	select {
	case <-raw.Done:
	case <-time.After(5 * time.Second):
		t.Fatal("raw process did not exit")
	}
	exited := waitForProcessExit(t, service, raw.Process.GetId())
	if exited.GetExitCode() != 0 {
		t.Fatalf("exit code = %d, want 0", exited.GetExitCode())
	}
}

func TestStartRawProcess_ConcurrentStdinWrites(t *testing.T) {
	service := newTestProcessService(t, t.TempDir(), 4)
	t.Setenv(helperEnvironment, "1")
	raw, err := service.StartRawProcess(context.Background(), RawProcessSpec{
		Name:      "raw-concurrent",
		Command:   os.Args[0],
		Arguments: rawHelperArguments("echo"),
	})
	if err != nil {
		t.Fatalf("StartRawProcess() error = %v", err)
	}
	const writers = 4
	var group sync.WaitGroup
	for i := 0; i < writers; i++ {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := raw.Stdin.Write([]byte("line\n")); err != nil {
				t.Errorf("concurrent Write() error = %v", err)
			}
		}()
	}
	group.Wait()
	if err := raw.Stdin.Close(); err != nil {
		t.Fatalf("Close(stdin) error = %v", err)
	}
	data, err := io.ReadAll(raw.Stdout)
	if err != nil {
		t.Fatalf("ReadAll(stdout) error = %v", err)
	}
	if got := strings.Count(string(data), "line\n"); got != writers {
		t.Fatalf("echoed %d lines, want %d", got, writers)
	}
	<-raw.Done
	waitForProcessExit(t, service, raw.Process.GetId())
}

func TestStartRawProcess_StdoutNotPersistedToLogs(t *testing.T) {
	service := newTestProcessService(t, t.TempDir(), 4)
	t.Setenv(helperEnvironment, "1")
	raw, err := service.StartRawProcess(context.Background(), RawProcessSpec{
		Name:      "raw-logs",
		Command:   os.Args[0],
		Arguments: rawHelperArguments("output"),
		Environment: map[string]string{
			"REMOTE_CODE_TEST_SECRET": "raw-secret",
		},
	})
	if err != nil {
		t.Fatalf("StartRawProcess() error = %v", err)
	}
	data, err := io.ReadAll(raw.Stdout)
	if err != nil {
		t.Fatalf("ReadAll(stdout) error = %v", err)
	}
	if !bytes.HasPrefix(data, []byte("stdout:\x00raw-secret")) {
		t.Fatalf("stdout = %q, want helper stdout payload", truncateForTest(string(data)))
	}
	<-raw.Done
	waitForProcessExit(t, service, raw.Process.GetId())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := newCaptureProcessLogStream(ctx)
	err = service.ObserveProcessLogs(&codev1.ObserveProcessLogsRequest{
		ProcessId: raw.Process.GetId(),
	}, stream)
	if err != nil {
		t.Fatalf("ObserveProcessLogs() error = %v", err)
	}
	var stdoutBytes, stderrBytes bytes.Buffer
	for _, message := range stream.snapshot() {
		payload := message.GetChunk()
		if payload == nil {
			continue
		}
		switch payload.GetStream() {
		case codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT:
			stdoutBytes.Write(payload.GetData())
		case codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR:
			stderrBytes.Write(payload.GetData())
		}
	}
	if stdoutBytes.Len() != 0 {
		t.Fatalf("stdout log = %q, want empty (raw stdout must not be persisted)", truncateForTest(stdoutBytes.String()))
	}
	if !bytes.Contains(stderrBytes.Bytes(), []byte("stderr:")) {
		t.Fatalf("stderr log = %q, want helper stderr payload", truncateForTest(stderrBytes.String()))
	}
}

func TestStartRawProcess_RejectsStreamProcessInput(t *testing.T) {
	service := newTestProcessService(t, t.TempDir(), 4)
	t.Setenv(helperEnvironment, "1")
	raw, err := service.StartRawProcess(context.Background(), RawProcessSpec{
		Name:      "raw-input",
		Command:   os.Args[0],
		Arguments: rawHelperArguments("sleep"),
	})
	if err != nil {
		t.Fatalf("StartRawProcess() error = %v", err)
	}
	defer stopProcess(t, service, raw.Process)

	record, input, err := service.acquireProcessInput(&codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: raw.Process.GetId()}})
	if err == nil {
		t.Fatal("acquireProcessInput() succeeded for a raw process")
	}
	if record != nil || input != nil {
		t.Fatalf("acquireProcessInput() returned %v, %v for a raw process", record, input)
	}
	if rpcerror.ReasonOf(err) != rpcerror.ProcessInputRaw {
		t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.ProcessInputRaw)
	}
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition", status.Code(err))
	}
}

func TestStartRawProcess_Validation(t *testing.T) {
	service := newTestProcessService(t, t.TempDir(), 4)
	if _, err := service.StartRawProcess(context.Background(), RawProcessSpec{}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StartRawProcess(empty) error = %v, want InvalidArgument", err)
	}
	if _, err := service.StartRawProcess(context.Background(), RawProcessSpec{
		Name: "raw-bad", Command: os.Args[0], Arguments: rawHelperArguments(), WorkingDirectory: "../escape",
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("StartRawProcess(escape) error = %v, want InvalidArgument", err)
	}
}

func truncateForTest(value string) string {
	if len(value) > 64 {
		return value[:64] + "..."
	}
	return value
}
