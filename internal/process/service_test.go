package process

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const helperEnvironment = "REMOTE_CODE_PROCESS_HELPER"

func TestProcessHelper(t *testing.T) {
	if os.Getenv(helperEnvironment) != "1" {
		return
	}
	arguments := os.Args
	for len(arguments) > 0 && arguments[0] != "--" {
		arguments = arguments[1:]
	}
	if len(arguments) < 2 {
		os.Exit(90)
	}
	arguments = arguments[1:]
	switch arguments[0] {
	case "check":
		if len(arguments) != 3 {
			os.Exit(91)
		}
		info, err := os.Stdin.Stat()
		if err != nil {
			os.Exit(92)
		}
		switch arguments[1] {
		case "pipe":
			if info.Mode()&os.ModeNamedPipe == 0 {
				os.Exit(93)
			}
		case "pty":
			if info.Mode()&os.ModeCharDevice == 0 {
				os.Exit(94)
			}
		default:
			os.Exit(95)
		}
		workingDirectory, err := os.Getwd()
		if err != nil || filepath.Base(workingDirectory) != arguments[2] {
			os.Exit(96)
		}
		os.Exit(0)
	case "exit":
		if len(arguments) != 2 {
			os.Exit(97)
		}
		code, err := strconv.Atoi(arguments[1])
		if err != nil {
			os.Exit(98)
		}
		os.Exit(code)
	case "output":
		if len(arguments) != 1 || os.Getenv("REMOTE_CODE_TEST_SECRET") == "" {
			os.Exit(109)
		}
		secret := os.Getenv("REMOTE_CODE_TEST_SECRET")
		_, _ = os.Stdout.Write(append([]byte("stdout:\x00"), []byte(secret)...))
		_, _ = os.Stderr.Write(append([]byte("stderr:\xff"), []byte(secret)...))
		os.Exit(0)
	case "pty-output":
		_, _ = os.Stdout.Write([]byte("pty-stdout\n"))
		_, _ = os.Stderr.Write([]byte("pty-stderr\n"))
		os.Exit(0)
	case "sleep":
		for {
			time.Sleep(time.Second)
		}
	case "ignore-term":
		if len(arguments) != 2 {
			os.Exit(100)
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGTERM)
		defer signal.Stop(signals)
		if err := os.WriteFile(arguments[1], []byte("ready"), 0o600); err != nil {
			os.Exit(101)
		}
		for {
			<-signals
		}
	case "group-parent":
		if len(arguments) != 4 {
			os.Exit(102)
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGUSR1)
		defer signal.Stop(signals)
		child := exec.Command(os.Args[0], "-test.run=^TestProcessHelper$", "--", "group-child", arguments[2], arguments[3])
		child.Stdin = nil
		child.Stdout = io.Discard
		child.Stderr = io.Discard
		if err := child.Start(); err != nil {
			os.Exit(103)
		}
		waitForHelperFile(arguments[2])
		if err := os.WriteFile(arguments[1], []byte("ready"), 0o600); err != nil {
			os.Exit(104)
		}
		for {
			<-signals
		}
	case "group-child":
		if len(arguments) != 3 {
			os.Exit(105)
		}
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, syscall.SIGUSR1)
		defer signal.Stop(signals)
		if err := os.WriteFile(arguments[1], []byte("ready"), 0o600); err != nil {
			os.Exit(106)
		}
		for range signals {
			if err := os.WriteFile(arguments[2], []byte("received"), 0o600); err != nil {
				os.Exit(107)
			}
		}
	default:
		os.Exit(99)
	}
}

func TestServiceStartsPipeAndPTYProcesses(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "work"), 0o755); err != nil {
		t.Fatal(err)
	}
	service := newTestProcessService(t, workspace, 2)
	ctx := context.Background()

	pipe, err := service.StartProcess(ctx, helperStartRequest("pipe-check", "work", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "check", "pipe", "work"))
	if err != nil {
		t.Fatalf("StartProcess(pipe) error = %v", err)
	}
	assertRunningProcess(t, pipe.GetProcess(), "pipe-check", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE)
	pipeExit := waitForProcessExit(t, service, pipe.GetProcess().GetId())
	if pipeExit.ExitCode == nil || pipeExit.GetExitCode() != 0 || pipeExit.ExitSignal != nil {
		t.Fatalf("pipe exit = %+v, want code 0", pipeExit)
	}

	terminal, err := service.StartProcess(ctx, helperStartRequest("pty-check", "work", codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, "check", "pty", "work"))
	if err != nil {
		t.Fatalf("StartProcess(pty) error = %v", err)
	}
	assertRunningProcess(t, terminal.GetProcess(), "pty-check", codev1.ProcessIOMode_PROCESS_IO_MODE_PTY)
	terminalExit := waitForProcessExit(t, service, terminal.GetProcess().GetId())
	if terminalExit.ExitCode == nil || terminalExit.GetExitCode() != 0 {
		t.Fatalf("pty exit = %+v, want code 0", terminalExit)
	}

	listed, err := service.ListProcesses(ctx, &codev1.ListProcessesRequest{All: true})
	if err != nil {
		t.Fatalf("ListProcesses() error = %v", err)
	}
	if len(listed.GetProcesses()) != 2 || listed.GetProcesses()[0].GetName() != "pipe-check" || listed.GetProcesses()[1].GetName() != "pty-check" {
		t.Fatalf("ListProcesses() = %+v, want stable start order", listed.GetProcesses())
	}
	reused, err := service.StartProcess(ctx, helperStartRequest("pipe-check", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0"))
	if err != nil {
		t.Errorf("StartProcess(reused exited name) error = %v", err)
	} else {
		_ = waitForProcessExit(t, service, reused.GetProcess().GetId())
	}
}

func TestServiceSignalsByNameIDAndPIDAndReaps(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	service := newTestProcessService(t, t.TempDir(), 2)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	byName, err := service.StartProcess(ctx, helperStartRequest("term-target", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep"))
	if err != nil {
		t.Fatalf("StartProcess(term target) error = %v", err)
	}
	terminated, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: "term-target"}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_TERM,
		Wait:    true,
	})
	if err != nil {
		t.Fatalf("SignalProcess(TERM by name) error = %v", err)
	}
	if terminated.GetProcess().GetState() != codev1.ProcessState_PROCESS_STATE_EXITED || terminated.GetProcess().GetExitSignal() != int32(syscall.SIGTERM) {
		t.Fatalf("SignalProcess(TERM) = %+v, want exited by SIGTERM", terminated.GetProcess())
	}
	if err := syscall.Kill(int(byName.GetProcess().GetPid()), 0); !errors.Is(err, syscall.ESRCH) {
		t.Errorf("reaped pid %d still exists: %v", byName.GetProcess().GetPid(), err)
	}

	byPID, err := service.StartProcess(ctx, helperStartRequest("kill-target", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, "sleep"))
	if err != nil {
		t.Fatalf("StartProcess(kill target) error = %v", err)
	}
	if _, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: byPID.GetProcess().GetId()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_STOP,
	}); err != nil {
		t.Fatalf("SignalProcess(STOP by id) error = %v", err)
	}
	if _, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: byPID.GetProcess().GetName()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_CONT,
	}); err != nil {
		t.Fatalf("SignalProcess(CONT by name) error = %v", err)
	}
	killed, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Pid{Pid: byPID.GetProcess().GetPid()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_KILL,
		Wait:    true,
	})
	if err != nil {
		t.Fatalf("SignalProcess(KILL by pid) error = %v", err)
	}
	if killed.GetProcess().GetExitSignal() != int32(syscall.SIGKILL) {
		t.Errorf("SignalProcess(KILL).ExitSignal = %d, want %d", killed.GetProcess().GetExitSignal(), syscall.SIGKILL)
	}

	_, err = service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: byPID.GetProcess().GetId()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_TERM,
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Errorf("SignalProcess(exited by id) code = %s, want FailedPrecondition", status.Code(err))
	}
}

func TestServiceRejectsDuplicateActiveName(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	service := newTestProcessService(t, t.TempDir(), 2)
	started, err := service.StartProcess(context.Background(), helperStartRequest(
		"duplicate", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep",
	))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.StartProcess(context.Background(), helperStartRequest(
		"duplicate", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep",
	)); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("StartProcess(duplicate active name) code = %s", status.Code(err))
	}
	stopProcess(t, service, started.GetProcess())
}

func TestServiceRejectsDeletingActiveProcess(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	service := newTestProcessService(t, t.TempDir(), 1)
	started, err := service.StartProcess(context.Background(), helperStartRequest(
		"active-delete", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep",
	))
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.DeleteProcess(context.Background(), &codev1.DeleteProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: started.GetProcess().GetName()}},
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("DeleteProcess(active) code = %s, want FailedPrecondition", status.Code(err))
	}
	listed, listErr := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{})
	if listErr != nil || len(listed.GetProcesses()) != 1 || listed.GetProcesses()[0].GetId() != started.GetProcess().GetId() {
		t.Fatalf("active process after rejected delete = %+v, %v", listed, listErr)
	}
	stopProcess(t, service, started.GetProcess())
}

func TestServiceValidatesStartsAndActiveLimit(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	service := newTestProcessService(t, workspace, 1)
	ctx := context.Background()

	invalid := []*codev1.StartProcessRequest{
		helperStartRequest("bad name", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0"),
		{Name: "missing-command", Command: "", IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE},
		helperStartRequest("absolute-cwd", "/tmp", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0"),
		helperStartRequest("parent-cwd", "../outside", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0"),
		{Name: "bad-env", Command: os.Args[0], Environment: map[string]string{"BAD=KEY": "value"}},
	}
	for _, request := range invalid {
		if _, err := service.StartProcess(ctx, request); status.Code(err) != codes.InvalidArgument && status.Code(err) != codes.FailedPrecondition {
			t.Errorf("StartProcess(%q) code = %s, want InvalidArgument or FailedPrecondition", request.GetName(), status.Code(err))
		}
	}
	if _, err := service.StartProcess(ctx, helperStartRequest("escape-cwd", "escape", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0")); status.Code(err) != codes.PermissionDenied {
		t.Errorf("StartProcess(symlink escape) code = %s, want PermissionDenied", status.Code(err))
	}

	running, err := service.StartProcess(ctx, helperStartRequest("only", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep"))
	if err != nil {
		t.Fatalf("StartProcess(only) error = %v", err)
	}
	if _, err := service.StartProcess(ctx, helperStartRequest("overflow", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep")); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("StartProcess(over limit) code = %s, want ResourceExhausted", status.Code(err))
	}
	if _, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{Signal: codev1.ProcessSignal_PROCESS_SIGNAL_TERM}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("SignalProcess(missing reference) code = %s, want InvalidArgument", status.Code(err))
	}
	if _, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: running.GetProcess().GetId()}},
		Signal:  codev1.ProcessSignal(99),
	}); status.Code(err) != codes.InvalidArgument {
		t.Errorf("SignalProcess(invalid signal) code = %s, want InvalidArgument", status.Code(err))
	}
	stopProcess(t, service, running.GetProcess())
}

func TestServiceShutdownForceKillsIgnoredTermination(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	readyFile := filepath.Join(workspace, "ready")
	service := newTestProcessService(t, workspace, 1)
	started, err := service.StartProcess(context.Background(), helperStartRequest("stubborn", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "ignore-term", readyFile))
	if err != nil {
		t.Fatalf("StartProcess() error = %v", err)
	}
	waitForFile(t, readyFile)
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := service.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Shutdown() error = %v, want DeadlineExceeded", err)
	}
	exited := waitForProcessExit(t, service, started.GetProcess().GetId())
	if exited.GetExitSignal() != int32(syscall.SIGKILL) {
		t.Errorf("Shutdown().ExitSignal = %d, want SIGKILL", exited.GetExitSignal())
	}
	if _, err := service.StartProcess(context.Background(), helperStartRequest("late", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "exit", "0")); status.Code(err) != codes.Unavailable {
		t.Errorf("StartProcess(after shutdown) code = %s, want Unavailable", status.Code(err))
	}
}

func TestServiceConcurrentStartListAndSignal(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	const processCount = 8
	service := newTestProcessService(t, t.TempDir(), processCount)
	results := make(chan *codev1.ProcessInfo, processCount)
	errorsChannel := make(chan error, processCount)
	var starts sync.WaitGroup
	for index := range processCount {
		starts.Add(1)
		go func() {
			defer starts.Done()
			response, err := service.StartProcess(context.Background(), helperStartRequest(
				"worker-"+strconv.Itoa(index), ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "sleep",
			))
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- response.GetProcess()
		}()
	}
	for range processCount {
		if _, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{}); err != nil {
			t.Fatalf("ListProcesses(concurrent starts) error = %v", err)
		}
	}
	starts.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Errorf("StartProcess(concurrent) error = %v", err)
	}
	processes := make([]*codev1.ProcessInfo, 0, processCount)
	for process := range results {
		processes = append(processes, process)
	}
	if len(processes) != processCount {
		t.Fatalf("concurrent starts returned %d processes, want %d", len(processes), processCount)
	}

	var signals sync.WaitGroup
	signalErrors := make(chan error, processCount)
	for _, info := range processes {
		signals.Add(1)
		go func() {
			defer signals.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
				Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: info.GetId()}},
				Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_KILL,
				Wait:    true,
			})
			if err != nil {
				signalErrors <- err
			}
		}()
	}
	signals.Wait()
	close(signalErrors)
	for err := range signalErrors {
		t.Errorf("SignalProcess(concurrent) error = %v", err)
	}
}

func TestServiceSignalsEntireProcessGroup(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	parentReady := filepath.Join(workspace, "parent-ready")
	childReady := filepath.Join(workspace, "child-ready")
	marker := filepath.Join(workspace, "group-signal")
	service := newTestProcessService(t, workspace, 1)
	started, err := service.StartProcess(context.Background(), helperStartRequest(
		"group", ".", codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, "group-parent", parentReady, childReady, marker,
	))
	if err != nil {
		t.Fatalf("StartProcess(group) error = %v", err)
	}
	waitForFile(t, parentReady)
	if _, err := service.SignalProcess(context.Background(), &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: "group"}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_USR1,
	}); err != nil {
		t.Fatalf("SignalProcess(USR1 group) error = %v", err)
	}
	waitForFile(t, marker)
	stopProcess(t, service, started.GetProcess())
}

func waitForFile(t *testing.T, name string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(name); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("file %s was not created", name)
}

func waitForHelperFile(name string) {
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(name); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	os.Exit(108)
}

func TestNewRejectsInvalidProcessConfiguration(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New(empty config) succeeded")
	}
	if _, err := New(Config{Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxProcesses: -1}); err == nil {
		t.Fatal("New(negative max) succeeded")
	}
	if _, err := New(Config{Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxProcesses: maxTrackedProcesses + 1}); err == nil {
		t.Fatal("New(over history max) succeeded")
	}
	file := filepath.Join(t.TempDir(), "runtime-file")
	if err := os.WriteFile(file, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Config{Workspace: t.TempDir(), RuntimeDirectory: file}); err == nil {
		t.Fatal("New(runtime file) succeeded")
	}
}

func newTestProcessService(t *testing.T, workspace string, maxProcesses int) *Service {
	t.Helper()
	service, err := New(Config{Workspace: workspace, RuntimeDirectory: t.TempDir(), MaxProcesses: maxProcesses})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = service.Shutdown(ctx)
		if err := service.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return service
}

func helperStartRequest(name, workingDirectory string, ioMode codev1.ProcessIOMode, arguments ...string) *codev1.StartProcessRequest {
	helperArguments := []string{"-test.run=^TestProcessHelper$", "--"}
	helperArguments = append(helperArguments, arguments...)
	return &codev1.StartProcessRequest{
		Name: name, Command: os.Args[0], Arguments: helperArguments, WorkingDirectory: workingDirectory, IoMode: ioMode,
	}
}

func assertRunningProcess(t *testing.T, info *codev1.ProcessInfo, name string, ioMode codev1.ProcessIOMode) {
	t.Helper()
	if info.GetName() != name || info.GetPid() <= 0 || !uuidPatternForTest(info.GetId()) || info.GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING || info.GetIoMode() != ioMode || info.GetStartedAt() == nil {
		t.Fatalf("process info = %+v, want running %s process with UUID and PID", info, name)
	}
}

func waitForProcessExit(t *testing.T, service *Service, id string) *codev1.ProcessInfo {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		response, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
		if err != nil {
			t.Fatalf("ListProcesses() error = %v", err)
		}
		for _, process := range response.GetProcesses() {
			if process.GetId() == id && process.GetState() == codev1.ProcessState_PROCESS_STATE_EXITED {
				return process
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("process %s did not exit", id)
	return nil
}

func stopProcess(t *testing.T, service *Service, info *codev1.ProcessInfo) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := service.SignalProcess(ctx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: info.GetId()}},
		Signal:  codev1.ProcessSignal_PROCESS_SIGNAL_KILL,
		Wait:    true,
	})
	if err != nil {
		t.Fatalf("SignalProcess(cleanup) error = %v", err)
	}
}

func uuidPatternForTest(value string) bool {
	if len(value) != 36 {
		return false
	}
	for index, character := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			if character != '-' {
				return false
			}
			continue
		}
		if !strings.ContainsRune("0123456789abcdef", character) {
			return false
		}
	}
	return true
}
