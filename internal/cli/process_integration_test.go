package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/server"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

type notifyingBuffer struct {
	bytes.Buffer
	wrote chan struct{}
	once  sync.Once
}

func (b *notifyingBuffer) Write(data []byte) (int, error) {
	written, err := b.Buffer.Write(data)
	if written > 0 {
		b.once.Do(func() { close(b.wrote) })
	}
	return written, err
}

func TestREPLUsesCurrentDirectoryForExecAndSupportsPSAll(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "test"), 0o755); err != nil {
		t.Fatal(err)
	}
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace,
		RuntimeDirectory: runtimeDirectory, MaxProcesses: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var stdout bytes.Buffer
	repl := New(client, nil, Config{Timeout: 5 * time.Second, Stdout: &stdout})
	if err := repl.changeDirectory([]string{"test"}); err != nil {
		t.Fatalf("cd test: %v", err)
	}
	if err := repl.startProcess([]string{"--name", "cwd-job", "-e", "CLI_TEST=value", "sleep", "30"}); err != nil {
		t.Fatalf("exec: %v", err)
	}
	active, err := client.ListProcesses(ctx)
	if err != nil || len(active) != 1 {
		t.Fatalf("ListProcesses(active) = %+v, %v", active, err)
	}
	started := active[0]
	if started.GetWorkingDirectory() != "/test" || !reflect.DeepEqual(started.GetEnvironmentKeys(), []string{"CLI_TEST"}) {
		t.Fatalf("started process = %+v", started)
	}
	if _, err := client.SignalProcess(ctx, &codev1.ProcessReference{
		Value: &codev1.ProcessReference_Id{Id: started.GetId()},
	}, codev1.ProcessSignal_PROCESS_SIGNAL_KILL, true); err != nil {
		t.Fatal(err)
	}
	stdout.Reset()
	if err := repl.listProcesses(nil); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stdout.Bytes(), []byte("cwd-job")) {
		t.Fatalf("ps output contains exited process: %s", stdout.String())
	}
	stdout.Reset()
	if err := repl.listProcesses([]string{"-a"}); err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("cwd-job")) || !bytes.Contains(stdout.Bytes(), []byte("exited")) {
		t.Fatalf("ps -a output = %s", stdout.String())
	}
	stdout.Reset()
	if err := repl.forgetProcess([]string{started.GetId()}); err != nil {
		t.Fatalf("forget: %v", err)
	}
	if !bytes.Contains(stdout.Bytes(), []byte("forgot cwd-job ("+started.GetId()+")")) {
		t.Fatalf("forget output = %s", stdout.String())
	}
	if _, err := os.Stat(filepath.Join(runtimeDirectory, started.GetId())); !os.IsNotExist(err) {
		t.Fatalf("process runtime directory still exists: %v", err)
	}
	stdout.Reset()
	if err := repl.listProcesses([]string{"-a"}); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(stdout.Bytes(), []byte("cwd-job")) {
		t.Fatalf("ps -a output contains forgotten process: %s", stdout.String())
	}
}

func TestREPLDiscoversAndStartsProcessTemplate(t *testing.T) {
	workspace := t.TempDir()
	definition := filepath.Join(t.TempDir(), "cli.process-template.yaml")
	definitionYAML := `version: 1
language: expr
templates:
  - name: sleeper
    description: Start a bounded sleep process.
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      required: [seconds]
      additionalProperties: false
      properties:
        seconds:
          type: string
          enum: ["30"]
          description: Sleep duration in seconds.
    command: sleep
    io_mode: pipe
    input_mode: disabled
    render: |-
      {"arguments": [parameters.seconds], "working_directory": ".", "environment": {}}
`
	if err := os.WriteFile(definition, []byte(definitionYAML), 0o600); err != nil {
		t.Fatal(err)
	}
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, RuntimeDirectory: t.TempDir(), MaxProcesses: 1,
		ProcessTemplates: processservice.TemplateConfig{DefinitionFiles: []string{definition}},
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var stdout bytes.Buffer
	repl := New(client, nil, Config{Timeout: 5 * time.Second, Stdout: &stdout})
	if err := repl.listProcessTemplates(nil); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(stdout.String(), "sleeper") || !strings.Contains(stdout.String(), "Start a bounded sleep process") {
		t.Fatalf("templates output = %s", stdout.String())
	}
	stdout.Reset()
	if err := repl.startProcessFromTemplate([]string{"--name", "templated-sleep", "--params", `{"seconds":"30"}`, "sleeper"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stdout.String(), `"seconds"`) || !strings.Contains(stdout.String(), "from template sleeper@") {
		t.Fatalf("exec-template output = %s", stdout.String())
	}
	active, err := client.ListProcesses(ctx)
	if err != nil || len(active) != 1 || active[0].GetName() != "templated-sleep" || !active[0].GetArgumentsRedacted() {
		t.Fatalf("templated process = %+v, %v", active, err)
	}
	if _, err := client.SignalProcess(ctx, &codev1.ProcessReference{
		Value: &codev1.ProcessReference_Id{Id: active[0].GetId()},
	}, codev1.ProcessSignal_PROCESS_SIGNAL_KILL, true); err != nil {
		t.Fatal(err)
	}
}

func TestREPLForgetSupportsMultipleSelectorsGlobsAndPartialFailures(t *testing.T) {
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxProcesses: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	var stdout, stderr bytes.Buffer
	repl := New(client, nil, Config{Timeout: 5 * time.Second, Stdout: &stdout, Stderr: &stderr})

	waitForName := func(name string) *codev1.ProcessInfo {
		t.Helper()
		for {
			processes, err := client.ListProcesses(ctx, true)
			if err != nil {
				t.Fatal(err)
			}
			for _, process := range processes {
				if process.GetName() == name && !isActiveProcessState(process.GetState()) {
					return process
				}
			}
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(10 * time.Millisecond):
			}
		}
	}
	for _, name := range []string{"repl-batch-a", "repl-batch-b"} {
		if err := repl.startProcess([]string{"--name", name, "true"}); err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		waitForName(name)
	}
	if err := repl.startProcess([]string{"--name", "repl-batch-active", "sleep", "30"}); err != nil {
		t.Fatal(err)
	}
	activeList, err := client.ListProcesses(ctx)
	if err != nil || len(activeList) != 1 {
		t.Fatalf("ListProcesses(active) = %+v, %v", activeList, err)
	}
	active := activeList[0]

	stdout.Reset()
	stderr.Reset()
	err = repl.forgetProcess([]string{"repl-batch-a", "repl-batch-*"})
	if err == nil || !strings.Contains(err.Error(), "forgot 2 processes; 1 operation failed") {
		t.Fatalf("forget error = %v", err)
	}
	if strings.Count(stdout.String(), "forgot repl-batch-a") != 1 || strings.Count(stdout.String(), "forgot repl-batch-b") != 1 {
		t.Errorf("forget stdout = %s", stdout.String())
	}
	if !strings.Contains(stderr.String(), "skipped repl-batch-active") || !strings.Contains(stderr.String(), "FailedPrecondition") {
		t.Errorf("forget stderr = %s", stderr.String())
	}
	all, err := client.ListProcesses(ctx, true)
	if err != nil || len(all) != 1 || all[0].GetId() != active.GetId() {
		t.Fatalf("ListProcesses(all) = %+v, %v", all, err)
	}
	if _, err := client.SignalProcess(ctx, &codev1.ProcessReference{
		Value: &codev1.ProcessReference_Id{Id: active.GetId()},
	}, codev1.ProcessSignal_PROCESS_SIGNAL_KILL, true); err != nil {
		t.Fatal(err)
	}
}

func TestREPLInterruptStopsLogFollowWithoutStoppingProcess(t *testing.T) {
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxProcesses: 1,
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("controller did not stop")
		}
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	started, err := client.StartProcessWithOptions(ctx, remoteclient.ProcessStartOptions{
		Name: "follow-running", Command: "sh", Arguments: []string{"-c", "printf 'follow-ready\\n'; sleep 30"},
		WorkingDirectory: ".", IOMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
	})
	if err != nil {
		t.Fatal(err)
	}

	output := &notifyingBuffer{wrote: make(chan struct{})}
	repl := New(client, nil, Config{Timeout: 5 * time.Second, Stdout: output})
	interrupts := make(chan context.CancelFunc, 1)
	repl.interruptContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		interruptContext, cancelInterrupt := context.WithCancel(parent)
		interrupts <- cancelInterrupt
		return interruptContext, cancelInterrupt
	}
	result := make(chan error, 1)
	go func() { result <- repl.observeProcessLogs([]string{"-f", started.GetId()}) }()

	var cancelFollow context.CancelFunc
	select {
	case cancelFollow = <-interrupts:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	select {
	case <-output.wrote:
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	cancelFollow()
	select {
	case err := <-result:
		if err != nil {
			t.Fatalf("observeProcessLogs() after interrupt error = %v", err)
		}
	case <-ctx.Done():
		t.Fatal(ctx.Err())
	}
	if !strings.Contains(output.String(), "follow-ready") {
		t.Fatalf("followed output = %q", output.String())
	}
	active, err := client.ListProcesses(ctx)
	if err != nil || len(active) != 1 || active[0].GetId() != started.GetId() || active[0].GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING {
		t.Fatalf("process after stopping log follow = %+v, %v", active, err)
	}
	if _, err := client.SignalProcess(ctx, &codev1.ProcessReference{
		Value: &codev1.ProcessReference_Id{Id: started.GetId()},
	}, codev1.ProcessSignal_PROCESS_SIGNAL_KILL, true); err != nil {
		t.Fatal(err)
	}
}
