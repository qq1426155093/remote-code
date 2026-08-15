package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/server"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

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
