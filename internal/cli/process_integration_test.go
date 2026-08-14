package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/server"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

func TestREPLUsesCurrentDirectoryForExecAndSupportsPSAll(t *testing.T) {
	workspace := t.TempDir()
	if err := os.Mkdir(filepath.Join(workspace, "test"), 0o755); err != nil {
		t.Fatal(err)
	}
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace,
		RuntimeDirectory: t.TempDir(), MaxProcesses: 2,
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
}
