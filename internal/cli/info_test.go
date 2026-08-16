package cli

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc"
)

type infoTestController struct {
	codev1.UnimplementedControllerServiceServer
	calls atomic.Int32
}

func (s *infoTestController) GetInfo(context.Context, *codev1.GetInfoRequest) (*codev1.GetInfoResponse, error) {
	controllerVersion := "connection-time"
	if s.calls.Add(1) > 1 {
		controllerVersion = "current"
	}
	return &codev1.GetInfoResponse{
		ControllerVersion: controllerVersion,
		ApiVersion:        "remote.code.v1",
		WorkspaceName:     "demo-workspace",
		MaxUploadBytes:    1048576,
		FileTransfers: &codev1.FileTransferCapabilities{
			ResumableUpload: true, ResumableDownload: true, PreferredChunkBytes: 65536,
		},
		MaxProcesses:         8,
		ProcessTemplateCount: 3,
	}, nil
}

func TestInfoCommandFetchesAndPrintsCurrentControllerInfo(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	controller := &infoTestController{}
	grpcServer := grpc.NewServer()
	codev1.RegisterControllerServiceServer(grpcServer, controller)
	go func() { _ = grpcServer.Serve(listener) }()
	t.Cleanup(grpcServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: listener.Addr().String()})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var output bytes.Buffer
	repl := New(client, nil, Config{Timeout: 5 * time.Second, Stdout: &output})
	action, err := repl.execute([]string{"info"})
	if err != nil {
		t.Fatal(err)
	}
	if action != commandContinue {
		t.Fatalf("execute(info) action = %v, want commandContinue", action)
	}
	want := "Controller version: current\n" +
		"API version: remote.code.v1\n" +
		"Workspace: demo-workspace\n" +
		"Max upload bytes: 1048576\n" +
		"Resumable upload: true\n" +
		"Resumable download: true\n" +
		"Preferred transfer chunk bytes: 65536\n" +
		"Max processes: 8\n" +
		"Process template count: 3\n"
	if got := output.String(); got != want {
		t.Fatalf("info output =\n%s\nwant:\n%s", got, want)
	}
	if got := controller.calls.Load(); got != 2 {
		t.Fatalf("GetInfo calls = %d, want 2 (connect and info command)", got)
	}
}
