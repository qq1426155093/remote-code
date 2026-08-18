package client_test

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/logging"
	"github.com/qq1426155093/remote-code/internal/server"
	client "github.com/qq1426155093/remote-code/pkg/client"
)

func TestObserveControllerLogsOverGRPC(t *testing.T) {
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(),
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
	remote, err := client.New(ctx, client.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if capabilities := remote.Info().GetControllerLogs(); capabilities == nil || !capabilities.GetAvailable() || capabilities.GetFormatVersion() != 2 {
		t.Fatalf("controller log capabilities = %+v", capabilities)
	}
	controller.Emit(logging.Event{Level: logging.LevelInfo, Component: "test", Name: "grpc_event", Message: "visible"})
	lines := uint64(1)
	stream, err := remote.ObserveControllerLogs(ctx, client.ControllerLogOptions{TailLines: &lines})
	if err != nil {
		t.Fatal(err)
	}
	var entries []*codev1.ControllerLogEntry
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatal(recvErr)
		}
		if entry := response.GetEntry(); entry != nil {
			entries = append(entries, entry)
		}
	}
	if len(entries) != 1 || entries[0].GetEvent() != "grpc_event" || entries[0].GetMessage() != "visible" {
		t.Fatalf("controller log entries = %+v", entries)
	}
}

func TestControllerShutdownCompletesControllerLogFollow(t *testing.T) {
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(),
	})
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, err := client.New(ctx, client.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	controller.Emit(logging.Event{Component: "test", Name: "follow_start"})
	stream, err := remote.ObserveControllerLogs(ctx, client.ControllerLogOptions{Follow: true})
	if err != nil {
		t.Fatal(err)
	}
	for received := 0; received < 3; received++ {
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("initial follow response %d: %v", received, err)
		}
	}
	shutdownDone := make(chan error, 1)
	go func() {
		shutdownContext, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		shutdownDone <- controller.Shutdown(shutdownContext)
	}()
	seenEnd := false
	for {
		response, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			t.Fatalf("follow after shutdown: %v", recvErr)
		}
		if response.GetEnd() != nil {
			seenEnd = true
		}
	}
	if !seenEnd {
		t.Fatal("follow stream ended without controller shutdown end response")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatalf("controller shutdown: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("controller shutdown did not finish")
	}
	select {
	case <-serveErrors:
	case <-time.After(5 * time.Second):
		t.Fatal("controller serve loop did not stop")
	}
}
