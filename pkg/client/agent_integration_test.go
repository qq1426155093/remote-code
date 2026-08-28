package client_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/agent"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"github.com/qq1426155093/remote-code/internal/server"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

// agentHelperEnvironment turns this test binary into a minimal ACP agent
// child: when set, the process answers prompts with a per-child turn counter
// so session reuse across turns is observable.
const agentHelperEnvironment = "REMOTE_CODE_CLIENT_AGENT_HELPER"

// TestAgentHelperProcess is the child side of the agent client tests. It
// exits when the controller closes its stdin.
func TestAgentHelperProcess(t *testing.T) {
	if os.Getenv(agentHelperEnvironment) != "1" {
		return
	}
	helper := &agentHelper{}
	connection := acp.NewAgentSideConnection(helper, os.Stdout, os.Stdin)
	connection.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	helper.conn = connection
	<-connection.Done()
}

// agentHelper speaks just enough ACP for the client wrapper tests: every
// session gets the same id and every prompt answers with "turn N" where N
// counts prompts across the child's lifetime.
type agentHelper struct {
	conn  *acp.AgentSideConnection
	turns int
}

func (h *agentHelper) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (h *agentHelper) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{SessionCapabilities: acp.SessionCapabilities{Close: &acp.SessionCloseCapabilities{}}},
		AgentInfo:         &acp.Implementation{Name: "client-agent-helper"},
	}, nil
}

func (h *agentHelper) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (h *agentHelper) Cancel(context.Context, acp.CancelNotification) error { return nil }

func (h *agentHelper) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}

func (h *agentHelper) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, nil
}

func (h *agentHelper) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionId: "client-helper-1"}, nil
}

func (h *agentHelper) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	h.turns++
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId, Update: acp.UpdateAgentMessageText("turn " + strconv.Itoa(h.turns)),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (h *agentHelper) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, nil
}

func (h *agentHelper) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}

func (h *agentHelper) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, nil
}

func (h *agentHelper) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// startControllerWithAgent boots a controller whose agent child is this test
// binary in helper mode.
func startControllerWithAgent(t *testing.T, workspace string) string {
	t.Helper()
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, RuntimeDirectory: t.TempDir(),
		Agent: agent.Config{
			Enabled: true,
			Command: os.Args[0],
			Arguments: []string{
				"-test.run=^TestAgentHelperProcess$", "-test.count=1",
			},
			Environment: map[string]string{agentHelperEnvironment: "1"},
		},
	})
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		if err := controller.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("controller.Shutdown() error = %v", err)
		}
		select {
		case <-serveErrors:
		case <-time.After(15 * time.Second):
			t.Error("controller Serve() did not return")
		}
	})
	return controller.Address()
}

// drainAgentTurn reads one turn to completion and returns its message texts
// in order plus the final stop reason.
func drainAgentTurn(t *testing.T, stream codev1.AgentService_QueryClient) (texts []string, stopReason string) {
	t.Helper()
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			t.Fatal("agent turn stream ended without a completed event")
		}
		if err != nil {
			t.Fatalf("agent turn Recv() error = %v", err)
		}
		switch payload := response.GetEvent().(type) {
		case *codev1.QueryResponse_SessionStarted:
			t.Fatalf("agent turn restarted session %q", payload.SessionStarted.GetSessionId())
		case *codev1.QueryResponse_Message:
			texts = append(texts, payload.Message.GetText())
		case *codev1.QueryResponse_Completed:
			return texts, payload.Completed.GetStopReason()
		}
	}
}

func TestClientAgentTurnLifecycleOverGRPC(t *testing.T) {
	address := startControllerWithAgent(t, t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	remote, err := remoteclient.New(ctx, remoteclient.Config{Address: address})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if !remote.Info().GetAgent().GetEnabled() {
		t.Fatal("connection-time info does not report the agent service")
	}

	stream, err := remote.AgentQuery(ctx, "first", remoteclient.AgentQueryOptions{})
	if err != nil {
		t.Fatal(err)
	}
	var sessionID string
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			t.Fatalf("first turn Recv() error = %v", err)
		}
		if started := response.GetSessionStarted(); started != nil {
			sessionID = started.GetSessionId()
		}
	}
	if sessionID == "" {
		t.Fatal("first turn did not report a session id")
	}

	// The second turn reuses the session: the shared child answers with its
	// lifetime turn counter instead of starting another session.
	stream, err = remote.AgentQuery(ctx, "second", remoteclient.AgentQueryOptions{SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	texts, stopReason := drainAgentTurn(t, stream)
	if len(texts) != 1 || texts[0] != "turn 2" {
		t.Fatalf("second turn texts = %v, want [turn 2]", texts)
	}
	if stopReason != string(acp.StopReasonEndTurn) {
		t.Fatalf("second turn stop reason = %q", stopReason)
	}

	if _, err := remote.AgentQuery(ctx, "   ", remoteclient.AgentQueryOptions{}); err == nil || status.Code(err) != codes.Unknown {
		t.Fatalf("blank prompt error = %v, want client-side rejection", err)
	}
	if err := remote.CloseAgentSession(ctx, sessionID); err != nil {
		t.Fatalf("CloseAgentSession() error = %v", err)
	}
	err = remote.CloseAgentSession(ctx, sessionID)
	if status.Code(err) != codes.NotFound || rpcerror.ReasonOf(err) != rpcerror.AgentSessionNotFound {
		t.Fatalf("double close error = %v, want NotFound/AGENT_SESSION_NOT_FOUND", err)
	}
}

func TestClientAgentGatedWhenServiceDisabled(t *testing.T) {
	address := startController(t, t.TempDir(), 1024, "")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	remote, err := remoteclient.New(ctx, remoteclient.Config{Address: address})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if remote.Info().GetAgent() != nil {
		t.Fatal("info reports an agent on a controller without one")
	}
	if _, err := remote.AgentQuery(ctx, "hi", remoteclient.AgentQueryOptions{}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("AgentQuery() error = %v, want FailedPrecondition", err)
	}
	if err := remote.CloseAgentSession(ctx, "any"); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CloseAgentSession() error = %v, want FailedPrecondition", err)
	}
}
