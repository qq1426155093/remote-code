package server

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/agent"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
)

// agentHelperEnvironment and agentHelperCwdLog turn the test binary into a
// minimal ACP agent child: when the marker is set, the process speaks ACP over
// its own stdio and appends every session cwd to the log path.
const (
	agentHelperEnvironment = "REMOTE_CODE_SERVER_AGENT_HELPER"
	agentHelperCwdLog      = "REMOTE_CODE_SERVER_AGENT_CWD_LOG"
)

// TestAgentHelperProcess is the child side of the agent integration tests. It
// exits when the controller closes its stdin (the clean ACP shutdown).
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

// agentHelper answers one scripted turn: a message chunk, a tool call that
// needs permission, the permission outcome echoed as another message, a usage
// report, and end_turn. Sessions record their cwd for confinement assertions.
type agentHelper struct {
	conn *acp.AgentSideConnection
}

func (h agentHelper) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (h agentHelper) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{SessionCapabilities: acp.SessionCapabilities{Close: &acp.SessionCloseCapabilities{}}},
		AgentInfo:         &acp.Implementation{Name: "agent-helper"},
	}, nil
}

func (h agentHelper) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (h agentHelper) Cancel(context.Context, acp.CancelNotification) error { return nil }

func (h agentHelper) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}

func (h agentHelper) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, nil
}

func (h agentHelper) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	if logPath := os.Getenv(agentHelperCwdLog); logPath != "" {
		file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return acp.NewSessionResponse{}, err
		}
		fmt.Fprintf(file, "%s\n", params.Cwd)
		file.Close()
	}
	return acp.NewSessionResponse{SessionId: "helper-1"}, nil
}

func (h agentHelper) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId, Update: acp.UpdateAgentMessageText("working"),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId, Update: acp.StartToolCall("call_1", "edit notes"),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	permission, err := h.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: params.SessionId,
		Options: []acp.PermissionOption{
			{OptionId: "once", Name: "Allow once", Kind: acp.PermissionOptionKindAllowOnce},
			{OptionId: "always", Name: "Always allow", Kind: acp.PermissionOptionKindAllowAlways},
		},
	})
	selected := "none"
	if permission.Outcome.Selected != nil {
		selected = string(permission.Outcome.Selected.OptionId)
	}
	if err != nil {
		return acp.PromptResponse{}, err
	}
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update:    acp.UpdateToolCall("call_1", acp.WithUpdateStatus(acp.ToolCallStatusCompleted)),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId, Update: acp.UpdateAgentMessageText("approved:" + selected),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId,
		Update: acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
			Size: 1000, Used: 42, Cost: &acp.Cost{Amount: 1.5, Currency: "USD"},
		}},
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (h agentHelper) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, nil
}

func (h agentHelper) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}

func (h agentHelper) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, nil
}

func (h agentHelper) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// startAgentController boots a full controller whose agent child is this test
// binary in helper mode, and returns connected gRPC clients plus the path the
// child appends session cwds to.
func startAgentController(t *testing.T, workspace string) (codev1.AgentServiceClient, codev1.ControllerServiceClient, string) {
	t.Helper()
	cwdLog := filepath.Join(t.TempDir(), "cwds.log")
	controller, err := New(Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, RuntimeDirectory: t.TempDir(), MaxProcesses: 2,
		Agent: agent.Config{
			Enabled: true,
			Command: os.Args[0],
			// The helper mode must survive -count reruns: the marker env is
			// what selects it, the run pattern only narrows the test.
			Arguments: []string{"-test.run=^TestAgentHelperProcess$", "-test.count=1"},
			Environment: map[string]string{
				agentHelperEnvironment: "1",
				agentHelperCwdLog:      cwdLog,
			},
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
		if err := controller.Shutdown(ctx); err != nil {
			t.Errorf("controller.Shutdown() error = %v", err)
		}
		select {
		case <-serveErrors:
		case <-time.After(15 * time.Second):
			t.Error("controller did not stop")
		}
	})
	connection, err := grpc.NewClient(controller.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return codev1.NewAgentServiceClient(connection), codev1.NewControllerServiceClient(connection), cwdLog
}

// collectQueryEvents drains a Query stream with a deadline.
func collectQueryEvents(t *testing.T, stream codev1.AgentService_QueryClient) []*codev1.QueryResponse {
	t.Helper()
	var events []*codev1.QueryResponse
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatalf("Query Recv() error = %v", err)
		}
		if len(events) > 100 {
			t.Fatal("Query stream produced an unreasonable number of events")
		}
		events = append(events, response)
	}
}

func TestAgentQueryOverGRPCHappyPath(t *testing.T) {
	workspace := t.TempDir()
	agentClient, controllerClient, _ := startAgentController(t, workspace)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	info, err := controllerClient.GetInfo(ctx, &codev1.GetInfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if info.GetAgent() == nil || !info.GetAgent().GetEnabled() || info.GetAgent().GetStarted() {
		t.Fatalf("GetInfo().Agent before first query = %+v", info.GetAgent())
	}

	stream, err := agentClient.Query(ctx, &codev1.QueryRequest{Prompt: "please proceed"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	events := collectQueryEvents(t, stream)

	var messages []string
	var sawToolCompleted, sawUsage bool
	for _, event := range events {
		switch payload := event.GetEvent().(type) {
		case *codev1.QueryResponse_SessionStarted:
			if payload.SessionStarted.GetSessionId() != "helper-1" {
				t.Fatalf("session id = %q, want helper-1", payload.SessionStarted.GetSessionId())
			}
		case *codev1.QueryResponse_Message:
			messages = append(messages, payload.Message.GetText())
		case *codev1.QueryResponse_Thought:
			t.Fatalf("unexpected thought event %+v", payload)
		case *codev1.QueryResponse_ToolCall:
			if payload.ToolCall.GetStatus() == string(acp.ToolCallStatusCompleted) {
				sawToolCompleted = true
			}
		case *codev1.QueryResponse_Usage:
			sawUsage = payload.Usage.GetContextUsed() == 42 && payload.Usage.GetCost().GetAmount() == 1.5
		}
	}
	if len(messages) != 2 || messages[0] != "working" || messages[1] != "approved:once" {
		t.Fatalf("messages = %v, want [working approved:once] (allow_once auto-approval)", messages)
	}
	if !sawToolCompleted || !sawUsage {
		t.Fatalf("tool completion = %v, usage = %v", sawToolCompleted, sawUsage)
	}
	if completed := events[len(events)-1].GetCompleted(); completed == nil || completed.GetStopReason() != string(acp.StopReasonEndTurn) {
		t.Fatalf("last event = %+v, want completed end_turn", events[len(events)-1])
	}

	// The turn settled, so its session already auto-closed: closing again is
	// an idempotent success, as is closing an id that never existed.
	if _, err := agentClient.CloseSession(ctx, &codev1.CloseSessionRequest{SessionId: "helper-1"}); err != nil {
		t.Fatalf("CloseSession() error = %v", err)
	}
	if _, err := agentClient.CloseSession(ctx, &codev1.CloseSessionRequest{SessionId: "helper-1"}); err != nil {
		t.Fatalf("double close error = %v, want nil", err)
	}
	if _, err := agentClient.CloseSession(ctx, &codev1.CloseSessionRequest{SessionId: "never-existed"}); err != nil {
		t.Fatalf("close of an unknown session error = %v, want nil", err)
	}
}

func TestAgentQueryWorkingDirectoryConfined(t *testing.T) {
	workspace := t.TempDir()
	nested := filepath.Join(workspace, "nested")
	if err := os.Mkdir(nested, 0o755); err != nil {
		t.Fatal(err)
	}
	agentClient, _, cwdLog := startAgentController(t, workspace)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	outside := "../outside"
	stream, err := agentClient.Query(ctx, &codev1.QueryRequest{Prompt: "hi", WorkingDirectory: &outside})
	if err != nil {
		t.Fatalf("Query(escape) error = %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument || rpcerror.ReasonOf(err) != rpcerror.AgentWorkingDirectory {
		t.Fatalf("Query(escape) error = %v, want InvalidArgument/AGENT_WORKING_DIRECTORY", err)
	}

	stream, err = agentClient.Query(ctx, &codev1.QueryRequest{Prompt: "hi", WorkingDirectory: ptrString("nested")})
	if err != nil {
		t.Fatalf("Query(nested) error = %v", err)
	}
	collectQueryEvents(t, stream)

	recorded, err := os.ReadFile(cwdLog)
	if err != nil || len(recorded) == 0 {
		t.Fatalf("cwd log = %q, %v", recorded, err)
	}
	resolvedWorkspace, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(resolvedWorkspace, "nested") + "\n"
	if got := string(recorded); got != want {
		t.Fatalf("agent session cwd = %q, want %q", got, want)
	}
}

func ptrString(value string) *string { return &value }

func TestAgentDisabledSurface(t *testing.T) {
	controller, err := New(Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxProcesses: 1,
	})
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
		<-serveErrors
	})
	connection, err := grpc.NewClient(controller.Address(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	info, err := codev1.NewControllerServiceClient(connection).GetInfo(ctx, &codev1.GetInfoRequest{})
	if err != nil {
		t.Fatalf("GetInfo() error = %v", err)
	}
	if info.GetAgent() != nil {
		t.Fatalf("GetInfo().Agent = %+v, want nil when disabled", info.GetAgent())
	}
	agentClient := codev1.NewAgentServiceClient(connection)
	stream, err := agentClient.Query(ctx, &codev1.QueryRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentDisabled {
		t.Fatalf("Query() error = %v, want FailedPrecondition/AGENT_DISABLED", err)
	}
}
