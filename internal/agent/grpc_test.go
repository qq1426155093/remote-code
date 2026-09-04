package agent

import (
	"context"
	"io"
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
)

// serveRPC exposes one RPC adapter on a private gRPC server and returns a
// connected client plus a shutdown function.
func serveRPC(t *testing.T, rpc *RPC) codev1.AgentServiceClient {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen() error = %v", err)
	}
	server := grpc.NewServer()
	codev1.RegisterAgentServiceServer(server, rpc)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	connection, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient() error = %v", err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	return codev1.NewAgentServiceClient(connection)
}

// receiveEvents collects a Query stream with a deadline so a stalled pump
// fails the test instead of hanging it.
func receiveEvents(t *testing.T, stream codev1.AgentService_QueryClient) []*codev1.QueryResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var events []*codev1.QueryResponse
	for {
		response, err := stream.Recv()
		if err == io.EOF {
			return events
		}
		if err != nil {
			t.Fatalf("Recv() error = %v", err)
		}
		if err := ctx.Err(); err != nil {
			t.Fatalf("timed out after %d events", len(events))
		}
		events = append(events, response)
	}
}

func TestRPCQuery_MapsEveryEventKind(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("answer")); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.update(ctx, prompt.SessionId, acp.StartToolCall("call_1", "read notes.txt",
				acp.WithStartRawInput(map[string]any{"path": "notes.txt"}),
			)); err != nil {
				return acp.PromptResponse{}, err
			}
			line := 7
			oldText := "stale"
			if err := a.update(ctx, prompt.SessionId, acp.UpdateToolCall("call_1",
				acp.WithUpdateTitle("read notes.txt"),
				acp.WithUpdateKind(acp.ToolKindRead),
				acp.WithUpdateStatus(acp.ToolCallStatusCompleted),
				acp.WithUpdateLocations([]acp.ToolCallLocation{{Path: "notes.txt", Line: &line}}),
				acp.WithUpdateRawOutput([]any{map[string]any{"type": "text", "text": "42 lines"}}),
				acp.WithUpdateContent([]acp.ToolCallContent{{Diff: &acp.ToolCallContentDiff{
					Type: "diff", Path: "notes.txt", OldText: &oldText, NewText: "fresh",
				}}}),
			)); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.update(ctx, prompt.SessionId, acp.UpdatePlan(acp.PlanEntry{
				Content: "summarize", Priority: acp.PlanEntryPriorityMedium, Status: acp.PlanEntryStatusCompleted,
			})); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.update(ctx, prompt.SessionId, acp.SessionUpdate{UsageUpdate: &acp.SessionUsageUpdate{
				Size: 200, Used: 100, Cost: &acp.Cost{Amount: 0.25, Currency: "USD"},
			}}); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})
	client := serveRPC(t, NewRPC(h.service))

	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	events := receiveEvents(t, stream)

	if len(events) != 7 {
		t.Fatalf("got %d events, want 7", len(events))
	}
	if started := events[0].GetSessionStarted(); started == nil || started.GetSessionId() != "sess-1" {
		t.Fatalf("first event = %+v, want session_started sess-1", events[0])
	}
	if message := events[1].GetMessage(); message == nil || message.GetText() != "answer" {
		t.Fatalf("second event = %+v, want message answer", events[1])
	}
	creation := events[2].GetToolCall()
	if creation == nil || creation.GetUpdate() || creation.GetTitle() != "read notes.txt" {
		t.Fatalf("tool call creation = %+v", events[2])
	}
	if input := creation.GetRawInput(); input == nil || input.GetStructValue().GetFields()["path"].GetStringValue() != "notes.txt" {
		t.Fatalf("tool call raw input = %+v, want notes.txt path", creation.GetRawInput())
	}
	update := events[3].GetToolCall()
	if update == nil || !update.GetUpdate() || update.GetKind() != string(acp.ToolKindRead) ||
		update.GetStatus() != string(acp.ToolCallStatusCompleted) {
		t.Fatalf("tool call update = %+v", events[3])
	}
	wantOutput := []any{map[string]any{"text": "42 lines", "type": "text"}}
	if output := update.GetRawOutput(); output == nil || !reflect.DeepEqual(output.AsInterface(), wantOutput) {
		t.Fatalf("tool call raw output = %+v, want %v", update.GetRawOutput(), wantOutput)
	}
	content := update.GetContent()
	if len(content) != 1 || content[0].GetStructValue().GetFields()["type"].GetStringValue() != "diff" ||
		content[0].GetStructValue().GetFields()["newText"].GetStringValue() != "fresh" ||
		content[0].GetStructValue().GetFields()["oldText"].GetStringValue() != "stale" {
		t.Fatalf("tool call content = %+v, want one stale→fresh diff on notes.txt", content)
	}
	if locations := update.GetLocations(); len(locations) != 1 || locations[0].GetPath() != "notes.txt" ||
		locations[0].GetLine() != 7 {
		t.Fatalf("tool call locations = %+v", update.GetLocations())
	}
	if plan := events[4].GetPlan(); plan == nil || len(plan.GetEntries()) != 1 ||
		plan.GetEntries()[0].GetContent() != "summarize" || plan.GetEntries()[0].GetStatus() != string(acp.PlanEntryStatusCompleted) {
		t.Fatalf("plan event = %+v", events[4])
	}
	usage := events[5].GetUsage()
	if usage == nil || usage.GetContextSize() != 200 || usage.GetContextUsed() != 100 ||
		usage.GetCost() == nil || usage.GetCost().GetCurrency() != "USD" {
		t.Fatalf("usage event = %+v", events[5])
	}
	completed := events[len(events)-1].GetCompleted()
	if completed == nil || completed.GetStopReason() != string(acp.StopReasonEndTurn) {
		t.Fatalf("last event = %+v, want completed end_turn", events[len(events)-1])
	}
}

func TestRPCQuery_ResumesSession(t *testing.T) {
	h := newHarness(t, nil)
	client := serveRPC(t, NewRPC(h.service))

	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	events := receiveEvents(t, stream)
	sessionID := events[0].GetSessionStarted().GetSessionId()

	stream, err = client.Query(context.Background(), &codev1.QueryRequest{Prompt: "second", SessionId: ptrOf(sessionID)})
	if err != nil {
		t.Fatalf("Query(reuse) error = %v", err)
	}
	for _, event := range receiveEvents(t, stream) {
		if event.GetSessionStarted() != nil {
			t.Fatal("follow-up turn must not emit session_started")
		}
	}
	if h.currentProcess(t).agent.sessionDirectory(0) == "" {
		t.Fatal("agent never created a session")
	}
}

func TestRPCQuery_RejectsInvalidPrompt(t *testing.T) {
	h := newHarness(t, nil)
	client := serveRPC(t, NewRPC(h.service))

	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: " "})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("Recv() error = %v, want InvalidArgument", err)
	}
	if h.dialCount() != 0 {
		t.Fatalf("dialed %d times, want 0", h.dialCount())
	}
}

func TestRPCQuery_WorkingDirectoryConfined(t *testing.T) {
	h := newHarness(t, nil)
	client := serveRPC(t, NewRPC(h.service))

	directory := "../outside"
	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: "hi", WorkingDirectory: &directory})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.InvalidArgument || rpcerror.ReasonOf(err) != rpcerror.AgentWorkingDirectory {
		t.Fatalf("Recv() error = %v, want InvalidArgument/AGENT_WORKING_DIRECTORY", err)
	}
}

// TestRPCCloseSession: sessions auto-close when their turn settles, so an RPC
// close of the settled id is an idempotent success and must not double-close
// on the agent.
func TestRPCCloseSession(t *testing.T) {
	h := newHarness(t, nil)
	client := serveRPC(t, NewRPC(h.service))

	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	sessionID := receiveEvents(t, stream)[0].GetSessionStarted().GetSessionId()

	if _, err := client.CloseSession(context.Background(), &codev1.CloseSessionRequest{SessionId: sessionID}); err != nil {
		t.Fatalf("CloseSession(settled) error = %v, want nil", err)
	}
	if closes := h.currentProcess(t).agent.closesReceived(); len(closes) != 1 || closes[0] != sessionID {
		t.Fatalf("agent closed sessions = %v, want exactly the auto-close of [%s]", closes, sessionID)
	}
	if _, err := client.CloseSession(context.Background(), &codev1.CloseSessionRequest{SessionId: "ghost"}); err != nil {
		t.Fatalf("CloseSession(unknown) error = %v, want nil", err)
	}
}

func TestRPCDisabled_SurfaceReportsReason(t *testing.T) {
	client := serveRPC(t, NewRPC(nil))

	if info := NewRPC(nil).Info(); info != nil {
		t.Fatalf("Info() = %+v, want nil when disabled", info)
	}
	stream, err := client.Query(context.Background(), &codev1.QueryRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("Query() error = %v", err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentDisabled {
		t.Fatalf("Recv() error = %v, want FailedPrecondition/AGENT_DISABLED", err)
	}
	if _, err := client.CloseSession(context.Background(), &codev1.CloseSessionRequest{SessionId: "s"}); status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentDisabled {
		t.Fatalf("CloseSession() error = %v, want FailedPrecondition/AGENT_DISABLED", err)
	}
	if _, err := client.ListQueries(context.Background(), &codev1.ListQueriesRequest{}); status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentDisabled {
		t.Fatalf("ListQueries() error = %v, want FailedPrecondition/AGENT_DISABLED", err)
	}
	if _, err := client.ListSessions(context.Background(), &codev1.ListSessionsRequest{}); status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentDisabled {
		t.Fatalf("ListSessions() error = %v, want FailedPrecondition/AGENT_DISABLED", err)
	}
}

func TestRPCInfo_ReportsBridgeStatus(t *testing.T) {
	h := newHarness(t, nil)
	rpc := NewRPC(h.service)

	info := rpc.Info()
	if !info.GetEnabled() || info.GetStarted() || info.GetGeneration() != 0 {
		t.Fatalf("Info() before first query = %+v", info)
	}
	if replay := info.GetReplay(); replay == nil || !replay.GetAvailable() || replay.GetFormatVersion() == 0 {
		t.Fatalf("Info() replay before first query = %+v, want available with a format version", info.GetReplay())
	}
	if listing := info.GetListing(); listing == nil || !listing.GetQueries() || !listing.GetSessions() ||
		listing.GetDefaultPageSize() != agentListDefaultPageSize || listing.GetMaxPageSize() != agentListMaxPageSize {
		t.Fatalf("Info() listing = %+v, want query/session capabilities and page bounds", listing)
	}
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	info = rpc.Info()
	// The turn settled, so its turn-scoped session already auto-closed.
	if !info.GetStarted() || info.GetGeneration() != 1 || info.GetSessions() != 0 || !info.GetCloseSupported() {
		t.Fatalf("Info() after a query = %+v", info)
	}
	if replay := info.GetReplay(); !replay.GetAvailable() || replay.GetMaxBytesPerQuery() != DefaultEventLogConfig().MaxBytesPerQuery {
		t.Fatalf("Info() replay after a query = %+v, want the store bounds", replay)
	}
}

func ptrOf(value string) *string { return &value }
