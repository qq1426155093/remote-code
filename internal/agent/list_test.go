package agent

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestListQueries_FiltersAndPaginatesRetainedRecords(t *testing.T) {
	h := newHarness(t, nil)
	clock := newAgentTestClock()
	h.service.now = clock.now
	h.service.queries.now = clock.now

	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatal(err)
	}
	firstFrames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatal(err)
	}
	firstID := firstFrames[0].GetQueryId()
	sessionID := firstFrames[0].GetSessionStarted().GetSessionId()

	clock.advance(time.Second)
	second, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "second", SessionID: sessionID})
	if err != nil {
		t.Fatal(err)
	}
	secondFrames := collectFrames(t, second)
	if err := second.Wait(); err != nil {
		t.Fatal(err)
	}
	secondID := secondFrames[0].GetQueryId()

	page, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetQueries()) != 1 || page.GetQueries()[0].GetQueryId() != secondID || page.GetNextPageToken() == "" {
		t.Fatalf("first query page = %+v, want newest %s plus token", page, secondID)
	}
	query := page.GetQueries()[0]
	if query.GetSessionId() != sessionID || query.GetState() != codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED ||
		query.GetCreatedAt() == nil || query.GetSettledAt() == nil || query.GetNextSequence() == 0 || query.GetStopReason() != "end_turn" {
		t.Fatalf("query summary = %+v", query)
	}

	next, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{
		PageSize: 1, PageToken: page.GetNextPageToken(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(next.GetQueries()) != 1 || next.GetQueries()[0].GetQueryId() != firstID || next.GetNextPageToken() != "" {
		t.Fatalf("second query page = %+v, want %s and no token", next, firstID)
	}

	filtered, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{
		SessionId: &sessionID, States: []codev1.AgentQueryState{codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED},
	})
	if err != nil || len(filtered.GetQueries()) != 2 {
		t.Fatalf("filtered queries = %+v, %v", filtered, err)
	}
	if _, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{
		States:    []codev1.AgentQueryState{codev1.AgentQueryState_AGENT_QUERY_STATE_RUNNING},
		PageToken: page.GetNextPageToken(),
	}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("reused token with different filter error = %v, want InvalidArgument", err)
	}
	if _, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{PageSize: agentListMaxPageSize + 1}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("oversized page error = %v, want InvalidArgument", err)
	}
}

func TestListQueries_ReportsLostTerminalStatusAndDisabledStore(t *testing.T) {
	h := newHarness(t, nil)
	queryID, writer, err := h.service.queries.Begin(QueryMetadata{SessionID: "lost-session"})
	if err != nil {
		t.Fatal(err)
	}
	terminal := rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process exited")
	if err := writer.Lost(terminal); err != nil {
		t.Fatal(err)
	}
	response, err := h.service.ListQueries(context.Background(), &codev1.ListQueriesRequest{
		States: []codev1.AgentQueryState{codev1.AgentQueryState_AGENT_QUERY_STATE_LOST},
	})
	if err != nil || len(response.GetQueries()) != 1 {
		t.Fatalf("lost queries = %+v, %v", response, err)
	}
	listed := response.GetQueries()[0]
	if listed.GetQueryId() != queryID || codes.Code(listed.GetTerminalStatus().GetCode()) != codes.Unavailable {
		t.Fatalf("lost query = %+v", listed)
	}

	service, err := New(Config{Enabled: true, WorkspaceRoot: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	_, err = service.ListQueries(context.Background(), &codev1.ListQueriesRequest{})
	if status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentQueryReplayDisabled {
		t.Fatalf("disabled replay listing error = %v, want FailedPrecondition/%s", err, rpcerror.AgentQueryReplayDisabled)
	}
}

func TestListSessions_ReturnsOnlyReusableCurrentGeneration(t *testing.T) {
	h := newHarness(t, func(agent *scriptedAgent) {
		agent.promptHook = func(agent *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if len(prompt.Prompt) > 0 && prompt.Prompt[0].Text != nil && prompt.Prompt[0].Text.Text == "stall" {
				_ = agent.waitForCancel(ctx)
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})
	clock := newAgentTestClock()
	h.service.now = clock.now
	h.service.queries.now = clock.now
	if err := os.MkdirAll(h.workspace+"/nested", 0o755); err != nil {
		t.Fatal(err)
	}

	idleTurn, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "idle"})
	if err != nil {
		t.Fatal(err)
	}
	idleFrames := collectFrames(t, idleTurn)
	if err := idleTurn.Wait(); err != nil {
		t.Fatal(err)
	}
	idleID := idleFrames[0].GetSessionStarted().GetSessionId()

	clock.advance(time.Second)
	runningTurn, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "stall", WorkingDirectory: "nested"})
	if err != nil {
		t.Fatal(err)
	}
	runningFirst := <-runningTurn.Events()
	runningID := runningFirst.GetSessionStarted().GetSessionId()
	runningQueryID := runningFirst.GetQueryId()

	page, err := h.service.ListSessions(context.Background(), &codev1.ListSessionsRequest{PageSize: 1})
	if err != nil {
		t.Fatal(err)
	}
	if len(page.GetSessions()) != 1 || page.GetSessions()[0].GetSessionId() != runningID || page.GetNextPageToken() == "" {
		t.Fatalf("first session page = %+v, want running session plus token", page)
	}
	running := page.GetSessions()[0]
	if running.GetState() != codev1.AgentSessionState_AGENT_SESSION_STATE_RUNNING || running.GetActiveQueryId() != runningQueryID ||
		running.GetWorkingDirectory() != "/nested" || running.GetGeneration() != 1 {
		t.Fatalf("running session = %+v", running)
	}

	next, err := h.service.ListSessions(context.Background(), &codev1.ListSessionsRequest{
		PageSize: 1, PageToken: page.GetNextPageToken(),
	})
	if err != nil || len(next.GetSessions()) != 1 || next.GetSessions()[0].GetSessionId() != idleID {
		t.Fatalf("second session page = %+v, %v", next, err)
	}
	idle := next.GetSessions()[0]
	if idle.GetState() != codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE || idle.GetWorkingDirectory() != "/" {
		t.Fatalf("idle session = %+v", idle)
	}

	filtered, err := h.service.ListSessions(context.Background(), &codev1.ListSessionsRequest{
		States: []codev1.AgentSessionState{codev1.AgentSessionState_AGENT_SESSION_STATE_RUNNING},
	})
	if err != nil || len(filtered.GetSessions()) != 1 || filtered.GetSessions()[0].GetSessionId() != runningID {
		t.Fatalf("running session filter = %+v, %v", filtered, err)
	}

	if err := h.service.CancelQuery(runningQueryID); err != nil {
		t.Fatal(err)
	}
	collectFrames(t, runningTurn)
	if err := runningTurn.Wait(); err != nil {
		t.Fatal(err)
	}
	if err := h.service.CloseSession(context.Background(), idleID); err != nil {
		t.Fatal(err)
	}
	remaining, err := h.service.ListSessions(context.Background(), &codev1.ListSessionsRequest{})
	if err != nil || len(remaining.GetSessions()) != 1 || remaining.GetSessions()[0].GetSessionId() != runningID ||
		remaining.GetSessions()[0].GetState() != codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE {
		t.Fatalf("sessions after settle and close = %+v, %v", remaining, err)
	}

	h.currentProcess(t).kill()
	select {
	case <-h.currentProcess(t).done:
	case <-time.After(5 * time.Second):
		t.Fatal("agent process did not exit")
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		afterCrash, listErr := h.service.ListSessions(context.Background(), &codev1.ListSessionsRequest{})
		if listErr != nil {
			t.Fatal(listErr)
		}
		if len(afterCrash.GetSessions()) == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("sessions survived process crash: %+v", afterCrash)
		}
		time.Sleep(time.Millisecond)
	}
}

type agentTestClock struct{ nanoseconds atomic.Int64 }

func newAgentTestClock() *agentTestClock {
	clock := &agentTestClock{}
	clock.nanoseconds.Store(time.Date(2026, time.September, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	return clock
}

func (c *agentTestClock) now() time.Time { return time.Unix(0, c.nanoseconds.Load()).UTC() }

func (c *agentTestClock) advance(duration time.Duration) { c.nanoseconds.Add(int64(duration)) }
