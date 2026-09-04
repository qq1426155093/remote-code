package agent

import (
	"context"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// recordObserver captures one ObserveQuery stream for assertions.
type recordObserver struct {
	mu      sync.Mutex
	header  *codev1.AgentQueryHeader
	frames  []*codev1.QueryResponse
	end     *codev1.AgentQueryEnd
	onEvent chan struct{} // closed-bound signal: receives on every event if non-nil
	eventCh chan *codev1.QueryResponse
}

func (r *recordObserver) QueryHeader(header *codev1.AgentQueryHeader) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.header = header
	return nil
}

func (r *recordObserver) QueryEvent(frame *codev1.QueryResponse) error {
	r.mu.Lock()
	r.frames = append(r.frames, frame)
	r.mu.Unlock()
	if r.eventCh != nil {
		r.eventCh <- frame
	}
	return nil
}

func (r *recordObserver) QueryEnd(end *codev1.AgentQueryEnd) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.end = end
	return nil
}

func (r *recordObserver) snapshot() (*codev1.AgentQueryHeader, []*codev1.QueryResponse, *codev1.AgentQueryEnd) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.header, r.frames, r.end
}

func (r *recordObserver) frameTexts() []string {
	_, frames, _ := r.snapshot()
	texts := make([]string, 0, len(frames))
	for _, frame := range frames {
		if text := frame.GetMessage().GetText(); text != "" {
			texts = append(texts, text)
		}
	}
	return texts
}

// observe runs one ObserveQuery call and returns its terminal error.
func observe(t *testing.T, service *Service, queryID string, from uint64, follow bool, observer QueryObserver) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- service.ObserveQuery(context.Background(), queryID, from, follow, observer) }()
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		t.Fatal("ObserveQuery did not return")
		return nil
	}
}

func TestStartTurn_FramesCarryQueryIDAndSequence(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("one")); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(frames) != 3 {
		t.Fatalf("got %d frames, want 3 (started, message, completed)", len(frames))
	}
	queryID := frames[0].GetQueryId()
	if queryID == "" {
		t.Fatal("frames carry no query id")
	}
	for index, frame := range frames {
		if frame.GetQueryId() != queryID {
			t.Fatalf("frame %d query id %q, want %q", index, frame.GetQueryId(), queryID)
		}
		if frame.GetSequence() != uint64(index) {
			t.Fatalf("frame %d sequence %d, want %d", index, frame.GetSequence(), index)
		}
	}
	snapshot, ok := h.service.queries.Stat(queryID)
	if !ok || snapshot.State != QueryStateSettled || snapshot.Next != 3 {
		t.Fatalf("record snapshot %+v found=%v, want settled next=3", snapshot, ok)
	}
}

func TestStartTurn_DisconnectDetachesTurn(t *testing.T) {
	disconnected := make(chan struct{})
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("working")); err != nil {
				return acp.PromptResponse{}, err
			}
			<-disconnected
			// More frames than the stream buffer can hold, so forwarding to
			// the gone caller provably fails and only the record is left.
			for i := 0; i < streamBufferCapacity+8; i++ {
				if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText(fmt.Sprintf("tail-%d", i))); err != nil {
					return acp.PromptResponse{}, err
				}
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	stream, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	var queryID string
	select {
	case frame := <-stream.Events():
		queryID = frame.GetQueryId()
	case <-time.After(10 * time.Second):
		t.Fatal("no frame before disconnect")
	}
	// The caller goes away mid-turn.
	cancel()
	close(disconnected)

	// Detach: the turn keeps running and settles on its own.
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() after disconnect = %v, want clean settle", err)
	}
	if got := h.currentProcess(t).agent.cancelsReceived(); len(got) != 0 {
		t.Fatalf("disconnect sent session/cancel %v, want none (detach semantics)", got)
	}
	snapshot, ok := h.service.queries.Stat(queryID)
	if !ok || snapshot.State != QueryStateSettled || snapshot.StopReason != string(acp.StopReasonEndTurn) {
		t.Fatalf("record snapshot %+v found=%v, want settled end_turn after detach", snapshot, ok)
	}
	// session_started + working + the tails + completed: everything the agent
	// produced must be on disk even though nobody was listening.
	want := uint64(2 + streamBufferCapacity + 8 + 1)
	if snapshot.Next != want {
		t.Fatalf("record next %d, want %d (all frames persisted)", snapshot.Next, want)
	}
}

func TestService_CancelQuery(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("busy")); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.waitForCancel(ctx); err != nil {
				return acp.PromptResponse{}, err
			}
			// Post-cancel updates must still be consumed by the bridge. The
			// SDK cancels the handler context on session/cancel, so a real
			// agent flushes its final update on a detached context.
			if err := a.update(context.WithoutCancel(ctx), prompt.SessionId, acp.UpdateAgentMessageText("late")); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	var queryID string
	select {
	case frame := <-stream.Events():
		queryID = frame.GetQueryId()
	case <-time.After(10 * time.Second):
		t.Fatal("no frame before cancel")
	}

	if err := h.service.CancelQuery(queryID); err != nil {
		t.Fatalf("CancelQuery() error = %v", err)
	}
	frames := append([]*codev1.QueryResponse{}, collectFrames(t, stream)...)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() after cancel = %v", err)
	}
	if got := h.currentProcess(t).agent.cancelsReceived(); len(got) != 1 || got[0] != "sess-1" {
		t.Fatalf("agent cancels = %v, want [sess-1]", got)
	}
	if got := frameTexts(frames); len(got) != 2 || got[0] != "busy" || got[1] != "late" {
		t.Fatalf("frames after cancel = %v, want the post-cancel update consumed", got)
	}
	// The cancelled settle is visible in the record, not just the stream.
	snapshot, ok := h.service.queries.Stat(queryID)
	if !ok || snapshot.StopReason != string(acp.StopReasonCancelled) {
		t.Fatalf("record snapshot %+v found=%v, want settled cancelled", snapshot, ok)
	}

	// Idempotent: cancelling a settled query succeeds without effect.
	if err := h.service.CancelQuery(queryID); err != nil {
		t.Fatalf("CancelQuery(settled) error = %v", err)
	}
	err = h.service.CancelQuery("missing")
	if status.Code(err) != codes.NotFound || rpcerror.ReasonOf(err) != rpcerror.AgentQueryNotFound {
		t.Fatalf("CancelQuery(unknown) error = %v (reason %q), want NotFound AGENT_QUERY_NOT_FOUND", err, rpcerror.ReasonOf(err))
	}
}

func TestService_ObserveQuery_ReplaysSettledFromSequence(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			for _, chunk := range []string{"alpha", "beta", "gamma"} {
				if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText(chunk)); err != nil {
					return acp.PromptResponse{}, err
				}
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	queryID := frames[0].GetQueryId()

	recorder := &recordObserver{}
	if err := observe(t, h.service, queryID, 2, false, recorder); err != nil {
		t.Fatalf("ObserveQuery() error = %v", err)
	}
	header, observed, end := recorder.snapshot()
	if header.GetState() != codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED {
		t.Fatalf("header state %v, want SETTLED", header.GetState())
	}
	if header.GetResolvedStartSequence() != 2 || header.GetSnapshotEndSequence() != 5 || header.GetEarliestSequence() != 0 {
		t.Fatalf("header window = %+v, want resolved 2, snapshot end 5, earliest 0", header)
	}
	if header.GetStopReason() != string(acp.StopReasonEndTurn) {
		t.Fatalf("header stop reason = %q, want end_turn", header.GetStopReason())
	}
	if got := recorder.frameTexts(); len(got) != 2 || got[0] != "beta" || got[1] != "gamma" {
		t.Fatalf("observed texts %v, want [beta gamma]", got)
	}
	for index, frame := range observed {
		if frame.GetSequence() != uint64(2+index) {
			t.Fatalf("frame %d sequence %d, want %d", index, frame.GetSequence(), 2+index)
		}
	}
	if end.GetNextSequence() != 5 || end.GetReason() != codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SETTLED {
		t.Fatalf("end = %+v, want next 5 SETTLED", end)
	}
}

func TestService_ObserveQuery_ReplaysToolCallPayloads(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.StartToolCall("call_1", "read notes.txt",
				acp.WithStartRawInput(map[string]any{"path": "notes.txt"}),
			)); err != nil {
				return acp.PromptResponse{}, err
			}
			oldText := "stale"
			if err := a.update(ctx, prompt.SessionId, acp.UpdateToolCall("call_1",
				acp.WithUpdateStatus(acp.ToolCallStatusCompleted),
				acp.WithUpdateRawOutput([]any{map[string]any{"type": "text", "text": "42 lines"}}),
				acp.WithUpdateContent([]acp.ToolCallContent{{Diff: &acp.ToolCallContentDiff{
					Type: "diff", Path: "notes.txt", OldText: &oldText, NewText: "fresh",
				}}}),
			)); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hello"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	queryID := frames[0].GetQueryId()

	recorder := &recordObserver{}
	if err := observe(t, h.service, queryID, 0, false, recorder); err != nil {
		t.Fatalf("ObserveQuery() error = %v", err)
	}
	_, observed, _ := recorder.snapshot()
	wantOutput := []any{map[string]any{"text": "42 lines", "type": "text"}}
	for _, frame := range observed {
		call := frame.GetToolCall()
		if call == nil {
			continue
		}
		if !call.GetUpdate() {
			if input := call.GetRawInput(); input == nil || input.GetStructValue().GetFields()["path"].GetStringValue() != "notes.txt" {
				t.Fatalf("replayed raw input = %+v, want notes.txt path", call.GetRawInput())
			}
			continue
		}
		if output := call.GetRawOutput(); output == nil || !reflect.DeepEqual(output.AsInterface(), wantOutput) {
			t.Fatalf("replayed raw output = %+v, want %v", call.GetRawOutput(), wantOutput)
		}
		content := call.GetContent()
		if len(content) != 1 || content[0].GetStructValue().GetFields()["path"].GetStringValue() != "notes.txt" ||
			content[0].GetStructValue().GetFields()["oldText"].GetStringValue() != "stale" ||
			content[0].GetStructValue().GetFields()["newText"].GetStringValue() != "fresh" {
			t.Fatalf("replayed content = %+v, want one stale→fresh diff on notes.txt", content)
		}
	}
}

func TestService_ObserveQuery_FollowsRunningQuery(t *testing.T) {
	proceed := make(chan struct{})
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("early")); err != nil {
				return acp.PromptResponse{}, err
			}
			<-proceed
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("late")); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	var queryID string
	// Drain the original stream so its buffer never blocks the pump.
	go func() {
		for range stream.Events() {
		}
	}()
	select {
	case frame := <-stream.Events():
		queryID = frame.GetQueryId()
	case <-time.After(10 * time.Second):
		t.Fatal("no frame before observing")
	}

	recorder := &recordObserver{}
	done := make(chan error, 1)
	go func() { done <- h.service.ObserveQuery(context.Background(), queryID, 0, true, recorder) }()

	// Let the turn finish; the observer must see both the replayed early
	// frames and the live tail, ending with the settle.
	deadline := time.After(20 * time.Second)
	waitHeader := func() *codev1.AgentQueryHeader {
		for {
			header, _, _ := recorder.snapshot()
			if header != nil {
				return header
			}
			select {
			case <-time.After(10 * time.Millisecond):
			case <-deadline:
				t.Fatal("observer never received a header")
			}
		}
	}
	header := waitHeader()
	if header.GetState() != codev1.AgentQueryState_AGENT_QUERY_STATE_RUNNING {
		t.Fatalf("header state %v, want RUNNING", header.GetState())
	}
	close(proceed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("ObserveQuery() error = %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("observer did not finish with the turn")
	}
	if got := recorder.frameTexts(); len(got) != 2 || got[0] != "early" || got[1] != "late" {
		t.Fatalf("observed texts %v, want [early late]", got)
	}
	_, observed, end := recorder.snapshot()
	last := observed[len(observed)-1]
	if last.GetCompleted() == nil || last.GetCompleted().GetStopReason() != string(acp.StopReasonEndTurn) {
		t.Fatalf("last observed frame = %+v, want completed end_turn", last)
	}
	if end.GetReason() != codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SETTLED || end.GetNextSequence() != 4 {
		t.Fatalf("end = %+v, want SETTLED next 4", end)
	}
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestService_ObserveQuery_AddressesAndErrors(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("busy")); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.waitForCancel(ctx); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	var queryID string
	select {
	case frame := <-stream.Events():
		queryID = frame.GetQueryId()
	case <-time.After(10 * time.Second):
		t.Fatal("no frame before observing")
	}

	err = observe(t, h.service, "missing", 0, false, &recordObserver{})
	if status.Code(err) != codes.NotFound || rpcerror.ReasonOf(err) != rpcerror.AgentQueryNotFound {
		t.Fatalf("unknown query error = %v (reason %q)", err, rpcerror.ReasonOf(err))
	}
	err = observe(t, h.service, queryID, 9, false, &recordObserver{})
	if status.Code(err) != codes.InvalidArgument || rpcerror.ReasonOf(err) != rpcerror.AgentQuerySequenceInvalid {
		t.Fatalf("beyond-next error = %v (reason %q), want InvalidArgument AGENT_QUERY_SEQUENCE_INVALID", err, rpcerror.ReasonOf(err))
	}
	_ = h.service.CancelQuery(queryID)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	// A settled record validates the window the same way a running one does.
	err = observe(t, h.service, queryID, 9, false, &recordObserver{})
	if status.Code(err) != codes.InvalidArgument || rpcerror.ReasonOf(err) != rpcerror.AgentQuerySequenceInvalid {
		t.Fatalf("settled beyond-next error = %v (reason %q), want InvalidArgument AGENT_QUERY_SEQUENCE_INVALID", err, rpcerror.ReasonOf(err))
	}
	// A settled record replays its terminal error path when there is one; a
	// clean cancel simply ends settled.
	recorder := &recordObserver{}
	if err := observe(t, h.service, queryID, 0, true, recorder); err != nil {
		t.Fatalf("observe settled error = %v", err)
	}
	if got := recorder.frameTexts(); len(got) != 1 || got[0] != "busy" {
		t.Fatalf("texts %v, want [busy]", got)
	}
}

func TestService_ObserveQuery_PrunedWindowRejected(t *testing.T) {
	h := newHarnessWithEvents(t, EventLogConfig{SegmentBytes: 64 << 10, MaxBytesPerQuery: 64 << 10, MaxTotalBytes: 64 << 10, RetentionAfterSettle: time.Hour, MaxObservers: 4}, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			big := strings.Repeat("p", 8<<10)
			for i := 0; i < 24; i++ {
				if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText(big)); err != nil {
					return acp.PromptResponse{}, err
				}
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "big"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	queryID := frames[0].GetQueryId()
	snapshot, _ := h.service.queries.Stat(queryID)
	if snapshot.Earliest == 0 {
		t.Fatal("test setup: per-query cap did not prune anything")
	}

	err = observe(t, h.service, queryID, 0, false, &recordObserver{})
	if status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentQueryEventsPruned {
		t.Fatalf("pruned error = %v (reason %q), want FailedPrecondition AGENT_QUERY_EVENTS_PRUNED", err, rpcerror.ReasonOf(err))
	}
	recorder := &recordObserver{}
	if err := observe(t, h.service, queryID, snapshot.Earliest, false, recorder); err != nil {
		t.Fatalf("observe retained window error = %v", err)
	}
	header, _, _ := recorder.snapshot()
	if !header.GetHistoryTruncated() {
		t.Fatal("header must report history_truncated when earliest > 0")
	}
}
