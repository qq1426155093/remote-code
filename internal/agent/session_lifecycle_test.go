package agent

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStartTurn_ClosesSessionAfterSettle locks the turn-scoped session
// lifecycle: one query creates exactly one session, the turn settles, the
// bridge forwards session/close, and the session leaves the table.
func TestStartTurn_ClosesSessionAfterSettle(t *testing.T) {
	h := newHarness(t, nil)

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(frames) == 0 || frames[0].GetSessionStarted().GetSessionId() != "sess-1" {
		t.Fatalf("first frame = %+v, want session_started sess-1", frames[0])
	}

	agent := h.currentProcess(t).agent
	// Wait() returned, so the auto-close has already been attempted.
	if closes := agent.closesReceived(); len(closes) != 1 || closes[0] != "sess-1" {
		t.Fatalf("agent closes = %v, want [sess-1] after settle", closes)
	}
	if snapshot := h.service.Snapshot(); snapshot.Sessions != 0 {
		t.Fatalf("sessions after settle = %d, want 0 (turn-scoped sessions do not linger)", snapshot.Sessions)
	}

	// Closing an already-auto-closed session is an idempotent success, and it
	// must not send a second session/close.
	if err := h.service.CloseSession(context.Background(), "sess-1"); err != nil {
		t.Fatalf("CloseSession(already closed) error = %v, want nil", err)
	}
	if closes := agent.closesReceived(); len(closes) != 1 {
		t.Fatalf("agent closes after manual close = %v, want still one", closes)
	}
}

// TestStartTurn_ClosesSessionWhenTurnFails: a turn that fails mid-prompt
// still closes its session and leaves nothing behind.
func TestStartTurn_ClosesSessionWhenTurnFails(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			return acp.PromptResponse{}, errors.New("model exploded")
		}
	})

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	collectFrames(t, stream)
	if err := stream.Wait(); status.Code(err) != codes.Unknown {
		t.Fatalf("Wait() error = %v, want Unknown", err)
	}
	if closes := h.currentProcess(t).agent.closesReceived(); len(closes) != 1 || closes[0] != "sess-1" {
		t.Fatalf("agent closes = %v, want the failed turn's session closed too", closes)
	}
	if snapshot := h.service.Snapshot(); snapshot.Sessions != 0 {
		t.Fatalf("sessions after failed turn = %d, want 0", snapshot.Sessions)
	}

	// The bridge stays usable after a failed turn.
	next, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "again"})
	if err != nil {
		t.Fatalf("StartTurn(again) error = %v", err)
	}
	collectFrames(t, next)
	if err := next.Wait(); status.Code(err) != codes.Unknown {
		t.Fatalf("Wait(again) error = %v, want Unknown", err)
	}
}

// TestStartTurn_ResumesSessionByID: passing session_id resumes the agent-side
// conversation via session/resume (no session_started frame) instead of
// opening a fresh session.
func TestStartTurn_ResumesSessionByID(t *testing.T) {
	h := newHarness(t, nil)

	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn(first) error = %v", err)
	}
	frames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatalf("Wait(first) error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()

	second, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "second", SessionID: sessionID})
	if err != nil {
		t.Fatalf("StartTurn(resume) error = %v", err)
	}
	resumedFrames := collectFrames(t, second)
	if err := second.Wait(); err != nil {
		t.Fatalf("Wait(resume) error = %v", err)
	}
	for _, frame := range resumedFrames {
		if frame.GetSessionStarted() != nil {
			t.Fatal("resumed turn must not emit session_started")
		}
	}

	agent := h.currentProcess(t).agent
	resumes := agent.resumesReceived()
	if len(resumes) != 1 || string(resumes[0].SessionId) != sessionID {
		t.Fatalf("agent resumes = %+v, want one for %q", resumes, sessionID)
	}
	root, err := filepath.EvalSymlinks(h.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if resumes[0].Cwd != root {
		t.Fatalf("resume cwd = %q, want workspace root %q", resumes[0].Cwd, root)
	}
	if got := len(agent.sessionCwds); got != 1 {
		t.Fatalf("agent created %d new sessions, want 1 (resume must not create)", got)
	}
	if prompts := agent.promptsReceived(); len(prompts) != 2 || string(prompts[1].SessionId) != sessionID {
		t.Fatalf("agent prompts = %+v, want the resumed prompt on %q", prompts, sessionID)
	}
	// The resumed turn's session closes like any other turn-scoped session.
	if closes := agent.closesReceived(); len(closes) != 2 || closes[0] != sessionID || closes[1] != sessionID {
		t.Fatalf("agent closes = %v, want both turns' sessions closed", closes)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d processes, want 1 (resume reuses the warm child)", h.dialCount())
	}
}

// TestStartTurn_ResumeUnknownSessionSurfacesAgentError: the bridge does not
// track session ids itself — session/resume passes through and the agent's
// refusal (unknown id, expired transcript) reaches the caller.
func TestStartTurn_ResumeUnknownSessionSurfacesAgentError(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.resumeErr = true
	})

	_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", SessionID: "ghost"})
	if status.Code(err) != codes.Unknown {
		t.Fatalf("StartTurn(unknown session) error = %v, want Unknown", err)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d processes, want 1 (resume still starts the agent)", h.dialCount())
	}
	if snapshot := h.service.Snapshot(); snapshot.Sessions != 0 {
		t.Fatalf("sessions after failed resume = %d, want 0", snapshot.Sessions)
	}

	// A subsequent query works; the failed resume left nothing behind.
	recovered, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "fresh"})
	if err != nil {
		t.Fatalf("StartTurn(after failed resume) error = %v", err)
	}
	collectFrames(t, recovered)
	if err := recovered.Wait(); err != nil {
		t.Fatalf("Wait(after failed resume) error = %v", err)
	}
}

// TestStartTurn_ResumeRequiresCapability: session_id on an agent that never
// advertised sessionCapabilities.resume is refused before any RPC reaches it.
func TestStartTurn_ResumeRequiresCapability(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.caps.SessionCapabilities.Resume = nil
	})

	_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", SessionID: "sess-9"})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartTurn(no resume capability) error = %v, want FailedPrecondition", err)
	}
	if got := h.currentProcess(t).agent.resumesReceived(); got != nil {
		t.Fatalf("agent resumes = %+v, want none without the capability", got)
	}
	if snapshot := h.service.Snapshot(); snapshot.Sessions != 0 {
		t.Fatalf("sessions = %d, want 0", snapshot.Sessions)
	}
}

// TestStartTurn_ResumeSurvivesProcessRestart: sessions are disk-backed on the
// agent side, so after a crash the next query with the same session id
// resumes on the restarted child instead of failing.
func TestStartTurn_ResumeSurvivesProcessRestart(t *testing.T) {
	var calls int
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			calls++
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn(first) error = %v", err)
	}
	frames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatalf("Wait(first) error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()

	h.currentProcess(t).kill()
	deadline := time.Now().Add(5 * time.Second)
	for h.service.Snapshot().Started {
		if time.Now().After(deadline) {
			t.Fatal("agent process did not exit")
		}
		time.Sleep(time.Millisecond)
	}

	resumed, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "after restart", SessionID: sessionID})
	if err != nil {
		t.Fatalf("StartTurn(resume after crash) error = %v", err)
	}
	resumedFrames := collectFrames(t, resumed)
	if err := resumed.Wait(); err != nil {
		t.Fatalf("Wait(resume after crash) error = %v", err)
	}
	for _, frame := range resumedFrames {
		if frame.GetSessionStarted() != nil {
			t.Fatal("resumed turn after restart must not emit session_started")
		}
	}
	if h.dialCount() != 2 {
		t.Fatalf("dialed %d processes, want 2 (restart after crash)", h.dialCount())
	}
	resumes := h.currentProcess(t).agent.resumesReceived()
	if len(resumes) != 1 || string(resumes[0].SessionId) != sessionID {
		t.Fatalf("new agent resumes = %+v, want one for %q", resumes, sessionID)
	}
}

// TestStartTurn_ConcurrentResumeSameSessionRejected: two callers resuming the
// same session id concurrently resolve to one turn and one AGENT_TURN_ACTIVE
// refusal — never two prompts on one agent session.
func TestStartTurn_ConcurrentResumeSameSessionRejected(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if len(prompt.Prompt) > 0 && prompt.Prompt[0].Text != nil && prompt.Prompt[0].Text.Text == "stall" {
				// Emit one update first: a resumed turn has no session_started
				// frame, so this is how the test learns the query id.
				if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("stalling")); err != nil {
					return acp.PromptResponse{}, err
				}
				if err := a.waitForCancel(ctx); err != nil {
					return acp.PromptResponse{}, err
				}
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn(first) error = %v", err)
	}
	frames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatalf("Wait(first) error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()

	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "stall", SessionID: sessionID})
	if err != nil {
		t.Fatalf("StartTurn(stall) error = %v", err)
	}
	queryIDs := make(chan string, 1)
	go func() {
		var seen bool
		for frame := range stream.Events() {
			if !seen {
				seen = true
				queryIDs <- frame.GetQueryId()
			}
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "clash", SessionID: sessionID})
		if status.Code(err) == codes.FailedPrecondition {
			if reason := rpcerror.ReasonOf(err); reason != rpcerror.AgentTurnActive {
				t.Fatalf("reason = %q, want %s", reason, rpcerror.AgentTurnActive)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("concurrent resume error = %v, want FailedPrecondition", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := h.service.CancelQuery(<-queryIDs); err != nil {
		t.Fatalf("CancelQuery() error = %v", err)
	}
	if err := stream.Wait(); err != nil {
		t.Fatalf("stalled Wait() error = %v", err)
	}
	if resumes := len(h.currentProcess(t).agent.resumesReceived()); resumes != 1 {
		t.Fatalf("agent resumes = %d, want 1 (the rejected clash must never reach the agent)", resumes)
	}
}
