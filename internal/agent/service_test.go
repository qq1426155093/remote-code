package agent

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestStartTurn_TurnLifecycle(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("first chunk")); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentThoughtText("thinking")); err != nil {
				return acp.PromptResponse{}, err
			}
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("second chunk")); err != nil {
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

	if len(frames) != 5 {
		t.Fatalf("got %d frames, want 5 (started, message, thought, message, completed)", len(frames))
	}
	if frames[0].GetSessionStarted() == nil || frames[0].GetSessionStarted().GetSessionId() != "sess-1" {
		t.Fatalf("first frame = %+v, want session_started sess-1", frames[0])
	}
	if got := frameTexts(frames); len(got) != 2 || got[0] != "first chunk" || got[1] != "second chunk" {
		t.Fatalf("message chunks = %v", got)
	}
	sawThought := false
	for _, frame := range frames {
		if frame.GetThought() != nil {
			sawThought = frame.GetThought().GetText() == "thinking"
		}
	}
	if !sawThought {
		t.Fatalf("thought frame missing from %v", frames)
	}
	last := frames[len(frames)-1]
	if last.GetCompleted() == nil || last.GetCompleted().GetStopReason() != string(acp.StopReasonEndTurn) {
		t.Fatalf("last frame = %+v, want completed end_turn", last)
	}

	prompts := h.currentProcess(t).agent.promptsReceived()
	if len(prompts) != 1 || len(prompts[0].Prompt) != 1 || prompts[0].Prompt[0].Text == nil || prompts[0].Prompt[0].Text.Text != "hello" {
		t.Fatalf("agent received prompts = %+v, want one text prompt %q", prompts, "hello")
	}
}

func TestStartTurn_ReuseSessionForFollowUp(t *testing.T) {
	h := newHarness(t, nil)

	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()

	second, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "second", SessionID: sessionID})
	if err != nil {
		t.Fatalf("StartTurn(reuse) error = %v", err)
	}
	frames = collectFrames(t, second)
	if err := second.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	for _, frame := range frames {
		if frame.GetSessionStarted() != nil {
			t.Fatal("follow-up turn must not emit session_started")
		}
	}
	if got := len(h.currentProcess(t).agent.promptsReceived()); got != 2 {
		t.Fatalf("agent received %d prompts, want 2", got)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 shared process", h.dialCount())
	}
}

func TestStartTurn_RejectsInvalidRequests(t *testing.T) {
	h := newHarness(t, nil)
	if _, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "  "}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("empty prompt error = %v, want InvalidArgument", err)
	}
	if h.dialCount() != 0 {
		t.Fatalf("dialed %d times, want 0 (validation must not start the process)", h.dialCount())
	}
}

func TestStartTurn_TurnActiveRejected(t *testing.T) {
	var calls int32
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			// Only the first prompt stalls; the follow-up after the cancel
			// must complete on its own.
			if atomic.AddInt32(&calls, 1) > 1 {
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			}
			if err := a.waitForCancel(ctx); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := make(chan []*codev1.QueryResponse, 1)
	queryIDs := make(chan string, 1)
	go func() {
		var collected []*codev1.QueryResponse
		for frame := range stream.Events() {
			if len(collected) == 0 {
				queryIDs <- frame.GetQueryId()
			}
			collected = append(collected, frame)
		}
		frames <- collected
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "again", SessionID: "sess-1"})
		if err == nil {
			t.Fatal("concurrent turn on the same session was accepted")
		}
		if status.Code(err) == codes.FailedPrecondition {
			if rpcerror.ReasonOf(err) != rpcerror.AgentTurnActive {
				t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.AgentTurnActive)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("concurrent turn error = %v, want FailedPrecondition", err)
		}
		time.Sleep(10 * time.Millisecond)
	}

	if err := h.service.CancelQuery(<-queryIDs); err != nil {
		t.Fatalf("CancelQuery() error = %v", err)
	}
	select {
	case <-frames:
	case <-time.After(20 * time.Second):
		t.Fatal("cancelled turn did not settle")
	}
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// The slot is free again.
	next, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "next", SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("StartTurn(after cancel) error = %v", err)
	}
	collectFrames(t, next)
	if err := next.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
}

func TestStartTurn_ParallelSessionsShareProcess(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("from "+string(prompt.SessionId))); err != nil {
				return acp.PromptResponse{}, err
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})

	const sessions = 4
	streams := make([]*TurnStream, sessions)
	for i := range streams {
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "parallel"})
		if err != nil {
			t.Fatalf("StartTurn(%d) error = %v", i, err)
		}
		streams[i] = stream
	}
	ids := make(map[string]bool, sessions)
	for i, stream := range streams {
		frames := collectFrames(t, stream)
		if err := stream.Wait(); err != nil {
			t.Fatalf("Wait(%d) error = %v", i, err)
		}
		if len(frames) == 0 || frames[0].GetSessionStarted() == nil {
			t.Fatalf("stream %d has no session_started", i)
		}
		ids[frames[0].GetSessionStarted().GetSessionId()] = true
	}
	if len(ids) != sessions {
		t.Fatalf("got %d distinct session ids, want %d", len(ids), sessions)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d processes, want 1 shared process", h.dialCount())
	}
}

func TestRequestPermission_AutoApprove(t *testing.T) {
	tests := []struct {
		name    string
		options []acp.PermissionOption
		want    string
	}{
		{
			name: "allow_once preferred over earlier allow_always and reject",
			options: []acp.PermissionOption{
				{Kind: acp.PermissionOptionKindRejectOnce, Name: "Deny", OptionId: "deny"},
				{Kind: acp.PermissionOptionKindAllowAlways, Name: "Always", OptionId: "always"},
				{Kind: acp.PermissionOptionKindAllowOnce, Name: "Once", OptionId: "once"},
			},
			want: "once",
		},
		{
			name: "allow_always when no allow_once offered",
			options: []acp.PermissionOption{
				{Kind: acp.PermissionOptionKindRejectAlways, Name: "Never", OptionId: "never"},
				{Kind: acp.PermissionOptionKindAllowAlways, Name: "Always", OptionId: "always"},
			},
			want: "always",
		},
		{
			name: "first option when nothing allows",
			options: []acp.PermissionOption{
				{Kind: acp.PermissionOptionKindRejectOnce, Name: "Deny", OptionId: "deny"},
				{Kind: acp.PermissionOptionKindRejectAlways, Name: "Never", OptionId: "never"},
			},
			want: "deny",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, func(a *scriptedAgent) {
				options := test.options
				a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
					if _, err := a.requestPermission(ctx, prompt.SessionId, options); err != nil {
						return acp.PromptResponse{}, err
					}
					return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
				}
			})
			stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "go"})
			if err != nil {
				t.Fatalf("StartTurn() error = %v", err)
			}
			collectFrames(t, stream)
			if err := stream.Wait(); err != nil {
				t.Fatalf("Wait() error = %v", err)
			}
			if got := h.currentProcess(t).agent.selectedOptionID(); got != test.want {
				t.Fatalf("selected option = %q, want %q", got, test.want)
			}
		})
	}
}

func TestSelectPermissionOption(t *testing.T) {
	option := func(kind acp.PermissionOptionKind, id string) acp.PermissionOption {
		return acp.PermissionOption{Kind: kind, Name: id, OptionId: acp.PermissionOptionId(id)}
	}
	selected, ok := selectPermissionOption([]acp.PermissionOption{
		option(acp.PermissionOptionKindAllowAlways, "always"),
		option(acp.PermissionOptionKindAllowOnce, "once"),
	})
	if !ok || string(selected.OptionId) != "once" {
		t.Fatalf("selected = %v, %v; want once, true", selected, ok)
	}
	selected, ok = selectPermissionOption([]acp.PermissionOption{
		option(acp.PermissionOptionKindRejectOnce, "deny"),
	})
	if !ok || string(selected.OptionId) != "deny" {
		t.Fatalf("selected = %v, %v; want deny, true", selected, ok)
	}
	if _, ok := selectPermissionOption(nil); ok {
		t.Fatal("no options must not select")
	}
}

func TestStartTurn_ProcessCrash(t *testing.T) {
	var calls int32
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if err := a.update(ctx, prompt.SessionId, acp.UpdateAgentMessageText("before crash")); err != nil {
				return acp.PromptResponse{}, err
			}
			// Only the first prompt across generations dies mid-turn; the
			// revived process answers the next query normally.
			if atomic.AddInt32(&calls, 1) > 1 {
				return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
			}
			<-a.conn.Done()
			<-a.deadDone
			return acp.PromptResponse{}, errors.New("agent died mid-turn")
		}
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "doomed"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	sawMessage := make(chan struct{})
	go func() {
		for frame := range stream.Events() {
			if frame.GetMessage() != nil {
				close(sawMessage)
				return
			}
		}
	}()
	select {
	case <-sawMessage:
	case <-time.After(10 * time.Second):
		t.Fatal("no message event before crash")
	}

	h.currentProcess(t).kill()

	waitDone := make(chan error, 1)
	go func() { waitDone <- stream.Wait() }()
	select {
	case err := <-waitDone:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("Wait() error = %v, want Unavailable", err)
		}
		if rpcerror.ReasonOf(err) != rpcerror.AgentProcessLost {
			t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.AgentProcessLost)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("crashed turn did not settle")
	}

	// Sessions are disk-backed on the agent side, so the crashed generation's
	// session resumes on the restarted child.
	resumed, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "resume", SessionID: "sess-1"})
	if err != nil {
		t.Fatalf("StartTurn(resume after crash) error = %v", err)
	}
	collectFrames(t, resumed)
	if err := resumed.Wait(); err != nil {
		t.Fatalf("resume after crash Wait() error = %v", err)
	}

	// A fresh query transparently restarts the agent process too.
	revived, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "revive"})
	if err != nil {
		t.Fatalf("StartTurn(after crash) error = %v", err)
	}
	frames := collectFrames(t, revived)
	if err := revived.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	if len(frames) == 0 || frames[0].GetSessionStarted().GetSessionId() != "sess-1" {
		t.Fatalf("revived session frame = %+v, want a fresh sess-1 on the new process", frames)
	}
	if h.dialCount() != 2 {
		t.Fatalf("dialed %d processes, want 2 (restart after crash)", h.dialCount())
	}
	snapshot := h.service.Snapshot()
	if !snapshot.Started || snapshot.Generation != 2 {
		t.Fatalf("snapshot = %+v, want started generation 2", snapshot)
	}
}

func TestStartTurn_DialFailureMapsToStartFailed(t *testing.T) {
	h := newHarness(t, nil)
	h.setDialError(errors.New("spawn exploded"))
	_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hello"})
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("error = %v, want Unavailable", err)
	}
	if rpcerror.ReasonOf(err) != rpcerror.AgentStartFailed {
		t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.AgentStartFailed)
	}
}

func TestStartTurn_WorkingDirectoryConfined(t *testing.T) {
	outside := t.TempDir()
	h := newHarness(t, nil)
	subdir := filepath.Join(h.workspace, "nested", "deep")
	if err := os.MkdirAll(subdir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(h.workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	plainFile := filepath.Join(h.workspace, "file.txt")
	if err := os.WriteFile(plainFile, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		workingDirectory string
		wantCode         codes.Code
		wantReason       rpcerror.Reason
	}{
		{workingDirectory: "nested/deep", wantCode: codes.OK},
		{workingDirectory: "", wantCode: codes.OK},
		{workingDirectory: "/etc", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
		{workingDirectory: "../outside", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
		{workingDirectory: "nested/../..", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
		{workingDirectory: "escape", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
		{workingDirectory: "missing", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
		{workingDirectory: "file.txt", wantCode: codes.InvalidArgument, wantReason: rpcerror.AgentWorkingDirectory},
	}
	for _, test := range tests {
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "cwd", WorkingDirectory: test.workingDirectory})
		if test.wantCode == codes.OK {
			if err != nil {
				t.Fatalf("working directory %q: %v", test.workingDirectory, err)
			}
			collectFrames(t, stream)
			if err := stream.Wait(); err != nil {
				t.Fatalf("working directory %q: Wait() = %v", test.workingDirectory, err)
			}
			continue
		}
		if status.Code(err) != test.wantCode {
			t.Fatalf("working directory %q: error = %v, want %s", test.workingDirectory, err, test.wantCode)
		}
		if rpcerror.ReasonOf(err) != test.wantReason {
			t.Fatalf("working directory %q: reason = %q, want %s", test.workingDirectory, rpcerror.ReasonOf(err), test.wantReason)
		}
	}

	resolved, err := filepath.EvalSymlinks(subdir)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.currentProcess(t).agent.sessionDirectory(0); got != resolved {
		t.Fatalf("nested session cwd = %q, want %q", got, resolved)
	}
	root, err := filepath.EvalSymlinks(h.workspace)
	if err != nil {
		t.Fatal(err)
	}
	if got := h.currentProcess(t).agent.sessionDirectory(1); got != root {
		t.Fatalf("default session cwd = %q, want workspace root %q", got, root)
	}
}

// TestCloseSession: sessions are turn-scoped and auto-close when their turn
// settles, so CloseSession only refuses while the turn is still running —
// every other id is already closed and closing it again is an idempotent
// success that must not spam the agent with session/close.
func TestCloseSession(t *testing.T) {
	t.Run("unknown or already auto-closed id is idempotent success", func(t *testing.T) {
		h := newHarness(t, nil)
		if err := h.service.CloseSession(context.Background(), "never-existed"); err != nil {
			t.Fatalf("CloseSession(unknown) error = %v, want nil", err)
		}
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi"})
		if err != nil {
			t.Fatal(err)
		}
		collectFrames(t, stream)
		if err := stream.Wait(); err != nil {
			t.Fatal(err)
		}
		if err := h.service.CloseSession(context.Background(), "sess-1"); err != nil {
			t.Fatalf("CloseSession(after settle) error = %v, want nil", err)
		}
		if got := h.currentProcess(t).agent.closesReceived(); len(got) != 1 || got[0] != "sess-1" {
			t.Fatalf("agent closes = %v, want exactly the one auto-close of [sess-1]", got)
		}
	})

	t.Run("empty session id is invalid", func(t *testing.T) {
		h := newHarness(t, nil)
		if err := h.service.CloseSession(context.Background(), ""); status.Code(err) != codes.InvalidArgument {
			t.Fatalf("CloseSession(empty) error = %v, want InvalidArgument", err)
		}
	})

	t.Run("rejected while a turn is active", func(t *testing.T) {
		h := newHarness(t, func(a *scriptedAgent) {
			a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
				if err := a.waitForCancel(ctx); err != nil {
					return acp.PromptResponse{}, err
				}
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			}
		})
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "slow"})
		if err != nil {
			t.Fatal(err)
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
			err := h.service.CloseSession(context.Background(), "sess-1")
			if status.Code(err) == codes.FailedPrecondition && rpcerror.ReasonOf(err) == rpcerror.AgentTurnActive {
				break
			}
			if time.Now().After(deadline) {
				t.Fatalf("CloseSession() error = %v, want FailedPrecondition", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
		if got := h.currentProcess(t).agent.closesReceived(); got != nil {
			t.Fatalf("agent closes while busy = %v, want none", got)
		}
		if err := h.service.CancelQuery(<-queryIDs); err != nil {
			t.Fatalf("CancelQuery() error = %v", err)
		}
		if err := stream.Wait(); err != nil {
			t.Fatalf("Wait() error = %v", err)
		}
	})
}

func TestShutdown_CancelsTurnsAndStopsAgent(t *testing.T) {
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

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	settled := make(chan error, 1)
	go func() { settled <- stream.Wait() }()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()
	if err := h.service.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("Shutdown() error = %v", err)
	}

	select {
	case err := <-settled:
		if err != nil {
			t.Fatalf("turn Wait() after shutdown = %v, want clean settle", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("turn did not settle during shutdown")
	}
	if got := h.currentProcess(t).agent.cancelsReceived(); len(got) != 1 {
		t.Fatalf("agent cancels = %v, want the live turn cancelled", got)
	}
	if _, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "after"}); status.Code(err) != codes.Unavailable {
		t.Fatalf("StartTurn(after shutdown) error = %v, want Unavailable", err)
	}
	// Idempotent.
	if err := h.service.Shutdown(shutdownCtx); err != nil {
		t.Fatalf("second Shutdown() error = %v", err)
	}
}

func TestSnapshot_BeforeStart(t *testing.T) {
	h := newHarness(t, nil)
	if snapshot := h.service.Snapshot(); snapshot.Started || snapshot.ProcessID != "" || snapshot.Sessions != 0 {
		t.Fatalf("snapshot before first query = %+v, want zero state", snapshot)
	}
}

func TestEventFromSessionUpdate_MapsKinds(t *testing.T) {
	notification := func(update acp.SessionUpdate) acp.SessionNotification {
		return acp.SessionNotification{SessionId: "s", Update: update}
	}

	event, ok := eventFromSessionUpdate(notification(acp.UpdateUserMessageText("echo")))
	if ok {
		t.Fatalf("user_message_chunk mapped to %+v, want dropped", event)
	}
	event, ok = eventFromSessionUpdate(notification(acp.UpdateAgentMessageText("hi")))
	if !ok || event.Kind != EventKindMessage || event.Message.Text != "hi" {
		t.Fatalf("agent_message_chunk mapped to %+v, %v", event, ok)
	}
	line := 12
	event, ok = eventFromSessionUpdate(notification(acp.StartToolCall("call_1", "Edit file", acp.WithStartKind(acp.ToolKindEdit), acp.WithStartLocations([]acp.ToolCallLocation{{Path: "a.go", Line: &line}}))))
	if !ok || event.Kind != EventKindToolCall || event.ToolCall.ToolCallID != "call_1" || event.ToolCall.Status != "" || len(event.ToolCall.Locations) != 1 || event.ToolCall.Locations[0].Line != 12 {
		t.Fatalf("tool_call mapped to %+v, %v", event, ok)
	}
	event, ok = eventFromSessionUpdate(notification(acp.UpdateToolCall("call_1", acp.WithUpdateStatus(acp.ToolCallStatusCompleted))))
	if !ok || !event.ToolCall.Update || event.ToolCall.Status != "completed" {
		t.Fatalf("tool_call_update mapped to %+v, %v", event, ok)
	}
	event, ok = eventFromSessionUpdate(notification(acp.UpdatePlan(acp.PlanEntry{Content: "step", Priority: acp.PlanEntryPriorityMedium, Status: acp.PlanEntryStatusInProgress})))
	if !ok || event.Kind != EventKindPlan || len(event.Plan.Entries) != 1 {
		t.Fatalf("plan mapped to %+v, %v", event, ok)
	}
}
