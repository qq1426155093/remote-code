package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

// scriptedAgent plays the agent role (claude-agent-acp) over in-memory pipes
// using the SDK's agent-side connection. Tests script turns through promptHook
// and inspect what the bridge sent via the recorded calls.
type scriptedAgent struct {
	conn *acp.AgentSideConnection
	// clientStdin is the client side of the stdin pipe; closing it emulates
	// killing the child (its reads end, then it exits and closes stdout).
	clientStdin io.WriteCloser
	// deadDone closes only after the fake child's pipes are fully torn down,
	// so hooks returning after it cannot race a final response through.
	deadDone <-chan struct{}

	caps acp.AgentCapabilities
	// promptHook scripts Prompt; nil answers end_turn immediately.
	promptHook func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error)
	// newSessionErr makes session/new fail with a JSON-RPC error.
	newSessionErr bool

	mu             sync.Mutex
	cancelArrived  chan struct{}
	sessionCwds    []string
	prompts        []acp.PromptRequest
	cancels        []string
	closedSessions []string
	nextSession    int
	selectedOption string
}

func (a *scriptedAgent) setConn(conn *acp.AgentSideConnection) { a.conn = conn }

func (a *scriptedAgent) closeClientStdin() { _ = a.clientStdin.Close() }

// --- Agent interface: the twelve spec methods ---

func (a *scriptedAgent) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (a *scriptedAgent) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: a.caps,
		AgentInfo:         &acp.Implementation{Name: "scripted-agent"},
	}, nil
}

func (a *scriptedAgent) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (a *scriptedAgent) Cancel(_ context.Context, params acp.CancelNotification) error {
	a.mu.Lock()
	a.cancels = append(a.cancels, string(params.SessionId))
	a.mu.Unlock()
	select {
	case a.cancelArrived <- struct{}{}:
	default:
	}
	return nil
}

func (a *scriptedAgent) CloseSession(_ context.Context, params acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.closedSessions = append(a.closedSessions, string(params.SessionId))
	return acp.CloseSessionResponse{}, nil
}

func (a *scriptedAgent) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, nil
}

func (a *scriptedAgent) NewSession(_ context.Context, params acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	if a.newSessionErr {
		return acp.NewSessionResponse{}, acp.NewMethodNotFound("session/new")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nextSession++
	a.sessionCwds = append(a.sessionCwds, params.Cwd)
	return acp.NewSessionResponse{SessionId: acp.SessionId(fmt.Sprintf("sess-%d", a.nextSession))}, nil
}

func (a *scriptedAgent) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	a.mu.Lock()
	a.prompts = append(a.prompts, params)
	a.mu.Unlock()
	if a.promptHook != nil {
		return a.promptHook(a, ctx, params)
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (a *scriptedAgent) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, nil
}

func (a *scriptedAgent) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}

func (a *scriptedAgent) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, nil
}

func (a *scriptedAgent) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// --- scripting helpers ---

func (a *scriptedAgent) update(ctx context.Context, sessionID acp.SessionId, update acp.SessionUpdate) error {
	return a.conn.SessionUpdate(ctx, acp.SessionNotification{SessionId: sessionID, Update: update})
}

func (a *scriptedAgent) requestPermission(ctx context.Context, sessionID acp.SessionId, options []acp.PermissionOption) (acp.RequestPermissionResponse, error) {
	response, err := a.conn.RequestPermission(ctx, acp.RequestPermissionRequest{
		SessionId: sessionID,
		Options:   options,
		ToolCall: acp.ToolCallUpdate{
			ToolCallId: "call_1",
			Title:      acp.Ptr("scripted action"),
		},
	})
	a.mu.Lock()
	if response.Outcome.Selected != nil {
		a.selectedOption = string(response.Outcome.Selected.OptionId)
	}
	a.mu.Unlock()
	return response, err
}

// waitForCancel blocks until the bridge sends session/cancel or the agent
// connection dies. The SDK also cancels the prompt handler's context on
// session/cancel; that counts as the cancel arriving too — a well-behaved
// agent flushes its final updates on a detached context afterwards, which is
// what the scripted hooks emulate.
func (a *scriptedAgent) waitForCancel(ctx context.Context) error {
	select {
	case <-a.cancelArrived:
		return nil
	case <-a.conn.Done():
		return errors.New("agent connection closed")
	case <-ctx.Done():
		return nil
	}
}

func (a *scriptedAgent) promptsReceived() []acp.PromptRequest {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]acp.PromptRequest(nil), a.prompts...)
}

func (a *scriptedAgent) selectedOptionID() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.selectedOption
}

func (a *scriptedAgent) cancelsReceived() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.cancels...)
}

func (a *scriptedAgent) closesReceived() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]string(nil), a.closedSessions...)
}

func (a *scriptedAgent) sessionDirectory(index int) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if index >= len(a.sessionCwds) {
		return ""
	}
	return a.sessionCwds[index]
}

// fakeProcess is one generation of the scripted agent child.
type fakeProcess struct {
	agent *scriptedAgent
	done  chan struct{}

	closeOnce  sync.Once
	mu         sync.Mutex
	terminated bool
}

// kill simulates an agent crash: the child's stdin pipe breaks first (the
// client's writes fail), then the child exits and closes its stdout.
func (f *fakeProcess) kill() {
	f.agent.closeClientStdin()
}

func (f *fakeProcess) wasTerminated() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.terminated
}

// harness wires the bridge to freshly spawned scripted agents, mirroring how
// a real agent process is dialed once per generation.
type harness struct {
	service   *Service
	workspace string

	mu        sync.Mutex
	dials     int
	dialErr   error
	processes []*fakeProcess
}

func (h *harness) currentProcess(t *testing.T) *fakeProcess {
	t.Helper()
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.processes) == 0 {
		t.Fatal("no agent process dialed yet")
	}
	return h.processes[len(h.processes)-1]
}

func (h *harness) dialCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.dials
}

func (h *harness) setDialError(err error) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.dialErr = err
}

// newHarness assembles a Service whose dial spawns an in-memory scripted
// agent per generation, with pipes that emulate a real child: closing the
// client's stdin makes the "process" exit and close its stdout. Replay is on,
// over a fresh runtime directory, with the default event-store bounds.
func newHarness(t *testing.T, configure func(*scriptedAgent)) *harness {
	t.Helper()
	return newHarnessWithEvents(t, EventLogConfig{}, configure)
}

// newHarnessWithEvents is newHarness with explicit event-store bounds; zero
// fields adopt the defaults exactly the way New does.
func newHarnessWithEvents(t *testing.T, events EventLogConfig, configure func(*scriptedAgent)) *harness {
	t.Helper()
	workspace := t.TempDir()
	h := &harness{workspace: workspace}
	service, err := New(Config{
		Enabled:          true,
		WorkspaceRoot:    workspace,
		RuntimeDirectory: t.TempDir(),
		Events:           events,
		Logger:           slog.New(slog.NewTextHandler(io.Discard, nil)),
		Dial: func(ctx context.Context) (transport, error) {
			h.mu.Lock()
			h.dials++
			dialErr := h.dialErr
			h.mu.Unlock()
			if dialErr != nil {
				return transport{}, dialErr
			}

			clientToAgentR, clientToAgentW := io.Pipe()
			agentToClientR, agentToClientW := io.Pipe()

			agent := &scriptedAgent{
				caps: acp.AgentCapabilities{
					SessionCapabilities: acp.SessionCapabilities{Close: &acp.SessionCloseCapabilities{}},
				},
				cancelArrived: make(chan struct{}, 1),
			}
			if configure != nil {
				configure(agent)
			}
			agentConn := acp.NewAgentSideConnection(agent, agentToClientW, clientToAgentR)
			agentConn.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
			agent.setConn(agentConn)
			agent.clientStdin = clientToAgentW

			process := &fakeProcess{agent: agent, done: make(chan struct{})}
			agent.deadDone = process.done
			// Emulate process exit: the child dies when its stdin closes or
			// its connection ends, and a dead process closes its stdout.
			go func() {
				<-agentConn.Done()
				process.closeOnce.Do(func() {
					_ = agentToClientW.Close()
					_ = clientToAgentW.Close()
					close(process.done)
				})
			}()

			h.mu.Lock()
			h.processes = append(h.processes, process)
			h.mu.Unlock()

			return transport{
				stdin:  clientToAgentW,
				stdout: agentToClientR,
				done:   process.done,
				terminate: func(ctx context.Context) error {
					process.mu.Lock()
					process.terminated = true
					process.mu.Unlock()
					_ = clientToAgentW.Close()
					select {
					case <-process.done:
						return nil
					case <-ctx.Done():
						return ctx.Err()
					}
				},
			}, nil
		},
	})
	if err != nil {
		t.Fatalf("assemble agent service: %v", err)
	}
	h.service = service
	t.Cleanup(func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = service.Shutdown(shutdownCtx)
	})
	return h
}

// collectFrames drains a turn's frames with a deadline so a stalled pump
// fails the test instead of hanging it.
func collectFrames(t *testing.T, stream *TurnStream) []*codev1.QueryResponse {
	t.Helper()
	collected := make(chan []*codev1.QueryResponse, 1)
	go func() {
		var frames []*codev1.QueryResponse
		for frame := range stream.Events() {
			frames = append(frames, frame)
		}
		collected <- frames
	}()
	select {
	case frames := <-collected:
		return frames
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for the turn to settle")
		return nil
	}
}

// frameTexts extracts the text of message frames, in order.
func frameTexts(frames []*codev1.QueryResponse) []string {
	texts := make([]string, 0, len(frames))
	for _, frame := range frames {
		if text := frame.GetMessage().GetText(); text != "" {
			texts = append(texts, text)
		}
	}
	return texts
}
