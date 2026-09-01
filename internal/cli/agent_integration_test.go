package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/qq1426155093/remote-code/internal/agent"
	"github.com/qq1426155093/remote-code/internal/server"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

// cliAgentHelperEnvironment turns this test binary into a minimal ACP agent
// child for the CLI integration tests.
const cliAgentHelperEnvironment = "REMOTE_CODE_CLI_AGENT_HELPER"

// TestCLIAgentHelperProcess is the child side of the CLI agent tests.
func TestCLIAgentHelperProcess(t *testing.T) {
	if os.Getenv(cliAgentHelperEnvironment) != "1" {
		return
	}
	helper := &cliAgentHelper{}
	connection := acp.NewAgentSideConnection(helper, os.Stdout, os.Stdin)
	connection.SetLogger(slog.New(slog.NewTextHandler(io.Discard, nil)))
	helper.conn = connection
	<-connection.Done()
}

// cliAgentHelper answers prompts with a per-child turn counter and stalls on
// the "stall" prompt until the controller cancels the turn.
type cliAgentHelper struct {
	conn  *acp.AgentSideConnection
	turns int
}

func (h *cliAgentHelper) Authenticate(context.Context, acp.AuthenticateRequest) (acp.AuthenticateResponse, error) {
	return acp.AuthenticateResponse{}, nil
}

func (h *cliAgentHelper) Initialize(context.Context, acp.InitializeRequest) (acp.InitializeResponse, error) {
	return acp.InitializeResponse{
		ProtocolVersion:   acp.ProtocolVersionNumber,
		AgentCapabilities: acp.AgentCapabilities{SessionCapabilities: acp.SessionCapabilities{Close: &acp.SessionCloseCapabilities{}}},
		AgentInfo:         &acp.Implementation{Name: "cli-agent-helper"},
	}, nil
}

func (h *cliAgentHelper) Logout(context.Context, acp.LogoutRequest) (acp.LogoutResponse, error) {
	return acp.LogoutResponse{}, nil
}

func (h *cliAgentHelper) Cancel(context.Context, acp.CancelNotification) error { return nil }

func (h *cliAgentHelper) CloseSession(context.Context, acp.CloseSessionRequest) (acp.CloseSessionResponse, error) {
	return acp.CloseSessionResponse{}, nil
}

func (h *cliAgentHelper) ListSessions(context.Context, acp.ListSessionsRequest) (acp.ListSessionsResponse, error) {
	return acp.ListSessionsResponse{}, nil
}

func (h *cliAgentHelper) NewSession(context.Context, acp.NewSessionRequest) (acp.NewSessionResponse, error) {
	return acp.NewSessionResponse{SessionId: "cli-helper-1"}, nil
}

func (h *cliAgentHelper) Prompt(ctx context.Context, params acp.PromptRequest) (acp.PromptResponse, error) {
	h.turns++
	if len(params.Prompt) > 0 && params.Prompt[0].Text != nil && params.Prompt[0].Text.Text == "stall" {
		<-ctx.Done()
		return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
	}
	if err := h.conn.SessionUpdate(ctx, acp.SessionNotification{
		SessionId: params.SessionId, Update: acp.UpdateAgentMessageText("turn " + strconv.Itoa(h.turns)),
	}); err != nil {
		return acp.PromptResponse{}, err
	}
	return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
}

func (h *cliAgentHelper) ResumeSession(context.Context, acp.ResumeSessionRequest) (acp.ResumeSessionResponse, error) {
	return acp.ResumeSessionResponse{}, nil
}

func (h *cliAgentHelper) SetSessionConfigOption(context.Context, acp.SetSessionConfigOptionRequest) (acp.SetSessionConfigOptionResponse, error) {
	return acp.SetSessionConfigOptionResponse{}, nil
}

func (h *cliAgentHelper) SetSessionMode(context.Context, acp.SetSessionModeRequest) (acp.SetSessionModeResponse, error) {
	return acp.SetSessionModeResponse{}, nil
}

func (h *cliAgentHelper) LoadSession(context.Context, acp.LoadSessionRequest) (acp.LoadSessionResponse, error) {
	return acp.LoadSessionResponse{}, nil
}

// lockedBuffer is a mutex-guarded buffer: the REPL renders on its own
// goroutine while the test polls the output.
type lockedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *lockedBuffer) Write(data []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(data)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// newAgentREPL boots a controller whose agent child is this test binary in
// helper mode, plus a REPL writing to a pollable buffer.
func newAgentREPL(t *testing.T) (*REPL, *lockedBuffer) {
	t.Helper()
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(),
		Agent: agent.Config{
			Enabled: true,
			Command: os.Args[0],
			Arguments: []string{
				"-test.run=^TestCLIAgentHelperProcess$", "-test.count=1",
			},
			Environment: map[string]string{cliAgentHelperEnvironment: "1"},
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	client, err := remoteclient.New(ctx, remoteclient.Config{Address: controller.Address()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	output := &lockedBuffer{}
	repl := New(client, nil, Config{Timeout: 15 * time.Second, Stdout: output})
	return repl, output
}

// waitForOutput polls the shared buffer until want appears.
func waitForOutput(t *testing.T, output *lockedBuffer, want string) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if bytes.Contains([]byte(output.String()), []byte(want)) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("output never contained %q:\n%s", want, output.String())
}

func TestREPLAgentObserveReplaysAndCancels(t *testing.T) {
	repl, output := newAgentREPL(t)

	if err := repl.agentQuery([]string{"first"}); err != nil {
		t.Fatalf("agentQuery() error = %v", err)
	}
	queryID := queryIDOf(t, output)
	if !bytes.Contains([]byte(output.String()), []byte("stop: end_turn")) {
		t.Fatalf("agent output =\n%s", output.String())
	}

	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentQueries(nil); err != nil {
		t.Fatalf("agentQueries() error = %v", err)
	}
	if listed := output.String(); !strings.Contains(listed, queryID) || !strings.Contains(listed, "settled") ||
		!strings.Contains(listed, "cli-helper-1") {
		t.Fatalf("agent query listing =\n%s", listed)
	}

	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentSessions(nil); err != nil {
		t.Fatalf("agentSessions() error = %v", err)
	}
	if listed := output.String(); !strings.Contains(listed, "cli-helper-1") || !strings.Contains(listed, "idle") {
		t.Fatalf("agent session listing =\n%s", listed)
	}

	// Full replay renders the retained frames through the same renderer.
	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentObserve([]string{queryID}); err != nil {
		t.Fatalf("agentObserve() error = %v", err)
	}
	replayed := output.String()
	for _, want := range []string{"session: cli-helper-1", "turn 1", "stop: end_turn", "end: settled (next sequence 3)"} {
		if !bytes.Contains([]byte(replayed), []byte(want)) {
			t.Fatalf("replay output missing %q:\n%s", want, replayed)
		}
	}

	// Resuming from a sequence skips what the caller already received: the
	// session line and the message are gone, only the completed frame remains.
	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentObserve([]string{"--from", "2", queryID}); err != nil {
		t.Fatalf("agentObserve(--from) error = %v", err)
	}
	if resumed := output.String(); !bytes.Contains([]byte(resumed), []byte("stop: end_turn")) ||
		bytes.Contains([]byte(resumed), []byte("session: cli-helper-1")) ||
		bytes.Contains([]byte(resumed), []byte("turn 1")) {
		t.Fatalf("resumed output =\n%s", resumed)
	}

	// A stalled turn is stopped by agent-cancel and stays replayable. The
	// prompt produces no output until it settles, so the query id only prints
	// with the synthesized session_started of a fresh session.
	repl.agentSession = ""
	stalled := make(chan error, 1)
	go func() { stalled <- repl.agentQuery([]string{"stall"}) }()
	stallID := queryIDOf(t, output)
	if err := repl.agentCancel([]string{stallID}); err != nil {
		t.Fatalf("agentCancel() error = %v", err)
	}
	waitForOutput(t, output, "stop: cancelled")
	select {
	case err := <-stalled:
		if err != nil {
			t.Fatalf("stalled agentQuery() error = %v", err)
		}
	case <-time.After(15 * time.Second):
		t.Fatal("stalled turn did not settle after cancel")
	}

	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentObserve([]string{stallID}); err != nil {
		t.Fatalf("agentObserve(cancelled) error = %v", err)
	}
	if cancelled := output.String(); !bytes.Contains([]byte(cancelled), []byte("stop: cancelled")) {
		t.Fatalf("cancelled replay =\n%s", cancelled)
	}

	if err := repl.agentObserve(nil); err == nil {
		t.Fatal("agentObserve without a query id succeeded")
	}
	if err := repl.agentCancel(nil); err == nil {
		t.Fatal("agentCancel without a query id succeeded")
	}
}

func TestREPLAgentQueryInterruptCancelsTurn(t *testing.T) {
	repl, output := newAgentREPL(t)

	// The "terminal" interrupts the turn right after its first frame.
	interrupted := make(chan struct{})
	go func() {
		waitForOutput(t, output, "query: ")
		close(interrupted)
	}()
	repl.interruptContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		streamContext, cancel := context.WithCancel(parent)
		go func() {
			<-interrupted
			cancel()
		}()
		return streamContext, cancel
	}

	if err := repl.agentQuery([]string{"stall"}); err != nil {
		t.Fatalf("agentQuery() error = %v", err)
	}
	queryID := queryIDOf(t, output)
	waitForOutput(t, output, "cancelled turn "+queryID)

	// Later commands get a fresh, un-interrupted stream context.
	repl.interruptContext = func(parent context.Context) (context.Context, context.CancelFunc) {
		return context.WithCancel(parent)
	}

	// The cancelled turn settled on the controller and is replayable.
	output.mu.Lock()
	output.buf.Reset()
	output.mu.Unlock()
	if err := repl.agentObserve([]string{queryID}); err != nil {
		t.Fatalf("agentObserve() error = %v", err)
	}
	if cancelled := output.String(); !bytes.Contains([]byte(cancelled), []byte("stop: cancelled")) {
		t.Fatalf("replay after interrupt =\n%s", cancelled)
	}
}

// queryIDOf extracts the query id the REPL printed for the newest turn.
func queryIDOf(t *testing.T, output *lockedBuffer) string {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		for _, line := range strings.Split(output.String(), "\n") {
			if fields := strings.Fields(line); len(fields) == 2 && fields[0] == "query:" {
				return fields[1]
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("no query id in output:\n%s", output.String())
	return ""
}
