package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TestStartTurn_EnvironmentValidatedBeforeStart locks the request-environment
// budget: the same limits the process registry enforces on any child, checked
// before the agent process is ever dialed.
func TestStartTurn_EnvironmentValidatedBeforeStart(t *testing.T) {
	h := newHarness(t, nil)

	total := make(map[string]string, 17)
	for i := range 17 {
		total[fmt.Sprintf("K%02d", i)] = strings.Repeat("v", 4000)
	}
	many := make(map[string]string, 300)
	for i := range 300 {
		many[fmt.Sprintf("K%03d", i)] = "v"
	}

	tests := []struct {
		name        string
		environment map[string]string
	}{
		{name: "empty key", environment: map[string]string{"": "value"}},
		{name: "key with equals", environment: map[string]string{"BAD=KEY": "value"}},
		{name: "key with space", environment: map[string]string{"BAD KEY": "value"}},
		{name: "key starting with digit", environment: map[string]string{"1BAD": "value"}},
		{name: "NUL in value", environment: map[string]string{"BAD": "va\x00lue"}},
		{name: "entry over budget", environment: map[string]string{"BIG": strings.Repeat("v", 5000)}},
		{name: "too many entries", environment: many},
		{name: "total over budget", environment: total},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Environment: test.environment})
			if status.Code(err) != codes.InvalidArgument {
				t.Fatalf("StartTurn() error = %v, want InvalidArgument", err)
			}
			if rpcerror.ReasonOf(err) != rpcerror.AgentEnvironment {
				t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.AgentEnvironment)
			}
		})
	}
	if h.dialCount() != 0 {
		t.Fatalf("dialed %d agent processes, want 0 (invalid environment must not start one)", h.dialCount())
	}
}

// TestStartTurn_EnvironmentMergesOverConfig: caller overrides win over the
// operator's [agent].environment, and an identical environment reuses the
// shared child.
func TestStartTurn_EnvironmentMergesOverConfig(t *testing.T) {
	h := newHarnessWithConfig(t, EventLogConfig{}, func(c *Config) {
		c.Environment = map[string]string{"OPERATOR": "config", "SHARED": "config"}
	}, nil)

	environment := map[string]string{"SHARED": "caller", "CALLER": "only"}
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Environment: environment})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	want := map[string]string{"OPERATOR": "config", "SHARED": "caller", "CALLER": "only"}
	if got := h.dialEnvironment(t, 0); !mapsEqual(got, want) {
		t.Fatalf("dial environment = %v, want %v", got, want)
	}

	again, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "again", Environment: environment})
	if err != nil {
		t.Fatalf("StartTurn(same environment) error = %v", err)
	}
	collectFrames(t, again)
	if err := again.Wait(); err != nil {
		t.Fatalf("same environment Wait() error = %v", err)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 (identical environment reuses the child)", h.dialCount())
	}
}

// TestStartTurn_EnvironmentAlternatingQueriesRestartChild: with turn-scoped
// sessions the bridge is idle between queries, so consecutive environments
// drive generation switches — same environment reuses the warm child, a
// changed one restarts it.
func TestStartTurn_EnvironmentAlternatingQueriesRestartChild(t *testing.T) {
	h := newHarness(t, nil)
	query := func(environment map[string]string) {
		t.Helper()
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "go", Environment: environment})
		if err != nil {
			t.Fatalf("StartTurn(%v) error = %v", environment, err)
		}
		frames := collectFrames(t, stream)
		if len(frames) == 0 || frames[0].GetSessionStarted() == nil {
			t.Fatalf("frames = %v, want a fresh session per query", frames)
		}
		if err := stream.Wait(); err != nil {
			t.Fatalf("Wait(%v) error = %v", environment, err)
		}
	}
	envA := map[string]string{"WHO": "a"}
	envB := map[string]string{"WHO": "b"}

	query(envA)
	query(envA)
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 while the environment holds", h.dialCount())
	}

	query(envB)
	if h.dialCount() != 2 {
		t.Fatalf("dialed %d agent processes, want 2 after the first switch", h.dialCount())
	}
	if got := h.dialEnvironment(t, 1); !mapsEqual(got, envB) {
		t.Fatalf("switched dial environment = %v, want %v", got, envB)
	}
	if snapshot := h.service.Snapshot(); snapshot.Generation != 2 {
		t.Fatalf("snapshot generation = %d, want 2", snapshot.Generation)
	}

	query(envA)
	if h.dialCount() != 3 {
		t.Fatalf("dialed %d agent processes, want 3 after switching back", h.dialCount())
	}
	if snapshot := h.service.Snapshot(); snapshot.Generation != 3 {
		t.Fatalf("snapshot generation = %d, want 3", snapshot.Generation)
	}
}

// TestStartTurn_ResumeWithEnvironmentSwitchesGeneration: a resumed
// conversation may run under a different environment than it was created
// with — the environment selects the child, not the session.
func TestStartTurn_ResumeWithEnvironmentSwitchesGeneration(t *testing.T) {
	h := newHarness(t, nil)
	first, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Environment: map[string]string{"WHO": "a"}})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, first)
	if err := first.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()

	second, err := h.service.StartTurn(context.Background(), TurnRequest{
		Prompt: "again", SessionID: sessionID, Environment: map[string]string{"WHO": "b"},
	})
	if err != nil {
		t.Fatalf("StartTurn(resume with environment) error = %v", err)
	}
	resumedFrames := collectFrames(t, second)
	if err := second.Wait(); err != nil {
		t.Fatalf("resumed Wait() error = %v", err)
	}
	for _, frame := range resumedFrames {
		if frame.GetSessionStarted() != nil {
			t.Fatal("resumed turn must not emit session_started")
		}
	}
	if h.dialCount() != 2 {
		t.Fatalf("dialed %d agent processes, want 2 (resume ran on a WHO=b child)", h.dialCount())
	}
	if got := h.dialEnvironment(t, 1); !mapsEqual(got, map[string]string{"WHO": "b"}) {
		t.Fatalf("resume dial environment = %v, want WHO=b", got)
	}
	resumes := h.currentProcess(t).agent.resumesReceived()
	if len(resumes) != 1 || string(resumes[0].SessionId) != sessionID {
		t.Fatalf("agent resumes = %+v, want one for %q", resumes, sessionID)
	}
}

// TestStartTurn_EnvironmentConflictWhileTurnActive: a different environment
// requires a new generation, refused while any turn still runs; once the turn
// settles (and its session auto-closes) the switch goes through.
func TestStartTurn_EnvironmentConflictWhileTurnActive(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if len(prompt.Prompt) > 0 && prompt.Prompt[0].Text != nil && prompt.Prompt[0].Text.Text == "slow" {
				if err := a.waitForCancel(ctx); err != nil {
					return acp.PromptResponse{}, err
				}
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "slow"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
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
		_, err := h.service.StartTurn(context.Background(), TurnRequest{
			Prompt: "other", Environment: map[string]string{"OTHER": "env"},
		})
		if status.Code(err) == codes.FailedPrecondition {
			if rpcerror.ReasonOf(err) != rpcerror.AgentEnvConflict {
				t.Fatalf("reason = %q, want %s", rpcerror.ReasonOf(err), rpcerror.AgentEnvConflict)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("different-environment query error = %v, want FailedPrecondition conflict", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 (conflict must not switch while a turn runs)", h.dialCount())
	}

	if err := h.service.CancelQuery(<-queryIDs); err != nil {
		t.Fatalf("CancelQuery() error = %v", err)
	}
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}

	// The cancelled turn's session is gone; the bridge is idle again.
	after, err := h.service.StartTurn(context.Background(), TurnRequest{
		Prompt: "after", Environment: map[string]string{"OTHER": "env"},
	})
	if err != nil {
		t.Fatalf("StartTurn(after settle) error = %v", err)
	}
	collectFrames(t, after)
	if err := after.Wait(); err != nil {
		t.Fatalf("after Wait() error = %v", err)
	}
	if h.dialCount() != 2 {
		t.Fatalf("dialed %d agent processes, want 2 after the switch", h.dialCount())
	}
}

// TestQueryRecord_StoresEnvironmentKeysWithoutValues: the replay record keeps
// the caller-supplied key names for debugging and never a value.
func TestQueryRecord_StoresEnvironmentKeysWithoutValues(t *testing.T) {
	h := newHarness(t, nil)
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{
		Prompt: "hi", Environment: map[string]string{"AGENT_TOKEN": "super-secret-value", "FEATURE": "on"},
	})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	queryID := frames[0].GetQueryId()

	data, err := os.ReadFile(filepath.Join(h.runtimeDir, queryStoreDirectoryName, queryID, "state.json"))
	if err != nil {
		t.Fatalf("read state.json: %v", err)
	}
	var state struct {
		EnvironmentKeys []string `json:"environment_keys"`
	}
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("decode state.json: %v", err)
	}
	if len(state.EnvironmentKeys) != 2 || state.EnvironmentKeys[0] != "AGENT_TOKEN" || state.EnvironmentKeys[1] != "FEATURE" {
		t.Fatalf("environment_keys = %v, want [AGENT_TOKEN FEATURE]", state.EnvironmentKeys)
	}

	var leaked []string
	err = filepath.WalkDir(h.runtimeDir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "super-secret-value") {
			leaked = append(leaked, path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk runtime directory: %v", err)
	}
	if len(leaked) != 0 {
		t.Fatalf("environment value leaked into %v", leaked)
	}
}

// TestStartTurn_ConcurrentDistinctEnvironmentsSingleWinner: while one
// environment's turn runs, every other environment is refused with a
// retryable conflict instead of dueling restarts.
func TestStartTurn_ConcurrentDistinctEnvironmentsSingleWinner(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.promptHook = func(a *scriptedAgent, ctx context.Context, prompt acp.PromptRequest) (acp.PromptResponse, error) {
			if len(prompt.Prompt) > 0 && prompt.Prompt[0].Text != nil && prompt.Prompt[0].Text.Text == "winner" {
				if err := a.waitForCancel(ctx); err != nil {
					return acp.PromptResponse{}, err
				}
				return acp.PromptResponse{StopReason: acp.StopReasonCancelled}, nil
			}
			return acp.PromptResponse{StopReason: acp.StopReasonEndTurn}, nil
		}
	})
	winnerReady := make(chan string, 1)
	winnerQuery := func() *TurnStream {
		stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "winner"})
		if err != nil {
			t.Errorf("StartTurn(winner) error = %v", err)
			return nil
		}
		go func() {
			var seen bool
			for frame := range stream.Events() {
				if !seen {
					seen = true
					winnerReady <- frame.GetQueryId()
				}
			}
		}()
		return stream
	}
	stream := winnerQuery()
	if stream == nil {
		return
	}

	const callers = 8
	var wg sync.WaitGroup
	conflicts := make(chan int, callers)
	for i := range callers {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, err := h.service.StartTurn(context.Background(), TurnRequest{
				Prompt: "go", Environment: map[string]string{"WHO": strconv.Itoa(i)},
			})
			switch {
			case err == nil:
				t.Errorf("StartTurn(%d) succeeded under a running different-environment turn", i)
			case status.Code(err) == codes.FailedPrecondition && rpcerror.ReasonOf(err) == rpcerror.AgentEnvConflict:
				conflicts <- i
			default:
				t.Errorf("StartTurn(%d) error = %v, want env conflict", i, err)
			}
		}(i)
	}
	wg.Wait()
	close(conflicts)
	if got := len(conflicts); got != callers {
		t.Fatalf("conflicts = %d, want %d", got, callers)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 (no switching under a running turn)", h.dialCount())
	}

	if err := h.service.CancelQuery(<-winnerReady); err != nil {
		t.Fatalf("CancelQuery() error = %v", err)
	}
	if err := stream.Wait(); err != nil {
		t.Fatalf("winner Wait() error = %v", err)
	}
	after, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "after"})
	if err != nil {
		t.Fatalf("StartTurn(after) error = %v", err)
	}
	collectFrames(t, after)
	if err := after.Wait(); err != nil {
		t.Fatalf("after Wait() error = %v", err)
	}
}

func mapsEqual(got, want map[string]string) bool {
	if len(got) != len(want) {
		return false
	}
	for key, value := range want {
		if gotValue, ok := got[key]; !ok || gotValue != value {
			return false
		}
	}
	return true
}
