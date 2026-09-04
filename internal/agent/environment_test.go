package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

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

// TestStartTurn_EnvironmentRidesSessionMeta: caller overrides reach the
// agent-side session as `_meta.claudeCode.options.env` — on session/new and on
// session/resume alike — carrying exactly the caller's map (the operator
// baseline stays on the child and never rides the session). A query without
// overrides leaves `_meta` absent, so agents without the extension see a
// plain request.
func TestStartTurn_EnvironmentRidesSessionMeta(t *testing.T) {
	h := newHarnessWithConfig(t, EventLogConfig{}, func(c *Config) {
		c.Environment = map[string]string{"OPERATOR": "config", "SHARED": "config"}
	}, nil)

	overrides := map[string]string{"SHARED": "caller", "CALLER": "only"}
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Environment: overrides})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if err := stream.Wait(); err != nil {
		t.Fatalf("Wait() error = %v", err)
	}
	sessionID := frames[0].GetSessionStarted().GetSessionId()
	if got := metaEnv(t, h.currentProcess(t).agent.sessionMeta(0)); !mapsEqual(got, overrides) {
		t.Fatalf("session/new env = %v, want exactly the caller overrides %v", got, overrides)
	}

	plain, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "plain"})
	if err != nil {
		t.Fatalf("StartTurn(no overrides) error = %v", err)
	}
	collectFrames(t, plain)
	if err := plain.Wait(); err != nil {
		t.Fatalf("plain Wait() error = %v", err)
	}
	if meta := h.currentProcess(t).agent.sessionMeta(1); meta != nil {
		t.Fatalf("session/new _meta = %v, want nil without overrides", meta)
	}

	resumed := map[string]string{"WHO": "b"}
	second, err := h.service.StartTurn(context.Background(), TurnRequest{
		Prompt: "again", SessionID: sessionID, Environment: resumed,
	})
	if err != nil {
		t.Fatalf("StartTurn(resume with environment) error = %v", err)
	}
	collectFrames(t, second)
	if err := second.Wait(); err != nil {
		t.Fatalf("resumed Wait() error = %v", err)
	}
	resumes := h.currentProcess(t).agent.resumesReceived()
	if len(resumes) != 1 || string(resumes[0].SessionId) != sessionID {
		t.Fatalf("agent resumes = %+v, want one for %q", resumes, sessionID)
	}
	if got := metaEnv(t, resumes[0].Meta); !mapsEqual(got, resumed) {
		t.Fatalf("session/resume env = %v, want %v", got, resumed)
	}
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 (the child never switches for environment)", h.dialCount())
	}
}

// TestStartTurn_EnvironmentAlternatingQueriesReuseChild: environments select
// nothing at the child anymore — consecutive queries with differing
// environments share the one warm child, each session carrying its own `_meta`.
func TestStartTurn_EnvironmentAlternatingQueriesReuseChild(t *testing.T) {
	h := newHarness(t, nil)
	sessions := 0
	query := func(environment map[string]string) {
		t.Helper()
		index := sessions
		sessions++
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
		if got := metaEnv(t, h.currentProcess(t).agent.sessionMeta(index)); !mapsEqual(got, environment) {
			t.Fatalf("session %d env = %v, want %v", index, got, environment)
		}
	}
	envA := map[string]string{"WHO": "a"}
	envB := map[string]string{"WHO": "b"}

	query(envA)
	query(envB)
	query(envA)
	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 regardless of environment", h.dialCount())
	}
	if snapshot := h.service.Snapshot(); snapshot.Generation != 1 {
		t.Fatalf("snapshot generation = %d, want 1 (no environment-driven generations)", snapshot.Generation)
	}
}

// TestStartTurn_ConcurrentDistinctEnvironmentsBothRun: two turns with
// different environments run in parallel sessions on the one shared child,
// each session's `_meta` carrying its own overrides — the conflict error this
// used to raise is gone.
func TestStartTurn_ConcurrentDistinctEnvironmentsBothRun(t *testing.T) {
	h := newHarness(t, nil)

	envs := []map[string]string{{"WHO": "a"}, {"WHO": "b"}}
	var wg sync.WaitGroup
	for _, env := range envs {
		wg.Add(1)
		go func(env map[string]string) {
			defer wg.Done()
			stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "go", Environment: env})
			if err != nil {
				t.Errorf("StartTurn(%v) error = %v", env, err)
				return
			}
			collectFrames(t, stream)
			if err := stream.Wait(); err != nil {
				t.Errorf("Wait(%v) error = %v", env, err)
			}
		}(env)
	}
	wg.Wait()

	if h.dialCount() != 1 {
		t.Fatalf("dialed %d agent processes, want 1 for concurrent distinct environments", h.dialCount())
	}
	agent := h.currentProcess(t).agent
	if got := agent.sessionMeta(0); got == nil || agent.sessionMeta(1) == nil {
		t.Fatalf("session metas = %v, want one per concurrent session", got)
	}
	first, second := metaEnv(t, agent.sessionMeta(0)), metaEnv(t, agent.sessionMeta(1))
	// Arrival order between concurrent turns is not deterministic, so compare
	// as a set: each session carried exactly one of the two environments.
	if !((mapsEqual(first, envs[0]) && mapsEqual(second, envs[1])) ||
		(mapsEqual(first, envs[1]) && mapsEqual(second, envs[0]))) {
		t.Fatalf("session envs = %v, %v, want one each of %v and %v", first, second, envs[0], envs[1])
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

// metaEnv digs the environment overrides out of a session request's `_meta`,
// failing the test when the extension payload has any other shape than
// {_meta: {claudeCode: {options: {env: {...}}}}}.
func metaEnv(t *testing.T, meta map[string]any) map[string]string {
	t.Helper()
	if meta == nil {
		return nil
	}
	claudeCode, ok := meta["claudeCode"].(map[string]any)
	if !ok {
		t.Fatalf("_meta = %v, want a claudeCode object", meta)
	}
	options, ok := claudeCode["options"].(map[string]any)
	if !ok {
		t.Fatalf("_meta.claudeCode = %v, want an options object", claudeCode)
	}
	raw, ok := options["env"].(map[string]any)
	if !ok {
		t.Fatalf("_meta.claudeCode.options = %v, want an env object", options)
	}
	env := make(map[string]string, len(raw))
	for key, value := range raw {
		text, ok := value.(string)
		if !ok {
			t.Fatalf("env[%q] = %T, want a string", key, value)
		}
		env[key] = text
	}
	return env
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
