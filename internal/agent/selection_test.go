package agent

import (
	"context"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/acp-go-sdk"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// agentPickerOption builds the config option claude-agent-acp would offer for
// the given personas, "default" included like the real adapter lists it.
func agentPickerOption(personas ...string) acp.SessionConfigOption {
	values := make(acp.SessionConfigSelectOptionsUngrouped, 0, len(personas)+1)
	values = append(values, acp.SessionConfigSelectOption{Value: defaultAgentValue, Name: "Default"})
	for _, persona := range personas {
		values = append(values, acp.SessionConfigSelectOption{
			Value: acp.SessionConfigValueId(persona), Name: persona,
		})
	}
	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id:           agentConfigID,
		CurrentValue: defaultAgentValue,
		Options:      acp.SessionConfigSelectOptions{Ungrouped: &values},
	}}
}

// modePickerOption builds a non-agent select option, standing in for the
// other config options a real child reports.
func modePickerOption() acp.SessionConfigOption {
	values := acp.SessionConfigSelectOptionsUngrouped{{Value: "default", Name: "Manual"}}
	return acp.SessionConfigOption{Select: &acp.SessionConfigOptionSelect{
		Id:           acp.SessionConfigId("mode"),
		CurrentValue: "default",
		Options:      acp.SessionConfigSelectOptions{Ungrouped: &values},
	}}
}

// TestStartTurn_AgentSelectionApplied covers the happy path: the requested
// persona is selected through session/set_config_option before the prompt,
// the discovery cache reports every offered persona, and the turn's record
// and replay header retain which agent ran it.
func TestStartTurn_AgentSelectionApplied(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer", "planner")}
	})
	ctx := context.Background()
	stream, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "review this", Agent: "reviewer"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	frames := collectFrames(t, stream)
	if len(frames) == 0 || frames[len(frames)-1].GetCompleted() == nil {
		t.Fatalf("turn did not settle with a completed frame")
	}

	process := h.currentProcess(t)
	selections := process.agent.agentSelectionsReceived()
	if len(selections) != 1 ||
		selections[0].ConfigId != agentConfigID ||
		selections[0].SessionId != "sess-1" ||
		selections[0].Value != "reviewer" {
		t.Fatalf("agent selections = %+v, want one reviewer choice on sess-1", selections)
	}
	if prompts := process.agent.promptsReceived(); len(prompts) != 1 {
		t.Fatalf("prompt count = %d, want 1", len(prompts))
	}
	if names := h.service.Snapshot().Agents; !reflect.DeepEqual(names, []string{"reviewer", "planner"}) {
		t.Fatalf("Snapshot().Agents = %v, want [reviewer planner]", names)
	}

	queries, err := h.service.ListQueries(ctx, &codev1.ListQueriesRequest{})
	if err != nil {
		t.Fatalf("ListQueries() error = %v", err)
	}
	if len(queries.Queries) != 1 || queries.Queries[0].GetAgent() != "reviewer" {
		t.Fatalf("listed queries = %+v, want one row carrying agent reviewer", queries.Queries)
	}
	observer := &recordObserver{}
	if err := h.service.ObserveQuery(ctx, queries.Queries[0].QueryId, 0, false, observer); err != nil {
		t.Fatalf("ObserveQuery() error = %v", err)
	}
	header, _, _ := observer.snapshot()
	if header.GetAgent() != "reviewer" {
		t.Fatalf("replay header agent = %q, want reviewer", header.GetAgent())
	}
}

// TestStartTurn_AgentSelectionEmptyKeepsDefault covers the default: no
// selection call is made, but the session's offered personas still populate
// the discovery cache.
func TestStartTurn_AgentSelectionEmptyKeepsDefault(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer")}
	})
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	collectFrames(t, stream)
	if selections := h.currentProcess(t).agent.agentSelectionsReceived(); len(selections) != 0 {
		t.Fatalf("set_config_option calls = %+v, want none", selections)
	}
	if names := h.service.Snapshot().Agents; !reflect.DeepEqual(names, []string{"reviewer"}) {
		t.Fatalf("Snapshot().Agents = %v, want [reviewer]", names)
	}
}

// TestStartTurn_AgentDefaultExplicitSelection covers naming "default": it is
// one of the picker's values, so the selection call goes through with it.
func TestStartTurn_AgentDefaultExplicitSelection(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer")}
	})
	stream, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Agent: "default"})
	if err != nil {
		t.Fatalf("StartTurn() error = %v", err)
	}
	collectFrames(t, stream)
	selections := h.currentProcess(t).agent.agentSelectionsReceived()
	if len(selections) != 1 || selections[0].Value != defaultAgentValue {
		t.Fatalf("agent selections = %+v, want one default choice", selections)
	}
}

// TestStartTurn_AgentNameInvalid covers a persona the session never offered:
// the query fails before prompting, the just-created session is handed back,
// and the discovery cache still learns the real personas.
func TestStartTurn_AgentNameInvalid(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer")}
	})
	_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Agent: "nobody"})
	if status.Code(err) != codes.InvalidArgument || rpcerror.ReasonOf(err) != rpcerror.AgentNameInvalid {
		t.Fatalf("error = %v, want InvalidArgument/%s", err, rpcerror.AgentNameInvalid)
	}
	process := h.currentProcess(t)
	if prompts := process.agent.promptsReceived(); len(prompts) != 0 {
		t.Fatalf("prompt count = %d, want 0", len(prompts))
	}
	if closes := process.agent.closesReceived(); len(closes) != 1 || closes[0] != "sess-1" {
		t.Fatalf("closed sessions = %v, want the leaked sess-1 handed back", closes)
	}
	if sessions := h.service.Snapshot().Sessions; sessions != 0 {
		t.Fatalf("session table holds %d sessions, want 0", sessions)
	}
	if names := h.service.Snapshot().Agents; !reflect.DeepEqual(names, []string{"reviewer"}) {
		t.Fatalf("Snapshot().Agents = %v, want [reviewer]", names)
	}
}

// TestStartTurn_AgentSelectionUnsupported covers a child that reports no
// agent picker: an explicit persona is refused regardless of whether the
// child reported other config options or none at all.
func TestStartTurn_AgentSelectionUnsupported(t *testing.T) {
	tests := []struct {
		name          string
		configOptions []acp.SessionConfigOption
	}{
		{name: "no options at all", configOptions: nil},
		{name: "options without an agent picker", configOptions: []acp.SessionConfigOption{modePickerOption()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			h := newHarness(t, func(a *scriptedAgent) { a.configOptions = test.configOptions })
			_, err := h.service.StartTurn(context.Background(), TurnRequest{Prompt: "hi", Agent: "reviewer"})
			if status.Code(err) != codes.FailedPrecondition || rpcerror.ReasonOf(err) != rpcerror.AgentSelectionUnsupported {
				t.Fatalf("error = %v, want FailedPrecondition/%s", err, rpcerror.AgentSelectionUnsupported)
			}
			process := h.currentProcess(t)
			if prompts := process.agent.promptsReceived(); len(prompts) != 0 {
				t.Fatalf("prompt count = %d, want 0", len(prompts))
			}
			if closes := process.agent.closesReceived(); len(closes) != 1 || closes[0] != "sess-1" {
				t.Fatalf("closed sessions = %v, want the leaked sess-1 handed back", closes)
			}
			if names := h.service.Snapshot().Agents; len(names) != 0 {
				t.Fatalf("Snapshot().Agents = %v, want none offered", names)
			}
		})
	}
}

// TestStartTurn_AgentSelectionOnResume covers a resumed conversation: the
// persona is selected on the resumed session id, and a refused selection
// would leave the conversation alone (it predates the query).
func TestStartTurn_AgentSelectionOnResume(t *testing.T) {
	h := newHarness(t, func(a *scriptedAgent) {
		a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer")}
	})
	ctx := context.Background()
	first, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn(first) error = %v", err)
	}
	frames := collectFrames(t, first)
	sessionID := frames[0].GetSessionStarted().GetSessionId()
	if sessionID == "" {
		t.Fatalf("first turn opened without a session_started frame")
	}
	if selections := h.currentProcess(t).agent.agentSelectionsReceived(); len(selections) != 0 {
		t.Fatalf("default turn selected agents = %+v, want none", selections)
	}

	second, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "second", SessionID: sessionID, Agent: "reviewer"})
	if err != nil {
		t.Fatalf("StartTurn(second) error = %v", err)
	}
	collectFrames(t, second)
	selections := h.currentProcess(t).agent.agentSelectionsReceived()
	if len(selections) != 1 || selections[0].SessionId != acp.SessionId(sessionID) || selections[0].Value != "reviewer" {
		t.Fatalf("agent selections = %+v, want one reviewer choice on %s", selections, sessionID)
	}
	if resumes := h.currentProcess(t).agent.resumesReceived(); len(resumes) != 1 {
		t.Fatalf("resume count = %d, want 1", len(resumes))
	}
}

// TestSnapshot_AgentsFollowGeneration covers the discovery cache's scope: a
// new generation of the child replaces the reported personas once one of its
// sessions has reported its own options.
func TestSnapshot_AgentsFollowGeneration(t *testing.T) {
	var offerPicker atomic.Bool
	offerPicker.Store(true)
	h := newHarness(t, func(a *scriptedAgent) {
		if offerPicker.Load() {
			a.configOptions = []acp.SessionConfigOption{agentPickerOption("reviewer")}
		} else {
			a.configOptions = []acp.SessionConfigOption{modePickerOption()}
		}
	})
	ctx := context.Background()
	first, err := h.service.StartTurn(ctx, TurnRequest{Prompt: "first"})
	if err != nil {
		t.Fatalf("StartTurn(first) error = %v", err)
	}
	collectFrames(t, first)
	if names := h.service.Snapshot().Agents; !reflect.DeepEqual(names, []string{"reviewer"}) {
		t.Fatalf("Snapshot().Agents = %v, want [reviewer]", names)
	}

	// A crashed child restarts as a fresh generation whose sessions offer no
	// agent picker; the reported personas must follow.
	offerPicker.Store(false)
	h.currentProcess(t).kill()
	for deadline := time.Now().Add(10 * time.Second); ; {
		if !h.service.Snapshot().Started {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("crashed child was not cleared")
		}
		time.Sleep(5 * time.Millisecond)
	}
	second, err := h.service.StartTurn(ctx, TurnRequest{
		Prompt: "second",
	})
	if err != nil {
		t.Fatalf("StartTurn(second) error = %v", err)
	}
	collectFrames(t, second)
	if dials := h.dialCount(); dials != 2 {
		t.Fatalf("dialed %d processes, want 2 generations", dials)
	}
	if names := h.service.Snapshot().Agents; len(names) != 0 {
		t.Fatalf("Snapshot().Agents = %v, want none after the switch", names)
	}
}
