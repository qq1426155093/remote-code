package cli

import (
	"bytes"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestParseAgentQueryOptions(t *testing.T) {
	options, err := parseAgentQueryOptions([]string{"hello", "world"})
	if err != nil || options.prompt != "hello world" || options.sessionID != "" || options.workingDirectory != "" {
		t.Fatalf("parseAgentQueryOptions() = %+v, %v", options, err)
	}
	options, err = parseAgentQueryOptions([]string{"--session", "s1", "--cwd", "src", "fix", "it"})
	if err != nil || options.prompt != "fix it" || options.sessionID != "s1" || options.workingDirectory != "src" {
		t.Fatalf("parseAgentQueryOptions() = %+v, %v", options, err)
	}
	options, err = parseAgentQueryOptions([]string{"--", "-dash", "prompt"})
	if err != nil || options.prompt != "-dash prompt" {
		t.Fatalf("parseAgentQueryOptions() after -- = %+v, %v", options, err)
	}
	if _, err := parseAgentQueryOptions([]string{"-x", "prompt"}); err == nil {
		t.Fatal("accepted unknown agent option")
	}
	if _, err := parseAgentQueryOptions([]string{"--session", "a", "--session", "b"}); err == nil {
		t.Fatal("accepted a repeated --session")
	}
	if _, err := parseAgentQueryOptions([]string{"--session", ""}); err == nil {
		t.Fatal("accepted an empty session id")
	}
	if options, err := parseAgentQueryOptions(nil); err != nil || options.prompt != "" {
		t.Fatalf("parseAgentQueryOptions(nil) = %+v, %v; the handler rejects the empty prompt", options, err)
	}
}

func TestAgentEventRenderer(t *testing.T) {
	line := int32(3)
	events := []*codev1.QueryResponse{
		{Event: &codev1.QueryResponse_SessionStarted{SessionStarted: &codev1.AgentSessionStarted{SessionId: "helper-1"}}},
		{Event: &codev1.QueryResponse_Message{Message: &codev1.AgentMessage{Text: "Hel"}}},
		{Event: &codev1.QueryResponse_Message{Message: &codev1.AgentMessage{Text: "lo"}}},
		{Event: &codev1.QueryResponse_Thought{Thought: &codev1.AgentThought{Text: "pondering"}}},
		{Event: &codev1.QueryResponse_ToolCall{ToolCall: &codev1.AgentToolCall{
			ToolCallId: "t1", Title: "edit notes", Kind: "edit",
		}}},
		{Event: &codev1.QueryResponse_ToolCall{ToolCall: &codev1.AgentToolCall{
			ToolCallId: "t1", Status: "completed", Update: true,
			Locations: []*codev1.AgentToolCallLocation{{Path: "notes.txt", Line: &line}},
		}}},
		{Event: &codev1.QueryResponse_Plan{Plan: &codev1.AgentPlan{Entries: []*codev1.AgentPlanEntry{
			{Content: "write", Status: "in_progress"}, {Content: "test", Priority: "high"},
		}}}},
		{Event: &codev1.QueryResponse_Usage{Usage: &codev1.AgentUsage{
			ContextSize: 1000, ContextUsed: 42, Cost: &codev1.AgentCost{Amount: 1.5, Currency: "USD"},
		}}},
		{Event: &codev1.QueryResponse_Message{Message: &codev1.AgentMessage{Text: "done\n"}}},
		{Event: &codev1.QueryResponse_Completed{Completed: &codev1.AgentTurnCompleted{StopReason: "end_turn"}}},
	}
	var output bytes.Buffer
	renderer := &agentEventRenderer{output: &output}
	for _, event := range events {
		if err := renderer.write(event); err != nil {
			t.Fatal(err)
		}
	}
	want := "session: helper-1\n" +
		"Hello" +
		"\n· pondering" +
		"\n→ [edit] edit notes" +
		"\n~ (completed) notes.txt:3" +
		"\nplan:" +
		"\n  [in_progress] write" +
		"\n  test (high)" +
		"\nusage: 42/1000 context, cost 1.50 USD" +
		"\ndone\n" +
		"stop: end_turn\n"
	if got := output.String(); got != want {
		t.Fatalf("agent event output =\n%q\nwant:\n%q", got, want)
	}
}

func TestAgentCloseSessionWithoutSession(t *testing.T) {
	var output bytes.Buffer
	repl := &REPL{stdout: &output, commands: defaultCommandRegistry}
	if _, err := repl.execute([]string{"agent-close"}); err == nil {
		t.Fatal("agent-close without a session succeeded")
	}
}

func TestParseAgentObserveOptions(t *testing.T) {
	options, err := parseAgentObserveOptions([]string{"q-1"})
	if err != nil || options.queryID != "q-1" || options.fromSequence != 0 || options.follow {
		t.Fatalf("parseAgentObserveOptions() = %+v, %v", options, err)
	}
	options, err = parseAgentObserveOptions([]string{"--from", "7", "--follow", "q-2"})
	if err != nil || options.queryID != "q-2" || options.fromSequence != 7 || !options.follow {
		t.Fatalf("parseAgentObserveOptions(flags) = %+v, %v", options, err)
	}
	options, err = parseAgentObserveOptions([]string{"-f", "q-3"})
	if err != nil || !options.follow {
		t.Fatalf("parseAgentObserveOptions(-f) = %+v, %v", options, err)
	}
	if _, err := parseAgentObserveOptions([]string{"--from", "x", "q"}); err == nil {
		t.Fatal("accepted a non-numeric --from")
	}
	if _, err := parseAgentObserveOptions([]string{"--from", "1", "--from", "2", "q"}); err == nil {
		t.Fatal("accepted a repeated --from")
	}
	if _, err := parseAgentObserveOptions([]string{"-x", "q"}); err == nil {
		t.Fatal("accepted an unknown agent-observe option")
	}
	if _, err := parseAgentObserveOptions(nil); err == nil {
		t.Fatal("accepted a missing query id")
	}
	if _, err := parseAgentObserveOptions([]string{"a", "b"}); err == nil {
		t.Fatal("accepted two query ids")
	}
}

func TestAgentCommandsRegistered(t *testing.T) {
	for _, name := range []string{"agent", "agent-query", "agent-close", "agent-observe", "agent-cancel"} {
		if _, ok := defaultCommandRegistry.lookup(name); !ok {
			t.Fatalf("command %q is not registered", name)
		}
	}
}
