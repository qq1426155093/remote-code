package cli

import (
	"bytes"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/protobuf/types/known/structpb"
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

func TestParseAgentQueryOptionsEnvironment(t *testing.T) {
	options, err := parseAgentQueryOptions([]string{"--env", "FOO=bar", "--env", "EMPTY=", "--env", "URL=https://example.com/x?y=1", "go"})
	if err != nil {
		t.Fatalf("parseAgentQueryOptions() error = %v", err)
	}
	want := map[string]string{"FOO": "bar", "EMPTY": "", "URL": "https://example.com/x?y=1"}
	if len(options.environment) != len(want) || options.environment["FOO"] != "bar" ||
		options.environment["EMPTY"] != "" || options.environment["URL"] != "https://example.com/x?y=1" {
		t.Fatalf("environment = %v, want %v", options.environment, want)
	}
	if options.prompt != "go" {
		t.Fatalf("prompt = %q, want %q", options.prompt, "go")
	}

	for _, invalid := range [][]string{
		{"--env", "NO_EQUALS", "go"},
		{"--env", "=value", "go"},
		{"--env"},
		{"--env", "FOO=bar", "--env", "FOO=again", "go"},
	} {
		if _, err := parseAgentQueryOptions(invalid); err == nil {
			t.Fatalf("parseAgentQueryOptions(%q) accepted an invalid --env", invalid)
		}
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

func TestAgentEventRendererToolContent(t *testing.T) {
	inputStruct, err := structpb.NewStruct(map[string]any{"command": "ls"})
	if err != nil {
		t.Fatal(err)
	}
	input := structpb.NewStructValue(inputStruct)
	outputBlock, err := structpb.NewStruct(map[string]any{"type": "text", "text": "notes.txt\nreply.md"})
	if err != nil {
		t.Fatal(err)
	}
	output := structpb.NewListValue(&structpb.ListValue{Values: []*structpb.Value{structpb.NewStructValue(outputBlock)}})
	diff, err := structpb.NewStruct(map[string]any{"type": "diff", "path": "reply.md", "newText": "hello"})
	if err != nil {
		t.Fatal(err)
	}
	events := []*codev1.QueryResponse{
		{Event: &codev1.QueryResponse_ToolCall{ToolCall: &codev1.AgentToolCall{
			ToolCallId: "call_1", Kind: "execute", Title: "run ls", Status: "pending", RawInput: input,
		}}},
		{Event: &codev1.QueryResponse_ToolCall{ToolCall: &codev1.AgentToolCall{
			ToolCallId: "call_1", Update: true, Status: "completed", RawOutput: output,
		}}},
		{Event: &codev1.QueryResponse_ToolCall{ToolCall: &codev1.AgentToolCall{
			ToolCallId: "call_2", Update: true, Kind: "edit", Status: "completed",
			Content: []*structpb.Value{structpb.NewStructValue(diff)},
		}}},
	}

	var compact bytes.Buffer
	compactRenderer := &agentEventRenderer{output: &compact}
	for _, event := range events {
		if err := compactRenderer.write(event); err != nil {
			t.Fatal(err)
		}
	}
	wantCompact := "→ [execute] run ls (pending)\n" +
		"~ (completed)\n" +
		"~ [edit] (completed)\n"
	if got := compact.String(); got != wantCompact {
		t.Fatalf("compact output =\n%q\nwant:\n%q", got, wantCompact)
	}

	var verbose bytes.Buffer
	verboseRenderer := &agentEventRenderer{output: &verbose, ShowToolContent: true}
	for _, event := range events {
		if err := verboseRenderer.write(event); err != nil {
			t.Fatal(err)
		}
	}
	wantVerbose := "→ [execute] run ls (pending)\n" +
		"  in  {\"command\":\"ls\"}\n" +
		"~ (completed)\n" +
		"  notes.txt\n" +
		"  reply.md\n" +
		"~ [edit] (completed)\n" +
		"  diff reply.md\n" +
		"  + hello\n"
	if got := verbose.String(); got != wantVerbose {
		t.Fatalf("verbose output =\n%q\nwant:\n%q", got, wantVerbose)
	}
}

func TestParseAgentQueryOptionsCompact(t *testing.T) {
	options, err := parseAgentQueryOptions([]string{"--compact", "go"})
	if err != nil || !options.compact || options.prompt != "go" {
		t.Fatalf("parseAgentQueryOptions(--compact) = %+v, %v", options, err)
	}
	if _, err := parseAgentQueryOptions([]string{"--compact", "--compact", "go"}); err == nil {
		t.Fatal("accepted a repeated --compact")
	}
	if _, err := parseAgentQueryOptions([]string{"--verbose", "go"}); err == nil {
		t.Fatal("accepted --verbose; tool content is the live default now")
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
	if err != nil || options.queryID != "q-1" || options.fromSequence != 0 || !options.follow {
		t.Fatalf("parseAgentObserveOptions() = %+v, %v", options, err)
	}
	options, err = parseAgentObserveOptions([]string{"--from", "7", "--no-follow", "q-2"})
	if err != nil || options.queryID != "q-2" || options.fromSequence != 7 || options.follow {
		t.Fatalf("parseAgentObserveOptions(flags) = %+v, %v", options, err)
	}
	if _, err := parseAgentObserveOptions([]string{"--from", "x", "q"}); err == nil {
		t.Fatal("accepted a non-numeric --from")
	}
	if _, err := parseAgentObserveOptions([]string{"--from", "1", "--from", "2", "q"}); err == nil {
		t.Fatal("accepted a repeated --from")
	}
	if _, err := parseAgentObserveOptions([]string{"--no-follow", "--no-follow", "q"}); err == nil {
		t.Fatal("accepted a repeated --no-follow")
	}
	if _, err := parseAgentObserveOptions([]string{"-f", "q"}); err == nil {
		t.Fatal("accepted an unknown agent-observe option")
	}
	if _, err := parseAgentObserveOptions([]string{"--verbose", "q"}); err == nil {
		t.Fatal("accepted --verbose; tool content is the observe default now")
	}
	options, err = parseAgentObserveOptions([]string{"--compact", "--no-follow", "q-3"})
	if err != nil || !options.compact || options.follow || options.queryID != "q-3" {
		t.Fatalf("parseAgentObserveOptions(--compact) = %+v, %v", options, err)
	}
	if _, err := parseAgentObserveOptions([]string{"--compact", "--compact", "q"}); err == nil {
		t.Fatal("accepted a repeated --compact")
	}
	if _, err := parseAgentObserveOptions(nil); err == nil {
		t.Fatal("accepted a missing query id")
	}
	if _, err := parseAgentObserveOptions([]string{"a", "b"}); err == nil {
		t.Fatal("accepted two query ids")
	}
}

func TestAgentCommandsRegistered(t *testing.T) {
	for _, name := range []string{"agent", "agent-query", "agent-close", "agent-observe", "agent-cancel", "agent-queries", "agent-sessions"} {
		if _, ok := defaultCommandRegistry.lookup(name); !ok {
			t.Fatalf("command %q is not registered", name)
		}
	}
}

func TestParseAgentListOptions(t *testing.T) {
	queries, err := parseAgentQueryListOptions([]string{
		"--session", "s-1", "--state", "running", "--state", "lost",
		"--page-size", "25", "--page-token", "next",
	})
	if err != nil || queries.sessionID != "s-1" || len(queries.states) != 2 || queries.pageSize != 25 || queries.pageToken != "next" {
		t.Fatalf("parseAgentQueryListOptions() = %+v, %v", queries, err)
	}
	sessions, err := parseAgentSessionListOptions([]string{"--state", "idle", "--page-size", "10"})
	if err != nil || len(sessions.states) != 1 || sessions.states[0] != codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE || sessions.pageSize != 10 {
		t.Fatalf("parseAgentSessionListOptions() = %+v, %v", sessions, err)
	}
	for _, arguments := range [][]string{
		{"--state", "unknown"}, {"--page-size", "0"}, {"--page-size", "x"},
		{"--page-token"}, {"--session", "a", "--session", "b"}, {"extra"},
	} {
		if _, err := parseAgentQueryListOptions(arguments); err == nil {
			t.Fatalf("agent query list accepted %v", arguments)
		}
	}
	if _, err := parseAgentSessionListOptions([]string{"--state", "settled"}); err == nil {
		t.Fatal("agent session list accepted query-only state")
	}
}
