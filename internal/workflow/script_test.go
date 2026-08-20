package workflow

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestCompileScriptAcceptsLiteralConditionalRoutes(t *testing.T) {
	program, operations, contextKeys, err := compileScript(`
let result = activity("review-agent", "manual", {task: parameters.task});
if result.status == "ok" {
  "accepted"
} else if parameters.repair {
  "repair"
} else {
  "rejected"
}
`, map[string][]string{
		"accepted": {"done"},
		"repair":   {"repair"},
		"rejected": {"failed"},
	})
	if err != nil {
		t.Fatalf("compile script: %v", err)
	}
	if program == nil {
		t.Fatal("compile script returned nil program")
	}
	if operations["review-agent"] != "manual" {
		t.Fatalf("operations = %#v", operations)
	}
	if len(contextKeys) != 0 {
		t.Fatalf("context keys = %#v", contextKeys)
	}
}

func TestCompileScriptRejectsDynamicAndUndeclaredRoutes(t *testing.T) {
	tests := []struct {
		name   string
		source string
		routes map[string][]string
		want   string
	}{
		{name: "variable", source: `let route = "a"; route`, routes: map[string][]string{"a": {"done"}}, want: "direct string literal"},
		{name: "undeclared", source: `if parameters.ok { "a" } else { "d" }`, routes: map[string][]string{"a": {"done"}, "b": {"done"}}, want: "undeclared=[d]"},
		{name: "unreachable declaration", source: `"a"`, routes: map[string][]string{"a": {"done"}, "b": {"done"}}, want: "unreachable=[b]"},
		{name: "conditional activity", source: `if activity("op", "manual", {}).status == "ok" { "a" } else { "b" }`, routes: map[string][]string{"a": {"done"}, "b": {"done"}}, want: "cannot be called from a conditional"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := compileScript(test.source, test.routes)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("compile error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestCompileScriptDoesNotInventReturnOrTryCatchSyntax(t *testing.T) {
	for _, source := range []string{`return "done"`, `try { "done" } catch { "failed" }`} {
		if _, _, _, err := compileScript(source, map[string][]string{"done": {"done"}, "failed": {"failed"}}); err == nil {
			t.Fatalf("compileScript(%q) succeeded", source)
		}
	}
}

func TestRunScriptSuspendsAndChecksReplay(t *testing.T) {
	source := `let result = activity("agent", "manual", {value: parameters.value}); if result.status == "ok" { "done" } else { "failed" }`
	program, operations, contextKeys, err := compileScript(source, map[string][]string{"done": {"success"}, "failed": {"failure"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled := &compiledNode{
		definition: NodeDefinition{Script: source, Routes: map[string][]string{"done": {"success"}, "failed": {"failure"}}},
		program:    program, operations: operations, contextKeys: contextKeys,
	}
	node := &NodeRun{ID: "work", State: NodeEvaluating, Activities: map[string]*Activity{}, ResolvedInputs: map[string]bool{}}
	_, err = runScript(t.Context(), node, compiled, map[string]any{"value": "one"}, map[string]string{}, nil)
	var suspension *suspendActivity
	if !errors.As(err, &suspension) || suspension.OperationID != "agent" {
		t.Fatalf("run error = %#v", err)
	}
	node.Activities["agent"] = &Activity{
		ID: "act-1", OperationID: "agent", ExecutorKind: "manual", InputHash: suspension.InputHash,
		State: ActivitySucceeded, Result: &ActivityResult{Status: "ok"},
	}
	result, err := runScript(t.Context(), node, compiled, map[string]any{"value": "one"}, map[string]string{}, nil)
	if err != nil || result.Route != "done" {
		t.Fatalf("run result = %#v, error = %v", result, err)
	}
	_, err = runScript(t.Context(), node, compiled, map[string]any{"value": "two"}, map[string]string{}, nil)
	if !errors.Is(err, ErrNonDeterminism) {
		t.Fatalf("replay error = %v, want ErrNonDeterminism", err)
	}
}

func TestRunScriptStagesWorkflowContextChanges(t *testing.T) {
	source := `
context_set("review.status", parameters.status);
context_delete("obsolete.key");
if context["prior.status"] == "ready" { "done" } else { "failed" }
`
	program, operations, contextKeys, err := compileScript(source, map[string][]string{
		"done": {"success"}, "failed": {"failure"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(operations) != 0 || len(contextKeys) != 2 {
		t.Fatalf("operations = %#v, context keys = %#v", operations, contextKeys)
	}
	compiled := &compiledNode{
		definition: NodeDefinition{Script: source, Routes: map[string][]string{"done": {"success"}, "failed": {"failure"}}},
		program:    program, operations: operations, contextKeys: contextKeys,
	}
	original := map[string]string{"prior.status": "ready", "obsolete.key": "old"}
	result, err := runScript(
		t.Context(),
		&NodeRun{ID: "work", State: NodeEvaluating, Activities: map[string]*Activity{}},
		compiled,
		map[string]any{"status": "accepted"},
		original,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Route != "done" || result.ContextWrites["review.status"] != "accepted" {
		t.Fatalf("script result = %#v", result)
	}
	if _, deleted := result.ContextDeletes["obsolete.key"]; !deleted {
		t.Fatalf("context deletes = %#v", result.ContextDeletes)
	}
	if original["obsolete.key"] != "old" {
		t.Fatalf("script mutated input context = %#v", original)
	}
}

func TestCompileScriptAllowsWorkflowContextMutationInBranches(t *testing.T) {
	source := `
if parameters.ok {
  context_set("branch.result", "accepted");
  "done"
} else {
  context_delete("branch.result");
  "failed"
}
`
	_, _, contextKeys, err := compileScript(source, map[string][]string{
		"done": {"success"}, "failed": {"failure"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, exists := contextKeys["branch.result"]; !exists || len(contextKeys) != 1 {
		t.Fatalf("context keys = %#v", contextKeys)
	}
}

func TestCompileScriptRejectsUnsafeWorkflowContextMutation(t *testing.T) {
	tests := []struct {
		name   string
		source string
		routes map[string][]string
		want   string
	}{
		{name: "dynamic key", source: `context_set(parameters.key, "value"); "done"`, routes: map[string][]string{"done": {"success"}}, want: "key must be a string literal"},
		{name: "invalid key", source: `context_set("bad key", "value"); "done"`, routes: map[string][]string{"done": {"success"}}, want: "key must be a string literal"},
		{name: "condition", source: `if context_set("state", "value") == nil { "done" } else { "failed" }`, routes: map[string][]string{"done": {"success"}, "failed": {"failure"}}, want: "cannot be called from a predicate, condition"},
		{name: "reserved context", source: `let context = {}; "done"`, routes: map[string][]string{"done": {"success"}}, want: "context is reserved"},
		{name: "internal context", source: `let hidden = __workflow_context; "done"`, routes: map[string][]string{"done": {"success"}}, want: "not script-visible"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, _, err := compileScript(test.source, test.routes)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("compile error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestValidateWorkflowContextLimits(t *testing.T) {
	tests := []struct {
		name    string
		context map[string]string
		want    string
	}{
		{name: "invalid key", context: map[string]string{"bad key": "value"}, want: "key must match"},
		{name: "NUL value", context: map[string]string{"valid.key": "bad\x00value"}, want: "without NUL"},
		{name: "large value", context: map[string]string{"valid.key": strings.Repeat("x", maxContextValueBytes+1)}, want: "at most"},
	}
	tooMany := make(map[string]string, maxContextEntries+1)
	for index := 0; index <= maxContextEntries; index++ {
		tooMany[fmt.Sprintf("key.%d", index)] = "value"
	}
	tests = append(tests, struct {
		name    string
		context map[string]string
		want    string
	}{name: "too many entries", context: tooMany, want: "more than"})
	largeTotal := make(map[string]string, maxContextBytes/maxContextValueBytes+1)
	for index := 0; index < maxContextBytes/maxContextValueBytes+1; index++ {
		largeTotal[fmt.Sprintf("key.%d", index)] = strings.Repeat("x", maxContextValueBytes)
	}
	tests = append(tests, struct {
		name    string
		context map[string]string
		want    string
	}{name: "large total", context: largeTotal, want: "exceeds"})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := validateWorkflowContext(test.context); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validation error = %v, want containing %q", err, test.want)
			}
		})
	}
}
