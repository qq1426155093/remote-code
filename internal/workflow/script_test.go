package workflow

import (
	"errors"
	"strings"
	"testing"
)

func TestCompileScriptAcceptsLiteralConditionalRoutes(t *testing.T) {
	program, operations, err := compileScript(`
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
			_, _, err := compileScript(test.source, test.routes)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("compile error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestCompileScriptDoesNotInventReturnOrTryCatchSyntax(t *testing.T) {
	for _, source := range []string{`return "done"`, `try { "done" } catch { "failed" }`} {
		if _, _, err := compileScript(source, map[string][]string{"done": {"done"}, "failed": {"failed"}}); err == nil {
			t.Fatalf("compileScript(%q) succeeded", source)
		}
	}
}

func TestRunScriptSuspendsAndChecksReplay(t *testing.T) {
	source := `let result = activity("agent", "manual", {value: parameters.value}); if result.status == "ok" { "done" } else { "failed" }`
	program, operations, err := compileScript(source, map[string][]string{"done": {"success"}, "failed": {"failure"}})
	if err != nil {
		t.Fatal(err)
	}
	compiled := &compiledNode{definition: NodeDefinition{Script: source, Routes: map[string][]string{"done": {"success"}, "failed": {"failure"}}}, program: program, operations: operations}
	node := &NodeRun{ID: "work", State: NodeEvaluating, Activities: map[string]*Activity{}, ResolvedInputs: map[string]bool{}}
	_, err = runScript(t.Context(), node, compiled, map[string]any{"value": "one"}, nil)
	var suspension *suspendActivity
	if !errors.As(err, &suspension) || suspension.OperationID != "agent" {
		t.Fatalf("run error = %#v", err)
	}
	node.Activities["agent"] = &Activity{
		ID: "act-1", OperationID: "agent", ExecutorKind: "manual", InputHash: suspension.InputHash,
		State: ActivitySucceeded, Result: &ActivityResult{Status: "ok"},
	}
	route, err := runScript(t.Context(), node, compiled, map[string]any{"value": "one"}, nil)
	if err != nil || route != "done" {
		t.Fatalf("run route = %q, error = %v", route, err)
	}
	_, err = runScript(t.Context(), node, compiled, map[string]any{"value": "two"}, nil)
	if !errors.Is(err, ErrNonDeterminism) {
		t.Fatalf("replay error = %v, want ErrNonDeterminism", err)
	}
}
