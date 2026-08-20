package workflow

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPrepareLoadsStrictWorkflowDefinition(t *testing.T) {
	workspace := t.TempDir()
	definition := writeWorkflowDefinition(t, validWorkflowYAML(`if parameters.ok { "done" } else { "failed" }`))
	registry, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if registry.Count() != 1 {
		t.Fatalf("registry count = %d", registry.Count())
	}
	summaries := registry.List()
	if len(summaries) != 1 || summaries[0].Name != "check" || len(summaries[0].Digest) != 64 || summaries[0].NodeCount != 3 {
		t.Fatalf("summaries = %#v", summaries)
	}
	definitionCopy, ok := registry.Get("check")
	if !ok || definitionCopy.Nodes[0].Timeout == 0 {
		t.Fatalf("definition = %#v, exists = %v", definitionCopy, ok)
	}
}

func TestRepositoryWorkflowExamplesPrepare(t *testing.T) {
	registry, err := Prepare(Config{
		Enabled:         true,
		DefinitionFiles: []string{filepath.Join("..", "..", "configs", "workflows", "review-change.workflow.yaml")},
	}, t.TempDir())
	if err != nil {
		t.Fatalf("prepare repository workflow examples: %v", err)
	}
	if registry.Count() != 1 {
		t.Fatalf("example registry count = %d", registry.Count())
	}
}

func TestPrepareRejectsUnsafeOrInvalidDefinitions(t *testing.T) {
	workspace := t.TempDir()
	inside := filepath.Join(workspace, "inside.workflow.yaml")
	if err := os.WriteFile(inside, []byte(validWorkflowYAML(`"done"`)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{inside}}, workspace); err == nil || !strings.Contains(err.Error(), "outside the workspace") {
		t.Fatalf("inside workspace error = %v", err)
	}

	target := writeWorkflowDefinition(t, validWorkflowYAML(`if parameters.ok { "done" } else { "failed" }`))
	symlink := filepath.Join(t.TempDir(), "link.workflow.yaml")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{symlink}}, workspace); err == nil {
		t.Fatal("Prepare accepted a symlink definition")
	}

	unknown := writeWorkflowDefinition(t, strings.Replace(validWorkflowYAML(`if parameters.ok { "done" } else { "failed" }`), "revision: 1", "revision: 1\n    surprise: true", 1))
	if _, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{unknown}}, workspace); err == nil || !strings.Contains(err.Error(), "unknown field") {
		t.Fatalf("unknown field error = %v", err)
	}

	undeclared := writeWorkflowDefinition(t, validWorkflowYAML(`if parameters.ok { "done" } else { "D" }`))
	if _, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{undeclared}}, workspace); err == nil || !strings.Contains(err.Error(), "undeclared=[D]") {
		t.Fatalf("undeclared route error = %v", err)
	}
}

func writeWorkflowDefinition(t *testing.T, contents string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "test.workflow.yaml")
	if err := os.WriteFile(name, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}

func validWorkflowYAML(script string) string {
	return `
version: 1
language: expr
workflows:
  - name: check
    description: Check one value.
    revision: 1
    entry: decide
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      properties:
        ok:
          type: boolean
      required: [ok]
      additionalProperties: false
    nodes:
      - id: decide
        timeout: 5m
        script: |-
          ` + script + `
        routes:
          done: [done]
          failed: [failed]
      - id: done
        terminal: succeeded
      - id: failed
        terminal: failed
`
}
