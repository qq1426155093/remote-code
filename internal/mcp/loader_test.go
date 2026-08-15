package mcpserver

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

const validDefinition = `
version: 1
namespace: test
description: Test tools
language: expr
tools:
  - name: echo
    title: Echo a path
    description: Return the supplied workspace-relative path without changing anything.
    capabilities: []
    annotations:
      read_only: true
      destructive: false
      idempotent: true
      open_world: false
    input_schema:
      type: object
      required: [path]
      additionalProperties: false
      properties:
        path:
          type: string
          description: A workspace-relative path.
    output_schema:
      type: string
    script: |-
      args.path
`

func TestPrepareCompilesStrictDefinition(t *testing.T) {
	workspace := t.TempDir()
	definition := writeDefinition(t, validDefinition)
	prepared, err := Prepare(Config{
		Enabled: true, DefinitionFiles: []string{definition}, Token: "test-token",
		ListenAddress: "127.0.0.1:9444",
	}, workspace, "127.0.0.1:9443")
	if err != nil {
		t.Fatalf("Prepare() error = %v", err)
	}
	if prepared.Registry == nil || len(prepared.Registry.ordered) != 1 || prepared.Registry.ordered[0].Name != "test.echo" {
		t.Fatalf("prepared registry = %#v", prepared.Registry)
	}
}

func TestPrepareRejectsUnsafeOrInconsistentDefinitions(t *testing.T) {
	workspace := t.TempDir()
	tests := []struct{ name, contents, want string }{
		{name: "duplicate key", contents: strings.Replace(validDefinition, "namespace: test", "namespace: test\nnamespace: again", 1), want: "duplicate mapping key"},
		{name: "folded script", contents: strings.Replace(validDefinition, "script: |-\n      args.path", "script: >-\n      args.path\n      + ''", 1), want: "literal block"},
		{name: "capability mismatch", contents: strings.Replace(validDefinition, "capabilities: []", "capabilities: [files.read]", 1), want: "do not exactly match"},
		{name: "external ref", contents: strings.Replace(validDefinition, "type: string\n    script", "$ref: https://example.com/string.json\n    script", 1), want: "local fragment"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := writeDefinition(t, test.contents)
			_, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{definition}, Token: "token", ListenAddress: "127.0.0.1:9444"}, workspace, "127.0.0.1:9443")
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("Prepare() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestPrepareRejectsDefinitionSymlinkAndWorkspaceFile(t *testing.T) {
	workspace := t.TempDir()
	inside := filepath.Join(workspace, "inside.mcp.yaml")
	if err := os.WriteFile(inside, []byte(validDefinition), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := Prepare(Config{Enabled: true, DefinitionFiles: []string{inside}, Token: "token", ListenAddress: "127.0.0.1:9444"}, workspace, "127.0.0.1:9443")
	if err == nil || !strings.Contains(err.Error(), "outside the workspace") {
		t.Fatalf("inside definition error = %v", err)
	}

	target := writeDefinition(t, validDefinition)
	symlink := filepath.Join(t.TempDir(), "link.mcp.yaml")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	_, err = Prepare(Config{Enabled: true, DefinitionFiles: []string{symlink}, Token: "token", ListenAddress: "127.0.0.1:9444"}, workspace, "127.0.0.1:9443")
	if err == nil || !strings.Contains(err.Error(), "open MCP definition") {
		t.Fatalf("symlink definition error = %v", err)
	}
}

func TestCursorRejectsTampering(t *testing.T) {
	var digest [32]byte
	digest[0] = 7
	codec, err := newCursorCodec(digest)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := codec.encode(10)
	if err != nil {
		t.Fatal(err)
	}
	if offset, err := codec.decode(encoded, 20); err != nil || offset != 10 {
		t.Fatalf("decode = %d, %v", offset, err)
	}
	replacement := byte('A')
	if encoded[len(encoded)-1] == replacement {
		replacement = 'B'
	}
	tampered := encoded[:len(encoded)-1] + string(replacement)
	if _, err := codec.decode(tampered, 20); err == nil {
		t.Fatal("tampered cursor decoded")
	}
}

func TestExampleDefinitionsCompile(t *testing.T) {
	paths := []string{filepath.Join("..", "..", "configs", "mcp", "file.mcp.yaml"), filepath.Join("..", "..", "configs", "mcp", "process.mcp.yaml")}
	_, err := Prepare(Config{
		Enabled: true, ListenAddress: "127.0.0.1:9444", Token: "test-token", DefinitionFiles: paths,
		AllowedHostCapabilities: []string{"files.read", "files.write", "processes.read", "processes.start"},
	}, t.TempDir(), "127.0.0.1:9443")
	if err != nil {
		t.Fatalf("example definitions do not compile: %v", err)
	}
}

func TestDecodeJSONValueRejectsDuplicateKeys(t *testing.T) {
	if _, err := decodeJSONValue([]byte(`{"path":"one","path":"two"}`)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate JSON error = %v", err)
	}
}

func writeDefinition(t *testing.T, contents string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "tools.mcp.yaml")
	if err := os.WriteFile(name, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}
