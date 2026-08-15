package mcpserver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/qq1426155093/remote-code/internal/files"
	processservice "github.com/qq1426155093/remote-code/internal/process"
)

func TestExampleP0P1ToolsExecuteAgainstControllerServices(t *testing.T) {
	workspace := t.TempDir()
	original := "one\ntwo\n"
	if err := os.WriteFile(filepath.Join(workspace, "sample.txt"), []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	prepared, err := Prepare(Config{
		Enabled: true, ListenAddress: "127.0.0.1:9444", Token: "test-token",
		DefinitionFiles: []string{
			filepath.Join("..", "..", "configs", "mcp", "controller.mcp.yaml"),
			filepath.Join("..", "..", "configs", "mcp", "file.mcp.yaml"),
			filepath.Join("..", "..", "configs", "mcp", "process.mcp.yaml"),
		},
		AllowedHostCapabilities: []string{
			"controller.read", "files.read", "files.write", "processes.read", "processes.start", "processes.signal",
			"process_templates.read", "process_templates.start",
		},
	}, workspace, "127.0.0.1:9443")
	if err != nil {
		t.Fatal(err)
	}
	fileService, err := files.New(files.Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer fileService.Close()
	processService, err := processservice.New(processservice.Config{Workspace: workspace, RuntimeDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer processService.Close()
	runner := newRunner(prepared.Config, prepared.Registry, &controllerHosts{files: fileService, processes: processService})
	call := func(name string, arguments any) any {
		t.Helper()
		var tool *CompiledTool
		for _, candidate := range prepared.Registry.ordered {
			if candidate.Name == name {
				tool = candidate
				break
			}
		}
		if tool == nil {
			t.Fatalf("tool %q was not compiled", name)
		}
		raw, marshalErr := json.Marshal(arguments)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		result := runner.call(context.Background(), tool, raw, "test", "2026-07-28")
		if result.IsError {
			t.Fatalf("tool %q failed: %s", name, resultText(result))
		}
		return result.StructuredContent
	}

	info, ok := call("controller.info", map[string]any{}).(map[string]any)
	if !ok || info["workspace_name"] == "" || info["api_version"] == "" {
		t.Fatalf("controller.info = %#v", info)
	}
	read, ok := call("file.read_range", map[string]any{
		"path": "sample.txt", "start_line": 1, "max_lines": 1, "max_bytes": 64,
	}).(map[string]any)
	if !ok || read["content"] != "one\n" || read["next_line"] != int64(2) {
		t.Fatalf("file.read_range = %#v", read)
	}
	search, ok := call("file.search", map[string]any{
		"path": ".", "glob": "**", "query": "two", "case_sensitive": true,
		"max_results": 10, "max_bytes": 1 << 20,
	}).(map[string]any)
	matches, matchesOK := search["matches"].([]any)
	if !ok || !matchesOK || len(matches) != 1 {
		t.Fatalf("file.search = %#v", search)
	}
	digest := sha256.Sum256([]byte(original))
	call("file.apply_patch", map[string]any{
		"path": "sample.txt", "expected_sha256": hex.EncodeToString(digest[:]),
		"patch": "@@ -1,2 +1,2 @@\n one\n-two\n+TWO\n",
	})
	updated, err := os.ReadFile(filepath.Join(workspace, "sample.txt"))
	if err != nil || string(updated) != "one\nTWO\n" {
		t.Fatalf("patched file = %q, %v", updated, err)
	}
	templates, ok := call("process.template_list", map[string]any{}).([]any)
	if !ok || len(templates) != 0 {
		t.Fatalf("process.template_list = %#v", templates)
	}
	started, ok := call("process.start", map[string]any{
		"command": "/bin/echo", "arguments": []string{"ready"}, "working_directory": ".",
		"io_mode": "pipe", "input_mode": "disabled",
	}).(map[string]any)
	if !ok || started["id"] == "" {
		t.Fatalf("process.start = %#v", started)
	}
	got, ok := call("process.get", map[string]any{"process": map[string]any{"id": started["id"]}}).(map[string]any)
	if !ok || got["id"] != started["id"] {
		t.Fatalf("process.get = %#v", got)
	}
}
