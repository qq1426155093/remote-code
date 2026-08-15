package mcpserver

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

func TestRunnerBudgetsActualStructuredAndTextResult(t *testing.T) {
	workspace := t.TempDir()
	definition := writeDefinition(t, validDefinition)
	prepared, err := Prepare(Config{
		Enabled: true, DefinitionFiles: []string{definition}, Token: "test-token",
		ListenAddress: "127.0.0.1:9444", MaxResponseBytes: minimumResponseBytes,
	}, workspace, "127.0.0.1:9443")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"path": strings.Repeat("x", 4_200)})
	if err != nil {
		t.Fatal(err)
	}
	runner := newRunner(prepared.Config, prepared.Registry, nil)
	tool := prepared.Registry.ordered[0]

	modern := runner.call(context.Background(), tool, raw, "test", "2026-07-28")
	if !modern.IsError || resultText(modern) != "tool result exceeded the response size limit" {
		t.Fatalf("modern result = %#v", modern)
	}
	legacy := runner.call(context.Background(), tool, raw, "test", "2025-11-25")
	if legacy.IsError || legacy.StructuredContent != nil {
		t.Fatalf("legacy result = %#v", legacy)
	}
}

func resultText(result *mcpsdk.CallToolResult) string {
	if result == nil || len(result.Content) != 1 {
		return ""
	}
	text, _ := result.Content[0].(*mcpsdk.TextContent)
	if text == nil {
		return ""
	}
	return text.Text
}
