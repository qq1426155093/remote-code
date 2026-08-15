package mcpserver

import (
	"math"
	"testing"

	processservice "github.com/qq1426155093/remote-code/internal/process"
)

func TestLogSnapshotJSONPreservesUint64OffsetsAsDecimalStrings(t *testing.T) {
	converted := logSnapshotJSON(&processservice.LogSnapshot{
		EarliestOffset: math.MaxUint64 - 2,
		SnapshotEnd:    math.MaxUint64,
		ResolvedStart:  math.MaxUint64 - 1,
		NextOffset:     math.MaxUint64,
	}, true).(map[string]any)
	if converted["earliest_offset"] != "18446744073709551613" ||
		converted["resolved_start_offset"] != "18446744073709551614" ||
		converted["next_offset"] != "18446744073709551615" {
		t.Fatalf("offsets = %#v", converted)
	}
}

func TestStartProcessTemplateArgumentStrictlyDecodesOptions(t *testing.T) {
	request, err := startProcessTemplateArgument(map[string]any{
		"template_name": "agent", "parameters": map[string]any{"model": "fast"},
		"process_name": "worker", "expected_template_revision": "revision",
		"terminal_size": map[string]any{"rows": int64(24), "columns": int64(80)},
	})
	if err != nil {
		t.Fatal(err)
	}
	if request.GetTemplateName() != "agent" || request.GetParameters().AsMap()["model"] != "fast" ||
		request.GetTerminalSize().GetRows() != 24 || request.GetTerminalSize().GetColumns() != 80 {
		t.Fatalf("request = %#v", request)
	}
	if _, err := startProcessTemplateArgument(map[string]any{"template_name": "agent", "unknown": true}); err == nil {
		t.Fatal("unknown process template option was accepted")
	}
}
