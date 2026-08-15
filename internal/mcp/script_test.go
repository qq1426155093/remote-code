package mcpserver

import (
	"strings"
	"testing"
)

func TestCompileScriptEnforcesSideEffectPolicy(t *testing.T) {
	tests := []struct {
		name, source, want string
		capabilities       []string
	}{
		{name: "read host in predicate", source: "map(args.paths, file_stat(#))", capabilities: []string{"files.read"}, want: "collection predicate"},
		{name: "range", source: "1..10", want: "range operator"},
		{name: "repeat", source: "repeat('x', 2)", want: "repeat is not allowed"},
		{name: "mutation not final", source: "let written = file_write_text(args.path, args.content, true, 420); written.path", capabilities: []string{"files.write"}, want: "final expression"},
		{name: "two mutations", source: "file_mkdir(args.one, false, 493); file_mkdir(args.two, false, 493)", capabilities: []string{"files.write"}, want: "at most one"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := compileScript(test.source, test.capabilities)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("compileScript() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestHostCatalogHasExpectedStableDefinitions(t *testing.T) {
	expected := map[string]hostDefinition{
		"controller_info":        {"controller_info", "controller.read", effectRead, 0},
		"file_stat":              {"file_stat", "files.read", effectRead, 1},
		"file_list":              {"file_list", "files.read", effectRead, 1},
		"file_tree":              {"file_tree", "files.read", effectRead, 1},
		"file_read_text":         {"file_read_text", "files.read", effectRead, 2},
		"file_read_range":        {"file_read_range", "files.read", effectRead, 4},
		"file_search":            {"file_search", "files.read", effectRead, 1},
		"file_write_text":        {"file_write_text", "files.write", effectDestructive, 4},
		"file_apply_patch":       {"file_apply_patch", "files.write", effectDestructive, 3},
		"file_mkdir":             {"file_mkdir", "files.write", effectMutate, 3},
		"file_move":              {"file_move", "files.write", effectDestructive, 3},
		"file_chmod":             {"file_chmod", "files.write", effectMutate, 2},
		"file_remove":            {"file_remove", "files.delete", effectDestructive, 2},
		"process_start":          {"process_start", "processes.start", effectMutate, 1},
		"process_list":           {"process_list", "processes.read", effectRead, 1},
		"process_get":            {"process_get", "processes.read", effectRead, 1},
		"process_signal":         {"process_signal", "processes.signal", effectDestructive, 3},
		"process_delete":         {"process_delete", "processes.delete", effectDestructive, 1},
		"process_logs":           {"process_logs", "processes.read", effectRead, 4},
		"process_logs_since":     {"process_logs_since", "processes.read", effectRead, 4},
		"process_template_list":  {"process_template_list", "process_templates.read", effectRead, 0},
		"process_template_get":   {"process_template_get", "process_templates.read", effectRead, 1},
		"process_template_start": {"process_template_start", "process_templates.start", effectMutate, 1},
	}
	if len(hostCatalog) != len(expected) {
		t.Fatalf("host catalog has %d entries, want %d", len(hostCatalog), len(expected))
	}
	for name, want := range expected {
		if got, ok := hostCatalog[name]; !ok || got != want {
			t.Errorf("hostCatalog[%q] = %#v, want %#v", name, got, want)
		}
	}
}
