package cli

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestParseProcessTemplateStartOptions(t *testing.T) {
	options, err := parseProcessTemplateStartOptions([]string{
		"--name", "worker", "--attach", "--params", `{"model":"fast"}`, "agent",
	})
	if err != nil {
		t.Fatal(err)
	}
	if options.processName != "worker" || !options.attach || options.parametersJSON != `{"model":"fast"}` || options.templateName != "agent" {
		t.Fatalf("options = %+v", options)
	}
	fileOptions, err := parseProcessTemplateStartOptions([]string{"--params-file", "params.json", "--", "agent"})
	if err != nil || fileOptions.parametersFile != "params.json" || fileOptions.templateName != "agent" {
		t.Fatalf("file options = %+v, %v", fileOptions, err)
	}
	invalid := [][]string{
		nil,
		{"--unknown", "agent"},
		{"--name", "agent"},
		{"--attach", "--attach", "agent"},
		{"--params", "", "agent"},
		{"--params", `{}`, "--params-file", "params.json", "agent"},
		{"one", "two"},
	}
	for _, arguments := range invalid {
		if _, err := parseProcessTemplateStartOptions(arguments); err == nil {
			t.Errorf("parseProcessTemplateStartOptions(%q) succeeded", arguments)
		}
	}
}

func TestReadProcessTemplateParameters(t *testing.T) {
	parameters, err := readProcessTemplateParameters(processTemplateStartOptions{parametersJSON: `{"model":"fast","debug":true,"flags":["a","b"]}`})
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]any{"model": "fast", "debug": true, "flags": []any{"a", "b"}}
	if !reflect.DeepEqual(parameters.AsMap(), want) {
		t.Fatalf("parameters = %#v, want %#v", parameters.AsMap(), want)
	}
	name := filepath.Join(t.TempDir(), "parameters.json")
	if err := os.WriteFile(name, []byte(`{"cwd":"work"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	fromFile, err := readProcessTemplateParameters(processTemplateStartOptions{parametersFile: name})
	if err != nil || fromFile.AsMap()["cwd"] != "work" {
		t.Fatalf("parameters file = %#v, %v", fromFile, err)
	}
	for _, raw := range []string{"", "[]", "null", "{broken"} {
		if _, err := readProcessTemplateParameters(processTemplateStartOptions{parametersJSON: raw}); err == nil && raw != "" {
			t.Errorf("readProcessTemplateParameters(%q) succeeded", raw)
		}
	}
}

func TestDisplayProcessCommandMarksTemplateArgumentsRedacted(t *testing.T) {
	process := &codev1.ProcessInfo{Command: "agent", ArgumentsRedacted: true, TemplateName: "configured"}
	if got := displayProcessCommand(process); got != "agent [arguments redacted]" {
		t.Fatalf("displayProcessCommand() = %q", got)
	}
	if got := shortTemplateRevision(strings.Repeat("a", 64)); got != strings.Repeat("a", 12) {
		t.Fatalf("shortTemplateRevision() = %q", got)
	}
}
