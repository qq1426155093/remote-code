package cli

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
)

func TestCommandRegistryRejectsInvalidDefinitions(t *testing.T) {
	handler := func(*REPL, []string) error { return nil }
	tests := []struct {
		name  string
		specs []commandSpec
		want  string
	}{
		{name: "empty name", specs: []commandSpec{{handler: handler}}, want: "command name is empty"},
		{name: "whitespace name", specs: []commandSpec{{name: "two words", handler: handler}}, want: "contains whitespace"},
		{name: "missing handler", specs: []commandSpec{{name: "missing"}}, want: "has no handler"},
		{name: "invalid action", specs: []commandSpec{{name: "invalid", handler: handler, action: commandAction(99)}}, want: "invalid action"},
		{
			name: "duplicate name",
			specs: []commandSpec{
				{name: "duplicate", handler: handler},
				{name: "duplicate", handler: handler},
			},
			want: "already registered",
		},
		{
			name: "duplicate alias",
			specs: []commandSpec{
				{name: "one", aliases: []string{"shared"}, handler: handler},
				{name: "two", aliases: []string{"shared"}, handler: handler},
			},
			want: "already registered",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := newCommandRegistry(test.specs)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("newCommandRegistry() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestCommandRegistryDispatchesAliasesAndPreservesArguments(t *testing.T) {
	var gotArguments []string
	registry, err := newCommandRegistry([]commandSpec{{
		name: "inspect", aliases: []string{"i"}, action: commandExit,
		handler: func(_ *REPL, arguments []string) error {
			gotArguments = append([]string(nil), arguments...)
			return nil
		},
	}})
	if err != nil {
		t.Fatal(err)
	}
	repl := &REPL{commands: registry}
	action, err := repl.execute([]string{"i", "first", "second"})
	if err != nil {
		t.Fatal(err)
	}
	if action != commandExit || !reflect.DeepEqual(gotArguments, []string{"first", "second"}) {
		t.Fatalf("execute() = %v, arguments %#v", action, gotArguments)
	}
	if got := registry.completionNames; !reflect.DeepEqual(got, []string{"i", "inspect"}) {
		t.Fatalf("completion names = %#v", got)
	}
}

func TestDefaultCommandHelpAndExitBehavior(t *testing.T) {
	var output bytes.Buffer
	repl := &REPL{commands: defaultCommandRegistry, stdout: &output, cwd: "."}
	action, err := repl.execute([]string{"help"})
	if err != nil || action != commandContinue {
		t.Fatalf("execute(help) = %v, %v", action, err)
	}
	wantHelp := `help [command]
info
pwd
cd [REMOTE_DIR]
ls [-l] [REMOTE_PATH]
tree [REMOTE_PATH]
stat REMOTE_PATH
cat REMOTE_FILE
upload LOCAL_FILE [REMOTE_FILE]
download REMOTE_FILE [LOCAL_FILE]
mkdir [-p] REMOTE_DIR
rm [-r] REMOTE_PATH
mv [-f] SOURCE DESTINATION
chmod OCTAL_MODE REMOTE_PATH
exec [--name NAME] [--pipe|--pty] [--stdin|--attach] [--cwd REMOTE_DIR] [-e KEY=VALUE ...] [--] CMD [ARG ...]
templates [TEMPLATE]
exec-template [--name NAME] [--attach] [--params JSON|--params-file LOCAL_FILE] TEMPLATE
ps [-a]
kill [-s SIGNAL] [-w] PROCESS
stdin PROCESS
attach PROCESS
windows | mux [-n TAIL_LINES] [PROCESS ...] (Ctrl-] ? shows keys)
forget PROCESS_OR_GLOB [PROCESS_OR_GLOB ...]
logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID (Ctrl-C stops following; process continues)
controller-logs | clogs [-f] [-n LINES|--tail LINES|--offset OFFSET] (Ctrl-C stops following)
clear
exit | quit
`
	if got := output.String(); got != wantHelp {
		t.Fatalf("help output =\n%s\nwant:\n%s", got, wantHelp)
	}

	output.Reset()
	if _, err := repl.execute([]string{"help", "logs"}); err != nil {
		t.Fatal(err)
	}
	wantLogs := "usage: logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID\nCtrl-C stops --follow; the process continues\n"
	if got := output.String(); got != wantLogs {
		t.Fatalf("help logs output = %q, want %q", got, wantLogs)
	}

	output.Reset()
	if _, err := repl.execute([]string{"help", "quit"}); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got != "usage: quit\n" {
		t.Fatalf("help quit output = %q", got)
	}

	output.Reset()
	if _, err := repl.execute([]string{"help", "mux"}); err != nil {
		t.Fatal(err)
	}
	wantWindows := "usage: mux [-n TAIL_LINES] [PROCESS ...]\nOpen a tiled PTY workspace; closing a window only detaches and does not stop its process\n"
	if got := output.String(); got != wantWindows {
		t.Fatalf("help mux output = %q, want %q", got, wantWindows)
	}

	action, err = repl.execute([]string{"quit"})
	if err != nil || action != commandExit {
		t.Fatalf("execute(quit) = %v, %v", action, err)
	}
	if _, err := repl.execute([]string{"quit", "now"}); err == nil || err.Error() != "usage: quit" {
		t.Fatalf("execute(quit now) error = %v", err)
	}
	if _, err := repl.execute([]string{"unknown"}); err == nil || err.Error() != `unknown command "unknown"; type 'help' for available commands` {
		t.Fatalf("execute(unknown) error = %v", err)
	}
	if _, err := repl.execute([]string{"info", "extra"}); err == nil || err.Error() != "usage: info" {
		t.Fatalf("execute(info extra) error = %v", err)
	}
}
