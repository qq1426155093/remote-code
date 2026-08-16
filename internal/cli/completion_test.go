package cli

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

type fakeCompletionClient struct {
	files     map[string][]*codev1.FileInfo
	requests  []string
	processes []*codev1.ProcessInfo
	templates []*codev1.ProcessTemplateSummary
}

func (f *fakeCompletionClient) List(_ context.Context, remotePath string) ([]*codev1.FileInfo, error) {
	f.requests = append(f.requests, remotePath)
	return f.files[remotePath], nil
}

func (f *fakeCompletionClient) ListProcesses(context.Context, ...bool) ([]*codev1.ProcessInfo, error) {
	return f.processes, nil
}

func (f *fakeCompletionClient) ListProcessTemplates(context.Context) ([]*codev1.ProcessTemplateSummary, error) {
	return f.templates, nil
}

func TestCompleterSuggestsCommandsOptionsAndArguments(t *testing.T) {
	client := &fakeCompletionClient{files: map[string][]*codev1.FileInfo{
		".": {
			{Name: ".hidden", Type: codev1.FileType_FILE_TYPE_REGULAR},
			{Name: "docs", Type: codev1.FileType_FILE_TYPE_DIRECTORY},
			{Name: "hello world.txt", Type: codev1.FileType_FILE_TYPE_REGULAR},
		},
	}, processes: []*codev1.ProcessInfo{
		{Name: "worker", State: codev1.ProcessState_PROCESS_STATE_RUNNING},
		{Name: "finished", State: codev1.ProcessState_PROCESS_STATE_EXITED},
		{Name: "failed", State: codev1.ProcessState_PROCESS_STATE_FAILED},
	}, templates: []*codev1.ProcessTemplateSummary{{Name: "agent"}}}
	completer := newCompleter(client, func() string { return "." }, time.Second, defaultCommandRegistry)
	tests := []struct {
		name       string
		line       string
		want       []string
		wantOffset int
	}{
		{name: "command", line: "tr", want: []string{"ee "}, wantOffset: 2},
		{name: "help argument", line: "help ch", want: []string{"mod "}, wantOffset: 2},
		{name: "option", line: "ls -", want: []string{"l "}, wantOffset: 1},
		{name: "directory", line: "cd d", want: []string{"ocs/"}, wantOffset: 1},
		{name: "escaped remote file", line: "cat he", want: []string{"llo\\ world.txt "}, wantOffset: 2},
		{name: "quoted remote file", line: "cat \"hello", want: []string{" world.txt\" "}, wantOffset: 6},
		{name: "mode hints", line: "chmod 064", want: []string{"0 ", "4 "}, wantOffset: 3},
		{name: "exec options", line: "exec --p", want: []string{"ipe ", "ty "}, wantOffset: 3},
		{name: "template name", line: "templates ag", want: []string{"ent "}, wantOffset: 2},
		{name: "exec template name", line: "exec-template ag", want: []string{"ent "}, wantOffset: 2},
		{name: "exec template options", line: "exec-template --a", want: []string{"ttach "}, wantOffset: 3},
		{name: "ps all", line: "ps -", want: []string{"a "}, wantOffset: 1},
		{name: "signal", line: "kill -s T", want: []string{"ERM "}, wantOffset: 1},
		{name: "running process", line: "kill wor", want: []string{"ker "}, wantOffset: 3},
		{name: "finished process", line: "forget fin", want: []string{"ished "}, wantOffset: 3},
		{name: "second forget process", line: "forget finished fa", want: []string{"iled "}, wantOffset: 2},
		{name: "log options", line: "logs --o", want: []string{"ffset "}, wantOffset: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, offset := completer.Do([]rune(test.line), len([]rune(test.line)))
			gotStrings := make([]string, len(got))
			for index := range got {
				gotStrings[index] = string(got[index])
			}
			if !reflect.DeepEqual(gotStrings, test.want) || offset != test.wantOffset {
				t.Fatalf("Do(%q) = %#v, %d; want %#v, %d", test.line, gotStrings, offset, test.want, test.wantOffset)
			}
		})
	}
}

func TestCompleterResolvesRemotePathsFromCurrentDirectory(t *testing.T) {
	client := &fakeCompletionClient{files: map[string][]*codev1.FileInfo{
		"docs": {{Name: "guide", Type: codev1.FileType_FILE_TYPE_DIRECTORY}},
	}}
	completer := newCompleter(client, func() string { return "docs" }, time.Second, defaultCommandRegistry)
	got, _ := completer.Do([]rune("tree g"), len([]rune("tree g")))
	if len(got) != 1 || string(got[0]) != "uide/" {
		t.Fatalf("Do(tree g) = %q, want guide directory completion", got)
	}
	if !reflect.DeepEqual(client.requests, []string{"docs"}) {
		t.Errorf("List() requests = %#v, want docs", client.requests)
	}
}

func TestCompleterSuggestsAttachablePTYProcesses(t *testing.T) {
	client := &fakeCompletionClient{files: map[string][]*codev1.FileInfo{}, processes: []*codev1.ProcessInfo{
		{Name: "editor", State: codev1.ProcessState_PROCESS_STATE_RUNNING, IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, InputMode: codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED, InputState: codev1.ProcessInputState_PROCESS_INPUT_STATE_OPEN},
		{Name: "reviewer", State: codev1.ProcessState_PROCESS_STATE_RUNNING, IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, InputMode: codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED, InputState: codev1.ProcessInputState_PROCESS_INPUT_STATE_OPEN},
		{Name: "-agent", State: codev1.ProcessState_PROCESS_STATE_RUNNING, IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, InputMode: codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED, InputState: codev1.ProcessInputState_PROCESS_INPUT_STATE_OPEN},
		{Name: "pipe", State: codev1.ProcessState_PROCESS_STATE_RUNNING, IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, InputMode: codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED, InputState: codev1.ProcessInputState_PROCESS_INPUT_STATE_OPEN},
	}}
	completer := newCompleter(client, func() string { return "." }, time.Second, defaultCommandRegistry)
	got, offset := completer.Do([]rune("attach ed"), len([]rune("attach ed")))
	if len(got) != 1 || string(got[0]) != "itor " || offset != 2 {
		t.Fatalf("attach completion = %q, %d", got, offset)
	}
	got, offset = completer.Do([]rune("windows editor re"), len([]rune("windows editor re")))
	if len(got) != 1 || string(got[0]) != "viewer " || offset != 2 {
		t.Fatalf("windows process completion = %q, %d", got, offset)
	}
	got, offset = completer.Do([]rune("mux --t"), len([]rune("mux --t")))
	if len(got) != 1 || string(got[0]) != "ail-lines " || offset != 3 {
		t.Fatalf("windows option completion = %q, %d", got, offset)
	}
	got, offset = completer.Do([]rune("windows -- -a"), len([]rune("windows -- -a")))
	if len(got) != 1 || string(got[0]) != "gent " || offset != 2 {
		t.Fatalf("windows option-terminated completion = %q, %d", got, offset)
	}
}

func TestCompleteLocalPath(t *testing.T) {
	directory := t.TempDir()
	if err := os.WriteFile(filepath.Join(directory, "local file.txt"), []byte("data"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(directory, "logs"), 0o700); err != nil {
		t.Fatal(err)
	}
	candidates := completeLocalPath(filepath.Join(directory, "lo"), completeFiles)
	want := []completionCandidate{
		{value: filepath.Join(directory, "local file.txt"), finish: true},
		{value: filepath.Join(directory, "logs") + string(filepath.Separator), finish: false},
	}
	if !reflect.DeepEqual(candidates, want) {
		t.Errorf("completeLocalPath() = %#v, want %#v", candidates, want)
	}
}
