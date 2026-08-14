package cli

import (
	"bytes"
	"errors"
	"reflect"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestParseCommandSupportsQuotedPaths(t *testing.T) {
	arguments, err := parseCommand(`upload "local file.txt" 'remote file.txt'`)
	if err != nil {
		t.Fatalf("parseCommand() error = %v", err)
	}
	want := []string{"upload", "local file.txt", "remote file.txt"}
	if !reflect.DeepEqual(arguments, want) {
		t.Fatalf("parseCommand() = %#v, want %#v", arguments, want)
	}
	if _, err := parseCommand(`upload "unterminated`); err == nil {
		t.Fatal("parseCommand() accepted unterminated quote")
	}
}

func TestResolveRemotePath(t *testing.T) {
	tests := []struct {
		name    string
		cwd     string
		input   string
		want    string
		wantErr bool
	}{
		{name: "current", cwd: "docs", input: "", want: "docs"},
		{name: "child", cwd: "docs", input: "design/v1.md", want: "docs/design/v1.md"},
		{name: "parent inside root", cwd: "docs/design", input: "../api", want: "docs/api"},
		{name: "virtual absolute", cwd: "docs", input: "/configs/app.yaml", want: "configs/app.yaml"},
		{name: "virtual root", cwd: "docs", input: "/", want: "."},
		{name: "escape", cwd: ".", input: "../outside", wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveRemotePath(test.cwd, test.input)
			if (err != nil) != test.wantErr {
				t.Fatalf("resolveRemotePath() error = %v, wantErr %v", err, test.wantErr)
			}
			if got != test.want {
				t.Errorf("resolveRemotePath() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestParseMode(t *testing.T) {
	if mode, err := parseMode("0640"); err != nil || mode != 0o640 {
		t.Fatalf("parseMode(0640) = %04o, %v", mode, err)
	}
	for _, value := range []string{"", "888", "1000", "12345"} {
		if _, err := parseMode(value); err == nil {
			t.Errorf("parseMode(%q) succeeded", value)
		}
	}
}

func TestLimitWriter(t *testing.T) {
	var output bytes.Buffer
	writer := &limitWriter{writer: &output, remaining: 4}
	n, err := writer.Write([]byte("abcdef"))
	if n != 4 || !errors.Is(err, errCatLimit) || output.String() != "abcd" {
		t.Fatalf("Write() = %d, %v, output %q; want 4, limit error, abcd", n, err, output.String())
	}
}

func TestPrintTree(t *testing.T) {
	root := &codev1.TreeNode{
		File: &codev1.FileInfo{Path: "/docs", Name: "docs", Type: codev1.FileType_FILE_TYPE_DIRECTORY},
		Children: []*codev1.TreeNode{
			{File: &codev1.FileInfo{Name: "alpha.txt", Type: codev1.FileType_FILE_TYPE_REGULAR}},
			{
				File: &codev1.FileInfo{Name: "nested", Type: codev1.FileType_FILE_TYPE_DIRECTORY},
				Children: []*codev1.TreeNode{
					{File: &codev1.FileInfo{Name: "link", Type: codev1.FileType_FILE_TYPE_SYMLINK, SymlinkTarget: "../alpha.txt"}},
				},
			},
		},
	}
	var output bytes.Buffer
	printTree(&output, root)
	want := "/docs\n├── alpha.txt\n└── nested\n    └── link -> ../alpha.txt\n"
	if got := output.String(); got != want {
		t.Errorf("printTree() =\n%s\nwant:\n%s", got, want)
	}
}
