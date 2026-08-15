package files

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSearchTextUsesRecursiveGlobAndSkipsSymlinksAndBinary(t *testing.T) {
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "src", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	testFiles := map[string][]byte{
		"src/a.go":        []byte("package a\n// Needle here\n"),
		"src/nested/b.go": []byte("package b\n// needle there\n"),
		"src/skip.txt":    []byte("needle\n"),
		"src/binary.go":   {'n', 'e', 'e', 'd', 'l', 'e', 0, '\n'},
	}
	for name, contents := range testFiles {
		if err := os.WriteFile(filepath.Join(workspace, filepath.FromSlash(name)), contents, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	outside := filepath.Join(t.TempDir(), "outside.go")
	if err := os.WriteFile(outside, []byte("needle outside\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "src", "outside.go")); err != nil {
		t.Fatal(err)
	}

	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	result, err := service.SearchText(context.Background(), SearchOptions{
		Path: "src", Glob: "**/*.go", Query: "needle", CaseSensitive: false, MaxResults: 20, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Matches) != 2 || result.Matches[0].Path != "/src/a.go" || result.Matches[0].Line != 2 ||
		result.Matches[1].Path != "/src/nested/b.go" || result.Matches[1].Column != 4 {
		t.Fatalf("search result = %+v", result)
	}
	if result.SkippedFiles != 1 {
		t.Fatalf("skipped files = %d, want binary file only", result.SkippedFiles)
	}
}

func TestSearchTextRejectsMissingRoot(t *testing.T) {
	service, err := New(Config{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	_, err = service.SearchText(context.Background(), SearchOptions{
		Path: "missing", Glob: "**", Query: "needle", CaseSensitive: true, MaxResults: 10, MaxBytes: 1 << 20,
	})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("SearchText() error = %v, want not found", err)
	}
}

func TestMatchDoublestarHandlesManyRecursiveComponents(t *testing.T) {
	pattern := strings.Repeat("**/", 128) + "target.go"
	candidate := strings.Repeat("directory/", 128) + "target.go"
	matched, err := matchDoublestar(pattern, candidate)
	if err != nil || !matched {
		t.Fatalf("matchDoublestar() = %t, %v", matched, err)
	}
}

func TestSearchTextRejectsExcessiveGlobComponents(t *testing.T) {
	service, err := New(Config{Workspace: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	_, err = service.SearchText(context.Background(), SearchOptions{
		Path: ".", Glob: strings.Repeat("**/", maxSearchGlobParts) + "target", Query: "x",
		CaseSensitive: true, MaxResults: 10, MaxBytes: 1 << 20,
	})
	if status.Code(err) != codes.InvalidArgument {
		t.Fatalf("SearchText() error = %v, want invalid argument", err)
	}
}

func TestSearchTextEnforcesResultLimit(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "many.txt"), []byte("x\nx\nx\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	result, err := service.SearchText(context.Background(), SearchOptions{
		Path: ".", Glob: "**", Query: "x", CaseSensitive: true, MaxResults: 2, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Matches) != 2 || !result.ResultTruncated {
		t.Fatalf("search result = %+v", result)
	}
}
