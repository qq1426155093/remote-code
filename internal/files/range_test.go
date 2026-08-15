package files

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestReadTextRangeReturnsCompleteResumableLines(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "large.txt"), []byte("one\ntwo\nthree\nfour\n"), 0o640); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()

	first, err := service.ReadTextRange(context.Background(), "large.txt", 2, 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	if first.Content != "two\nthree\n" || first.LineCount != 2 || first.NextLine != 4 || first.EOF || !first.Truncated {
		t.Fatalf("first range = %+v", first)
	}
	second, err := service.ReadTextRange(context.Background(), "large.txt", first.NextLine, 2, 64)
	if err != nil {
		t.Fatal(err)
	}
	if second.Content != "four\n" || second.LineCount != 1 || second.NextLine != 0 || !second.EOF || second.Truncated {
		t.Fatalf("second range = %+v", second)
	}
}

func TestReadTextRangeDoesNotReturnPartialLine(t *testing.T) {
	workspace := t.TempDir()
	if err := os.WriteFile(filepath.Join(workspace, "lines.txt"), []byte("short\nlonger-line\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	result, err := service.ReadTextRange(context.Background(), "lines.txt", 1, 10, 6)
	if err != nil {
		t.Fatal(err)
	}
	if result.Content != "short\n" || result.NextLine != 2 || !result.Truncated {
		t.Fatalf("range = %+v", result)
	}
}
