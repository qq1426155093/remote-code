//go:build darwin

package darwinfd

import (
	"os"
	"path/filepath"
	"testing"
)

func TestPathIdentifiesOpenFile(t *testing.T) {
	name := filepath.Join(t.TempDir(), "definition.yaml")
	if err := os.WriteFile(name, []byte("test"), 0o600); err != nil {
		t.Fatal(err)
	}
	file, err := os.Open(name)
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	got, err := Path(int(file.Fd()))
	if err != nil {
		t.Fatal(err)
	}
	want, err := filepath.EvalSymlinks(name)
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Clean(got) != filepath.Clean(want) {
		t.Fatalf("Path() = %q, want %q", got, want)
	}
}
