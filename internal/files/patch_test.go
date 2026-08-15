package files

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestApplyTextPatchChecksDigestAndAppliesUnifiedDiff(t *testing.T) {
	workspace := t.TempDir()
	name := filepath.Join(workspace, "notes.txt")
	original := "one\ntwo\nthree\n"
	if err := os.WriteFile(name, []byte(original), 0o640); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	digest := sha256.Sum256([]byte(original))
	patch := "--- a/ignored.txt\n+++ b/ignored.txt\n@@ -1,3 +1,3 @@\n one\n-two\n+TWO\n three\n"
	response, err := service.ApplyTextPatch(context.Background(), "notes.txt", hex.EncodeToString(digest[:]), patch)
	if err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "one\nTWO\nthree\n" || response.GetFile().GetMode() != 0o640 {
		t.Fatalf("contents = %q, response = %+v", contents, response)
	}
	if _, err := service.ApplyTextPatch(context.Background(), "notes.txt", hex.EncodeToString(digest[:]), patch); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("second ApplyTextPatch() error = %v", err)
	}
}

func TestApplyTextPatchRejectsMismatchedContextAndMultipleFiles(t *testing.T) {
	workspace := t.TempDir()
	original := "one\n"
	if err := os.WriteFile(filepath.Join(workspace, "notes.txt"), []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	service, err := New(Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	digest := sha256.Sum256([]byte(original))
	sha := hex.EncodeToString(digest[:])
	badContext := "@@ -1 +1 @@\n-other\n+new\n"
	if _, err := service.ApplyTextPatch(context.Background(), "notes.txt", sha, badContext); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("context error = %v", err)
	}
	multiple := strings.Join([]string{
		"--- a/one", "+++ b/one", "@@ -1 +1 @@", "-one", "+ONE", "--- a/two", "+++ b/two", "@@ -1 +1 @@", "-two", "+TWO", "",
	}, "\n")
	if _, err := service.ApplyTextPatch(context.Background(), "notes.txt", sha, multiple); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("multiple-file error = %v", err)
	}
}

func TestApplyUnifiedHunksSupportsInsertionAndDeletion(t *testing.T) {
	patch := "@@ -1,0 +2,1 @@\n+between\n@@ -2,1 +2,0 @@\n-last\n"
	hunks, err := parseUnifiedPatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	got, err := applyUnifiedHunks("first\nlast\n", hunks)
	if err != nil {
		t.Fatal(err)
	}
	if got != "first\nbetween\n" {
		t.Fatalf("patched = %q", got)
	}
}

func TestApplyUnifiedHunksAllowsContentThatLooksLikeFileHeaders(t *testing.T) {
	patch := "@@ -1,2 +1,2 @@\n--- old option\n+++ new option\n unchanged\n"
	hunks, err := parseUnifiedPatch(patch)
	if err != nil {
		t.Fatal(err)
	}
	got, err := applyUnifiedHunks("-- old option\nunchanged\n", hunks)
	if err != nil {
		t.Fatal(err)
	}
	if got != "++ new option\nunchanged\n" {
		t.Fatalf("patched = %q", got)
	}
}
