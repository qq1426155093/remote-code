package files

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestServiceFileLifecycle(t *testing.T) {
	workspace := t.TempDir()
	service := newTestService(t, workspace, 0)
	ctx := context.Background()

	if _, err := service.Mkdir(ctx, &codev1.MkdirRequest{Path: "docs/nested", Mode: 0o755, Parents: true}); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "docs", "zeta.txt"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "docs", "alpha.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}

	listed, err := service.List(ctx, &codev1.ListRequest{Path: "docs"})
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	wantNames := []string{"alpha.txt", "nested", "zeta.txt"}
	if len(listed.GetFiles()) != len(wantNames) {
		t.Fatalf("List() returned %d files, want %d", len(listed.GetFiles()), len(wantNames))
	}
	for index, want := range wantNames {
		if got := listed.GetFiles()[index].GetName(); got != want {
			t.Errorf("List()[%d].Name = %q, want %q", index, got, want)
		}
	}

	chmod, err := service.Chmod(ctx, &codev1.ChmodRequest{Path: "docs/alpha.txt", Mode: 0o600})
	if err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	if got := chmod.GetFile().GetMode(); got != 0o600 {
		t.Errorf("Chmod().Mode = %04o, want 0600", got)
	}

	if _, err := service.Move(ctx, &codev1.MoveRequest{Source: "docs/alpha.txt", Destination: "docs/zeta.txt"}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("Move(no overwrite) code = %s, want AlreadyExists; error = %v", status.Code(err), err)
	}
	moved, err := service.Move(ctx, &codev1.MoveRequest{Source: "docs/alpha.txt", Destination: "docs/renamed.txt"})
	if err != nil {
		t.Fatalf("Move() error = %v", err)
	}
	if got := moved.GetFile().GetPath(); got != "/docs/renamed.txt" {
		t.Errorf("Move().Path = %q, want /docs/renamed.txt", got)
	}
	if _, err := service.Remove(ctx, &codev1.RemoveRequest{Path: "docs", Recursive: false}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Remove(non-empty) code = %s, want FailedPrecondition; error = %v", status.Code(err), err)
	}
	if _, err := service.Remove(ctx, &codev1.RemoveRequest{Path: "docs", Recursive: true}); err != nil {
		t.Fatalf("Remove(recursive) error = %v", err)
	}
	if _, err := os.Stat(filepath.Join(workspace, "docs")); !os.IsNotExist(err) {
		t.Fatalf("workspace/docs still exists, stat error = %v", err)
	}
}

func TestServiceRejectsUnsafePaths(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(workspace, "inside"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "inside", "ok.txt"), []byte("ok"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("inside", filepath.Join(workspace, "internal-link")); err != nil {
		t.Fatal(err)
	}
	service := newTestService(t, workspace, 0)
	ctx := context.Background()

	tests := []struct {
		name string
		path string
		code codes.Code
	}{
		{name: "absolute", path: "/etc/passwd", code: codes.InvalidArgument},
		{name: "parent", path: "../secret.txt", code: codes.InvalidArgument},
		{name: "embedded parent", path: "inside/../inside/ok.txt", code: codes.InvalidArgument},
		{name: "symlink escape", path: "escape/secret.txt", code: codes.PermissionDenied},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := service.Stat(ctx, &codev1.StatRequest{Path: test.path})
			if got := status.Code(err); got != test.code {
				t.Fatalf("Stat(%q) code = %s, want %s; error = %v", test.path, got, test.code, err)
			}
		})
	}

	info, err := service.Stat(ctx, &codev1.StatRequest{Path: "internal-link/ok.txt"})
	if err != nil {
		t.Fatalf("Stat(internal symlink) error = %v", err)
	}
	if info.GetFile().GetType() != codev1.FileType_FILE_TYPE_REGULAR {
		t.Errorf("internal symlink target type = %s, want regular", info.GetFile().GetType())
	}
	link, err := service.Stat(ctx, &codev1.StatRequest{Path: "escape"})
	if err != nil {
		t.Fatalf("Stat(final escape symlink) error = %v", err)
	}
	if link.GetFile().GetSymlinkTarget() != "" {
		t.Errorf("escaping symlink target leaked as %q", link.GetFile().GetSymlinkTarget())
	}
	for _, rootPath := range []string{"", "."} {
		_, err := service.Remove(ctx, &codev1.RemoveRequest{Path: rootPath, Recursive: true})
		if got := status.Code(err); got != codes.InvalidArgument {
			t.Errorf("Remove(%q) code = %s, want InvalidArgument", rootPath, got)
		}
	}
}

func TestServiceRejectsInvalidConfigurationAndMode(t *testing.T) {
	if _, err := New(Config{}); err == nil {
		t.Fatal("New() with empty workspace succeeded")
	}
	if _, err := New(Config{Workspace: t.TempDir(), MaxUploadBytes: -1}); err == nil {
		t.Fatal("New() with negative upload limit succeeded")
	}
	service := newTestService(t, t.TempDir(), 0)
	_, err := service.Mkdir(context.Background(), &codev1.MkdirRequest{Path: "bad", Mode: 0o1777})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Mkdir(invalid mode) code = %s, want InvalidArgument", got)
	}
}

func newTestService(t *testing.T, workspace string, maxUploadBytes int64) *Service {
	t.Helper()
	service, err := New(Config{Workspace: workspace, MaxUploadBytes: maxUploadBytes})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := service.Close(); err != nil {
			t.Errorf("Close() error = %v", err)
		}
	})
	return service
}
