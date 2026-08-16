package files

import (
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

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

func TestServiceTreeReturnsStructuredHierarchy(t *testing.T) {
	workspace := t.TempDir()
	if err := os.MkdirAll(filepath.Join(workspace, "docs", "nested"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "docs", "zeta.txt"), []byte("z"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(workspace, "docs", "nested", "alpha.txt"), []byte("a"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("nested", filepath.Join(workspace, "docs", "nested-link")); err != nil {
		t.Fatal(err)
	}
	service := newTestService(t, workspace, 0)

	response, err := service.Tree(context.Background(), &codev1.TreeRequest{Path: "docs"})
	if err != nil {
		t.Fatalf("Tree() error = %v", err)
	}
	root := response.GetRoot()
	if root.GetFile().GetPath() != "/docs" || root.GetFile().GetType() != codev1.FileType_FILE_TYPE_DIRECTORY {
		t.Fatalf("Tree().Root = %+v, want /docs directory", root.GetFile())
	}
	children := root.GetChildren()
	wantNames := []string{"nested", "nested-link", "zeta.txt"}
	if len(children) != len(wantNames) {
		t.Fatalf("Tree().Root.Children = %d entries, want %d", len(children), len(wantNames))
	}
	for index, want := range wantNames {
		if got := children[index].GetFile().GetName(); got != want {
			t.Errorf("Tree().Root.Children[%d].Name = %q, want %q", index, got, want)
		}
	}
	if nested := children[0]; len(nested.GetChildren()) != 1 || nested.GetChildren()[0].GetFile().GetName() != "alpha.txt" {
		t.Errorf("Tree() nested children = %+v, want alpha.txt", nested.GetChildren())
	}
	if link := children[1]; link.GetFile().GetType() != codev1.FileType_FILE_TYPE_SYMLINK || len(link.GetChildren()) != 0 {
		t.Errorf("Tree() symlink = %+v, want a symlink leaf", link)
	}

	fileResponse, err := service.Tree(context.Background(), &codev1.TreeRequest{Path: "docs/zeta.txt"})
	if err != nil {
		t.Fatalf("Tree(file) error = %v", err)
	}
	if fileResponse.GetRoot().GetFile().GetName() != "zeta.txt" || len(fileResponse.GetRoot().GetChildren()) != 0 {
		t.Errorf("Tree(file).Root = %+v, want zeta.txt leaf", fileResponse.GetRoot())
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := service.Tree(canceled, &codev1.TreeRequest{}); status.Code(err) != codes.Canceled {
		t.Errorf("Tree(canceled) code = %s, want Canceled", status.Code(err))
	}
}

func TestServiceTreeEnforcesDepthLimit(t *testing.T) {
	workspace := t.TempDir()
	deepest := workspace
	for range maxTreeDepth + 1 {
		deepest = filepath.Join(deepest, "d")
	}
	if err := os.MkdirAll(deepest, 0o755); err != nil {
		t.Fatal(err)
	}
	service := newTestService(t, workspace, 0)
	if _, err := service.Tree(context.Background(), &codev1.TreeRequest{}); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("Tree(deep hierarchy) code = %s, want ResourceExhausted", status.Code(err))
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
	tree, err := service.Tree(ctx, &codev1.TreeRequest{Path: "escape"})
	if err != nil {
		t.Fatalf("Tree(final escape symlink) error = %v", err)
	}
	if tree.GetRoot().GetFile().GetSymlinkTarget() != "" || len(tree.GetRoot().GetChildren()) != 0 {
		t.Errorf("Tree(escape) = %+v, want a non-leaking symlink leaf", tree.GetRoot())
	}
	if _, err := service.Tree(ctx, &codev1.TreeRequest{Path: "escape/secret.txt"}); status.Code(err) != codes.PermissionDenied {
		t.Errorf("Tree(symlink escape) code = %s, want PermissionDenied", status.Code(err))
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
	if _, err := New(Config{Workspace: t.TempDir(), Transfers: TransferConfig{MaxUploadSessions: -1}}); err == nil {
		t.Fatal("New() with an invalid upload session limit succeeded")
	}
	service := newTestService(t, t.TempDir(), 0)
	_, err := service.Mkdir(context.Background(), &codev1.MkdirRequest{Path: "bad", Mode: 0o1777})
	if got := status.Code(err); got != codes.InvalidArgument {
		t.Fatalf("Mkdir(invalid mode) code = %s, want InvalidArgument", got)
	}
}

func TestTransferStateDirectoryRejectsConcurrentControllers(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	first, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory})
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory}); err == nil {
		_ = second.Close()
		t.Fatal("second service acquired the same transfer state directory")
	}
}

func TestResumableTransfersCanBeDisabled(t *testing.T) {
	service, err := New(Config{Workspace: t.TempDir(), Transfers: TransferConfig{Disabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	if capabilities := service.FileTransferCapabilities(); capabilities.GetResumableUpload() || capabilities.GetResumableDownload() {
		t.Fatalf("disabled capabilities = %+v", capabilities)
	}
	if _, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{}); status.Code(err) != codes.Unimplemented {
		t.Fatalf("disabled CreateUploadSession() code = %s, want Unimplemented", status.Code(err))
	}
}

func TestUploadSessionQuotaAndAbortReleaseReservation(t *testing.T) {
	workspace := t.TempDir()
	service, err := New(Config{
		Workspace: workspace, RuntimeDirectory: t.TempDir(), MaxUploadBytes: 8,
		Transfers: TransferConfig{MaxUploadSessions: 1, MaxStagingBytes: 8},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	content := []byte("first")
	digest := sha256.Sum256(content)
	first, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "first", Path: "first.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o600,
	})
	if err != nil {
		t.Fatal(err)
	}
	firstSession, ok := service.transfers.get(first.GetSession().GetUploadId())
	if !ok {
		t.Fatal("first upload session was not found")
	}
	firstSession.mu.Lock()
	firstTempPath := firstSession.record.TempPath
	firstSession.mu.Unlock()
	if _, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "staging-target", Path: firstTempPath, Size: int64(len(content)), Sha256: digest[:], Mode: 0o600, Overwrite: true,
	}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("CreateUploadSession(active staging target) code = %s, want FailedPrecondition", status.Code(err))
	}
	if _, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "second", Path: "second.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o600,
	}); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("second active session code = %s, want ResourceExhausted", status.Code(err))
	}
	if _, err := service.Remove(context.Background(), &codev1.RemoveRequest{Path: "first.bin"}); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("Remove(active target) code = %s, want FailedPrecondition", status.Code(err))
	}
	if _, err := service.AbortUploadSession(context.Background(), &codev1.AbortUploadSessionRequest{UploadId: first.GetSession().GetUploadId()}); err != nil {
		t.Fatal(err)
	}
	if _, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "second", Path: "second.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o600,
	}); err != nil {
		t.Fatalf("CreateUploadSession() after abort error = %v", err)
	}
}

func TestExpiredFinalizingUploadReconcilesPublishedTarget(t *testing.T) {
	service, err := New(Config{
		Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxUploadBytes: 8,
		Transfers: TransferConfig{UploadSessionTTL: time.Second, CompletedSessionTTL: time.Minute, MaxStagingBytes: 32},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close()
	digest := sha256.Sum256(nil)
	created, err := service.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "finalizing", Path: "published.bin", Sha256: digest[:], Mode: 0o600,
	})
	if err != nil {
		t.Fatal(err)
	}
	session, ok := service.transfers.get(created.GetSession().GetUploadId())
	if !ok {
		t.Fatal("created upload session was not found")
	}
	session.mu.Lock()
	record := session.record
	session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING
	session.record.ExpiresAt = time.Now().Add(-time.Second)
	if err := service.transfers.persistRecord(session.record); err != nil {
		session.mu.Unlock()
		t.Fatal(err)
	}
	session.mu.Unlock()
	if err := service.root.Link(record.TempPath, record.TargetPath); err != nil {
		t.Fatal(err)
	}

	service.transfers.cleanupExpired(time.Now())
	response, err := service.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: record.UploadID})
	if err != nil {
		t.Fatal(err)
	}
	if response.GetSession().GetState() != codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE {
		t.Fatalf("reconciled session state = %s, want COMPLETE", response.GetSession().GetState())
	}
	if _, err := service.root.Lstat(record.TempPath); !os.IsNotExist(err) {
		t.Fatalf("published staging link still exists: %v", err)
	}
	service.transfers.mu.Lock()
	activeUploads := service.transfers.activeUploads
	reservedBytes := service.transfers.reservedBytes
	service.transfers.mu.Unlock()
	if activeUploads != 0 || reservedBytes != 0 {
		t.Fatalf("reconciled reservation = (%d uploads, %d bytes), want zero", activeUploads, reservedBytes)
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
