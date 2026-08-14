package client_test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/server"
	client "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestClientFileLifecycleOverGRPC(t *testing.T) {
	workspace := t.TempDir()
	address := startController(t, workspace, 1024, "")
	remote := connectClient(t, address, "")
	ctx := context.Background()

	if info := remote.Info(); info.GetApiVersion() != "remote.code.v1" || info.GetMaxUploadBytes() != 1024 {
		t.Fatalf("Info() = %+v, want API remote.code.v1 and limit 1024", info)
	}
	if _, err := remote.Mkdir(ctx, "docs", 0o755, false); err != nil {
		t.Fatalf("Mkdir() error = %v", err)
	}
	content := []byte("hello from grpc\n")
	digest := sha256.Sum256(content)
	upload, err := remote.Upload(ctx, "docs/hello.txt", bytes.NewReader(content), int64(len(content)), 0o640, digest[:], false)
	if err != nil {
		t.Fatalf("Upload() error = %v", err)
	}
	if upload.GetSize() != int64(len(content)) || !bytes.Equal(upload.GetSha256(), digest[:]) {
		t.Fatalf("Upload() = %+v, want verified size and digest", upload)
	}

	files, err := remote.List(ctx, "docs")
	if err != nil {
		t.Fatalf("List() error = %v", err)
	}
	if len(files) != 1 || files[0].GetName() != "hello.txt" {
		t.Fatalf("List() = %+v, want hello.txt", files)
	}
	var downloaded bytes.Buffer
	result, err := remote.Download(ctx, "docs/hello.txt", &downloaded)
	if err != nil {
		t.Fatalf("Download() error = %v", err)
	}
	if !bytes.Equal(downloaded.Bytes(), content) || !bytes.Equal(result.SHA256, digest[:]) {
		t.Fatalf("Download() content = %q, digest = %x", downloaded.Bytes(), result.SHA256)
	}

	localTarget := filepath.Join(t.TempDir(), "local.txt")
	if _, err := remote.DownloadFile(ctx, "docs/hello.txt", localTarget); err != nil {
		t.Fatalf("DownloadFile() error = %v", err)
	}
	localContent, err := os.ReadFile(localTarget)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(localContent, content) {
		t.Errorf("DownloadFile() content = %q, want %q", localContent, content)
	}

	chmod, err := remote.Chmod(ctx, "docs/hello.txt", 0o600)
	if err != nil {
		t.Fatalf("Chmod() error = %v", err)
	}
	if chmod.GetMode() != 0o600 {
		t.Errorf("Chmod().Mode = %04o, want 0600", chmod.GetMode())
	}
	moved, err := remote.Move(ctx, "docs/hello.txt", "docs/moved.txt", false)
	if err != nil {
		t.Fatalf("Move() error = %v", err)
	}
	if moved.GetPath() != "/docs/moved.txt" {
		t.Errorf("Move().Path = %q", moved.GetPath())
	}
	if err := remote.Remove(ctx, "docs", true); err != nil {
		t.Fatalf("Remove() error = %v", err)
	}
	if _, err := remote.Stat(ctx, "docs"); status.Code(err) != codes.NotFound {
		t.Fatalf("Stat(removed) code = %s, want NotFound", status.Code(err))
	}
}

func TestUploadValidationAndCleanupOverGRPC(t *testing.T) {
	workspace := t.TempDir()
	address := startController(t, workspace, 32, "")
	remote := connectClient(t, address, "")
	ctx := context.Background()
	content := []byte("valid")
	digest := sha256.Sum256(content)

	if _, err := remote.Upload(ctx, "file.txt", bytes.NewReader(content), int64(len(content)), 0o600, digest[:], false); err != nil {
		t.Fatalf("initial Upload() error = %v", err)
	}
	if _, err := remote.Upload(ctx, "file.txt", bytes.NewReader(content), int64(len(content)), 0o600, digest[:], false); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("second Upload() code = %s, want AlreadyExists; error = %v", status.Code(err), err)
	}

	badDigest := make([]byte, sha256.Size)
	if _, err := remote.Upload(ctx, "bad.txt", bytes.NewReader(content), int64(len(content)), 0o600, badDigest, false); status.Code(err) != codes.DataLoss {
		t.Fatalf("bad digest Upload() code = %s, want DataLoss; error = %v", status.Code(err), err)
	}
	oversized := bytes.Repeat([]byte("x"), 33)
	oversizedDigest := sha256.Sum256(oversized)
	if _, err := remote.Upload(ctx, "large.txt", bytes.NewReader(oversized), int64(len(oversized)), 0o600, oversizedDigest[:], false); status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("large Upload() code = %s, want ResourceExhausted; error = %v", status.Code(err), err)
	}
	if _, err := remote.Upload(ctx, "/absolute.txt", bytes.NewReader(content), int64(len(content)), 0o600, digest[:], false); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("absolute Upload() code = %s, want InvalidArgument; error = %v", status.Code(err), err)
	}

	entries, err := os.ReadDir(workspace)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".remote-code-upload-") || entry.Name() == "bad.txt" || entry.Name() == "large.txt" {
			t.Errorf("failed upload left %q behind", entry.Name())
		}
	}
}

func TestSymlinkEscapeAndAuthenticationOverGRPC(t *testing.T) {
	workspace := t.TempDir()
	outside := t.TempDir()
	if err := os.WriteFile(filepath.Join(outside, "secret.txt"), []byte("secret"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(workspace, "escape")); err != nil {
		t.Fatal(err)
	}
	address := startController(t, workspace, 1024, "test-token")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unauthenticated, err := client.New(ctx, client.Config{Address: address})
	if err == nil {
		_ = unauthenticated.Close()
		t.Fatal("New() without token succeeded")
	}
	if got := status.Code(err); got != codes.Unauthenticated {
		t.Fatalf("New() without token code = %s, want Unauthenticated; error = %v", got, err)
	}

	remote := connectClient(t, address, "test-token")
	if _, err := remote.Stat(context.Background(), "escape/secret.txt"); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("Stat(symlink escape) code = %s, want PermissionDenied; error = %v", status.Code(err), err)
	}
	link, err := remote.Stat(context.Background(), "escape")
	if err != nil {
		t.Fatalf("Stat(symlink itself) error = %v", err)
	}
	if link.GetType() != codev1.FileType_FILE_TYPE_SYMLINK || link.GetSymlinkTarget() != "" {
		t.Fatalf("Stat(symlink) = %+v, want non-leaking symlink metadata", link)
	}
}

func startController(t *testing.T, workspace string, maxUploadBytes int64, token string) string {
	t.Helper()
	controller, err := server.New(server.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, MaxUploadBytes: maxUploadBytes, Token: token,
	})
	if err != nil {
		t.Fatalf("server.New() error = %v", err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := controller.Shutdown(ctx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("controller.Shutdown() error = %v", err)
		}
		select {
		case <-serveErrors:
		case <-time.After(5 * time.Second):
			t.Error("controller Serve() did not return")
		}
	})
	return controller.Address()
}

func connectClient(t *testing.T, address, token string) *client.Client {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	remote, err := client.New(ctx, client.Config{Address: address, Token: token})
	if err != nil {
		t.Fatalf("client.New() error = %v", err)
	}
	t.Cleanup(func() {
		if err := remote.Close(); err != nil {
			t.Errorf("Client.Close() error = %v", err)
		}
	})
	return remote
}
