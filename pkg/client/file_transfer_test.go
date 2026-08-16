package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"os"
	"path/filepath"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	fileservice "github.com/qq1426155093/remote-code/internal/files"
	controllerserver "github.com/qq1426155093/remote-code/internal/server"
)

func TestFileHelpersResumePersistedUploadAndDownloadState(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	controller, err := controllerserver.New(controllerserver.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, RuntimeDirectory: runtimeDirectory,
		MaxUploadBytes: 1 << 20, MaxProcesses: 1,
		FileTransfers: fileservice.TransferConfig{
			CheckpointBytes: 1, CheckpointInterval: time.Hour, MaxStagingBytes: 4 << 20,
			UploadSessionTTL: time.Hour, CompletedSessionTTL: time.Hour,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
	})
	stateDirectory := t.TempDir()
	remote, err := New(context.Background(), Config{Address: controller.Address(), TransferStateDirectory: stateDirectory})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()

	uploadContent := []byte("resume this upload from its durable checkpoint")
	localUpload := filepath.Join(t.TempDir(), "source.bin")
	if err := os.WriteFile(localUpload, uploadContent, 0o640); err != nil {
		t.Fatal(err)
	}
	uploadDigest := sha256.Sum256(uploadContent)
	created, err := remote.files.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "persisted-upload-request", Path: "uploaded.bin", Size: int64(len(uploadContent)),
		Sha256: uploadDigest[:], Mode: 0o640, Overwrite: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := created.GetSession().GetUploadId()
	stream, err := remote.files.TransferUpload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Open{
		Open: &codev1.TransferUploadOpen{UploadId: uploadID},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatal(err)
	}
	prefix := uploadContent[:12]
	prefixDigest := sha256.Sum256(prefix)
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
		Chunk: &codev1.TransferUploadChunk{Offset: 0, Data: prefix, Sha256: prefixDigest[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	if checkpoint, err := stream.Recv(); err != nil || checkpoint.GetCheckpoint().GetCommittedOffset() != int64(len(prefix)) {
		t.Fatalf("upload checkpoint = %+v, %v", checkpoint, err)
	}
	_ = stream.CloseSend()
	for {
		if _, err := stream.Recv(); err != nil {
			break
		}
	}
	absUpload, _ := filepath.Abs(localUpload)
	uploadStatePath, err := remote.localTransferStatePath("upload", absUpload, "uploaded.bin")
	if err != nil {
		t.Fatal(err)
	}
	uploadInfo, _ := os.Stat(localUpload)
	if err := persistLocalState(uploadStatePath, &localUploadState{
		Version: localTransferStateVersion, RequestID: "persisted-upload-request", UploadID: uploadID,
		Address: remote.address, Workspace: remote.info.GetWorkspaceName(), LocalPath: absUpload, RemotePath: "uploaded.bin",
		Size: uploadInfo.Size(), SHA256: uploadDigest[:], Mode: uint32(uploadInfo.Mode().Perm()), Overwrite: true,
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.UploadFile(context.Background(), localUpload, "uploaded.bin", true); err != nil {
		t.Fatal(err)
	}
	uploaded, err := os.ReadFile(filepath.Join(workspace, "uploaded.bin"))
	if err != nil || !bytes.Equal(uploaded, uploadContent) {
		t.Fatalf("resumed upload = %q, %v", uploaded, err)
	}
	if _, err := os.Stat(uploadStatePath); !os.IsNotExist(err) {
		t.Fatalf("completed upload state still exists: %v", err)
	}
	terminal, err := remote.files.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "terminal-upload-request", Path: "replacement.bin", Size: int64(len(uploadContent)),
		Sha256: uploadDigest[:], Mode: uint32(uploadInfo.Mode().Perm()),
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := remote.files.AbortUploadSession(context.Background(), &codev1.AbortUploadSessionRequest{UploadId: terminal.GetSession().GetUploadId()}); err != nil {
		t.Fatal(err)
	}
	replacementStatePath, err := remote.localTransferStatePath("upload", absUpload, "replacement.bin")
	if err != nil {
		t.Fatal(err)
	}
	if err := persistLocalState(replacementStatePath, &localUploadState{
		Version: localTransferStateVersion, RequestID: "terminal-upload-request",
		Address: remote.address, Workspace: remote.info.GetWorkspaceName(), LocalPath: absUpload, RemotePath: "replacement.bin",
		Size: uploadInfo.Size(), SHA256: uploadDigest[:], Mode: uint32(uploadInfo.Mode().Perm()),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.UploadFile(context.Background(), localUpload, "replacement.bin", false); err != nil {
		t.Fatalf("UploadFile() did not replace a terminal idempotency tombstone: %v", err)
	}
	replaced, err := os.ReadFile(filepath.Join(workspace, "replacement.bin"))
	if err != nil || !bytes.Equal(replaced, uploadContent) {
		t.Fatalf("replacement upload = %q, %v", replaced, err)
	}

	downloadContent := bytes.Repeat([]byte("resumable-download-"), 8_000)
	if err := os.WriteFile(filepath.Join(workspace, "download.bin"), downloadContent, 0o604); err != nil {
		t.Fatal(err)
	}
	localDownload := filepath.Join(t.TempDir(), "downloaded.bin")
	absDownload, _ := filepath.Abs(localDownload)
	downloadStatePath, err := remote.localTransferStatePath("download", absDownload, "download.bin")
	if err != nil {
		t.Fatal(err)
	}
	downloadState, err := remote.prepareDownloadState(downloadStatePath, "download.bin", absDownload)
	if err != nil {
		t.Fatal(err)
	}
	downloadContext, cancelDownload := context.WithCancel(context.Background())
	downloadStream, err := remote.files.DownloadRange(downloadContext, &codev1.DownloadRangeRequest{Path: "download.bin"})
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := downloadStream.Recv()
	if err != nil || metadata.GetMetadata() == nil {
		t.Fatalf("download metadata = %+v, %v", metadata, err)
	}
	firstChunk, err := downloadStream.Recv()
	if err != nil || firstChunk.GetChunk() == nil {
		t.Fatalf("download chunk = %+v, %v", firstChunk, err)
	}
	cancelDownload()
	part, err := os.OpenFile(downloadState.PartPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := part.Write(firstChunk.GetChunk().GetData()); err != nil {
		t.Fatal(err)
	}
	if err := part.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := part.Close(); err != nil {
		t.Fatal(err)
	}
	downloadState.Offset = int64(len(firstChunk.GetChunk().GetData()))
	downloadState.Revision = append([]byte(nil), metadata.GetMetadata().GetRevision()...)
	if err := persistLocalState(downloadStatePath, downloadState); err != nil {
		t.Fatal(err)
	}
	result, err := remote.DownloadFile(context.Background(), "download.bin", localDownload)
	if err != nil {
		t.Fatal(err)
	}
	downloaded, err := os.ReadFile(localDownload)
	if err != nil || !bytes.Equal(downloaded, downloadContent) || result.Size != int64(len(downloadContent)) {
		t.Fatalf("resumed download size = %d, result = %+v, error = %v", len(downloaded), result, err)
	}
	if _, err := os.Stat(downloadStatePath); !os.IsNotExist(err) {
		t.Fatalf("completed download state still exists: %v", err)
	}

	changingOld := bytes.Repeat([]byte("old-generation-"), 8_000)
	changingNew := []byte("new remote generation")
	changingRemote := filepath.Join(workspace, "changing.bin")
	if err := os.WriteFile(changingRemote, changingOld, 0o640); err != nil {
		t.Fatal(err)
	}
	changingLocal := filepath.Join(t.TempDir(), "changing.bin")
	absChanging, _ := filepath.Abs(changingLocal)
	changingStatePath, err := remote.localTransferStatePath("download", absChanging, "changing.bin")
	if err != nil {
		t.Fatal(err)
	}
	changingState, err := remote.prepareDownloadState(changingStatePath, "changing.bin", absChanging)
	if err != nil {
		t.Fatal(err)
	}
	changingContext, cancelChanging := context.WithCancel(context.Background())
	defer cancelChanging()
	changingStream, err := remote.files.DownloadRange(changingContext, &codev1.DownloadRangeRequest{Path: "changing.bin"})
	if err != nil {
		t.Fatal(err)
	}
	changingMetadata, err := changingStream.Recv()
	if err != nil || changingMetadata.GetMetadata() == nil {
		t.Fatalf("changing download metadata = %+v, %v", changingMetadata, err)
	}
	changingChunk, err := changingStream.Recv()
	if err != nil || changingChunk.GetChunk() == nil {
		t.Fatalf("changing download chunk = %+v, %v", changingChunk, err)
	}
	cancelChanging()
	changingPart, err := os.OpenFile(changingState.PartPath, os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := changingPart.Write(changingChunk.GetChunk().GetData()); err != nil {
		_ = changingPart.Close()
		t.Fatal(err)
	}
	if err := changingPart.Sync(); err != nil {
		_ = changingPart.Close()
		t.Fatal(err)
	}
	if err := changingPart.Close(); err != nil {
		t.Fatal(err)
	}
	changingState.Offset = int64(len(changingChunk.GetChunk().GetData()))
	changingState.Revision = append([]byte(nil), changingMetadata.GetMetadata().GetRevision()...)
	if err := persistLocalState(changingStatePath, changingState); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(changingRemote, changingNew, 0o640); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.DownloadFile(context.Background(), "changing.bin", changingLocal); err != nil {
		t.Fatalf("DownloadFile() did not restart after a remote revision change: %v", err)
	}
	changed, err := os.ReadFile(changingLocal)
	if err != nil || !bytes.Equal(changed, changingNew) {
		t.Fatalf("restarted changing download = %q, %v", changed, err)
	}

	zeroRemote := filepath.Join(workspace, "zero-checkpoint.bin")
	if err := os.WriteFile(zeroRemote, []byte("old before any local bytes"), 0o600); err != nil {
		t.Fatal(err)
	}
	zeroLocal := filepath.Join(t.TempDir(), "zero-checkpoint.bin")
	absZero, _ := filepath.Abs(zeroLocal)
	zeroStatePath, err := remote.localTransferStatePath("download", absZero, "zero-checkpoint.bin")
	if err != nil {
		t.Fatal(err)
	}
	zeroState, err := remote.prepareDownloadState(zeroStatePath, "zero-checkpoint.bin", absZero)
	if err != nil {
		t.Fatal(err)
	}
	zeroContext, cancelZero := context.WithCancel(context.Background())
	defer cancelZero()
	zeroStream, err := remote.files.DownloadRange(zeroContext, &codev1.DownloadRangeRequest{Path: "zero-checkpoint.bin"})
	if err != nil {
		t.Fatal(err)
	}
	zeroMetadata, err := zeroStream.Recv()
	if err != nil || zeroMetadata.GetMetadata() == nil {
		t.Fatalf("zero-checkpoint metadata = %+v, %v", zeroMetadata, err)
	}
	cancelZero()
	zeroState.Revision = append([]byte(nil), zeroMetadata.GetMetadata().GetRevision()...)
	if err := persistLocalState(zeroStatePath, zeroState); err != nil {
		t.Fatal(err)
	}
	zeroNew := []byte("new before any local bytes")
	if err := os.WriteFile(zeroRemote, zeroNew, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.DownloadFile(context.Background(), "zero-checkpoint.bin", zeroLocal); err != nil {
		t.Fatalf("DownloadFile() did not refresh a zero-offset revision: %v", err)
	}
	zeroDownloaded, err := os.ReadFile(zeroLocal)
	if err != nil || !bytes.Equal(zeroDownloaded, zeroNew) {
		t.Fatalf("zero-checkpoint download = %q, %v", zeroDownloaded, err)
	}
}

func TestFileHelpersFallBackWhenResumableTransfersAreDisabled(t *testing.T) {
	workspace := t.TempDir()
	controller, err := controllerserver.New(controllerserver.Config{
		ListenAddress: "127.0.0.1:0", Workspace: workspace, RuntimeDirectory: t.TempDir(),
		MaxUploadBytes: 1024, MaxProcesses: 1, FileTransfers: fileservice.TransferConfig{Disabled: true},
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = controller.Serve() }()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = controller.Shutdown(ctx)
	})
	remote, err := New(context.Background(), Config{Address: controller.Address(), TransferStateDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer remote.Close()
	if capabilities := remote.Info().GetFileTransfers(); capabilities.GetResumableUpload() || capabilities.GetResumableDownload() {
		t.Fatalf("disabled file transfer capabilities = %+v", capabilities)
	}
	content := []byte("legacy fallback")
	localSource := filepath.Join(t.TempDir(), "source.txt")
	if err := os.WriteFile(localSource, content, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.UploadFile(context.Background(), localSource, "fallback.txt", false); err != nil {
		t.Fatal(err)
	}
	localTarget := filepath.Join(t.TempDir(), "target.txt")
	if _, err := remote.DownloadFile(context.Background(), "fallback.txt", localTarget); err != nil {
		t.Fatal(err)
	}
	got, err := os.ReadFile(localTarget)
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("fallback download = %q, %v", got, err)
	}
}
