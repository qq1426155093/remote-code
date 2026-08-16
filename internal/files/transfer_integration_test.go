package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestResumableUploadSurvivesDisconnectAndServiceRestart(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	config := Config{
		Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxUploadBytes: 1024,
		Transfers: TransferConfig{
			CheckpointBytes: 1, CheckpointInterval: time.Hour, MaxStagingBytes: 4096,
			UploadSessionTTL: time.Hour, CompletedSessionTTL: time.Hour,
		},
	}
	client, stop := startFileTransferTestServer(t, config)
	content := []byte("resumable upload payload")
	digest := sha256.Sum256(content)
	created, err := client.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "request-one", Path: "artifact.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o640,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := created.GetSession().GetUploadId()
	duplicate, err := client.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "request-one", Path: "artifact.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o640,
	})
	if err != nil || duplicate.GetSession().GetUploadId() != uploadID {
		t.Fatalf("idempotent CreateUploadSession() = %+v, %v", duplicate, err)
	}
	if _, err := client.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "request-one", Path: "artifact.bin", Size: int64(len(content)) + 1, Sha256: digest[:], Mode: 0o640,
	}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("conflicting CreateUploadSession() code = %s, want AlreadyExists", status.Code(err))
	}
	listed, err := client.List(context.Background(), &codev1.ListRequest{})
	if err != nil || len(listed.GetFiles()) != 0 {
		t.Fatalf("List() exposed upload staging file: %+v, %v", listed, err)
	}

	streamContext, cancel := context.WithCancel(context.Background())
	stream, err := client.TransferUpload(streamContext)
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Open{
		Open: &codev1.TransferUploadOpen{UploadId: uploadID},
	}}); err != nil {
		t.Fatal(err)
	}
	if ready, err := stream.Recv(); err != nil || ready.GetReady().GetCommittedOffset() != 0 {
		t.Fatalf("ready = %+v, %v", ready, err)
	}
	first := content[:9]
	firstDigest := sha256.Sum256(first)
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
		Chunk: &codev1.TransferUploadChunk{Offset: 0, Data: first, Sha256: firstDigest[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	checkpoint, err := stream.Recv()
	if err != nil || checkpoint.GetCheckpoint().GetCommittedOffset() != int64(len(first)) {
		t.Fatalf("checkpoint = %+v, %v", checkpoint, err)
	}
	cancel()
	_, _ = stream.Recv()
	statusBeforeRestart, err := client.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: uploadID})
	if err != nil || statusBeforeRestart.GetSession().GetCommittedOffset() != int64(len(first)) {
		t.Fatalf("GetUploadSession() before restart = %+v, %v", statusBeforeRestart, err)
	}
	stop()

	client, stop = startFileTransferTestServer(t, config)
	defer stop()
	statusAfterRestart, err := client.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: uploadID})
	if err != nil || statusAfterRestart.GetSession().GetCommittedOffset() != int64(len(first)) {
		t.Fatalf("GetUploadSession() after restart = %+v, %v", statusAfterRestart, err)
	}
	stream, err = client.TransferUpload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Open{
		Open: &codev1.TransferUploadOpen{UploadId: uploadID, KnownOffset: int64(len(first))},
	}}); err != nil {
		t.Fatal(err)
	}
	if ready, err := stream.Recv(); err != nil || ready.GetReady().GetCommittedOffset() != int64(len(first)) {
		t.Fatalf("resumed ready = %+v, %v", ready, err)
	}
	remainder := content[len(first):]
	remainderDigest := sha256.Sum256(remainder)
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
		Chunk: &codev1.TransferUploadChunk{Offset: int64(len(first)), Data: remainder, Sha256: remainderDigest[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Finish{Finish: &codev1.TransferUploadFinish{}}}); err != nil {
		t.Fatal(err)
	}
	var completed *codev1.UploadResponse
	for completed == nil {
		response, err := stream.Recv()
		if err != nil {
			t.Fatal(err)
		}
		if response.GetComplete() != nil {
			completed = response.GetComplete().GetResult()
		}
	}
	if completed.GetSize() != int64(len(content)) || !bytes.Equal(completed.GetSha256(), digest[:]) {
		t.Fatalf("completed upload = %+v", completed)
	}
	got, err := os.ReadFile(filepath.Join(workspace, "artifact.bin"))
	if err != nil || !bytes.Equal(got, content) {
		t.Fatalf("published upload = %q, %v", got, err)
	}
	queried, err := client.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: uploadID})
	if err != nil || queried.GetSession().GetState() != codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE || queried.GetSession().GetResult() == nil {
		t.Fatalf("completed GetUploadSession() = %+v, %v", queried, err)
	}
	duplicate, err = client.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "request-one", Path: "artifact.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o640,
	})
	if err != nil || duplicate.GetSession().GetUploadId() != uploadID || duplicate.GetSession().GetState() != codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE {
		t.Fatalf("completed idempotent CreateUploadSession() = %+v, %v", duplicate, err)
	}
	stop()
	client, stop = startFileTransferTestServer(t, config)
	defer stop()
	recovered, err := client.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: uploadID})
	if err != nil || recovered.GetSession().GetState() != codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE || recovered.GetSession().GetResult() == nil {
		t.Fatalf("completed session after restart = %+v, %v", recovered, err)
	}
}

func TestResumableUploadRejectsInvalidChunkOffsetAndDigest(t *testing.T) {
	config := Config{Workspace: t.TempDir(), RuntimeDirectory: t.TempDir(), MaxUploadBytes: 1024}
	client, stop := startFileTransferTestServer(t, config)
	defer stop()
	content := []byte("chunk validation")
	digest := sha256.Sum256(content)
	created, err := client.CreateUploadSession(context.Background(), &codev1.CreateUploadSessionRequest{
		RequestId: "invalid-chunks", Path: "invalid.bin", Size: int64(len(content)), Sha256: digest[:], Mode: 0o600,
	})
	if err != nil {
		t.Fatal(err)
	}
	uploadID := created.GetSession().GetUploadId()

	open := func() codev1.FileService_TransferUploadClient {
		stream, err := client.TransferUpload(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Open{
			Open: &codev1.TransferUploadOpen{UploadId: uploadID},
		}}); err != nil {
			t.Fatal(err)
		}
		if ready, err := stream.Recv(); err != nil || ready.GetReady().GetCommittedOffset() != 0 {
			t.Fatalf("ready = %+v, %v", ready, err)
		}
		return stream
	}

	chunk := []byte("chunk")
	chunkDigest := sha256.Sum256(chunk)
	stream := open()
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
		Chunk: &codev1.TransferUploadChunk{Offset: 1, Data: chunk, Sha256: chunkDigest[:]},
	}}); err != nil {
		t.Fatal(err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("invalid offset code = %s, want OutOfRange; error = %v", status.Code(err), err)
	}
	assertTransferError(t, err, codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_OFFSET_MISMATCH, 0)

	stream = open()
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
		Chunk: &codev1.TransferUploadChunk{Data: chunk, Sha256: make([]byte, sha256.Size)},
	}}); err != nil {
		t.Fatal(err)
	}
	if _, err := stream.Recv(); status.Code(err) != codes.DataLoss {
		t.Fatalf("invalid chunk digest code = %s, want DataLoss; error = %v", status.Code(err), err)
	}
	queried, err := client.GetUploadSession(context.Background(), &codev1.GetUploadSessionRequest{UploadId: uploadID})
	if err != nil || queried.GetSession().GetCommittedOffset() != 0 {
		t.Fatalf("session after rejected chunks = %+v, %v", queried, err)
	}
}

func TestDownloadRangeResumesAcrossServiceRestartAndRejectsChangedFile(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	content := bytes.Repeat([]byte("download-range-"), 12_000)
	remoteName := filepath.Join(workspace, "large.bin")
	if err := os.WriteFile(remoteName, content, 0o640); err != nil {
		t.Fatal(err)
	}
	config := Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxUploadBytes: 1 << 20}
	client, stop := startFileTransferTestServer(t, config)
	stream, err := client.DownloadRange(context.Background(), &codev1.DownloadRangeRequest{Path: "large.bin"})
	if err != nil {
		t.Fatal(err)
	}
	metadataFrame, err := stream.Recv()
	if err != nil || metadataFrame.GetMetadata() == nil {
		t.Fatalf("metadata = %+v, %v", metadataFrame, err)
	}
	revision := append([]byte(nil), metadataFrame.GetMetadata().GetRevision()...)
	chunkFrame, err := stream.Recv()
	if err != nil || chunkFrame.GetChunk() == nil {
		t.Fatalf("first chunk = %+v, %v", chunkFrame, err)
	}
	prefix := append([]byte(nil), chunkFrame.GetChunk().GetData()...)
	stop()

	client, stop = startFileTransferTestServer(t, config)
	defer stop()
	prefixDigest := sha256.Sum256(prefix)
	stream, err = client.DownloadRange(context.Background(), &codev1.DownloadRangeRequest{
		Path: "large.bin", Offset: int64(len(prefix)), ExpectedRevision: revision, PrefixSha256: prefixDigest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	assembled := append([]byte(nil), prefix...)
	var summary *codev1.DownloadRangeSummary
	for {
		response, recvErr := stream.Recv()
		if recvErr != nil {
			if errors.Is(recvErr, io.EOF) {
				break
			}
			t.Fatal(recvErr)
		}
		if chunk := response.GetChunk(); chunk != nil {
			if chunk.GetOffset() != int64(len(assembled)) {
				t.Fatalf("chunk offset = %d, want %d", chunk.GetOffset(), len(assembled))
			}
			assembled = append(assembled, chunk.GetData()...)
		}
		if response.GetSummary() != nil {
			summary = response.GetSummary()
		}
	}
	fullDigest := sha256.Sum256(content)
	if !bytes.Equal(assembled, content) || summary == nil || !bytes.Equal(summary.GetSha256(), fullDigest[:]) {
		t.Fatalf("resumed download size = %d, summary = %+v", len(assembled), summary)
	}
	wrongPrefix := make([]byte, sha256.Size)
	corrupt, err := client.DownloadRange(context.Background(), &codev1.DownloadRangeRequest{
		Path: "large.bin", Offset: int64(len(prefix)), ExpectedRevision: revision, PrefixSha256: wrongPrefix,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := corrupt.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("corrupt prefix DownloadRange() code = %s, want FailedPrecondition; error = %v", status.Code(err), err)
	} else {
		assertTransferError(t, err, codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_PREFIX_MISMATCH, 0)
	}
	if err := os.WriteFile(remoteName, []byte("changed"), 0o640); err != nil {
		t.Fatal(err)
	}
	changed, err := client.DownloadRange(context.Background(), &codev1.DownloadRangeRequest{
		Path: "large.bin", Offset: int64(len(prefix)), ExpectedRevision: revision, PrefixSha256: prefixDigest[:],
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := changed.Recv(); status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("changed DownloadRange() code = %s, want FailedPrecondition; error = %v", status.Code(err), err)
	} else {
		assertTransferError(t, err, codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED, 0)
	}
}

func startFileTransferTestServer(t *testing.T, config Config) (codev1.FileServiceClient, func()) {
	t.Helper()
	service, err := New(config)
	if err != nil {
		t.Fatal(err)
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		_ = service.Close()
		t.Fatal(err)
	}
	server := grpc.NewServer()
	codev1.RegisterFileServiceServer(server, service)
	go func() { _ = server.Serve(listener) }()
	connection, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Stop()
		_ = service.Close()
		t.Fatal(err)
	}
	stopped := false
	return codev1.NewFileServiceClient(connection), func() {
		if stopped {
			return
		}
		stopped = true
		_ = connection.Close()
		server.Stop()
		_ = listener.Close()
		_ = service.Close()
	}
}

func assertTransferError(t *testing.T, err error, reason codev1.FileTransferErrorReason, expectedOffset int64) {
	t.Helper()
	for _, detail := range status.Convert(err).Details() {
		transfer, ok := detail.(*codev1.FileTransferError)
		if ok && transfer.GetReason() == reason && transfer.GetExpectedOffset() == expectedOffset {
			return
		}
	}
	t.Fatalf("error %v has no FileTransferError(%s, %d)", err, reason, expectedOffset)
}
