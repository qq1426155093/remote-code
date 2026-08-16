package client

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"golang.org/x/sys/unix"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	localTransferStateVersion  = 1
	maxTransferAttempts        = 5
	localCheckpointBytes       = int64(4 << 20)
	localCheckpointInterval    = time.Second
	maxLocalTransferStateBytes = int64(64 << 10)
)

type localUploadState struct {
	Version    int    `json:"version"`
	RequestID  string `json:"request_id"`
	UploadID   string `json:"upload_id,omitempty"`
	Address    string `json:"address"`
	Workspace  string `json:"workspace"`
	LocalPath  string `json:"local_path"`
	RemotePath string `json:"remote_path"`
	Size       int64  `json:"size"`
	SHA256     []byte `json:"sha256"`
	Mode       uint32 `json:"mode"`
	Overwrite  bool   `json:"overwrite"`
}

type localDownloadState struct {
	Version    int    `json:"version"`
	Address    string `json:"address"`
	Workspace  string `json:"workspace"`
	RemotePath string `json:"remote_path"`
	LocalPath  string `json:"local_path"`
	PartPath   string `json:"part_path"`
	Offset     int64  `json:"offset"`
	Revision   []byte `json:"revision,omitempty"`
}

func (c *Client) uploadFileResumable(ctx context.Context, localPath, remotePath string, overwrite bool) (*codev1.UploadResponse, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open local upload: %w", err)
	}
	defer file.Close()
	initial, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat local upload: %w", err)
	}
	if !initial.Mode().IsRegular() {
		return nil, errors.New("local upload is not a regular file")
	}
	if c.info.GetMaxUploadBytes() > 0 && initial.Size() > c.info.GetMaxUploadBytes() {
		return nil, fmt.Errorf("local upload exceeds the controller's %d byte limit", c.info.GetMaxUploadBytes())
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, fmt.Errorf("hash local upload: %w", err)
	}
	digest := hash.Sum(nil)
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind local upload: %w", err)
	}
	absLocal, err := filepath.Abs(localPath)
	if err != nil {
		return nil, fmt.Errorf("resolve local upload path: %w", err)
	}
	statePath, err := c.localTransferStatePath("upload", absLocal, remotePath)
	if err != nil {
		return nil, err
	}
	stateLock, err := lockLocalTransferState(statePath)
	if err != nil {
		return nil, err
	}
	defer stateLock.close()
	state, err := loadLocalUploadState(statePath)
	if err != nil {
		return nil, err
	}
	if state != nil && !state.matches(c, absLocal, remotePath, initial, digest, overwrite) {
		if state.UploadID != "" {
			_, _ = c.files.AbortUploadSession(ctx, &codev1.AbortUploadSessionRequest{UploadId: state.UploadID})
		}
		if err := os.Remove(statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return nil, fmt.Errorf("remove stale upload state: %w", err)
		}
		state = nil
	}
	if state == nil {
		requestID, err := newTransferRequestID()
		if err != nil {
			return nil, err
		}
		state = &localUploadState{
			Version: localTransferStateVersion, RequestID: requestID, Address: c.address,
			Workspace: c.info.GetWorkspaceName(), LocalPath: absLocal, RemotePath: remotePath,
			Size: initial.Size(), SHA256: digest, Mode: uint32(initial.Mode().Perm()), Overwrite: overwrite,
		}
		if err := persistLocalState(statePath, state); err != nil {
			return nil, fmt.Errorf("persist local upload state: %w", err)
		}
	}

	session, err := c.ensureUploadSession(ctx, statePath, state)
	if err != nil {
		return nil, err
	}
	if session.GetState() == codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE {
		return finishLocalUploadState(statePath, session.GetResult(), initial.Size(), digest)
	}
	if session.GetState() != codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN {
		return nil, fmt.Errorf("upload session is %s; remove the local transfer state to restart", session.GetState())
	}

	var lastErr error
	for attempt := 0; attempt < maxTransferAttempts; attempt++ {
		current, statErr := file.Stat()
		if statErr != nil {
			return nil, fmt.Errorf("restat local upload: %w", statErr)
		}
		if current.Size() != initial.Size() || !current.ModTime().Equal(initial.ModTime()) {
			return nil, errors.New("local upload changed during transfer")
		}
		result, attemptErr := c.transferUploadAttempt(ctx, file, state.UploadID, state.Size, state.SHA256)
		if attemptErr == nil {
			return finishLocalUploadState(statePath, result, state.Size, state.SHA256)
		}
		lastErr = attemptErr
		statusResponse, queryErr := c.files.GetUploadSession(ctx, &codev1.GetUploadSessionRequest{UploadId: state.UploadID})
		if queryErr == nil && statusResponse.GetSession().GetState() == codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE {
			return finishLocalUploadState(statePath, statusResponse.GetSession().GetResult(), state.Size, state.SHA256)
		}
		if !isRetryableTransferError(attemptErr) || ctx.Err() != nil {
			return nil, attemptErr
		}
		if queryErr != nil && !isRetryableTransferError(queryErr) {
			return nil, queryErr
		}
		if err := waitTransferRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("upload remains incomplete after %d attempts: %w", maxTransferAttempts, lastErr)
}

func (c *Client) ensureUploadSession(ctx context.Context, statePath string, local *localUploadState) (*codev1.UploadSession, error) {
	if local.UploadID != "" {
		response, err := c.files.GetUploadSession(ctx, &codev1.GetUploadSessionRequest{UploadId: local.UploadID})
		if err == nil {
			session := response.GetSession()
			if session == nil {
				return nil, status.Error(codes.DataLoss, "get upload session response is incomplete")
			}
			switch session.GetState() {
			case codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN, codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE:
				return session, nil
			case codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING:
				return session, nil
			case codev1.UploadSessionState_UPLOAD_SESSION_STATE_ABORTED,
				codev1.UploadSessionState_UPLOAD_SESSION_STATE_EXPIRED,
				codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED:
				if err := replaceLocalUploadRequest(statePath, local); err != nil {
					return nil, err
				}
			default:
				return nil, status.Error(codes.DataLoss, "get upload session returned an invalid state")
			}
		} else if status.Code(err) != codes.NotFound {
			return nil, err
		}
		if local.UploadID != "" {
			local.UploadID = ""
		}
	}
	for attempt := 0; attempt < 2; attempt++ {
		response, err := c.files.CreateUploadSession(ctx, &codev1.CreateUploadSessionRequest{
			RequestId: local.RequestID, Path: local.RemotePath, Size: local.Size, Sha256: local.SHA256,
			Mode: local.Mode, Overwrite: local.Overwrite,
		})
		if err != nil {
			return nil, err
		}
		session := response.GetSession()
		if session == nil || session.GetUploadId() == "" {
			return nil, status.Error(codes.DataLoss, "create upload session response is incomplete")
		}
		switch session.GetState() {
		case codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN,
			codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING,
			codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE:
			local.UploadID = session.GetUploadId()
			if err := persistLocalState(statePath, local); err != nil {
				return nil, fmt.Errorf("persist upload session ID: %w", err)
			}
			return session, nil
		case codev1.UploadSessionState_UPLOAD_SESSION_STATE_ABORTED,
			codev1.UploadSessionState_UPLOAD_SESSION_STATE_EXPIRED,
			codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED:
			if attempt == 0 {
				if err := replaceLocalUploadRequest(statePath, local); err != nil {
					return nil, err
				}
				continue
			}
			return nil, status.Error(codes.FailedPrecondition, "new upload request returned a terminal session")
		default:
			return nil, status.Error(codes.DataLoss, "create upload session returned an invalid state")
		}
	}
	return nil, status.Error(codes.Internal, "could not create an upload session")
}

func replaceLocalUploadRequest(statePath string, local *localUploadState) error {
	requestID, err := newTransferRequestID()
	if err != nil {
		return err
	}
	local.RequestID = requestID
	local.UploadID = ""
	if err := persistLocalState(statePath, local); err != nil {
		return fmt.Errorf("replace terminal upload session state: %w", err)
	}
	return nil
}

func (c *Client) transferUploadAttempt(ctx context.Context, file *os.File, uploadID string, size int64, digest []byte) (*codev1.UploadResponse, error) {
	attemptContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.files.TransferUpload(attemptContext)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Open{
		Open: &codev1.TransferUploadOpen{UploadId: uploadID},
	}}); err != nil {
		return nil, err
	}
	first, err := stream.Recv()
	if err != nil {
		return nil, err
	}
	ready := first.GetReady()
	if ready == nil || ready.GetCommittedOffset() < 0 || ready.GetCommittedOffset() > size {
		return nil, status.Error(codes.DataLoss, "upload transfer did not return a valid ready frame")
	}
	offset := ready.GetCommittedOffset()
	if _, err := file.Seek(offset, io.SeekStart); err != nil {
		return nil, fmt.Errorf("seek local upload: %w", err)
	}

	type receiveResult struct {
		result *codev1.UploadResponse
		err    error
	}
	received := make(chan receiveResult, 1)
	go func(lastCheckpoint int64) {
		for {
			response, recvErr := stream.Recv()
			if errors.Is(recvErr, io.EOF) {
				received <- receiveResult{err: status.Error(codes.DataLoss, "upload transfer ended without a complete frame")}
				return
			}
			if recvErr != nil {
				received <- receiveResult{err: recvErr}
				return
			}
			if checkpoint := response.GetCheckpoint(); checkpoint != nil {
				if checkpoint.GetCommittedOffset() < lastCheckpoint || checkpoint.GetCommittedOffset() > size {
					received <- receiveResult{err: status.Error(codes.DataLoss, "upload checkpoint is invalid")}
					return
				}
				lastCheckpoint = checkpoint.GetCommittedOffset()
				continue
			}
			if complete := response.GetComplete(); complete != nil {
				received <- receiveResult{result: complete.GetResult()}
				return
			}
			received <- receiveResult{err: status.Error(codes.DataLoss, "upload transfer returned an unexpected frame")}
			return
		}
	}(offset)

	buffer := make([]byte, transferChunkSize)
	for offset < size {
		want := int64(len(buffer))
		if size-offset < want {
			want = size - offset
		}
		n, readErr := io.ReadFull(file, buffer[:want])
		if n > 0 {
			data := append([]byte(nil), buffer[:n]...)
			chunkDigest := sha256.Sum256(data)
			if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Chunk{
				Chunk: &codev1.TransferUploadChunk{Offset: offset, Data: data, Sha256: chunkDigest[:]},
			}}); err != nil {
				cancel()
				return nil, err
			}
			offset += int64(n)
		}
		if readErr != nil && !errors.Is(readErr, io.EOF) && !errors.Is(readErr, io.ErrUnexpectedEOF) {
			cancel()
			return nil, fmt.Errorf("read local upload: %w", readErr)
		}
		if n == 0 {
			cancel()
			return nil, io.ErrUnexpectedEOF
		}
	}
	if err := stream.Send(&codev1.TransferUploadRequest{Payload: &codev1.TransferUploadRequest_Finish{Finish: &codev1.TransferUploadFinish{}}}); err != nil {
		cancel()
		return nil, err
	}
	if err := stream.CloseSend(); err != nil {
		cancel()
		return nil, err
	}
	select {
	case response := <-received:
		if response.err != nil {
			return nil, response.err
		}
		if response.result == nil || response.result.GetSize() != size || !bytes.Equal(response.result.GetSha256(), digest) {
			return nil, status.Error(codes.DataLoss, "completed upload response failed verification")
		}
		return response.result, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (c *Client) downloadFileResumable(ctx context.Context, remotePath, localPath string) (*DownloadResult, error) {
	absLocal, err := filepath.Abs(localPath)
	if err != nil {
		return nil, fmt.Errorf("resolve local download path: %w", err)
	}
	statePath, err := c.localTransferStatePath("download", absLocal, remotePath)
	if err != nil {
		return nil, err
	}
	stateLock, err := lockLocalTransferState(statePath)
	if err != nil {
		return nil, err
	}
	defer stateLock.close()
	state, err := c.prepareDownloadState(statePath, remotePath, absLocal)
	if err != nil {
		return nil, err
	}
	restarted := false
	var lastErr error
	for attempt := 0; attempt < maxTransferAttempts; attempt++ {
		result, attemptErr := c.downloadRangeAttempt(ctx, statePath, state)
		if attemptErr == nil {
			file, err := os.OpenFile(state.PartPath, os.O_RDWR|unix.O_NOFOLLOW, 0)
			if err != nil {
				return nil, fmt.Errorf("open completed local download: %w", err)
			}
			if err := file.Chmod(os.FileMode(result.File.GetMode())); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("set local download permissions: %w", err)
			}
			if err := file.Sync(); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("sync local download: %w", err)
			}
			if err := file.Close(); err != nil {
				return nil, fmt.Errorf("close local download: %w", err)
			}
			if err := os.Rename(state.PartPath, absLocal); err != nil {
				return nil, fmt.Errorf("publish local download: %w", err)
			}
			if err := syncLocalDirectory(filepath.Dir(absLocal)); err != nil {
				return nil, fmt.Errorf("sync local download directory: %w", err)
			}
			if err := os.Remove(statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return nil, fmt.Errorf("remove completed download state: %w", err)
			}
			return result, nil
		}
		lastErr = attemptErr
		if !restarted && isDownloadRevisionError(attemptErr) {
			if err := removeOwnedDownloadState(statePath, state); err != nil {
				return nil, err
			}
			state, err = c.prepareDownloadState(statePath, remotePath, absLocal)
			if err != nil {
				return nil, err
			}
			restarted = true
			continue
		}
		if !isRetryableTransferError(attemptErr) || ctx.Err() != nil {
			return nil, attemptErr
		}
		if err := waitTransferRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, fmt.Errorf("download remains incomplete after %d attempts: %w", maxTransferAttempts, lastErr)
}

func (c *Client) prepareDownloadState(statePath, remotePath, absLocal string) (*localDownloadState, error) {
	state, err := loadLocalDownloadState(statePath)
	if err != nil {
		return nil, err
	}
	if state != nil {
		if !state.matches(c, remotePath, absLocal) {
			return nil, errors.New("local download state does not match the requested transfer")
		}
		info, err := os.Lstat(state.PartPath)
		if errors.Is(err, fs.ErrNotExist) {
			_ = os.Remove(statePath)
			state = nil
		} else if err != nil {
			return nil, fmt.Errorf("stat local download part: %w", err)
		} else if !info.Mode().IsRegular() || filepath.Dir(state.PartPath) != filepath.Dir(absLocal) || !validDownloadPartName(state.PartPath) ||
			info.Size() < state.Offset || (state.Offset > 0 && len(state.Revision) != sha256.Size) {
			return nil, errors.New("local download part is inconsistent with its state")
		}
	}
	if state != nil {
		return state, nil
	}
	part, err := os.CreateTemp(filepath.Dir(absLocal), ".remote-code-download-*.part")
	if err != nil {
		return nil, fmt.Errorf("create local download part: %w", err)
	}
	partPath := part.Name()
	if err := part.Chmod(0o600); err != nil {
		_ = part.Close()
		_ = os.Remove(partPath)
		return nil, err
	}
	if err := part.Close(); err != nil {
		_ = os.Remove(partPath)
		return nil, err
	}
	state = &localDownloadState{
		Version: localTransferStateVersion, Address: c.address, Workspace: c.info.GetWorkspaceName(),
		RemotePath: remotePath, LocalPath: absLocal, PartPath: partPath,
	}
	if err := persistLocalState(statePath, state); err != nil {
		_ = os.Remove(partPath)
		return nil, fmt.Errorf("persist local download state: %w", err)
	}
	return state, nil
}

func (c *Client) downloadRangeAttempt(ctx context.Context, statePath string, state *localDownloadState) (*DownloadResult, error) {
	file, err := os.OpenFile(state.PartPath, os.O_RDWR|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, fmt.Errorf("open local download part: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if info.Size() < state.Offset {
		return nil, errors.New("local download part is shorter than its durable offset")
	}
	if info.Size() != state.Offset {
		if err := file.Truncate(state.Offset); err != nil {
			return nil, fmt.Errorf("truncate uncommitted local download data: %w", err)
		}
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}
	hash := sha256.New()
	if state.Offset > 0 {
		if _, err := io.CopyN(hash, file, state.Offset); err != nil {
			return nil, fmt.Errorf("hash local download prefix: %w", err)
		}
	}
	request := &codev1.DownloadRangeRequest{Path: state.RemotePath, Offset: state.Offset}
	if state.Offset > 0 {
		request.ExpectedRevision = append([]byte(nil), state.Revision...)
		request.PrefixSha256 = hash.Sum(nil)
	}
	stream, err := c.files.DownloadRange(ctx, request)
	if err != nil {
		return nil, err
	}
	accepted := state.Offset
	lastDurable := state.Offset
	lastCheckpoint := time.Now()
	var metadata *codev1.DownloadRangeMetadata
	for {
		frame, recvErr := stream.Recv()
		if recvErr != nil {
			if accepted != lastDurable {
				if checkpointErr := checkpointLocalDownload(file, statePath, state, accepted); checkpointErr != nil {
					return nil, checkpointErr
				}
			}
			if errors.Is(recvErr, io.EOF) {
				return nil, status.Error(codes.DataLoss, "download range ended without a summary")
			}
			return nil, recvErr
		}
		switch payload := frame.GetPayload().(type) {
		case *codev1.DownloadRangeResponse_Metadata:
			if metadata != nil || payload.Metadata == nil || payload.Metadata.GetFile() == nil || payload.Metadata.GetOffset() != state.Offset || len(payload.Metadata.GetRevision()) != sha256.Size {
				return nil, status.Error(codes.DataLoss, "download range metadata is invalid")
			}
			if state.Offset > 0 && !bytes.Equal(state.Revision, payload.Metadata.GetRevision()) {
				return nil, status.Error(codes.DataLoss, "download server returned an unexpected revision")
			}
			metadata = payload.Metadata
			if !bytes.Equal(state.Revision, metadata.GetRevision()) {
				state.Revision = append([]byte(nil), metadata.GetRevision()...)
				if err := persistLocalState(statePath, state); err != nil {
					return nil, fmt.Errorf("persist download revision: %w", err)
				}
			}
		case *codev1.DownloadRangeResponse_Chunk:
			chunk := payload.Chunk
			if metadata == nil || chunk == nil || len(chunk.GetData()) == 0 || chunk.GetOffset() != accepted || len(chunk.GetSha256()) != sha256.Size {
				return nil, status.Error(codes.DataLoss, "download range chunk is invalid or out of order")
			}
			chunkDigest := sha256.Sum256(chunk.GetData())
			if !bytes.Equal(chunkDigest[:], chunk.GetSha256()) {
				return nil, status.Error(codes.DataLoss, "download range chunk sha256 mismatch")
			}
			if err := writeAll(file, chunk.GetData()); err != nil {
				return nil, err
			}
			_, _ = hash.Write(chunk.GetData())
			accepted += int64(len(chunk.GetData()))
			if accepted-lastDurable >= localCheckpointBytes || time.Since(lastCheckpoint) >= localCheckpointInterval {
				if err := checkpointLocalDownload(file, statePath, state, accepted); err != nil {
					return nil, err
				}
				lastDurable = accepted
				lastCheckpoint = time.Now()
			}
		case *codev1.DownloadRangeResponse_Summary:
			summary := payload.Summary
			if metadata == nil || summary == nil || summary.GetSize() != accepted || metadata.GetFile().GetSize() != accepted ||
				!bytes.Equal(summary.GetRevision(), state.Revision) || !bytes.Equal(summary.GetSha256(), hash.Sum(nil)) {
				return nil, status.Error(codes.DataLoss, "download range summary failed verification")
			}
			if err := checkpointLocalDownload(file, statePath, state, accepted); err != nil {
				return nil, err
			}
			return &DownloadResult{File: metadata.GetFile(), Size: accepted, SHA256: append([]byte(nil), summary.GetSha256()...)}, nil
		default:
			return nil, status.Error(codes.DataLoss, "download range frame has no payload")
		}
	}
}

func checkpointLocalDownload(file *os.File, statePath string, state *localDownloadState, offset int64) error {
	if err := file.Sync(); err != nil {
		return fmt.Errorf("sync local download checkpoint: %w", err)
	}
	previous := state.Offset
	state.Offset = offset
	if err := persistLocalState(statePath, state); err != nil {
		state.Offset = previous
		return fmt.Errorf("persist local download checkpoint: %w", err)
	}
	return nil
}

func finishLocalUploadState(statePath string, result *codev1.UploadResponse, size int64, digest []byte) (*codev1.UploadResponse, error) {
	if result == nil || result.GetFile() == nil || result.GetSize() != size || !bytes.Equal(result.GetSha256(), digest) {
		return nil, status.Error(codes.DataLoss, "completed upload session result failed verification")
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return nil, fmt.Errorf("remove completed upload state: %w", err)
	}
	return result, nil
}

func (s *localUploadState) matches(c *Client, localPath, remotePath string, info os.FileInfo, digest []byte, overwrite bool) bool {
	return s.Version == localTransferStateVersion && s.Address == c.address && s.Workspace == c.info.GetWorkspaceName() &&
		s.LocalPath == localPath && s.RemotePath == remotePath && s.Size == info.Size() && s.Mode == uint32(info.Mode().Perm()) &&
		s.Overwrite == overwrite && bytes.Equal(s.SHA256, digest) && s.RequestID != ""
}

func (s *localDownloadState) matches(c *Client, remotePath, localPath string) bool {
	return s.Version == localTransferStateVersion && s.Address == c.address && s.Workspace == c.info.GetWorkspaceName() &&
		s.RemotePath == remotePath && s.LocalPath == localPath && s.PartPath != "" && s.Offset >= 0
}

func loadLocalUploadState(name string) (*localUploadState, error) {
	var state localUploadState
	found, err := loadLocalState(name, &state)
	if err != nil || !found {
		return nil, err
	}
	return &state, nil
}

func loadLocalDownloadState(name string) (*localDownloadState, error) {
	var state localDownloadState
	found, err := loadLocalState(name, &state)
	if err != nil || !found {
		return nil, err
	}
	return &state, nil
}

func loadLocalState(name string, target any) (bool, error) {
	file, err := os.OpenFile(name, os.O_RDONLY|unix.O_NOFOLLOW, 0)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("read local transfer state: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return false, fmt.Errorf("stat local transfer state: %w", err)
	}
	if !info.Mode().IsRegular() || info.Size() < 0 || info.Size() > maxLocalTransferStateBytes {
		return false, errors.New("local transfer state is not a bounded regular file")
	}
	data, err := io.ReadAll(io.LimitReader(file, maxLocalTransferStateBytes+1))
	if err != nil {
		return false, fmt.Errorf("read local transfer state: %w", err)
	}
	if err := json.Unmarshal(data, target); err != nil {
		return false, fmt.Errorf("decode local transfer state: %w", err)
	}
	return true, nil
}

func persistLocalState(name string, state any) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	temporary, err := os.CreateTemp(filepath.Dir(name), ".state-*")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	remove := true
	defer func() {
		_ = temporary.Close()
		if remove {
			_ = os.Remove(temporaryName)
		}
	}()
	if err := temporary.Chmod(0o600); err != nil {
		return err
	}
	if err := writeAll(temporary, data); err != nil {
		return err
	}
	if err := temporary.Sync(); err != nil {
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, name); err != nil {
		return err
	}
	remove = false
	return syncLocalDirectory(filepath.Dir(name))
}

func syncLocalDirectory(name string) error {
	directory, err := os.Open(name)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

type localTransferLock struct {
	file *os.File
}

func lockLocalTransferState(statePath string) (*localTransferLock, error) {
	file, err := os.OpenFile(statePath+".lock", os.O_CREATE|os.O_RDWR|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open local transfer lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		return nil, errors.New("the same local file transfer is already active")
	}
	return &localTransferLock{file: file}, nil
}

func (l *localTransferLock) close() {
	if l == nil || l.file == nil {
		return
	}
	_ = unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	_ = l.file.Close()
	l.file = nil
}

func (c *Client) localTransferStatePath(direction, localPath, remotePath string) (string, error) {
	directory := c.transferStateDirectory
	if directory == "" {
		cache, err := os.UserCacheDir()
		if err != nil {
			return "", fmt.Errorf("resolve local transfer state directory: %w", err)
		}
		directory = filepath.Join(cache, "remote-code", "transfers")
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return "", fmt.Errorf("create local transfer state directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return "", fmt.Errorf("secure local transfer state directory: %w", err)
	}
	key := sha256.Sum256([]byte(direction + "\x00" + c.address + "\x00" + c.info.GetWorkspaceName() + "\x00" + localPath + "\x00" + remotePath))
	return filepath.Join(directory, direction+"-"+hex.EncodeToString(key[:])+".json"), nil
}

func newTransferRequestID() (string, error) {
	random := make([]byte, 16)
	if _, err := rand.Read(random); err != nil {
		return "", fmt.Errorf("generate transfer request ID: %w", err)
	}
	return hex.EncodeToString(random), nil
}

func removeOwnedDownloadState(statePath string, state *localDownloadState) error {
	if filepath.Dir(state.PartPath) != filepath.Dir(state.LocalPath) || !validDownloadPartName(state.PartPath) {
		return errors.New("refusing to remove an invalid local download part")
	}
	if err := os.Remove(state.PartPath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stale local download part: %w", err)
	}
	if err := os.Remove(statePath); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("remove stale local download state: %w", err)
	}
	return nil
}

func validDownloadPartName(name string) bool {
	base := filepath.Base(name)
	return strings.HasPrefix(base, ".remote-code-download-") && strings.HasSuffix(base, ".part")
}

func isRetryableTransferError(err error) bool {
	switch status.Code(err) {
	case codes.Unavailable, codes.DeadlineExceeded, codes.Aborted, codes.Canceled:
		return true
	default:
		return false
	}
}

func isDownloadRevisionError(err error) bool {
	converted := status.Convert(err)
	for _, detail := range converted.Details() {
		transfer, ok := detail.(*codev1.FileTransferError)
		if !ok {
			continue
		}
		return transfer.GetReason() == codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED ||
			transfer.GetReason() == codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_PREFIX_MISMATCH
	}
	return false
}

func waitTransferRetry(ctx context.Context, attempt int) error {
	delay := 250 * time.Millisecond * time.Duration(1<<min(attempt, 4))
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
