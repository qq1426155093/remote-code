package files

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"strconv"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) CreateUploadSession(_ context.Context, request *codev1.CreateUploadSessionRequest) (*codev1.CreateUploadSessionResponse, error) {
	if s.transfers == nil || s.transfers.disabled {
		return nil, status.Error(codes.Unimplemented, "resumable upload is disabled")
	}
	if request.GetRequestId() == "" || len(request.GetRequestId()) > 128 {
		return nil, status.Error(codes.InvalidArgument, "upload request ID must be between 1 and 128 bytes")
	}
	rel, err := cleanMutablePath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if request.GetSize() < 0 {
		return nil, status.Error(codes.InvalidArgument, "upload size cannot be negative")
	}
	if request.GetSize() > s.maxUploadBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "upload exceeds the %d byte limit", s.maxUploadBytes)
	}
	if len(request.GetSha256()) != sha256.Size {
		return nil, status.Error(codes.InvalidArgument, "upload sha256 must contain 32 bytes")
	}
	if err := validateMode(request.GetMode()); err != nil {
		return nil, err
	}

	store := s.transfers
	store.cleanupExpired(time.Now())
	store.mu.Lock()
	if uploadID, ok := store.requests[request.GetRequestId()]; ok {
		session := store.sessions[uploadID]
		store.mu.Unlock()
		session.mu.Lock()
		if !sameUploadRequest(session.record, request, rel) {
			session.mu.Unlock()
			return nil, status.Error(codes.AlreadyExists, "upload request ID is already used with different metadata")
		}
		release, err := store.reconcileFinalizingUploadLocked(session)
		if err != nil {
			session.mu.Unlock()
			return nil, err
		}
		record := session.record
		response := &codev1.CreateUploadSessionResponse{Session: uploadSessionProto(record)}
		session.mu.Unlock()
		if release {
			store.releasePaths(record)
		}
		return response, nil
	}
	if store.mutationConflictsLocked(rel) {
		store.mu.Unlock()
		return nil, transferStatus(codes.FailedPrecondition, "upload target conflicts with an active transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if err := s.validateUploadTarget(rel, request.GetOverwrite()); err != nil {
		store.mu.Unlock()
		return nil, err
	}
	if store.activeUploads >= store.config.MaxUploadSessions {
		store.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted, "active upload sessions exceed the %d session limit", store.config.MaxUploadSessions)
	}
	if request.GetSize() > store.config.MaxStagingBytes-store.reservedBytes {
		store.mu.Unlock()
		return nil, status.Errorf(codes.ResourceExhausted, "upload staging exceeds the %d byte limit", store.config.MaxStagingBytes)
	}
	uploadID, err := store.newUploadID()
	if err != nil {
		store.mu.Unlock()
		return nil, status.Error(codes.Internal, "allocate upload session")
	}
	tempPath := path.Join(path.Dir(rel), ".remote-code-upload-"+uploadID)
	now := time.Now().UTC()
	record := uploadRecord{
		Version: transferStateVersion, WorkspaceID: store.workspaceID, UploadID: uploadID,
		RequestID: request.GetRequestId(), TargetPath: rel, TempPath: tempPath,
		Size: request.GetSize(), SHA256: append([]byte(nil), request.GetSha256()...),
		Mode: request.GetMode(), Overwrite: request.GetOverwrite(),
		State:     codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN,
		CreatedAt: now, UpdatedAt: now, ExpiresAt: now.Add(store.config.UploadSessionTTL),
	}
	if err := store.persistRecord(record); err != nil {
		store.mu.Unlock()
		return nil, status.Error(codes.Internal, "persist upload session")
	}
	file, err := s.root.OpenFile(tempPath, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		_ = os.Remove(store.recordPath(uploadID))
		store.mu.Unlock()
		return nil, fileError("create upload temporary file", rel, err)
	}
	if err := file.Close(); err != nil {
		_ = s.root.Remove(tempPath)
		_ = os.Remove(store.recordPath(uploadID))
		store.mu.Unlock()
		return nil, fileError("close upload temporary file", rel, err)
	}
	if err := s.syncWorkspaceDirectory(path.Dir(tempPath)); err != nil {
		_ = s.root.Remove(tempPath)
		_ = os.Remove(store.recordPath(uploadID))
		store.mu.Unlock()
		return nil, fileError("sync upload temporary directory", rel, err)
	}
	session := &uploadSession{record: record}
	store.sessions[uploadID] = session
	store.requests[record.RequestID] = uploadID
	store.targets[rel] = uploadID
	store.temps[tempPath] = uploadID
	store.activeUploads++
	store.reservedBytes += record.Size
	store.mu.Unlock()
	return &codev1.CreateUploadSessionResponse{Session: uploadSessionProto(record)}, nil
}

func (s *Service) GetUploadSession(_ context.Context, request *codev1.GetUploadSessionRequest) (*codev1.GetUploadSessionResponse, error) {
	if s.transfers == nil || s.transfers.disabled {
		return nil, status.Error(codes.Unimplemented, "resumable upload is disabled")
	}
	s.transfers.cleanupExpired(time.Now())
	session, ok := s.transfers.get(request.GetUploadId())
	if !ok {
		return nil, status.Error(codes.NotFound, "upload session was not found")
	}
	session.mu.Lock()
	release, err := s.transfers.reconcileFinalizingUploadLocked(session)
	if err != nil {
		session.mu.Unlock()
		return nil, err
	}
	record := session.record
	response := &codev1.GetUploadSessionResponse{Session: uploadSessionProto(session.record)}
	session.mu.Unlock()
	if release {
		s.transfers.releasePaths(record)
	}
	return response, nil
}

func (s *Service) AbortUploadSession(_ context.Context, request *codev1.AbortUploadSessionRequest) (*codev1.AbortUploadSessionResponse, error) {
	if s.transfers == nil || s.transfers.disabled {
		return nil, status.Error(codes.Unimplemented, "resumable upload is disabled")
	}
	session, ok := s.transfers.get(request.GetUploadId())
	if !ok {
		return nil, status.Error(codes.NotFound, "upload session was not found")
	}
	session.mu.Lock()
	if session.active {
		session.mu.Unlock()
		return nil, status.Error(codes.Aborted, "upload session has an active transfer")
	}
	release, err := s.transfers.reconcileFinalizingUploadLocked(session)
	if err != nil {
		session.mu.Unlock()
		return nil, err
	}
	if release {
		record := session.record
		result := uploadSessionProto(record)
		session.mu.Unlock()
		s.transfers.releasePaths(record)
		return &codev1.AbortUploadSessionResponse{Session: result}, nil
	}
	if !isOpenUploadState(session.record.State) {
		result := uploadSessionProto(session.record)
		session.mu.Unlock()
		return &codev1.AbortUploadSessionResponse{Session: result}, nil
	}
	record := session.record
	session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_ABORTED
	session.record.UpdatedAt = time.Now().UTC()
	session.record.ExpiresAt = session.record.UpdatedAt.Add(s.transfers.config.CompletedSessionTTL)
	if err := s.transfers.persistRecord(session.record); err != nil {
		session.record = record
		session.mu.Unlock()
		return nil, status.Error(codes.Internal, "persist aborted upload session")
	}
	result := uploadSessionProto(session.record)
	session.mu.Unlock()
	s.transfers.removeSessionTemp(record.TempPath)
	s.transfers.releasePaths(record)
	return &codev1.AbortUploadSessionResponse{Session: result}, nil
}

func (s *Service) TransferUpload(stream grpc.BidiStreamingServer[codev1.TransferUploadRequest, codev1.TransferUploadResponse]) error {
	if s.transfers == nil || s.transfers.disabled {
		return status.Error(codes.Unimplemented, "resumable upload is disabled")
	}
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "the first upload transfer frame must contain open")
	}
	if err != nil {
		return contextError(err)
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "the first upload transfer frame must contain open")
	}
	session, ok := s.transfers.get(open.GetUploadId())
	if !ok {
		return status.Error(codes.NotFound, "upload session was not found")
	}
	session.mu.Lock()
	if session.record.State != codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN {
		session.mu.Unlock()
		return transferStatus(codes.FailedPrecondition, "upload session is not open", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_SESSION_STATE, 0)
	}
	if session.active {
		session.mu.Unlock()
		return status.Error(codes.Aborted, "upload session already has an active transfer")
	}
	if time.Now().After(session.record.ExpiresAt) {
		session.mu.Unlock()
		return transferStatus(codes.FailedPrecondition, "upload session has expired", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_SESSION_STATE, 0)
	}
	session.active = true
	record := session.record
	session.mu.Unlock()
	defer func() {
		session.mu.Lock()
		session.active = false
		session.mu.Unlock()
	}()

	file, err := s.root.OpenFile(record.TempPath, os.O_RDWR, 0)
	if err != nil {
		return fileError("open upload temporary file", record.TargetPath, err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil {
		return fileError("stat upload temporary file", record.TargetPath, err)
	}
	if !info.Mode().IsRegular() || info.Size() < record.CommittedOffset {
		return s.failUpload(session, codes.DataLoss, "upload temporary file is inconsistent")
	}
	if info.Size() != record.CommittedOffset {
		if err := file.Truncate(record.CommittedOffset); err != nil {
			return fileError("truncate uncommitted upload data", record.TargetPath, err)
		}
	}
	if _, err := file.Seek(record.CommittedOffset, io.SeekStart); err != nil {
		return fileError("seek upload temporary file", record.TargetPath, err)
	}
	if err := stream.Send(&codev1.TransferUploadResponse{Payload: &codev1.TransferUploadResponse_Ready{
		Ready: &codev1.TransferUploadReady{CommittedOffset: record.CommittedOffset},
	}}); err != nil {
		return contextError(err)
	}

	accepted := record.CommittedOffset
	lastCheckpoint := time.Now()
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			committed, checkpointErr := s.checkpointUpload(session, file, accepted)
			if checkpointErr != nil {
				return checkpointErr
			}
			if committed {
				_ = stream.Send(uploadCheckpointResponse(accepted))
			}
			return nil
		}
		if recvErr != nil {
			_, _ = s.checkpointUpload(session, file, accepted)
			return contextError(recvErr)
		}
		switch payload := frame.GetPayload().(type) {
		case *codev1.TransferUploadRequest_Chunk:
			chunk := payload.Chunk
			if chunk == nil || len(chunk.GetData()) == 0 {
				return status.Error(codes.InvalidArgument, "upload chunk data is required")
			}
			if len(chunk.GetData()) > maxReceivedChunkSize {
				return status.Errorf(codes.ResourceExhausted, "upload chunk exceeds the %d byte limit", maxReceivedChunkSize)
			}
			if len(chunk.GetSha256()) != sha256.Size {
				return status.Error(codes.InvalidArgument, "upload chunk sha256 must contain 32 bytes")
			}
			digest := sha256.Sum256(chunk.GetData())
			if !bytes.Equal(digest[:], chunk.GetSha256()) {
				return status.Error(codes.DataLoss, "upload chunk sha256 mismatch")
			}
			if chunk.GetOffset() != accepted {
				return transferStatus(codes.OutOfRange, "upload chunk offset does not match the expected offset", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_OFFSET_MISMATCH, accepted)
			}
			if int64(len(chunk.GetData())) > record.Size-accepted {
				return status.Error(codes.DataLoss, "upload contains more bytes than declared")
			}
			if err := writeFull(file, chunk.GetData()); err != nil {
				return fileError("write resumable upload", record.TargetPath, err)
			}
			accepted += int64(len(chunk.GetData()))
			if accepted-record.CommittedOffset >= s.transfers.config.CheckpointBytes || time.Since(lastCheckpoint) >= s.transfers.config.CheckpointInterval {
				committed, err := s.checkpointUpload(session, file, accepted)
				if err != nil {
					return err
				}
				if committed {
					record.CommittedOffset = accepted
					lastCheckpoint = time.Now()
					if err := stream.Send(uploadCheckpointResponse(accepted)); err != nil {
						return contextError(err)
					}
				}
			}
		case *codev1.TransferUploadRequest_Finish:
			if payload.Finish == nil {
				return status.Error(codes.InvalidArgument, "upload finish frame is empty")
			}
			if accepted != record.Size {
				return status.Errorf(codes.FailedPrecondition, "upload finish requires %d bytes, received %d", record.Size, accepted)
			}
			if _, err := s.checkpointUpload(session, file, accepted); err != nil {
				return err
			}
			if err := file.Close(); err != nil {
				return fileError("close resumable upload", record.TargetPath, err)
			}
			closed = true
			result, err := s.finalizeUpload(session)
			if err != nil {
				return err
			}
			if err := stream.Send(&codev1.TransferUploadResponse{Payload: &codev1.TransferUploadResponse_Complete{
				Complete: &codev1.TransferUploadComplete{Result: result},
			}}); err != nil {
				return contextError(err)
			}
			return nil
		default:
			return status.Error(codes.InvalidArgument, "upload open frame may only appear first")
		}
	}
}

func (s *Service) checkpointUpload(session *uploadSession, file *os.File, accepted int64) (bool, error) {
	session.mu.Lock()
	if accepted == session.record.CommittedOffset {
		session.mu.Unlock()
		return false, nil
	}
	if session.record.State != codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN {
		session.mu.Unlock()
		return false, transferStatus(codes.FailedPrecondition, "upload session is not open", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_SESSION_STATE, 0)
	}
	if err := file.Sync(); err != nil {
		session.mu.Unlock()
		return false, fileError("sync resumable upload", session.record.TargetPath, err)
	}
	previous := session.record
	now := time.Now().UTC()
	session.record.CommittedOffset = accepted
	session.record.UpdatedAt = now
	session.record.ExpiresAt = now.Add(s.transfers.config.UploadSessionTTL)
	if err := s.transfers.persistRecord(session.record); err != nil {
		session.record = previous
		session.mu.Unlock()
		return false, status.Error(codes.Internal, "persist upload checkpoint")
	}
	session.mu.Unlock()
	return true, nil
}

func (s *Service) finalizeUpload(session *uploadSession) (*codev1.UploadResponse, error) {
	session.mu.Lock()
	if session.record.State != codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN || session.record.CommittedOffset != session.record.Size {
		session.mu.Unlock()
		return nil, transferStatus(codes.FailedPrecondition, "upload session is not ready to finalize", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_SESSION_STATE, 0)
	}
	previous := session.record
	session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING
	session.record.UpdatedAt = time.Now().UTC()
	if err := s.transfers.persistRecord(session.record); err != nil {
		session.record = previous
		session.mu.Unlock()
		return nil, status.Error(codes.Internal, "persist finalizing upload session")
	}
	record := session.record
	session.mu.Unlock()

	file, err := s.root.OpenFile(record.TempPath, os.O_RDWR, 0)
	if err != nil {
		return nil, s.failUpload(session, codes.DataLoss, "open upload temporary file for verification")
	}
	hash := sha256.New()
	size, copyErr := io.Copy(hash, file)
	if copyErr != nil {
		_ = file.Close()
		return nil, s.failUpload(session, codes.Internal, "read upload temporary file for verification")
	}
	if size != record.Size || !bytes.Equal(hash.Sum(nil), record.SHA256) {
		_ = file.Close()
		return nil, s.failUpload(session, codes.DataLoss, "upload size or sha256 mismatch")
	}
	if err := file.Chmod(os.FileMode(record.Mode)); err != nil {
		_ = file.Close()
		return nil, s.failUpload(session, codes.Internal, "set resumable upload permissions")
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return nil, s.failUpload(session, codes.Internal, "sync verified resumable upload")
	}
	if err := file.Close(); err != nil {
		return nil, s.failUpload(session, codes.Internal, "close verified resumable upload")
	}
	if err := s.validateUploadTarget(record.TargetPath, record.Overwrite); err != nil {
		return nil, s.failUpload(session, status.Code(err), err.Error())
	}
	if record.Overwrite {
		err = s.root.Rename(record.TempPath, record.TargetPath)
		if err != nil {
			return nil, s.failUpload(session, status.Code(fileError("publish resumable upload", record.TargetPath, err)), "publish resumable upload")
		}
	} else {
		err = s.root.Link(record.TempPath, record.TargetPath)
		if err != nil {
			return nil, s.failUpload(session, status.Code(fileError("publish resumable upload", record.TargetPath, err)), "publish resumable upload")
		}
		if err := s.root.Remove(record.TempPath); err != nil {
			// Linking publishes the target before the staging entry is removed.
			// Keep the durable state FINALIZING so a later session query can
			// verify the target and finish recovery instead of reporting a
			// successfully published file as a failed upload.
			return nil, fileError("remove published upload staging file", record.TargetPath, err)
		}
	}
	if err := s.syncWorkspaceDirectory(path.Dir(record.TargetPath)); err != nil {
		return nil, fileError("sync resumable upload directory", record.TargetPath, err)
	}
	info, err := s.lstat(record.TargetPath)
	if err != nil {
		return nil, fileError("stat resumable upload", record.TargetPath, err)
	}
	result := &codev1.UploadResponse{File: info, Size: record.Size, Sha256: append([]byte(nil), record.SHA256...)}
	session.mu.Lock()
	now := time.Now().UTC()
	session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE
	session.record.UpdatedAt = now
	session.record.ExpiresAt = now.Add(s.transfers.config.CompletedSessionTTL)
	session.record.Result = &uploadResultRecord{
		Path: info.GetPath(), Name: info.GetName(), Size: info.GetSize(), Mode: info.GetMode(),
		ModifiedAt: info.GetModifiedAt().AsTime(), SHA256: append([]byte(nil), record.SHA256...),
	}
	persistErr := s.transfers.persistRecord(session.record)
	session.mu.Unlock()
	s.transfers.releasePaths(record)
	if persistErr != nil {
		return nil, status.Error(codes.Internal, "persist completed upload session")
	}
	return result, nil
}

// reconcileFinalizingUploadLocked resolves a finalization interrupted after its
// state transition. The caller holds session.mu. The returned flag indicates
// that the target/temp reservation must be released after unlocking.
func (s *transferStore) reconcileFinalizingUploadLocked(session *uploadSession) (bool, error) {
	if session.record.State != codev1.UploadSessionState_UPLOAD_SESSION_STATE_FINALIZING || session.active {
		return false, nil
	}
	record := session.record
	if info, err := s.root.Lstat(record.TempPath); err == nil && info.Mode().IsRegular() {
		if result, verifyErr := s.verifyPublishedUpload(record); verifyErr == nil {
			if err := s.root.Remove(record.TempPath); err != nil {
				return false, fileError("remove published upload staging file", record.TargetPath, err)
			}
			if err := syncRootDirectory(s.root, path.Dir(record.TempPath)); err != nil {
				return false, fileError("sync published upload directory", record.TargetPath, err)
			}
			now := time.Now().UTC()
			session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE
			session.record.Result = result
			session.record.UpdatedAt = now
			session.record.ExpiresAt = now.Add(s.config.CompletedSessionTTL)
			if err := s.persistRecord(session.record); err != nil {
				session.record = record
				return false, status.Error(codes.Internal, "recover published upload session")
			}
			return true, nil
		}
		session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_OPEN
		session.record.UpdatedAt = time.Now().UTC()
		if err := s.persistRecord(session.record); err != nil {
			session.record = record
			return false, status.Error(codes.Internal, "recover finalizing upload session")
		}
		return false, nil
	} else if err != nil && !errors.Is(err, fs.ErrNotExist) {
		return false, fileError("inspect finalizing upload session", record.TargetPath, err)
	}
	result, verifyErr := s.verifyPublishedUpload(record)
	if verifyErr == nil {
		if err := syncRootDirectory(s.root, path.Dir(record.TargetPath)); err != nil {
			return false, fileError("sync recovered upload directory", record.TargetPath, err)
		}
	}
	now := time.Now().UTC()
	session.record.UpdatedAt = now
	session.record.ExpiresAt = now.Add(s.config.CompletedSessionTTL)
	if verifyErr == nil {
		session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_COMPLETE
		session.record.Result = result
	} else {
		session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED
	}
	if err := s.persistRecord(session.record); err != nil {
		session.record = record
		return false, status.Error(codes.Internal, "persist recovered upload session")
	}
	return true, nil
}

func (s *Service) syncWorkspaceDirectory(rel string) error {
	return syncRootDirectory(s.root, rel)
}

func syncRootDirectory(root *os.Root, rel string) error {
	directory, err := root.OpenFile(rel, os.O_RDONLY, 0)
	if err != nil {
		return err
	}
	defer directory.Close()
	return directory.Sync()
}

func (s *Service) failUpload(session *uploadSession, code codes.Code, message string) error {
	if code == codes.OK || code == codes.Unknown {
		code = codes.Internal
	}
	session.mu.Lock()
	record := session.record
	now := time.Now().UTC()
	session.record.State = codev1.UploadSessionState_UPLOAD_SESSION_STATE_FAILED
	session.record.UpdatedAt = now
	session.record.ExpiresAt = now.Add(s.transfers.config.CompletedSessionTTL)
	_ = s.transfers.persistRecord(session.record)
	session.mu.Unlock()
	s.transfers.removeSessionTemp(record.TempPath)
	s.transfers.releasePaths(record)
	return status.Error(code, message)
}

func (s *Service) validateUploadTarget(rel string, overwrite bool) error {
	existing, err := s.root.Lstat(rel)
	if err == nil {
		if !overwrite {
			return status.Errorf(codes.AlreadyExists, "upload target %q already exists", displayPath(rel))
		}
		if !existing.Mode().IsRegular() {
			return status.Errorf(codes.FailedPrecondition, "upload target %q is not a regular file", displayPath(rel))
		}
		return nil
	}
	if !errors.Is(err, fs.ErrNotExist) {
		return fileError("inspect upload target", rel, err)
	}
	return nil
}

func sameUploadRequest(record uploadRecord, request *codev1.CreateUploadSessionRequest, rel string) bool {
	return record.TargetPath == rel && record.Size == request.GetSize() && bytes.Equal(record.SHA256, request.GetSha256()) &&
		record.Mode == request.GetMode() && record.Overwrite == request.GetOverwrite()
}

func uploadCheckpointResponse(offset int64) *codev1.TransferUploadResponse {
	return &codev1.TransferUploadResponse{Payload: &codev1.TransferUploadResponse_Checkpoint{
		Checkpoint: &codev1.TransferUploadCheckpoint{CommittedOffset: offset},
	}}
}

// transferStatus carries two details: FileTransferError for clients that
// already read the enum, and ErrorInfo so transfers report their reason the same
// way every other service does. FileTransferError stays on the wire and is not
// deprecated by this change.
func transferStatus(code codes.Code, message string, reason codev1.FileTransferErrorReason, expectedOffset int64) error {
	base := status.New(code, message)
	transfer := &codev1.FileTransferError{Reason: reason, ExpectedOffset: expectedOffset}
	mapped := transferReason(reason)
	if mapped == "" {
		withDetails, err := base.WithDetails(transfer)
		if err != nil {
			return base.Err()
		}
		return withDetails.Err()
	}
	var metadata map[string]string
	if reason == codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_OFFSET_MISMATCH {
		metadata = map[string]string{"expected_offset": strconv.FormatInt(expectedOffset, 10)}
	}
	withDetails, err := base.WithDetails(transfer, rpcerror.Info(mapped, metadata))
	if err != nil {
		return base.Err()
	}
	return withDetails.Err()
}

func transferReason(reason codev1.FileTransferErrorReason) rpcerror.Reason {
	switch reason {
	case codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_OFFSET_MISMATCH:
		return rpcerror.TransferOffsetMismatch
	case codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED:
		return rpcerror.TransferFileChanged
	case codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_PREFIX_MISMATCH:
		return rpcerror.TransferPrefixMismatch
	case codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_SESSION_STATE:
		return rpcerror.TransferSessionState
	case codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER:
		return rpcerror.TransferActiveTransfer
	default:
		return ""
	}
}
