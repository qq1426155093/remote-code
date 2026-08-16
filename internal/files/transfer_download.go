package files

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strconv"
	"syscall"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (s *Service) DownloadRange(request *codev1.DownloadRangeRequest, stream grpc.ServerStreamingServer[codev1.DownloadRangeResponse]) error {
	if s.transfers == nil || s.transfers.disabled {
		return status.Error(codes.Unimplemented, "resumable download is disabled")
	}
	if request.GetOffset() < 0 {
		return status.Error(codes.InvalidArgument, "download offset cannot be negative")
	}
	if request.GetOffset() == 0 {
		if len(request.GetExpectedRevision()) != 0 || len(request.GetPrefixSha256()) != 0 {
			return status.Error(codes.InvalidArgument, "revision and prefix sha256 must be empty at offset zero")
		}
	} else {
		if len(request.GetExpectedRevision()) != sha256.Size {
			return status.Error(codes.InvalidArgument, "download revision must contain 32 bytes when offset is non-zero")
		}
		if len(request.GetPrefixSha256()) != sha256.Size {
			return status.Error(codes.InvalidArgument, "download prefix sha256 must contain 32 bytes when offset is non-zero")
		}
	}
	select {
	case s.transfers.downloadSlots <- struct{}{}:
		defer func() { <-s.transfers.downloadSlots }()
	case <-stream.Context().Done():
		return contextError(stream.Context().Err())
	}

	rel, err := cleanPath(request.GetPath())
	if err != nil {
		return err
	}
	if s.isTransferTemp(rel) {
		return status.Error(codes.NotFound, "file was not found")
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fileError("download range", rel, err)
	}
	defer file.Close()
	initial, err := file.Stat()
	if err != nil {
		return fileError("stat download range", rel, err)
	}
	if !initial.Mode().IsRegular() {
		return status.Errorf(codes.FailedPrecondition, "download target %q is not a regular file", displayPath(rel))
	}
	revision := s.fileRevision(rel, initial)
	if len(request.GetExpectedRevision()) != 0 && !hmac.Equal(revision, request.GetExpectedRevision()) {
		return transferStatus(codes.FailedPrecondition, "download source changed since the previous attempt", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED, 0)
	}
	if request.GetOffset() > initial.Size() {
		return transferStatus(codes.OutOfRange, "download offset exceeds the file size", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_OFFSET_MISMATCH, initial.Size())
	}

	hash := sha256.New()
	remainingPrefix := request.GetOffset()
	buffer := make([]byte, transferChunkSize)
	for remainingPrefix > 0 {
		if err := stream.Context().Err(); err != nil {
			return contextError(err)
		}
		want := int64(len(buffer))
		if remainingPrefix < want {
			want = remainingPrefix
		}
		n, readErr := io.ReadFull(file, buffer[:want])
		if n > 0 {
			_, _ = hash.Write(buffer[:n])
			remainingPrefix -= int64(n)
		}
		if readErr != nil {
			return transferStatus(codes.FailedPrecondition, "download source changed while validating the prefix", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED, 0)
		}
	}
	if request.GetOffset() > 0 && !bytes.Equal(hash.Sum(nil), request.GetPrefixSha256()) {
		return transferStatus(codes.FailedPrecondition, "download prefix sha256 does not match the remote file", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_PREFIX_MISMATCH, 0)
	}
	metadata := &codev1.DownloadRangeMetadata{
		File: makeFileInfo(rel, initial), Revision: append([]byte(nil), revision...), Offset: request.GetOffset(),
	}
	if err := stream.Send(&codev1.DownloadRangeResponse{Payload: &codev1.DownloadRangeResponse_Metadata{Metadata: metadata}}); err != nil {
		return contextError(err)
	}

	offset := request.GetOffset()
	remaining := initial.Size() - offset
	for remaining > 0 {
		if err := stream.Context().Err(); err != nil {
			return contextError(err)
		}
		want := int64(len(buffer))
		if remaining < want {
			want = remaining
		}
		n, readErr := file.Read(buffer[:want])
		if n > 0 {
			data := append([]byte(nil), buffer[:n]...)
			chunkDigest := sha256.Sum256(data)
			_, _ = hash.Write(data)
			if err := stream.Send(&codev1.DownloadRangeResponse{Payload: &codev1.DownloadRangeResponse_Chunk{
				Chunk: &codev1.DownloadRangeChunk{Offset: offset, Data: data, Sha256: chunkDigest[:]},
			}}); err != nil {
				return contextError(err)
			}
			offset += int64(n)
			remaining -= int64(n)
		}
		if readErr != nil && !(errors.Is(readErr, io.EOF) && remaining == 0) {
			return transferStatus(codes.FailedPrecondition, "download source changed while reading", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED, 0)
		}
		if n == 0 {
			return status.Error(codes.DataLoss, "download source returned no data before the declared size")
		}
	}
	final, err := file.Stat()
	if err != nil {
		return fileError("restat download range", rel, err)
	}
	finalRevision := s.fileRevision(rel, final)
	if !hmac.Equal(revision, finalRevision) {
		return transferStatus(codes.Aborted, "download source changed during transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_FILE_CHANGED, 0)
	}
	if err := stream.Send(&codev1.DownloadRangeResponse{Payload: &codev1.DownloadRangeResponse_Summary{
		Summary: &codev1.DownloadRangeSummary{Size: offset, Sha256: hash.Sum(nil), Revision: finalRevision},
	}}); err != nil {
		return contextError(err)
	}
	return nil
}

func (s *Service) fileRevision(rel string, info os.FileInfo) []byte {
	mac := hmac.New(sha256.New, s.transfers.revisionKey)
	_, _ = io.WriteString(mac, "remote-code-file-revision-v1\x00")
	_, _ = io.WriteString(mac, s.transfers.workspaceID)
	_, _ = io.WriteString(mac, "\x00"+rel+"\x00")
	_, _ = io.WriteString(mac, strconv.FormatInt(info.Size(), 10))
	_, _ = io.WriteString(mac, "\x00"+strconv.FormatUint(uint64(info.Mode()), 10))
	_, _ = io.WriteString(mac, "\x00"+strconv.FormatInt(info.ModTime().UnixNano(), 10))
	_, _ = io.WriteString(mac, "\x00"+stablePlatformFileIdentity(info.Sys()))
	return mac.Sum(nil)
}

func stablePlatformFileIdentity(value any) string {
	reflected := reflect.ValueOf(value)
	if !reflected.IsValid() {
		return ""
	}
	if reflected.Kind() == reflect.Pointer {
		if reflected.IsNil() {
			return ""
		}
		reflected = reflected.Elem()
	}
	if reflected.Kind() != reflect.Struct {
		return fmt.Sprintf("%T", value)
	}
	parts := make([]string, 0, 4)
	for _, name := range []string{"Dev", "Ino"} {
		if field := reflected.FieldByName(name); field.IsValid() {
			parts = append(parts, name+"="+reflectNumber(field))
		}
	}
	for _, name := range []string{"Ctim", "Ctimespec"} {
		field := reflected.FieldByName(name)
		if !field.IsValid() || field.Kind() != reflect.Struct {
			continue
		}
		parts = append(parts, name+"="+reflectNumber(field.FieldByName("Sec"))+":"+reflectNumber(field.FieldByName("Nsec")))
		break
	}
	return fmt.Sprintf("%T:%v", value, parts)
}

func reflectNumber(value reflect.Value) string {
	if !value.IsValid() {
		return ""
	}
	switch value.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return strconv.FormatInt(value.Int(), 10)
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return strconv.FormatUint(value.Uint(), 10)
	default:
		return ""
	}
}
