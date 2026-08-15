package files

import (
	"context"
	"crypto/sha256"
	"io"
	"os"
	"syscall"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type BinaryRead struct {
	File   *codev1.FileInfo
	Data   []byte
	Size   int64
	SHA256 []byte
}

// ReadBytes reads one bounded regular file for an in-process adapter.
func (s *Service) ReadBytes(ctx context.Context, name string, maxBytes int64) (*BinaryRead, error) {
	if maxBytes <= 0 || maxBytes > s.maxUploadBytes {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be positive and no greater than the file size limit")
	}
	rel, err := cleanPath(name)
	if err != nil {
		return nil, err
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fileError("read resource", rel, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fileError("stat resource", rel, err)
	}
	if !info.Mode().IsRegular() {
		return nil, status.Errorf(codes.FailedPrecondition, "resource target %q is not a regular file", displayPath(rel))
	}
	if info.Size() > maxBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "resource exceeds the %d byte limit", maxBytes)
	}
	data, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: file}, maxBytes+1))
	if err != nil {
		return nil, contextError(err)
	}
	if int64(len(data)) > maxBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "resource exceeds the %d byte limit", maxBytes)
	}
	digest := sha256.Sum256(data)
	return &BinaryRead{File: makeFileInfo(rel, info), Data: data, Size: int64(len(data)), SHA256: digest[:]}, nil
}
