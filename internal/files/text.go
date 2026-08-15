package files

import (
	"context"
	"crypto/sha256"
	"errors"
	"io"
	"io/fs"
	"os"
	"path"
	"syscall"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// TextRead is the bounded result of reading a UTF-8 workspace file.
type TextRead struct {
	File   *codev1.FileInfo
	Text   string
	Size   int64
	SHA256 []byte
}

// ReadText reads a regular UTF-8 file through the pinned workspace root.
func (s *Service) ReadText(ctx context.Context, name string, maxBytes int64) (*TextRead, error) {
	if maxBytes <= 0 || maxBytes > s.maxUploadBytes {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be positive and no greater than the file size limit")
	}
	rel, err := cleanPath(name)
	if err != nil {
		return nil, err
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fileError("read text", rel, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fileError("stat text file", rel, err)
	}
	if !info.Mode().IsRegular() {
		return nil, status.Errorf(codes.FailedPrecondition, "read target %q is not a regular file", displayPath(rel))
	}
	if info.Size() > maxBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "read target exceeds the %d byte limit", maxBytes)
	}
	contents, err := io.ReadAll(io.LimitReader(&contextReader{ctx: ctx, reader: file}, maxBytes+1))
	if err != nil {
		return nil, contextError(err)
	}
	if int64(len(contents)) > maxBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "read target exceeds the %d byte limit", maxBytes)
	}
	if !utf8.Valid(contents) {
		return nil, status.Error(codes.FailedPrecondition, "read target is not valid UTF-8")
	}
	digest := sha256.Sum256(contents)
	return &TextRead{File: makeFileInfo(rel, info), Text: string(contents), Size: int64(len(contents)), SHA256: digest[:]}, nil
}

// WriteText atomically writes UTF-8 text through the same publication rules
// as the streaming upload API.
func (s *Service) WriteText(ctx context.Context, name, text string, overwrite bool, mode uint32) (*codev1.UploadResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, contextError(err)
	}
	if !utf8.ValidString(text) {
		return nil, status.Error(codes.InvalidArgument, "content must be valid UTF-8")
	}
	if int64(len(text)) > s.maxUploadBytes {
		return nil, status.Errorf(codes.ResourceExhausted, "write exceeds the %d byte limit", s.maxUploadBytes)
	}
	rel, err := cleanMutablePath(name)
	if err != nil {
		return nil, err
	}
	if err := validateMode(mode); err != nil {
		return nil, err
	}
	if existing, err := s.root.Lstat(rel); err == nil {
		if !overwrite {
			return nil, status.Errorf(codes.AlreadyExists, "write target %q already exists", displayPath(rel))
		}
		if !existing.Mode().IsRegular() {
			return nil, status.Errorf(codes.FailedPrecondition, "write target %q is not a regular file", displayPath(rel))
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fileError("inspect write target", rel, err)
	}
	tempPath, file, err := s.createUploadTemp(path.Dir(rel))
	if err != nil {
		return nil, fileError("create write temporary file", rel, err)
	}
	published := false
	defer func() {
		_ = file.Close()
		if !published {
			_ = s.root.Remove(tempPath)
		}
	}()
	contents := []byte(text)
	for offset := 0; offset < len(contents); {
		if err := ctx.Err(); err != nil {
			return nil, contextError(err)
		}
		end := offset + transferChunkSize
		if end > len(contents) {
			end = len(contents)
		}
		if err := writeFull(file, contents[offset:end]); err != nil {
			return nil, fileError("write text", rel, err)
		}
		offset = end
	}
	if err := file.Sync(); err != nil {
		return nil, fileError("sync text", rel, err)
	}
	if err := file.Chmod(os.FileMode(mode)); err != nil {
		return nil, fileError("set text permissions", rel, err)
	}
	if err := file.Close(); err != nil {
		return nil, fileError("close text", rel, err)
	}
	if overwrite {
		err = s.root.Rename(tempPath, rel)
	} else {
		err = s.root.Link(tempPath, rel)
		if err == nil {
			err = s.root.Remove(tempPath)
		}
	}
	if err != nil {
		return nil, fileError("publish text", rel, err)
	}
	published = true
	info, err := s.lstat(rel)
	if err != nil {
		return nil, fileError("stat written file", rel, err)
	}
	digest := sha256.Sum256(contents)
	return &codev1.UploadResponse{File: info, Size: int64(len(text)), Sha256: digest[:]}, nil
}

type contextReader struct {
	ctx    context.Context
	reader io.Reader
}

func (r *contextReader) Read(buffer []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	return r.reader.Read(buffer)
}
