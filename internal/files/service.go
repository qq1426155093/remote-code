package files

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	defaultMaxUploadBytes = int64(1 << 30)
	transferChunkSize     = 64 << 10
	maxReceivedChunkSize  = 1 << 20
	maxPathBytes          = 4096
	maxTreeEntries        = 3_000
	maxTreeDepth          = 128
)

// Service implements the versioned gRPC file API inside one workspace root.
// It is safe for concurrent use.
type Service struct {
	codev1.UnimplementedFileServiceServer

	root           *os.Root
	workspaceName  string
	maxUploadBytes int64
	transfers      *transferStore
}

// Config controls a file service instance.
type Config struct {
	Workspace        string
	RuntimeDirectory string
	MaxUploadBytes   int64
	Transfers        TransferConfig
}

// New opens and pins the configured workspace directory.
func New(config Config) (*Service, error) {
	if config.Workspace == "" {
		return nil, errors.New("workspace is required")
	}
	abs, err := filepath.Abs(config.Workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace: %w", err)
	}
	info, err := os.Stat(abs)
	if err != nil {
		return nil, fmt.Errorf("stat workspace: %w", err)
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("workspace %q is not a directory", abs)
	}
	root, err := os.OpenRoot(abs)
	if err != nil {
		return nil, fmt.Errorf("open workspace: %w", err)
	}
	maxUploadBytes := config.MaxUploadBytes
	if maxUploadBytes == 0 {
		maxUploadBytes = defaultMaxUploadBytes
	}
	if maxUploadBytes < 0 {
		_ = root.Close()
		return nil, errors.New("max upload bytes must be positive")
	}
	transfers, err := newTransferStore(root, abs, config.RuntimeDirectory, maxUploadBytes, config.Transfers)
	if err != nil {
		_ = root.Close()
		return nil, fmt.Errorf("open file transfer store: %w", err)
	}
	return &Service{
		root:           root,
		workspaceName:  filepath.Base(abs),
		maxUploadBytes: maxUploadBytes,
		transfers:      transfers,
	}, nil
}

// Close releases the pinned workspace directory handle.
func (s *Service) Close() error {
	if s.transfers != nil {
		s.transfers.close()
	}
	return s.root.Close()
}

// WorkspaceName returns a display-only name and never the absolute path.
func (s *Service) WorkspaceName() string {
	return s.workspaceName
}

// MaxUploadBytes returns the enforced per-file upload limit.
func (s *Service) MaxUploadBytes() int64 {
	return s.maxUploadBytes
}

// FileTransferCapabilities returns the negotiated resumable-transfer limits.
func (s *Service) FileTransferCapabilities() *codev1.FileTransferCapabilities {
	if s.transfers == nil || s.transfers.disabled {
		return &codev1.FileTransferCapabilities{}
	}
	return &codev1.FileTransferCapabilities{
		ResumableUpload:         true,
		ResumableDownload:       true,
		PreferredChunkBytes:     transferChunkSize,
		MaxChunkBytes:           maxReceivedChunkSize,
		UploadSessionTtlSeconds: int64(s.transfers.config.UploadSessionTTL / time.Second),
	}
}

func (s *Service) Stat(_ context.Context, request *codev1.StatRequest) (*codev1.StatResponse, error) {
	rel, err := cleanPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.isTransferTemp(rel) {
		return nil, status.Error(codes.NotFound, "file was not found")
	}
	info, err := s.lstat(rel)
	if err != nil {
		return nil, fileError("stat", rel, err)
	}
	return &codev1.StatResponse{File: info}, nil
}

func (s *Service) List(_ context.Context, request *codev1.ListRequest) (*codev1.ListResponse, error) {
	rel, err := cleanPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.isTransferTemp(rel) {
		return nil, status.Error(codes.NotFound, "file was not found")
	}
	info, err := s.root.Lstat(rel)
	if err != nil {
		return nil, fileError("list", rel, err)
	}
	if !info.IsDir() {
		file, err := s.fileInfo(rel, info)
		if err != nil {
			return nil, fileError("list", rel, err)
		}
		return &codev1.ListResponse{Files: []*codev1.FileInfo{file}}, nil
	}

	directory, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, fileError("list", rel, err)
	}
	defer directory.Close()
	names, err := directory.Readdirnames(-1)
	if err != nil {
		return nil, fileError("list", rel, err)
	}
	sort.Strings(names)
	files := make([]*codev1.FileInfo, 0, len(names))
	for _, name := range names {
		child := path.Join(rel, name)
		if s.isTransferTemp(child) {
			continue
		}
		info, err := s.lstat(child)
		if err != nil {
			return nil, fileError("list", child, err)
		}
		files = append(files, info)
	}
	return &codev1.ListResponse{Files: files}, nil
}

// Tree returns a recursively nested view of a file or directory. Symbolic
// links are deliberately not followed and therefore always remain leaf nodes.
func (s *Service) Tree(ctx context.Context, request *codev1.TreeRequest) (*codev1.TreeResponse, error) {
	rel, err := cleanPath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.isTransferTemp(rel) {
		return nil, status.Error(codes.NotFound, "file was not found")
	}
	entryCount := 0
	root, err := s.buildTree(ctx, rel, 0, &entryCount)
	if err != nil {
		return nil, err
	}
	return &codev1.TreeResponse{Root: root}, nil
}

func (s *Service) buildTree(ctx context.Context, rel string, depth int, entryCount *int) (*codev1.TreeNode, error) {
	if err := ctx.Err(); err != nil {
		return nil, contextError(err)
	}
	*entryCount = *entryCount + 1
	if *entryCount > maxTreeEntries {
		return nil, status.Errorf(codes.ResourceExhausted, "tree contains more than %d entries", maxTreeEntries)
	}
	info, err := s.lstat(rel)
	if err != nil {
		return nil, fileError("tree", rel, err)
	}
	node := &codev1.TreeNode{File: info}
	if info.GetType() != codev1.FileType_FILE_TYPE_DIRECTORY {
		return node, nil
	}
	if depth >= maxTreeDepth {
		return nil, status.Errorf(codes.ResourceExhausted, "tree exceeds the maximum depth of %d", maxTreeDepth)
	}

	directory, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_DIRECTORY, 0)
	if err != nil {
		return nil, fileError("tree", rel, err)
	}
	remainingEntries := maxTreeEntries - *entryCount
	names, readErr := directory.Readdirnames(remainingEntries + 1)
	closeErr := directory.Close()
	if readErr != nil && !errors.Is(readErr, io.EOF) {
		return nil, fileError("tree", rel, readErr)
	}
	if closeErr != nil {
		return nil, fileError("tree", rel, closeErr)
	}
	if len(names) > remainingEntries {
		return nil, status.Errorf(codes.ResourceExhausted, "tree contains more than %d entries", maxTreeEntries)
	}
	sort.Strings(names)
	node.Children = make([]*codev1.TreeNode, 0, len(names))
	for _, name := range names {
		childPath := path.Join(rel, name)
		if s.isTransferTemp(childPath) {
			continue
		}
		child, err := s.buildTree(ctx, childPath, depth+1, entryCount)
		if err != nil {
			return nil, err
		}
		node.Children = append(node.Children, child)
	}
	return node, nil
}

func (s *Service) Upload(stream grpc.ClientStreamingServer[codev1.UploadRequest, codev1.UploadResponse]) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "upload metadata is required")
	}
	if err != nil {
		return contextError(err)
	}
	metadata := first.GetMetadata()
	if metadata == nil {
		return status.Error(codes.InvalidArgument, "the first upload frame must contain metadata")
	}
	rel, err := cleanMutablePath(metadata.GetPath())
	if err != nil {
		return err
	}
	if s.transferMutationConflicts(rel) {
		return transferStatus(codes.FailedPrecondition, "upload target has an active transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if metadata.GetSize() < 0 {
		return status.Error(codes.InvalidArgument, "upload size cannot be negative")
	}
	if metadata.GetSize() > s.maxUploadBytes {
		return status.Errorf(codes.ResourceExhausted, "upload exceeds the %d byte limit", s.maxUploadBytes)
	}
	if len(metadata.GetSha256()) != sha256.Size {
		return status.Error(codes.InvalidArgument, "upload sha256 must contain 32 bytes")
	}
	if err := validateMode(metadata.GetMode()); err != nil {
		return err
	}
	if existing, err := s.root.Lstat(rel); err == nil {
		if !metadata.GetOverwrite() {
			return status.Errorf(codes.AlreadyExists, "upload target %q already exists", displayPath(rel))
		}
		if !existing.Mode().IsRegular() {
			return status.Errorf(codes.FailedPrecondition, "upload target %q is not a regular file", displayPath(rel))
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return fileError("inspect upload target", rel, err)
	}

	tempPath, file, err := s.createUploadTemp(path.Dir(rel))
	if err != nil {
		return fileError("create upload temporary file", rel, err)
	}
	published := false
	defer func() {
		_ = file.Close()
		if !published {
			_ = s.root.Remove(tempPath)
		}
	}()

	hash := sha256.New()
	written := int64(0)
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return contextError(recvErr)
		}
		chunk, ok := frame.GetPayload().(*codev1.UploadRequest_Chunk)
		if !ok {
			return status.Error(codes.InvalidArgument, "upload metadata may only appear in the first frame")
		}
		if len(chunk.Chunk) > maxReceivedChunkSize {
			return status.Errorf(codes.ResourceExhausted, "upload chunk exceeds the %d byte limit", maxReceivedChunkSize)
		}
		written += int64(len(chunk.Chunk))
		if written > s.maxUploadBytes {
			return status.Errorf(codes.ResourceExhausted, "upload exceeds the %d byte limit", s.maxUploadBytes)
		}
		if written > metadata.GetSize() {
			return status.Error(codes.DataLoss, "upload contains more bytes than declared")
		}
		if err := writeFull(file, chunk.Chunk); err != nil {
			return fileError("write upload", rel, err)
		}
		_, _ = hash.Write(chunk.Chunk)
	}

	if written != metadata.GetSize() {
		return status.Errorf(codes.DataLoss, "upload size mismatch: declared %d bytes, received %d", metadata.GetSize(), written)
	}
	digest := hash.Sum(nil)
	if !equalBytes(digest, metadata.GetSha256()) {
		return status.Error(codes.DataLoss, "upload sha256 mismatch")
	}
	if err := file.Sync(); err != nil {
		return fileError("sync upload", rel, err)
	}
	if err := file.Chmod(os.FileMode(metadata.GetMode())); err != nil {
		return fileError("set upload permissions", rel, err)
	}
	if err := file.Close(); err != nil {
		return fileError("close upload", rel, err)
	}

	if metadata.GetOverwrite() {
		if err := s.root.Rename(tempPath, rel); err != nil {
			return fileError("publish upload", rel, err)
		}
	} else {
		if err := s.root.Link(tempPath, rel); err != nil {
			return fileError("publish upload", rel, err)
		}
		if err := s.root.Remove(tempPath); err != nil {
			return fileError("remove upload temporary link", rel, err)
		}
	}
	published = true

	info, err := s.lstat(rel)
	if err != nil {
		return fileError("stat uploaded file", rel, err)
	}
	return stream.SendAndClose(&codev1.UploadResponse{File: info, Size: written, Sha256: digest})
}

func (s *Service) Download(request *codev1.DownloadRequest, stream grpc.ServerStreamingServer[codev1.DownloadResponse]) error {
	rel, err := cleanPath(request.GetPath())
	if err != nil {
		return err
	}
	if s.isTransferTemp(rel) {
		return status.Error(codes.NotFound, "file was not found")
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return fileError("download", rel, err)
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return fileError("stat download", rel, err)
	}
	if !stat.Mode().IsRegular() {
		return status.Errorf(codes.FailedPrecondition, "download target %q is not a regular file", displayPath(rel))
	}
	info := makeFileInfo(rel, stat)
	if err := stream.Send(&codev1.DownloadResponse{Payload: &codev1.DownloadResponse_Metadata{
		Metadata: &codev1.DownloadMetadata{File: info},
	}}); err != nil {
		return contextError(err)
	}

	hash := sha256.New()
	total := int64(0)
	buffer := make([]byte, transferChunkSize)
	for {
		n, readErr := file.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			total += int64(n)
			_, _ = hash.Write(chunk)
			if err := stream.Send(&codev1.DownloadResponse{Payload: &codev1.DownloadResponse_Chunk{Chunk: chunk}}); err != nil {
				return contextError(err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return fileError("read download", rel, readErr)
		}
	}
	return stream.Send(&codev1.DownloadResponse{Payload: &codev1.DownloadResponse_Summary{
		Summary: &codev1.DownloadSummary{Size: total, Sha256: hash.Sum(nil)},
	}})
}

func (s *Service) Remove(_ context.Context, request *codev1.RemoveRequest) (*codev1.RemoveResponse, error) {
	rel, err := cleanMutablePath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.transferMutationConflicts(rel) {
		return nil, transferStatus(codes.FailedPrecondition, "path has an active file transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if _, err := s.root.Lstat(rel); err != nil {
		return nil, fileError("remove", rel, err)
	}
	if request.GetRecursive() {
		err = s.root.RemoveAll(rel)
	} else {
		err = s.root.Remove(rel)
	}
	if err != nil {
		return nil, fileError("remove", rel, err)
	}
	return &codev1.RemoveResponse{Path: displayPath(rel)}, nil
}

func (s *Service) Move(_ context.Context, request *codev1.MoveRequest) (*codev1.MoveResponse, error) {
	source, err := cleanMutablePath(request.GetSource())
	if err != nil {
		return nil, err
	}
	destination, err := cleanMutablePath(request.GetDestination())
	if err != nil {
		return nil, err
	}
	if s.transferMutationConflicts(source) || s.transferMutationConflicts(destination) {
		return nil, transferStatus(codes.FailedPrecondition, "path has an active file transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if source == destination {
		return nil, status.Error(codes.InvalidArgument, "move source and destination must differ")
	}
	if _, err := s.root.Lstat(source); err != nil {
		return nil, fileError("move", source, err)
	}
	if destinationInfo, err := s.root.Lstat(destination); err == nil {
		if !request.GetOverwrite() {
			return nil, status.Errorf(codes.AlreadyExists, "move target %q already exists", displayPath(destination))
		}
		if destinationInfo.IsDir() {
			return nil, status.Errorf(codes.FailedPrecondition, "move target %q is a directory", displayPath(destination))
		}
	} else if !errors.Is(err, fs.ErrNotExist) {
		return nil, fileError("inspect move target", destination, err)
	}
	if err := s.root.Rename(source, destination); err != nil {
		return nil, fileError("move", source, err)
	}
	info, err := s.lstat(destination)
	if err != nil {
		return nil, fileError("stat moved file", destination, err)
	}
	return &codev1.MoveResponse{File: info}, nil
}

func (s *Service) Chmod(_ context.Context, request *codev1.ChmodRequest) (*codev1.ChmodResponse, error) {
	rel, err := cleanMutablePath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.transferMutationConflicts(rel) {
		return nil, transferStatus(codes.FailedPrecondition, "path has an active file transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if err := validateMode(request.GetMode()); err != nil {
		return nil, err
	}
	lstat, err := s.root.Lstat(rel)
	if err != nil {
		return nil, fileError("chmod", rel, err)
	}
	if lstat.Mode()&os.ModeSymlink != 0 {
		return nil, status.Errorf(codes.FailedPrecondition, "cannot chmod symbolic link %q", displayPath(rel))
	}
	file, err := s.root.OpenFile(rel, os.O_RDONLY|syscall.O_NONBLOCK, 0)
	if err != nil {
		return nil, fileError("chmod", rel, err)
	}
	defer file.Close()
	if err := file.Chmod(os.FileMode(request.GetMode())); err != nil {
		return nil, fileError("chmod", rel, err)
	}
	info, err := s.lstat(rel)
	if err != nil {
		return nil, fileError("stat chmod target", rel, err)
	}
	return &codev1.ChmodResponse{File: info}, nil
}

func (s *Service) Mkdir(_ context.Context, request *codev1.MkdirRequest) (*codev1.MkdirResponse, error) {
	rel, err := cleanMutablePath(request.GetPath())
	if err != nil {
		return nil, err
	}
	if s.transferMutationConflicts(rel) {
		return nil, transferStatus(codes.FailedPrecondition, "path has an active file transfer", codev1.FileTransferErrorReason_FILE_TRANSFER_ERROR_REASON_ACTIVE_TRANSFER, 0)
	}
	if err := validateMode(request.GetMode()); err != nil {
		return nil, err
	}
	mode := os.FileMode(request.GetMode())
	if request.GetParents() {
		err = s.root.MkdirAll(rel, mode)
	} else {
		err = s.root.Mkdir(rel, mode)
	}
	if err != nil {
		return nil, fileError("mkdir", rel, err)
	}
	info, err := s.lstat(rel)
	if err != nil {
		return nil, fileError("stat directory", rel, err)
	}
	return &codev1.MkdirResponse{File: info}, nil
}

func (s *Service) lstat(rel string) (*codev1.FileInfo, error) {
	info, err := s.root.Lstat(rel)
	if err != nil {
		return nil, err
	}
	return s.fileInfo(rel, info)
}

func (s *Service) isTransferTemp(rel string) bool {
	return s.transfers != nil && !s.transfers.disabled && s.transfers.isTemp(rel)
}

func (s *Service) transferMutationConflicts(rel string) bool {
	return s.transfers != nil && !s.transfers.disabled && s.transfers.mutationConflicts(rel)
}

func (s *Service) fileInfo(rel string, info os.FileInfo) (*codev1.FileInfo, error) {
	result := makeFileInfo(rel, info)
	if info.Mode()&os.ModeSymlink == 0 {
		return result, nil
	}
	target, err := s.root.Readlink(rel)
	if err != nil {
		return nil, err
	}
	if !path.IsAbs(target) {
		resolved := path.Clean(path.Join(path.Dir(rel), target))
		if resolved != ".." && !strings.HasPrefix(resolved, "../") {
			result.SymlinkTarget = target
		}
	}
	return result, nil
}

func makeFileInfo(rel string, info os.FileInfo) *codev1.FileInfo {
	fileType := codev1.FileType_FILE_TYPE_OTHER
	switch {
	case info.Mode()&os.ModeSymlink != 0:
		fileType = codev1.FileType_FILE_TYPE_SYMLINK
	case info.IsDir():
		fileType = codev1.FileType_FILE_TYPE_DIRECTORY
	case info.Mode().IsRegular():
		fileType = codev1.FileType_FILE_TYPE_REGULAR
	}
	return &codev1.FileInfo{
		Path:       displayPath(rel),
		Name:       displayName(rel),
		Type:       fileType,
		Size:       info.Size(),
		Mode:       uint32(info.Mode().Perm()),
		ModifiedAt: timestamppb.New(info.ModTime()),
	}
}

func cleanPath(raw string) (string, error) {
	if len(raw) > maxPathBytes {
		return "", status.Errorf(codes.InvalidArgument, "path exceeds %d bytes", maxPathBytes)
	}
	if strings.IndexByte(raw, 0) >= 0 {
		return "", status.Error(codes.InvalidArgument, "path contains a NUL byte")
	}
	if raw == "" {
		return ".", nil
	}
	if path.IsAbs(raw) {
		return "", status.Error(codes.InvalidArgument, "absolute paths are not allowed")
	}
	for _, component := range strings.Split(raw, "/") {
		if component == ".." {
			return "", status.Error(codes.InvalidArgument, "parent path components are not allowed")
		}
	}
	clean := path.Clean(raw)
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", status.Error(codes.InvalidArgument, "path escapes the workspace")
	}
	return clean, nil
}

func cleanMutablePath(raw string) (string, error) {
	if raw == "" {
		return "", status.Error(codes.InvalidArgument, "path is required")
	}
	rel, err := cleanPath(raw)
	if err != nil {
		return "", err
	}
	if rel == "." {
		return "", status.Error(codes.InvalidArgument, "the workspace root cannot be modified")
	}
	return rel, nil
}

func validateMode(mode uint32) error {
	if mode&^uint32(0o777) != 0 {
		return status.Error(codes.InvalidArgument, "mode must be between 0000 and 0777")
	}
	return nil
}

func (s *Service) createUploadTemp(directory string) (string, *os.File, error) {
	for range 16 {
		random := make([]byte, 12)
		if _, err := rand.Read(random); err != nil {
			return "", nil, err
		}
		name := ".remote-code-upload-" + hex.EncodeToString(random)
		tempPath := path.Join(directory, name)
		file, err := s.root.OpenFile(tempPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err == nil {
			return tempPath, file, nil
		}
		if !errors.Is(err, fs.ErrExist) {
			return "", nil, err
		}
	}
	return "", nil, errors.New("could not allocate a unique temporary file")
}

func writeFull(writer io.Writer, data []byte) error {
	for len(data) > 0 {
		n, err := writer.Write(data)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		data = data[n:]
	}
	return nil
}

func equalBytes(left, right []byte) bool {
	if len(left) != len(right) {
		return false
	}
	var difference byte
	for index := range left {
		difference |= left[index] ^ right[index]
	}
	return difference == 0
}

func displayPath(rel string) string {
	if rel == "." {
		return "/"
	}
	return "/" + rel
}

func displayName(rel string) string {
	if rel == "." {
		return "/"
	}
	return path.Base(rel)
}

func fileError(operation, rel string, err error) error {
	if err == nil {
		return nil
	}
	if status.Code(err) != codes.Unknown {
		return err
	}
	code := codes.Internal
	switch {
	case errors.Is(err, context.Canceled):
		code = codes.Canceled
	case errors.Is(err, context.DeadlineExceeded):
		code = codes.DeadlineExceeded
	case errors.Is(err, fs.ErrNotExist):
		code = codes.NotFound
	case errors.Is(err, syscall.ENOTEMPTY), errors.Is(err, syscall.EISDIR), errors.Is(err, syscall.ENOTDIR):
		code = codes.FailedPrecondition
	case errors.Is(err, fs.ErrExist):
		code = codes.AlreadyExists
	case errors.Is(err, fs.ErrPermission), strings.Contains(err.Error(), "path escapes from parent"):
		code = codes.PermissionDenied
	case errors.Is(err, syscall.EXDEV):
		code = codes.FailedPrecondition
	}
	return status.Errorf(code, "%s %q failed", operation, displayPath(rel))
}

func contextError(err error) error {
	if errors.Is(err, context.Canceled) {
		return status.Error(codes.Canceled, "request canceled")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return status.Error(codes.DeadlineExceeded, "request deadline exceeded")
	}
	return err
}
