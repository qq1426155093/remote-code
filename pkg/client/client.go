package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const (
	transferChunkSize = 64 << 10
	maxMessageBytes   = 16 << 20
)

// Config controls transport security and authentication.
type Config struct {
	Address       string
	TLSCAFile     string
	TLSServerName string
	Token         string
}

// Client is a reusable typed remote-code connection.
type Client struct {
	connection *grpc.ClientConn
	controller codev1.ControllerServiceClient
	files      codev1.FileServiceClient
	processes  codev1.ProcessServiceClient
	info       *codev1.GetInfoResponse
}

// DownloadResult describes a verified download.
type DownloadResult struct {
	File   *codev1.FileInfo
	Size   int64
	SHA256 []byte
}

// ProcessStartOptions describes one generic remote command invocation.
type ProcessStartOptions struct {
	Name             string
	Command          string
	Arguments        []string
	WorkingDirectory string
	IOMode           codev1.ProcessIOMode
	Environment      map[string]string
}

// New creates a connection and verifies it with GetInfo before returning.
func New(ctx context.Context, config Config) (*Client, error) {
	if config.Address == "" {
		config.Address = "127.0.0.1:9443"
	}
	transportCredentials, err := loadTransportCredentials(config)
	if err != nil {
		return nil, err
	}
	options := []grpc.DialOption{
		grpc.WithTransportCredentials(transportCredentials),
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(maxMessageBytes),
			grpc.MaxCallSendMsgSize(maxMessageBytes),
		),
	}
	if config.Token != "" {
		options = append(options, grpc.WithPerRPCCredentials(bearerCredentials{token: config.Token}))
	}
	connection, err := grpc.NewClient(config.Address, options...)
	if err != nil {
		return nil, fmt.Errorf("create gRPC client: %w", err)
	}
	result := &Client{
		connection: connection,
		controller: codev1.NewControllerServiceClient(connection),
		files:      codev1.NewFileServiceClient(connection),
		processes:  codev1.NewProcessServiceClient(connection),
	}
	info, err := result.controller.GetInfo(ctx, &codev1.GetInfoRequest{})
	if err != nil {
		_ = connection.Close()
		return nil, fmt.Errorf("connect to controller: %w", err)
	}
	result.info = info
	return result, nil
}

// Close closes the underlying HTTP/2 connection.
func (c *Client) Close() error {
	return c.connection.Close()
}

// Info returns the immutable connection-time controller information.
func (c *Client) Info() *codev1.GetInfoResponse {
	return c.info
}

func (c *Client) Stat(ctx context.Context, remotePath string) (*codev1.FileInfo, error) {
	response, err := c.files.Stat(ctx, &codev1.StatRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	return response.GetFile(), nil
}

func (c *Client) List(ctx context.Context, remotePath string) ([]*codev1.FileInfo, error) {
	response, err := c.files.List(ctx, &codev1.ListRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	return response.GetFiles(), nil
}

// Tree returns the controller's structured hierarchy rooted at remotePath.
func (c *Client) Tree(ctx context.Context, remotePath string) (*codev1.TreeNode, error) {
	response, err := c.files.Tree(ctx, &codev1.TreeRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	if response.GetRoot() == nil || response.GetRoot().GetFile() == nil {
		return nil, status.Error(codes.DataLoss, "tree response has no root")
	}
	return response.GetRoot(), nil
}

// StartProcess launches a concrete command. It is kept as a convenience
// wrapper for callers that do not need environment overrides.
func (c *Client) StartProcess(ctx context.Context, name, command string, arguments []string, workingDirectory string, ioMode codev1.ProcessIOMode) (*codev1.ProcessInfo, error) {
	return c.StartProcessWithOptions(ctx, ProcessStartOptions{
		Name: name, Command: command, Arguments: arguments, WorkingDirectory: workingDirectory, IOMode: ioMode,
	})
}

// StartProcessWithOptions launches a concrete command directly, without shell
// interpretation, and applies the supplied environment overrides.
func (c *Client) StartProcessWithOptions(ctx context.Context, options ProcessStartOptions) (*codev1.ProcessInfo, error) {
	response, err := c.processes.StartProcess(ctx, &codev1.StartProcessRequest{
		Name: options.Name, Command: options.Command, Arguments: options.Arguments,
		WorkingDirectory: options.WorkingDirectory, IoMode: options.IOMode, Environment: options.Environment,
	})
	if err != nil {
		return nil, err
	}
	if response.GetProcess() == nil {
		return nil, status.Error(codes.DataLoss, "start process response has no process")
	}
	return response.GetProcess(), nil
}

// ListProcesses returns active processes by default. Passing true includes
// persistent exited, failed, and lost history.
func (c *Client) ListProcesses(ctx context.Context, all ...bool) ([]*codev1.ProcessInfo, error) {
	includeAll := len(all) > 0 && all[0]
	response, err := c.processes.ListProcesses(ctx, &codev1.ListProcessesRequest{All: includeAll})
	if err != nil {
		return nil, err
	}
	return response.GetProcesses(), nil
}

// SignalProcess sends a signal to the selected managed process group.
func (c *Client) SignalProcess(ctx context.Context, process *codev1.ProcessReference, signal codev1.ProcessSignal, wait bool) (*codev1.ProcessInfo, error) {
	response, err := c.processes.SignalProcess(ctx, &codev1.SignalProcessRequest{Process: process, Signal: signal, Wait: wait})
	if err != nil {
		return nil, err
	}
	if response.GetProcess() == nil {
		return nil, status.Error(codes.DataLoss, "signal process response has no process")
	}
	return response.GetProcess(), nil
}

// DeleteProcess permanently deletes one terminal process record and its logs.
func (c *Client) DeleteProcess(ctx context.Context, process *codev1.ProcessReference) (*codev1.ProcessInfo, error) {
	response, err := c.processes.DeleteProcess(ctx, &codev1.DeleteProcessRequest{Process: process})
	if err != nil {
		return nil, err
	}
	if response.GetProcess() == nil {
		return nil, status.Error(codes.DataLoss, "delete process response has no process")
	}
	return response.GetProcess(), nil
}

func (c *Client) Remove(ctx context.Context, remotePath string, recursive bool) error {
	_, err := c.files.Remove(ctx, &codev1.RemoveRequest{Path: remotePath, Recursive: recursive})
	return err
}

func (c *Client) Move(ctx context.Context, source, destination string, overwrite bool) (*codev1.FileInfo, error) {
	response, err := c.files.Move(ctx, &codev1.MoveRequest{Source: source, Destination: destination, Overwrite: overwrite})
	if err != nil {
		return nil, err
	}
	return response.GetFile(), nil
}

func (c *Client) Chmod(ctx context.Context, remotePath string, mode fs.FileMode) (*codev1.FileInfo, error) {
	response, err := c.files.Chmod(ctx, &codev1.ChmodRequest{Path: remotePath, Mode: uint32(mode.Perm())})
	if err != nil {
		return nil, err
	}
	return response.GetFile(), nil
}

func (c *Client) Mkdir(ctx context.Context, remotePath string, mode fs.FileMode, parents bool) (*codev1.FileInfo, error) {
	response, err := c.files.Mkdir(ctx, &codev1.MkdirRequest{Path: remotePath, Mode: uint32(mode.Perm()), Parents: parents})
	if err != nil {
		return nil, err
	}
	return response.GetFile(), nil
}

// UploadFile hashes and streams one local regular file.
func (c *Client) UploadFile(ctx context.Context, localPath, remotePath string, overwrite bool) (*codev1.UploadResponse, error) {
	file, err := os.Open(localPath)
	if err != nil {
		return nil, fmt.Errorf("open local upload: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("stat local upload: %w", err)
	}
	if !info.Mode().IsRegular() {
		return nil, errors.New("local upload is not a regular file")
	}
	if c.info.GetMaxUploadBytes() > 0 && info.Size() > c.info.GetMaxUploadBytes() {
		return nil, fmt.Errorf("local upload exceeds the controller's %d byte limit", c.info.GetMaxUploadBytes())
	}
	hash := sha256.New()
	if _, err := io.Copy(hash, file); err != nil {
		return nil, fmt.Errorf("hash local upload: %w", err)
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return nil, fmt.Errorf("rewind local upload: %w", err)
	}
	return c.Upload(ctx, remotePath, file, info.Size(), info.Mode().Perm(), hash.Sum(nil), overwrite)
}

// Upload sends metadata followed by bounded chunks from reader.
func (c *Client) Upload(ctx context.Context, remotePath string, reader io.Reader, size int64, mode fs.FileMode, digest []byte, overwrite bool) (*codev1.UploadResponse, error) {
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.files.Upload(streamContext)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&codev1.UploadRequest{Payload: &codev1.UploadRequest_Metadata{Metadata: &codev1.UploadMetadata{
		Path: remotePath, Size: size, Sha256: digest, Mode: uint32(mode.Perm()), Overwrite: overwrite,
	}}}); err != nil {
		return nil, uploadSendError(stream, err)
	}
	buffer := make([]byte, transferChunkSize)
	for {
		n, readErr := reader.Read(buffer)
		if n > 0 {
			chunk := append([]byte(nil), buffer[:n]...)
			if err := stream.Send(&codev1.UploadRequest{Payload: &codev1.UploadRequest_Chunk{Chunk: chunk}}); err != nil {
				return nil, uploadSendError(stream, err)
			}
		}
		if errors.Is(readErr, io.EOF) {
			break
		}
		if readErr != nil {
			return nil, fmt.Errorf("read upload: %w", readErr)
		}
	}
	response, err := stream.CloseAndRecv()
	if err != nil {
		return nil, err
	}
	if response.GetFile() == nil || response.GetSize() != size || !bytes.Equal(response.GetSha256(), digest) {
		return nil, status.Error(codes.DataLoss, "upload response verification failed")
	}
	return response, nil
}

// Download streams one remote file, validates framing, size and SHA-256, then returns metadata.
func (c *Client) Download(ctx context.Context, remotePath string, writer io.Writer) (*DownloadResult, error) {
	streamContext, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := c.files.Download(streamContext, &codev1.DownloadRequest{Path: remotePath})
	if err != nil {
		return nil, err
	}
	var metadata *codev1.DownloadMetadata
	var summary *codev1.DownloadSummary
	hash := sha256.New()
	total := int64(0)
	for {
		frame, recvErr := stream.Recv()
		if errors.Is(recvErr, io.EOF) {
			break
		}
		if recvErr != nil {
			return nil, recvErr
		}
		switch payload := frame.GetPayload().(type) {
		case *codev1.DownloadResponse_Metadata:
			if metadata != nil || total != 0 || summary != nil || payload.Metadata.GetFile() == nil {
				return nil, status.Error(codes.DataLoss, "invalid download metadata frame")
			}
			metadata = payload.Metadata
		case *codev1.DownloadResponse_Chunk:
			if metadata == nil || summary != nil {
				return nil, status.Error(codes.DataLoss, "download chunk is out of order")
			}
			if err := writeAll(writer, payload.Chunk); err != nil {
				return nil, err
			}
			total += int64(len(payload.Chunk))
			_, _ = hash.Write(payload.Chunk)
		case *codev1.DownloadResponse_Summary:
			if metadata == nil || summary != nil || payload.Summary == nil {
				return nil, status.Error(codes.DataLoss, "invalid download summary frame")
			}
			summary = payload.Summary
		default:
			return nil, status.Error(codes.DataLoss, "download frame has no payload")
		}
	}
	if metadata == nil || summary == nil {
		return nil, status.Error(codes.DataLoss, "download ended without metadata and summary")
	}
	digest := hash.Sum(nil)
	if total != metadata.GetFile().GetSize() || total != summary.GetSize() {
		return nil, status.Error(codes.DataLoss, "download size mismatch")
	}
	if !bytes.Equal(digest, summary.GetSha256()) {
		return nil, status.Error(codes.DataLoss, "download sha256 mismatch")
	}
	return &DownloadResult{File: metadata.GetFile(), Size: total, SHA256: digest}, nil
}

// DownloadFile downloads to a same-directory temporary file and atomically publishes it.
func (c *Client) DownloadFile(ctx context.Context, remotePath, localPath string) (*DownloadResult, error) {
	directory := filepath.Dir(localPath)
	temp, err := os.CreateTemp(directory, ".remote-code-download-*")
	if err != nil {
		return nil, fmt.Errorf("create local download temporary file: %w", err)
	}
	tempName := temp.Name()
	published := false
	defer func() {
		_ = temp.Close()
		if !published {
			_ = os.Remove(tempName)
		}
	}()
	result, err := c.Download(ctx, remotePath, temp)
	if err != nil {
		return nil, err
	}
	if err := temp.Chmod(os.FileMode(result.File.GetMode())); err != nil {
		return nil, fmt.Errorf("set local download permissions: %w", err)
	}
	if err := temp.Sync(); err != nil {
		return nil, fmt.Errorf("sync local download: %w", err)
	}
	if err := temp.Close(); err != nil {
		return nil, fmt.Errorf("close local download: %w", err)
	}
	if err := os.Rename(tempName, localPath); err != nil {
		return nil, fmt.Errorf("publish local download: %w", err)
	}
	published = true
	return result, nil
}

func loadTransportCredentials(config Config) (credentials.TransportCredentials, error) {
	if config.TLSCAFile == "" {
		return insecure.NewCredentials(), nil
	}
	pem, err := os.ReadFile(config.TLSCAFile)
	if err != nil {
		return nil, fmt.Errorf("read TLS CA file: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, errors.New("TLS CA file contains no valid certificates")
	}
	return credentials.NewTLS(&tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    pool,
		ServerName: config.TLSServerName,
	}), nil
}

type bearerCredentials struct {
	token string
}

func (credentials bearerCredentials) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer " + credentials.token}, nil
}

func (bearerCredentials) RequireTransportSecurity() bool {
	return false
}

func writeAll(writer io.Writer, data []byte) error {
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

func uploadSendError(stream grpc.ClientStreamingClient[codev1.UploadRequest, codev1.UploadResponse], sendErr error) error {
	_, receiveErr := stream.CloseAndRecv()
	if receiveErr != nil {
		return receiveErr
	}
	return sendErr
}
