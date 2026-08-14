package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/qq1426155093/remote-code/internal/files"
	"github.com/qq1426155093/remote-code/internal/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const maxMessageBytes = 2 << 20

// Config describes one controller server.
type Config struct {
	ListenAddress       string
	Workspace           string
	MaxUploadBytes      int64
	TLSCertificateFile  string
	TLSKeyFile          string
	Token               string
	AllowInsecureRemote bool
}

// Server owns the listener, gRPC server and workspace handle.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	files      *files.Service
	closeOnce  sync.Once
}

// New validates the configuration and binds the listening socket.
func New(config Config) (*Server, error) {
	if config.ListenAddress == "" {
		config.ListenAddress = "127.0.0.1:9443"
	}
	if (config.TLSCertificateFile == "") != (config.TLSKeyFile == "") {
		return nil, errors.New("tls-cert and tls-key must be provided together")
	}
	if config.TLSCertificateFile == "" && !config.AllowInsecureRemote && !isLoopbackAddress(config.ListenAddress) {
		return nil, errors.New("refusing insecure non-loopback listener; configure TLS or pass --allow-insecure-remote")
	}

	fileService, err := files.New(files.Config{Workspace: config.Workspace, MaxUploadBytes: config.MaxUploadBytes})
	if err != nil {
		return nil, err
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		_ = fileService.Close()
		return nil, fmt.Errorf("listen on %s: %w", config.ListenAddress, err)
	}

	options := []grpc.ServerOption{
		grpc.MaxRecvMsgSize(maxMessageBytes),
		grpc.MaxSendMsgSize(maxMessageBytes),
	}
	if config.TLSCertificateFile != "" {
		transportCredentials, err := credentials.NewServerTLSFromFile(config.TLSCertificateFile, config.TLSKeyFile)
		if err != nil {
			_ = listener.Close()
			_ = fileService.Close()
			return nil, fmt.Errorf("load TLS certificate: %w", err)
		}
		options = append(options, grpc.Creds(transportCredentials))
	}
	if config.Token != "" {
		options = append(options,
			grpc.UnaryInterceptor(auth.UnaryServerInterceptor(config.Token)),
			grpc.StreamInterceptor(auth.StreamServerInterceptor(config.Token)),
		)
	}
	grpcServer := grpc.NewServer(options...)
	codev1.RegisterControllerServiceServer(grpcServer, &controllerService{files: fileService})
	codev1.RegisterFileServiceServer(grpcServer, fileService)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	return &Server{grpcServer: grpcServer, listener: listener, files: fileService}, nil
}

// Address returns the actual bound address, useful when port 0 is configured.
func (s *Server) Address() string {
	return s.listener.Addr().String()
}

// Serve blocks while accepting gRPC traffic.
func (s *Server) Serve() error {
	return s.grpcServer.Serve(s.listener)
}

// Shutdown gracefully drains requests until ctx expires, then forces a stop.
func (s *Server) Shutdown(ctx context.Context) error {
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	var shutdownErr error
	select {
	case <-done:
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-done
		shutdownErr = ctx.Err()
	}
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		if err := s.files.Close(); shutdownErr == nil {
			shutdownErr = err
		}
	})
	return shutdownErr
}

type controllerService struct {
	codev1.UnimplementedControllerServiceServer
	files *files.Service
}

func (s *controllerService) GetInfo(context.Context, *codev1.GetInfoRequest) (*codev1.GetInfoResponse, error) {
	return &codev1.GetInfoResponse{
		ControllerVersion: version.Version,
		ApiVersion:        version.APIVersion,
		WorkspaceName:     s.files.WorkspaceName(),
		MaxUploadBytes:    s.files.MaxUploadBytes(),
	}, nil
}

func isLoopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	host = strings.Trim(host, "[]")
	if strings.EqualFold(host, "localhost") {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
