package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/qq1426155093/remote-code/internal/files"
	mcpserver "github.com/qq1426155093/remote-code/internal/mcp"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/version"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

const maxMessageBytes = 16 << 20

// Config describes one controller server.
type Config struct {
	ListenAddress       string
	Workspace           string
	MaxUploadBytes      int64
	TLSCertificateFile  string
	TLSKeyFile          string
	Token               string
	AllowInsecureRemote bool
	RuntimeDirectory    string
	MaxProcesses        int
	ProcessLogs         processservice.LogConfig
	ProcessTemplates    processservice.TemplateConfig
	MCP                 mcpserver.Config
}

// Prepared contains validated controller configuration, compiled process
// templates, and compiled MCP tools. Preparing does not bind listeners, render
// a process template, or invoke any tool host function.
type Prepared struct {
	Config           Config
	ProcessTemplates *processservice.TemplateRegistry
	MCP              *mcpserver.Prepared
}

// Server owns the listener, gRPC server and workspace handle.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	files      *files.Service
	processes  *processservice.Service
	mcpServer  *mcpserver.Server
	closeOnce  sync.Once
}

// New validates the configuration and binds the listening socket.
func New(config Config) (*Server, error) {
	prepared, err := Prepare(config)
	if err != nil {
		return nil, err
	}
	return NewPrepared(prepared)
}

// Prepare validates transport configuration and compiles the optional MCP
// registry without opening a listener.
func Prepare(config Config) (*Prepared, error) {
	if config.ListenAddress == "" {
		config.ListenAddress = "127.0.0.1:9443"
	}
	config.MCP.ApplyDefaults()
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	workspaceInfo, err := os.Stat(config.Workspace)
	if err != nil {
		return nil, fmt.Errorf("stat workspace: %w", err)
	}
	if !workspaceInfo.IsDir() {
		return nil, fmt.Errorf("workspace %q is not a directory", config.Workspace)
	}
	if config.TLSCertificateFile != "" {
		if _, err := tls.LoadX509KeyPair(config.TLSCertificateFile, config.TLSKeyFile); err != nil {
			return nil, fmt.Errorf("load TLS certificate: %w", err)
		}
	}
	config.MCP.Token = config.Token
	config.MCP.TLSCertificateFile = config.TLSCertificateFile
	config.MCP.TLSKeyFile = config.TLSKeyFile
	processTemplates, err := processservice.PrepareTemplates(config.ProcessTemplates, config.Workspace)
	if err != nil {
		return nil, fmt.Errorf("prepare process templates: %w", err)
	}
	mcpPrepared, err := mcpserver.Prepare(config.MCP, config.Workspace, config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("prepare MCP server: %w", err)
	}
	return &Prepared{Config: config, ProcessTemplates: processTemplates, MCP: mcpPrepared}, nil
}

// NewPrepared creates services and binds listeners from prepared configuration.
func NewPrepared(prepared *Prepared) (*Server, error) {
	if prepared == nil {
		return nil, errors.New("prepared server configuration is required")
	}
	config := prepared.Config

	fileService, err := files.New(files.Config{Workspace: config.Workspace, MaxUploadBytes: config.MaxUploadBytes})
	if err != nil {
		return nil, err
	}
	processService, err := processservice.New(processservice.Config{
		Workspace: config.Workspace, RuntimeDirectory: config.RuntimeDirectory, MaxProcesses: config.MaxProcesses,
		Logs: config.ProcessLogs, Templates: prepared.ProcessTemplates,
	})
	if err != nil {
		_ = fileService.Close()
		return nil, err
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		_ = processService.Close()
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
			_ = processService.Close()
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
	codev1.RegisterControllerServiceServer(grpcServer, &controllerService{files: fileService, processes: processService})
	codev1.RegisterFileServiceServer(grpcServer, fileService)
	codev1.RegisterProcessServiceServer(grpcServer, processService)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	var mcpHTTPServer *mcpserver.Server
	if config.MCP.Enabled {
		mcpHTTPServer, err = mcpserver.NewServer(prepared.MCP, fileService, processService)
		if err != nil {
			_ = listener.Close()
			_ = processService.Close()
			_ = fileService.Close()
			return nil, err
		}
	}
	return &Server{grpcServer: grpcServer, listener: listener, files: fileService, processes: processService, mcpServer: mcpHTTPServer}, nil
}

// ValidateConfig checks transport-level invariants without opening files or a
// listener. Service-specific filesystem validation remains in New.
func ValidateConfig(config Config) error {
	if config.ListenAddress == "" {
		config.ListenAddress = "127.0.0.1:9443"
	}
	if (config.TLSCertificateFile == "") != (config.TLSKeyFile == "") {
		return errors.New("tls-cert and tls-key must be provided together")
	}
	if config.TLSCertificateFile == "" && !config.AllowInsecureRemote && !isLoopbackAddress(config.ListenAddress) {
		return errors.New("refusing insecure non-loopback listener; configure TLS or pass --allow-insecure-remote")
	}
	if config.MCP.Enabled && config.TLSCertificateFile == "" && !config.AllowInsecureRemote && !isLoopbackAddress(config.MCP.ListenAddress) {
		return errors.New("refusing insecure non-loopback MCP listener; configure TLS or pass --allow-insecure-remote")
	}
	return nil
}

// Address returns the actual bound address, useful when port 0 is configured.
func (s *Server) Address() string {
	return s.listener.Addr().String()
}

// MCPAddress returns the actual MCP address, or an empty string when disabled.
func (s *Server) MCPAddress() string {
	if s.mcpServer == nil {
		return ""
	}
	return s.mcpServer.Address()
}

// Serve blocks while accepting gRPC traffic.
func (s *Server) Serve() error {
	if s.mcpServer == nil {
		return s.grpcServer.Serve(s.listener)
	}
	errorsChannel := make(chan error, 2)
	go func() { errorsChannel <- fmt.Errorf("serve gRPC: %w", s.grpcServer.Serve(s.listener)) }()
	go func() { errorsChannel <- fmt.Errorf("serve MCP HTTP: %w", s.mcpServer.Serve()) }()
	return <-errorsChannel
}

// Shutdown gracefully drains requests until ctx expires, then forces a stop.
func (s *Server) Shutdown(ctx context.Context) error {
	var shutdownErr error
	s.processes.BeginShutdown()
	if s.mcpServer != nil {
		if err := s.mcpServer.Shutdown(ctx); err != nil {
			shutdownErr = err
		}
	}
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
	if err := s.processes.Shutdown(ctx); shutdownErr == nil {
		shutdownErr = err
	}
	select {
	case <-done:
	case <-ctx.Done():
		s.grpcServer.Stop()
		<-done
		if shutdownErr == nil {
			shutdownErr = ctx.Err()
		}
	}
	s.closeOnce.Do(func() {
		_ = s.listener.Close()
		if err := s.processes.Close(); shutdownErr == nil {
			shutdownErr = err
		}
		if err := s.files.Close(); shutdownErr == nil {
			shutdownErr = err
		}
	})
	return shutdownErr
}

type controllerService struct {
	codev1.UnimplementedControllerServiceServer
	files     *files.Service
	processes *processservice.Service
}

func (s *controllerService) GetInfo(context.Context, *codev1.GetInfoRequest) (*codev1.GetInfoResponse, error) {
	return &codev1.GetInfoResponse{
		ControllerVersion:    version.Version,
		ApiVersion:           version.APIVersion,
		WorkspaceName:        s.files.WorkspaceName(),
		MaxUploadBytes:       s.files.MaxUploadBytes(),
		MaxProcesses:         uint32(s.processes.MaxProcesses()),
		ProcessTemplateCount: uint32(s.processes.ProcessTemplateCount()),
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
