package server

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/qq1426155093/remote-code/internal/controllerlog"
	"github.com/qq1426155093/remote-code/internal/files"
	"github.com/qq1426155093/remote-code/internal/logging"
	mcpserver "github.com/qq1426155093/remote-code/internal/mcp"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"github.com/qq1426155093/remote-code/internal/version"
	"github.com/qq1426155093/remote-code/internal/workflow"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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
	ControllerLogs      controllerlog.Config
	ProcessTemplates    processservice.TemplateConfig
	FileTransfers       files.TransferConfig
	MCP                 mcpserver.Config
	Workflows           workflow.Config
}

// Prepared contains validated controller configuration, compiled process
// templates, and compiled MCP tools. Preparing does not bind listeners, render
// a process template, or invoke any tool host function.
type Prepared struct {
	Config           Config
	ProcessTemplates *processservice.TemplateRegistry
	MCP              *mcpserver.Prepared
	Workflows        *workflow.Registry
}

// Server owns the listener, gRPC server and workspace handle.
type Server struct {
	grpcServer *grpc.Server
	listener   net.Listener
	files      *files.Service
	processes  *processservice.Service
	logger     *controllerlog.Logger
	logs       *controllerlog.Service
	mcpServer  *mcpserver.Server
	workflows  *workflow.Service
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
	config.Workflows.ApplyDefaults()
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
	// The MCP listener authenticates callers with its own credential when one
	// is configured, and otherwise reuses the gRPC token. Separate credentials
	// are what make an MCP-only topology meaningful: the MCP token then no
	// longer grants the full gRPC surface. This is the single place the
	// fallback is applied.
	if config.MCP.Token == "" {
		config.MCP.Token = config.Token
	}
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
	workflowRegistry, err := workflow.Prepare(config.Workflows, config.Workspace)
	if err != nil {
		return nil, fmt.Errorf("prepare workflows: %w", err)
	}
	return &Prepared{Config: config, ProcessTemplates: processTemplates, MCP: mcpPrepared, Workflows: workflowRegistry}, nil
}

// NewPrepared creates services and binds listeners from prepared configuration.
func NewPrepared(prepared *Prepared) (*Server, error) {
	if prepared == nil {
		return nil, errors.New("prepared server configuration is required")
	}
	logger, err := controllerlog.Open(prepared.Config.RuntimeDirectory, prepared.Config.ControllerLogs, os.Stderr)
	if err != nil {
		// Keep service construction usable when the optional persistent log path
		// is unavailable, but retain a visible bounded stderr diagnostic.
		logger = controllerlog.NewFallback(os.Stderr)
		fmt.Fprintf(os.Stderr, "controller runtime log unavailable: %v\n", err)
	}
	return NewPreparedWithLogger(prepared, logger)
}

// NewPreparedWithLogger creates services and binds listeners using an already
// opened controller logger. The server takes ownership and closes it during
// shutdown, including when construction fails.
func NewPreparedWithLogger(prepared *Prepared, logger *controllerlog.Logger) (*Server, error) {
	if prepared == nil {
		if logger != nil {
			_ = logger.Close()
		}
		return nil, errors.New("prepared server configuration is required")
	}
	if logger == nil {
		logger = controllerlog.NewFallback(os.Stderr)
	}
	config := prepared.Config

	fileService, err := files.New(files.Config{
		Workspace: config.Workspace, RuntimeDirectory: config.RuntimeDirectory,
		MaxUploadBytes: config.MaxUploadBytes, Transfers: config.FileTransfers,
	})
	if err != nil {
		_ = logger.Close()
		return nil, err
	}
	processService, err := processservice.New(processservice.Config{
		Workspace: config.Workspace, RuntimeDirectory: config.RuntimeDirectory, MaxProcesses: config.MaxProcesses,
		Logs: config.ProcessLogs, Templates: prepared.ProcessTemplates, Logger: logger,
	})
	if err != nil {
		_ = fileService.Close()
		_ = logger.Close()
		return nil, err
	}
	var workflowService *workflow.Service
	if config.Workflows.Enabled {
		workflowService, err = workflow.New(config.Workflows, config.RuntimeDirectory, prepared.Workflows)
		if err != nil {
			_ = processService.Close()
			_ = fileService.Close()
			_ = logger.Close()
			return nil, fmt.Errorf("start workflow service: %w", err)
		}
		logging.Emit(logger, logging.Event{
			Level: logging.LevelInfo, Component: "workflow", Name: "service_started",
			Message: "workflow service started",
			Fields: map[string]string{
				"definitions":    strconv.Itoa(prepared.Workflows.Count()),
				"recovered_runs": strconv.Itoa(workflowService.RunCount()),
			},
		})
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		if workflowService != nil {
			_ = workflowService.Close()
		}
		_ = processService.Close()
		_ = fileService.Close()
		_ = logger.Close()
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
			if workflowService != nil {
				_ = workflowService.Close()
			}
			_ = processService.Close()
			_ = fileService.Close()
			_ = logger.Close()
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
	controllerLogs := controllerlog.NewService(logger)
	codev1.RegisterControllerServiceServer(grpcServer, &controllerService{files: fileService, processes: processService, logs: controllerLogs})
	codev1.RegisterFileServiceServer(grpcServer, fileService)
	codev1.RegisterProcessServiceServer(grpcServer, processService)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
	var mcpHTTPServer *mcpserver.Server
	if config.MCP.Enabled {
		mcpHTTPServer, err = mcpserver.NewServer(prepared.MCP, fileService, processService, logger)
		if err != nil {
			_ = listener.Close()
			if workflowService != nil {
				_ = workflowService.Close()
			}
			_ = processService.Close()
			_ = fileService.Close()
			_ = logger.Close()
			return nil, err
		}
	}
	return &Server{grpcServer: grpcServer, listener: listener, files: fileService, processes: processService, logger: logger, logs: controllerLogs, mcpServer: mcpHTTPServer, workflows: workflowService}, nil
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
	maxUploadBytes := config.MaxUploadBytes
	if maxUploadBytes == 0 {
		maxUploadBytes = 1 << 30
	}
	if maxUploadBytes < 0 {
		return errors.New("max upload bytes must be positive")
	}
	if err := files.ValidateTransferConfig(config.FileTransfers, maxUploadBytes); err != nil {
		return fmt.Errorf("invalid file transfer configuration: %w", err)
	}
	if err := controllerlog.ValidateConfig(config.ControllerLogs); err != nil {
		return fmt.Errorf("invalid controller log configuration: %w", err)
	}
	if err := workflow.ValidateConfig(config.Workflows); err != nil {
		return fmt.Errorf("invalid workflow configuration: %w", err)
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

// Emit records a bounded controller event for the current runtime. It is
// useful to bootstrap code that lives outside the service packages while
// keeping the logger ownership inside Server.
func (s *Server) Emit(event logging.Event) { logging.Emit(s.logger, event) }

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
	if s.workflows != nil {
		s.workflows.BeginShutdown()
	}
	logging.Emit(s.logger, logging.Event{
		Level: logging.LevelInfo, Component: "controller", Name: "shutdown_started",
		Message: "controller shutdown started",
	})
	if s.mcpServer != nil {
		if err := s.mcpServer.Shutdown(ctx); err != nil {
			shutdownErr = err
		}
	}
	if err := s.processes.Shutdown(ctx); shutdownErr == nil {
		shutdownErr = err
	}
	if s.workflows != nil {
		logging.Emit(s.logger, logging.Event{
			Level: logging.LevelInfo, Component: "workflow", Name: "service_stopping",
			Message: "workflow service is stopping; durable activities remain recoverable",
		})
	}
	logging.Emit(s.logger, logging.Event{
		Level: logging.LevelInfo, Component: "controller", Name: "shutdown_draining",
		Message: "controller services stopped; draining gRPC observers",
	})
	// Finalization broadcasts to controller-log follow streams. It must happen
	// before GracefulStop, otherwise a follow stream can keep the gRPC drain
	// alive indefinitely while waiting for a notification that shutdown owns.
	if err := s.logger.Finalize(); shutdownErr == nil {
		shutdownErr = err
	}
	done := make(chan struct{})
	go func() {
		s.grpcServer.GracefulStop()
		close(done)
	}()
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
		if s.workflows != nil {
			if err := s.workflows.Close(); shutdownErr == nil {
				shutdownErr = err
			}
		}
		if err := s.processes.Close(); shutdownErr == nil {
			shutdownErr = err
		}
		if err := s.files.Close(); shutdownErr == nil {
			shutdownErr = err
		}
		if err := s.logger.Close(); shutdownErr == nil {
			shutdownErr = err
		}
	})
	return shutdownErr
}

type controllerService struct {
	codev1.UnimplementedControllerServiceServer
	files     *files.Service
	processes *processservice.Service
	logs      *controllerlog.Service
}

func (s *controllerService) ObserveControllerLogs(request *codev1.ObserveControllerLogsRequest, stream codev1.ControllerService_ObserveControllerLogsServer) error {
	if s.logs == nil {
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ControllerLogsUnavailable, "controller runtime logs are unavailable")
	}
	return s.logs.ObserveControllerLogs(request, stream)
}

func (s *controllerService) GetInfo(context.Context, *codev1.GetInfoRequest) (*codev1.GetInfoResponse, error) {
	return &codev1.GetInfoResponse{
		ControllerVersion:    version.Version,
		ApiVersion:           version.APIVersion,
		WorkspaceName:        s.files.WorkspaceName(),
		MaxUploadBytes:       s.files.MaxUploadBytes(),
		MaxProcesses:         uint32(s.processes.MaxProcesses()),
		ProcessTemplateCount: uint32(s.processes.ProcessTemplateCount()),
		FileTransfers:        s.files.FileTransferCapabilities(),
		ControllerLogs:       s.logsCapabilities(),
	}, nil
}

func (s *controllerService) logsCapabilities() *codev1.ControllerLogCapabilities {
	if s.logs == nil {
		return &codev1.ControllerLogCapabilities{}
	}
	return s.logs.Capabilities()
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
