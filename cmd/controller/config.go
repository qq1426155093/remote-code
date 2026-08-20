package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/pelletier/go-toml/v2"
	"github.com/qq1426155093/remote-code/internal/auth"
	controllerlog "github.com/qq1426155093/remote-code/internal/controllerlog"
	fileservice "github.com/qq1426155093/remote-code/internal/files"
	mcpserver "github.com/qq1426155093/remote-code/internal/mcp"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/server"
	"github.com/qq1426155093/remote-code/internal/workflow"
)

const (
	controllerConfigVersionV1 = 1
	controllerConfigVersionV2 = 2
	controllerConfigVersionV3 = 3
	controllerConfigVersionV4 = 4
	controllerConfigVersionV5 = 5
	controllerConfigVersionV6 = 6
	controllerConfigVersionV7 = 7
	controllerConfigVersionV8 = 8
	maxControllerConfigBytes  = 1 << 20
	maxConfiguredProcesses    = 4096
)

type controllerOptions struct {
	serverConfig server.Config
	tokenFile    string
	mcpTokenFile string
	configFile   string
	checkConfig  bool
	showVersion  bool
}

type controllerFileConfig struct {
	Version             int     `toml:"version"`
	Workspace           *string `toml:"workspace"`
	ListenAddress       *string `toml:"listen_address"`
	RuntimeDirectory    *string `toml:"runtime_directory"`
	MaxUploadBytes      *int64  `toml:"max_upload_bytes"`
	MaxProcesses        *int    `toml:"max_processes"`
	AllowInsecureRemote *bool   `toml:"allow_insecure_remote"`
	TLS                 struct {
		CertificateFile *string `toml:"certificate_file"`
		KeyFile         *string `toml:"key_file"`
	} `toml:"tls"`
	Auth struct {
		TokenFile *string `toml:"token_file"`
	} `toml:"auth"`
	ProcessLogs struct {
		MaxBytesPerProcess *int64  `toml:"max_bytes_per_process"`
		MaxTotalBytes      *int64  `toml:"max_total_bytes"`
		SegmentBytes       *int64  `toml:"segment_bytes"`
		RetentionAfterExit *string `toml:"retention_after_exit"`
		MaxObservers       *int    `toml:"max_observers_per_process"`
	} `toml:"process_logs"`
	ControllerLogs   *controllerLogFileConfig   `toml:"controller_logs"`
	ProcessTemplates *processTemplateFileConfig `toml:"process_templates"`
	FileTransfers    *fileTransferFileConfig    `toml:"file_transfers"`
	MCP              *mcpFileConfig             `toml:"mcp"`
	Workflows        *workflowFileConfig        `toml:"workflows"`
}

type controllerLogFileConfig struct {
	MaxBytesPerController *int64  `toml:"max_bytes_per_controller"`
	MaxTotalBytes         *int64  `toml:"max_total_bytes"`
	SegmentBytes          *int64  `toml:"segment_bytes"`
	RetentionAfterRestart *string `toml:"retention_after_restart"`
	MaxObservers          *int    `toml:"max_observers"`
}

type fileTransferFileConfig struct {
	ResumableEnabled       *bool   `toml:"resumable_enabled"`
	UploadSessionTTL       *string `toml:"upload_session_ttl"`
	CompletedSessionTTL    *string `toml:"completed_session_ttl"`
	MaxActiveUploads       *int    `toml:"max_active_upload_sessions"`
	MaxStagingBytes        *int64  `toml:"max_staging_bytes"`
	CheckpointBytes        *int64  `toml:"checkpoint_bytes"`
	CheckpointInterval     *string `toml:"checkpoint_interval"`
	MaxConcurrentDownloads *int    `toml:"max_concurrent_downloads"`
}

type processTemplateFileConfig struct {
	DefinitionFiles *[]string       `toml:"definition_files"`
	ExtraParameters *map[string]any `toml:"extra_parameters"`
}

type mcpFileConfig struct {
	Enabled                 *bool     `toml:"enabled"`
	ListenAddress           *string   `toml:"listen_address"`
	EndpointPath            *string   `toml:"endpoint_path"`
	TokenFile               *string   `toml:"token_file"`
	DefinitionFiles         *[]string `toml:"definition_files"`
	AllowedOrigins          *[]string `toml:"allowed_origins"`
	AllowedHostCapabilities *[]string `toml:"allowed_host_capabilities"`
	MaxRequestBytes         *int64    `toml:"max_request_bytes"`
	MaxResponseBytes        *int64    `toml:"max_response_bytes"`
	MaxConcurrentCalls      *int      `toml:"max_concurrent_calls"`
	RequestsPerSecond       *float64  `toml:"requests_per_second"`
	RequestBurst            *int      `toml:"request_burst"`
	DefaultToolTimeout      *string   `toml:"default_tool_timeout"`
	MaxToolTimeout          *string   `toml:"max_tool_timeout"`
	ToolListPageSize        *int      `toml:"tool_list_page_size"`
}

type workflowFileConfig struct {
	Enabled             *bool     `toml:"enabled"`
	DefinitionFiles     *[]string `toml:"definition_files"`
	MaxActiveRuns       *int      `toml:"max_active_runs"`
	MaxActiveAttempts   *int      `toml:"max_active_attempts"`
	LeaseDuration       *string   `toml:"lease_duration"`
	RetryInitialBackoff *string   `toml:"retry_initial_backoff"`
	RetryMaxBackoff     *string   `toml:"retry_max_backoff"`
	ReconcileInterval   *string   `toml:"reconcile_interval"`
}

func defaultControllerOptions() controllerOptions {
	options := controllerOptions{serverConfig: server.Config{
		ListenAddress:    "127.0.0.1:9443",
		MaxUploadBytes:   1 << 30,
		RuntimeDirectory: "/var/run/remote-code-controller",
		MaxProcesses:     16,
		ProcessLogs: processservice.LogConfig{
			MaxBytesPerProcess: 64 << 20,
			MaxTotalBytes:      4 << 30,
			SegmentBytes:       4 << 20,
			RetentionAfterExit: 7 * 24 * time.Hour,
			MaxObservers:       8,
		},
		ControllerLogs: controllerlog.DefaultConfig(),
		FileTransfers: fileservice.TransferConfig{
			UploadSessionTTL: 24 * time.Hour, CompletedSessionTTL: time.Hour,
			MaxUploadSessions: 64, MaxStagingBytes: 4 << 30,
			CheckpointBytes: 4 << 20, CheckpointInterval: time.Second, MaxConcurrentDownloads: 16,
		},
	}}
	options.serverConfig.MCP.ApplyDefaults()
	options.serverConfig.Workflows.ApplyDefaults()
	return options
}

// parseControllerOptions loads TOML first and binds flags to the merged values,
// so only flags explicitly present in args override file values.
func parseControllerOptions(args []string, output io.Writer) (controllerOptions, error) {
	options := defaultControllerOptions()
	configFile, err := findConfigFile(args)
	if err != nil {
		return controllerOptions{}, err
	}
	if configFile != "" {
		fileConfig, err := loadControllerConfig(configFile)
		if err != nil {
			return controllerOptions{}, err
		}
		if err := applyControllerFileConfig(&options, fileConfig); err != nil {
			return controllerOptions{}, err
		}
		options.configFile = configFile
	}

	flags := flag.NewFlagSet("remote-code-controller", flag.ContinueOnError)
	flags.SetOutput(output)
	flags.StringVar(&options.configFile, "config", options.configFile, "TOML configuration file")
	flags.StringVar(&options.serverConfig.Workspace, "workspace", options.serverConfig.Workspace, "workspace directory (required)")
	flags.StringVar(&options.serverConfig.ListenAddress, "listen-addr", options.serverConfig.ListenAddress, "gRPC listen address")
	flags.Int64Var(&options.serverConfig.MaxUploadBytes, "max-upload-bytes", options.serverConfig.MaxUploadBytes, "maximum bytes per uploaded file")
	flags.BoolVar(&options.serverConfig.FileTransfers.Disabled, "disable-resumable-transfers", options.serverConfig.FileTransfers.Disabled, "disable resumable file transfers")
	flags.DurationVar(&options.serverConfig.FileTransfers.UploadSessionTTL, "upload-session-ttl", options.serverConfig.FileTransfers.UploadSessionTTL, "inactive resumable upload lifetime")
	flags.DurationVar(&options.serverConfig.FileTransfers.CompletedSessionTTL, "completed-upload-session-ttl", options.serverConfig.FileTransfers.CompletedSessionTTL, "completed upload result lifetime")
	flags.IntVar(&options.serverConfig.FileTransfers.MaxUploadSessions, "max-upload-sessions", options.serverConfig.FileTransfers.MaxUploadSessions, "maximum active resumable uploads")
	flags.Int64Var(&options.serverConfig.FileTransfers.MaxStagingBytes, "max-upload-staging-bytes", options.serverConfig.FileTransfers.MaxStagingBytes, "maximum reserved resumable upload bytes")
	flags.Int64Var(&options.serverConfig.FileTransfers.CheckpointBytes, "upload-checkpoint-bytes", options.serverConfig.FileTransfers.CheckpointBytes, "bytes between durable upload checkpoints")
	flags.DurationVar(&options.serverConfig.FileTransfers.CheckpointInterval, "upload-checkpoint-interval", options.serverConfig.FileTransfers.CheckpointInterval, "maximum interval between upload checkpoints")
	flags.IntVar(&options.serverConfig.FileTransfers.MaxConcurrentDownloads, "max-concurrent-downloads", options.serverConfig.FileTransfers.MaxConcurrentDownloads, "maximum concurrent resumable downloads")
	flags.StringVar(&options.serverConfig.RuntimeDirectory, "runtime-dir", options.serverConfig.RuntimeDirectory, "persistent process runtime directory")
	flags.IntVar(&options.serverConfig.MaxProcesses, "max-processes", options.serverConfig.MaxProcesses, "maximum concurrently active managed processes")
	flags.Int64Var(&options.serverConfig.ProcessLogs.MaxBytesPerProcess, "process-log-max-bytes", options.serverConfig.ProcessLogs.MaxBytesPerProcess, "maximum retained log bytes per process")
	flags.Int64Var(&options.serverConfig.ProcessLogs.MaxTotalBytes, "process-log-max-total-bytes", options.serverConfig.ProcessLogs.MaxTotalBytes, "maximum retained process log bytes in total")
	flags.Int64Var(&options.serverConfig.ProcessLogs.SegmentBytes, "process-log-segment-bytes", options.serverConfig.ProcessLogs.SegmentBytes, "target process log segment size")
	flags.DurationVar(&options.serverConfig.ProcessLogs.RetentionAfterExit, "process-log-retention", options.serverConfig.ProcessLogs.RetentionAfterExit, "process log retention after exit")
	flags.IntVar(&options.serverConfig.ProcessLogs.MaxObservers, "process-log-max-observers", options.serverConfig.ProcessLogs.MaxObservers, "maximum concurrent log observers per process")
	flags.Int64Var(&options.serverConfig.ControllerLogs.MaxBytesPerController, "controller-log-max-bytes", options.serverConfig.ControllerLogs.MaxBytesPerController, "maximum retained controller runtime log bytes")
	flags.Int64Var(&options.serverConfig.ControllerLogs.MaxTotalBytes, "controller-log-max-total-bytes", options.serverConfig.ControllerLogs.MaxTotalBytes, "maximum total controller runtime log bytes")
	flags.Int64Var(&options.serverConfig.ControllerLogs.SegmentBytes, "controller-log-segment-bytes", options.serverConfig.ControllerLogs.SegmentBytes, "target controller runtime log segment size")
	flags.DurationVar(&options.serverConfig.ControllerLogs.RetentionAfterRestart, "controller-log-retention", options.serverConfig.ControllerLogs.RetentionAfterRestart, "controller log retention across restarts")
	flags.IntVar(&options.serverConfig.ControllerLogs.MaxObservers, "controller-log-max-observers", options.serverConfig.ControllerLogs.MaxObservers, "maximum concurrent controller log observers")
	flags.StringVar(&options.serverConfig.TLSCertificateFile, "tls-cert", options.serverConfig.TLSCertificateFile, "TLS certificate PEM file")
	flags.StringVar(&options.serverConfig.TLSKeyFile, "tls-key", options.serverConfig.TLSKeyFile, "TLS private key PEM file")
	flags.StringVar(&options.tokenFile, "token-file", options.tokenFile, "optional bearer token file")
	flags.StringVar(&options.mcpTokenFile, "mcp-token-file", options.mcpTokenFile, "optional MCP bearer token file; defaults to the gRPC token")
	flags.BoolVar(&options.serverConfig.AllowInsecureRemote, "allow-insecure-remote", options.serverConfig.AllowInsecureRemote, "allow plaintext gRPC on a non-loopback listener")
	flags.BoolVar(&options.checkConfig, "check-config", false, "validate configuration and exit")
	flags.BoolVar(&options.showVersion, "version", false, "print version and exit")
	if err := flags.Parse(args); err != nil {
		return controllerOptions{}, err
	}
	if flags.NArg() != 0 {
		return controllerOptions{}, fmt.Errorf("unexpected positional arguments: %s", strings.Join(flags.Args(), " "))
	}
	return options, nil
}

func findConfigFile(args []string) (string, error) {
	valueFlags := map[string]bool{
		"workspace": true, "listen-addr": true, "max-upload-bytes": true,
		"runtime-dir": true, "max-processes": true, "tls-cert": true,
		"tls-key": true, "token-file": true, "mcp-token-file": true,
		"process-log-max-bytes":       true,
		"process-log-max-total-bytes": true, "process-log-segment-bytes": true,
		"process-log-retention": true, "process-log-max-observers": true,
		"controller-log-max-bytes": true, "controller-log-max-total-bytes": true,
		"controller-log-segment-bytes": true, "controller-log-retention": true,
		"controller-log-max-observers": true,
		"upload-session-ttl":           true, "completed-upload-session-ttl": true,
		"max-upload-sessions": true, "max-upload-staging-bytes": true,
		"upload-checkpoint-bytes": true, "upload-checkpoint-interval": true,
		"max-concurrent-downloads": true,
	}
	var result string
	for index := 0; index < len(args); index++ {
		argument := args[index]
		if argument == "--" {
			break
		}
		nameValue := ""
		switch {
		case strings.HasPrefix(argument, "--"):
			nameValue = argument[2:]
		case strings.HasPrefix(argument, "-") && argument != "-":
			nameValue = argument[1:]
		default:
			return result, nil
		}
		name, inlineValue, inline := strings.Cut(nameValue, "=")
		if name != "config" {
			if valueFlags[name] && !inline && index+1 < len(args) {
				index++
			}
			continue
		}
		if result != "" {
			return "", errors.New("--config may only be specified once")
		}
		if inline {
			if inlineValue == "" {
				return "", errors.New("--config requires a non-empty path")
			}
			result = inlineValue
			continue
		}
		if index+1 >= len(args) || args[index+1] == "" {
			return "", errors.New("--config requires a path")
		}
		index++
		result = args[index]
	}
	return result, nil
}

func loadControllerConfig(name string) (controllerFileConfig, error) {
	file, err := os.Open(name)
	if err != nil {
		return controllerFileConfig{}, fmt.Errorf("open controller config %q: %w", name, err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return controllerFileConfig{}, fmt.Errorf("stat controller config %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return controllerFileConfig{}, fmt.Errorf("controller config %q is not a regular file", name)
	}
	if info.Size() <= 0 || info.Size() > maxControllerConfigBytes {
		return controllerFileConfig{}, fmt.Errorf("controller config %q must be between 1 and %d bytes", name, maxControllerConfigBytes)
	}
	var config controllerFileConfig
	decoder := toml.NewDecoder(io.LimitReader(file, maxControllerConfigBytes+1))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		details := err.Error()
		var strictError *toml.StrictMissingError
		var decodeError *toml.DecodeError
		switch {
		case errors.As(err, &strictError):
			details = strictError.String()
		case errors.As(err, &decodeError):
			details = decodeError.String()
		}
		return controllerFileConfig{}, fmt.Errorf("decode controller config %q: %s", name, details)
	}
	if config.Version != controllerConfigVersionV1 && config.Version != controllerConfigVersionV2 && config.Version != controllerConfigVersionV3 && config.Version != controllerConfigVersionV4 && config.Version != controllerConfigVersionV5 && config.Version != controllerConfigVersionV6 && config.Version != controllerConfigVersionV7 && config.Version != controllerConfigVersionV8 {
		return controllerFileConfig{}, fmt.Errorf("controller config version must be %d, %d, %d, %d, %d, %d, %d, or %d", controllerConfigVersionV1, controllerConfigVersionV2, controllerConfigVersionV3, controllerConfigVersionV4, controllerConfigVersionV5, controllerConfigVersionV6, controllerConfigVersionV7, controllerConfigVersionV8)
	}
	if config.Version == controllerConfigVersionV1 && config.MCP != nil {
		return controllerFileConfig{}, errors.New("controller config version 1 does not support the mcp table")
	}
	if config.Version < controllerConfigVersionV3 && config.ProcessTemplates != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support the process_templates table", config.Version)
	}
	if config.Version < controllerConfigVersionV4 && config.ProcessTemplates != nil && config.ProcessTemplates.ExtraParameters != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support process_templates.extra_parameters", config.Version)
	}
	if config.Version < controllerConfigVersionV5 && config.FileTransfers != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support the file_transfers table", config.Version)
	}
	if config.Version < controllerConfigVersionV6 && config.ControllerLogs != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support the controller_logs table", config.Version)
	}
	if config.Version < controllerConfigVersionV7 && config.MCP != nil && config.MCP.TokenFile != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support mcp.token_file", config.Version)
	}
	if config.Version < controllerConfigVersionV8 && config.Workflows != nil {
		return controllerFileConfig{}, fmt.Errorf("controller config version %d does not support the workflows table", config.Version)
	}
	return config, nil
}

func applyControllerFileConfig(options *controllerOptions, config controllerFileConfig) error {
	if config.Workspace != nil {
		options.serverConfig.Workspace = *config.Workspace
	}
	if config.ListenAddress != nil {
		options.serverConfig.ListenAddress = *config.ListenAddress
	}
	if config.RuntimeDirectory != nil {
		options.serverConfig.RuntimeDirectory = *config.RuntimeDirectory
	}
	if config.MaxUploadBytes != nil {
		options.serverConfig.MaxUploadBytes = *config.MaxUploadBytes
	}
	if config.MaxProcesses != nil {
		options.serverConfig.MaxProcesses = *config.MaxProcesses
	}
	if config.AllowInsecureRemote != nil {
		options.serverConfig.AllowInsecureRemote = *config.AllowInsecureRemote
	}
	if config.TLS.CertificateFile != nil {
		options.serverConfig.TLSCertificateFile = *config.TLS.CertificateFile
	}
	if config.TLS.KeyFile != nil {
		options.serverConfig.TLSKeyFile = *config.TLS.KeyFile
	}
	if config.Auth.TokenFile != nil {
		options.tokenFile = *config.Auth.TokenFile
	}
	if config.ProcessLogs.MaxBytesPerProcess != nil {
		options.serverConfig.ProcessLogs.MaxBytesPerProcess = *config.ProcessLogs.MaxBytesPerProcess
	}
	if config.ProcessLogs.MaxTotalBytes != nil {
		options.serverConfig.ProcessLogs.MaxTotalBytes = *config.ProcessLogs.MaxTotalBytes
	}
	if config.ProcessLogs.SegmentBytes != nil {
		options.serverConfig.ProcessLogs.SegmentBytes = *config.ProcessLogs.SegmentBytes
	}
	if config.ProcessLogs.RetentionAfterExit != nil {
		retention, err := time.ParseDuration(*config.ProcessLogs.RetentionAfterExit)
		if err != nil {
			return fmt.Errorf("invalid process_logs.retention_after_exit: %w", err)
		}
		options.serverConfig.ProcessLogs.RetentionAfterExit = retention
	}
	if config.ProcessLogs.MaxObservers != nil {
		options.serverConfig.ProcessLogs.MaxObservers = *config.ProcessLogs.MaxObservers
	}
	if config.ControllerLogs != nil {
		if config.ControllerLogs.MaxBytesPerController != nil {
			options.serverConfig.ControllerLogs.MaxBytesPerController = *config.ControllerLogs.MaxBytesPerController
		}
		if config.ControllerLogs.MaxTotalBytes != nil {
			options.serverConfig.ControllerLogs.MaxTotalBytes = *config.ControllerLogs.MaxTotalBytes
		}
		if config.ControllerLogs.SegmentBytes != nil {
			options.serverConfig.ControllerLogs.SegmentBytes = *config.ControllerLogs.SegmentBytes
		}
		if config.ControllerLogs.RetentionAfterRestart != nil {
			retention, err := time.ParseDuration(*config.ControllerLogs.RetentionAfterRestart)
			if err != nil {
				return fmt.Errorf("invalid controller_logs.retention_after_restart: %w", err)
			}
			options.serverConfig.ControllerLogs.RetentionAfterRestart = retention
		}
		if config.ControllerLogs.MaxObservers != nil {
			options.serverConfig.ControllerLogs.MaxObservers = *config.ControllerLogs.MaxObservers
		}
	}
	if config.ProcessTemplates != nil {
		if config.ProcessTemplates.DefinitionFiles != nil {
			options.serverConfig.ProcessTemplates.DefinitionFiles = append([]string(nil), (*config.ProcessTemplates.DefinitionFiles)...)
		}
		if config.ProcessTemplates.ExtraParameters != nil {
			options.serverConfig.ProcessTemplates.ExtraParameters = *config.ProcessTemplates.ExtraParameters
		}
	}
	if config.FileTransfers != nil {
		if err := applyFileTransferConfig(&options.serverConfig.FileTransfers, *config.FileTransfers); err != nil {
			return err
		}
	}
	if config.MCP != nil {
		if config.MCP.TokenFile != nil {
			options.mcpTokenFile = *config.MCP.TokenFile
		}
		if err := applyMCPFileConfig(&options.serverConfig.MCP, *config.MCP); err != nil {
			return err
		}
	}
	if config.Workflows != nil {
		if err := applyWorkflowFileConfig(&options.serverConfig.Workflows, *config.Workflows); err != nil {
			return err
		}
	}
	return nil
}

func applyWorkflowFileConfig(config *workflow.Config, file workflowFileConfig) error {
	if file.Enabled != nil {
		config.Enabled = *file.Enabled
	}
	if file.DefinitionFiles != nil {
		config.DefinitionFiles = append([]string(nil), (*file.DefinitionFiles)...)
	}
	if file.MaxActiveRuns != nil {
		config.MaxActiveRuns = *file.MaxActiveRuns
	}
	if file.MaxActiveAttempts != nil {
		config.MaxActiveAttempts = *file.MaxActiveAttempts
	}
	durations := []struct {
		name   string
		source *string
		target *time.Duration
	}{
		{name: "lease_duration", source: file.LeaseDuration, target: &config.LeaseDuration},
		{name: "retry_initial_backoff", source: file.RetryInitialBackoff, target: &config.RetryInitialBackoff},
		{name: "retry_max_backoff", source: file.RetryMaxBackoff, target: &config.RetryMaxBackoff},
		{name: "reconcile_interval", source: file.ReconcileInterval, target: &config.ReconcileInterval},
	}
	for _, duration := range durations {
		if duration.source == nil {
			continue
		}
		value, err := time.ParseDuration(*duration.source)
		if err != nil {
			return fmt.Errorf("invalid workflows.%s: %w", duration.name, err)
		}
		*duration.target = value
	}
	return nil
}

func applyFileTransferConfig(config *fileservice.TransferConfig, file fileTransferFileConfig) error {
	if file.ResumableEnabled != nil {
		config.Disabled = !*file.ResumableEnabled
	}
	if file.UploadSessionTTL != nil {
		value, err := time.ParseDuration(*file.UploadSessionTTL)
		if err != nil {
			return fmt.Errorf("invalid file_transfers.upload_session_ttl: %w", err)
		}
		config.UploadSessionTTL = value
	}
	if file.CompletedSessionTTL != nil {
		value, err := time.ParseDuration(*file.CompletedSessionTTL)
		if err != nil {
			return fmt.Errorf("invalid file_transfers.completed_session_ttl: %w", err)
		}
		config.CompletedSessionTTL = value
	}
	if file.MaxActiveUploads != nil {
		config.MaxUploadSessions = *file.MaxActiveUploads
	}
	if file.MaxStagingBytes != nil {
		config.MaxStagingBytes = *file.MaxStagingBytes
	}
	if file.CheckpointBytes != nil {
		config.CheckpointBytes = *file.CheckpointBytes
	}
	if file.CheckpointInterval != nil {
		value, err := time.ParseDuration(*file.CheckpointInterval)
		if err != nil {
			return fmt.Errorf("invalid file_transfers.checkpoint_interval: %w", err)
		}
		config.CheckpointInterval = value
	}
	if file.MaxConcurrentDownloads != nil {
		config.MaxConcurrentDownloads = *file.MaxConcurrentDownloads
	}
	return nil
}

func applyMCPFileConfig(config *mcpserver.Config, file mcpFileConfig) error {
	if file.Enabled != nil {
		config.Enabled = *file.Enabled
	}
	if file.ListenAddress != nil {
		config.ListenAddress = *file.ListenAddress
	}
	if file.EndpointPath != nil {
		config.EndpointPath = *file.EndpointPath
	}
	if file.DefinitionFiles != nil {
		config.DefinitionFiles = append([]string(nil), (*file.DefinitionFiles)...)
	}
	if file.AllowedOrigins != nil {
		config.AllowedOrigins = append([]string(nil), (*file.AllowedOrigins)...)
	}
	if file.AllowedHostCapabilities != nil {
		config.AllowedHostCapabilities = append([]string(nil), (*file.AllowedHostCapabilities)...)
	}
	if file.MaxRequestBytes != nil {
		config.MaxRequestBytes = *file.MaxRequestBytes
	}
	if file.MaxResponseBytes != nil {
		config.MaxResponseBytes = *file.MaxResponseBytes
	}
	if file.MaxConcurrentCalls != nil {
		config.MaxConcurrentCalls = *file.MaxConcurrentCalls
	}
	if file.RequestsPerSecond != nil {
		config.RequestsPerSecond = *file.RequestsPerSecond
	}
	if file.RequestBurst != nil {
		config.RequestBurst = *file.RequestBurst
	}
	if file.ToolListPageSize != nil {
		config.ToolListPageSize = *file.ToolListPageSize
	}
	if file.DefaultToolTimeout != nil {
		value, err := time.ParseDuration(*file.DefaultToolTimeout)
		if err != nil {
			return fmt.Errorf("invalid mcp.default_tool_timeout: %w", err)
		}
		config.DefaultToolTimeout = value
	}
	if file.MaxToolTimeout != nil {
		value, err := time.ParseDuration(*file.MaxToolTimeout)
		if err != nil {
			return fmt.Errorf("invalid mcp.max_tool_timeout: %w", err)
		}
		config.MaxToolTimeout = value
	}
	return nil
}

func (o controllerOptions) validatedServerConfig() (server.Config, error) {
	config := o.serverConfig
	if config.Workspace == "" {
		return server.Config{}, errors.New("workspace is required; configure workspace or pass --workspace")
	}
	if config.ListenAddress == "" {
		return server.Config{}, errors.New("listen address is required")
	}
	if config.RuntimeDirectory == "" {
		return server.Config{}, errors.New("runtime directory is required")
	}
	if config.MaxUploadBytes <= 0 {
		return server.Config{}, errors.New("max upload bytes must be positive")
	}
	if err := fileservice.ValidateTransferConfig(config.FileTransfers, config.MaxUploadBytes); err != nil {
		return server.Config{}, fmt.Errorf("invalid file transfer configuration: %w", err)
	}
	if config.MaxProcesses <= 0 || config.MaxProcesses > maxConfiguredProcesses {
		return server.Config{}, fmt.Errorf("max processes must be between 1 and %d", maxConfiguredProcesses)
	}
	if err := processservice.ValidateLogConfig(config.ProcessLogs); err != nil {
		return server.Config{}, err
	}
	if err := controllerlog.ValidateConfig(config.ControllerLogs); err != nil {
		return server.Config{}, err
	}
	if (config.TLSCertificateFile == "") != (config.TLSKeyFile == "") {
		return server.Config{}, errors.New("tls certificate and key must be provided together")
	}
	workspaceInfo, err := os.Stat(config.Workspace)
	if err != nil {
		return server.Config{}, fmt.Errorf("stat workspace: %w", err)
	}
	if !workspaceInfo.IsDir() {
		return server.Config{}, fmt.Errorf("workspace %q is not a directory", config.Workspace)
	}
	if o.tokenFile != "" {
		token, err := auth.ReadTokenFile(o.tokenFile)
		if err != nil {
			return server.Config{}, err
		}
		config.Token = token
	}
	// An explicit MCP token file separates the two entry points. Leaving it
	// unset keeps the historical behavior, and server.Prepare applies the
	// fallback so the resolution rule lives in exactly one place.
	if o.mcpTokenFile != "" {
		token, err := auth.ReadTokenFile(o.mcpTokenFile)
		if err != nil {
			return server.Config{}, fmt.Errorf("mcp: %w", err)
		}
		config.MCP.Token = token
	}
	config.MCP.TLSCertificateFile = config.TLSCertificateFile
	config.MCP.TLSKeyFile = config.TLSKeyFile
	if err := server.ValidateConfig(config); err != nil {
		return server.Config{}, err
	}
	return config, nil
}
