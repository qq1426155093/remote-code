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
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/server"
)

const (
	controllerConfigVersion  = 1
	maxControllerConfigBytes = 1 << 20
	maxConfiguredProcesses   = 4096
)

type controllerOptions struct {
	serverConfig server.Config
	tokenFile    string
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
}

func defaultControllerOptions() controllerOptions {
	return controllerOptions{serverConfig: server.Config{
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
	}}
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
	flags.StringVar(&options.serverConfig.RuntimeDirectory, "runtime-dir", options.serverConfig.RuntimeDirectory, "persistent process runtime directory")
	flags.IntVar(&options.serverConfig.MaxProcesses, "max-processes", options.serverConfig.MaxProcesses, "maximum concurrently active managed processes")
	flags.Int64Var(&options.serverConfig.ProcessLogs.MaxBytesPerProcess, "process-log-max-bytes", options.serverConfig.ProcessLogs.MaxBytesPerProcess, "maximum retained log bytes per process")
	flags.Int64Var(&options.serverConfig.ProcessLogs.MaxTotalBytes, "process-log-max-total-bytes", options.serverConfig.ProcessLogs.MaxTotalBytes, "maximum retained process log bytes in total")
	flags.Int64Var(&options.serverConfig.ProcessLogs.SegmentBytes, "process-log-segment-bytes", options.serverConfig.ProcessLogs.SegmentBytes, "target process log segment size")
	flags.DurationVar(&options.serverConfig.ProcessLogs.RetentionAfterExit, "process-log-retention", options.serverConfig.ProcessLogs.RetentionAfterExit, "process log retention after exit")
	flags.IntVar(&options.serverConfig.ProcessLogs.MaxObservers, "process-log-max-observers", options.serverConfig.ProcessLogs.MaxObservers, "maximum concurrent log observers per process")
	flags.StringVar(&options.serverConfig.TLSCertificateFile, "tls-cert", options.serverConfig.TLSCertificateFile, "TLS certificate PEM file")
	flags.StringVar(&options.serverConfig.TLSKeyFile, "tls-key", options.serverConfig.TLSKeyFile, "TLS private key PEM file")
	flags.StringVar(&options.tokenFile, "token-file", options.tokenFile, "optional bearer token file")
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
		"tls-key": true, "token-file": true, "process-log-max-bytes": true,
		"process-log-max-total-bytes": true, "process-log-segment-bytes": true,
		"process-log-retention": true, "process-log-max-observers": true,
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
	if config.Version != controllerConfigVersion {
		return controllerFileConfig{}, fmt.Errorf("controller config version must be %d", controllerConfigVersion)
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
	if config.MaxProcesses <= 0 || config.MaxProcesses > maxConfiguredProcesses {
		return server.Config{}, fmt.Errorf("max processes must be between 1 and %d", maxConfiguredProcesses)
	}
	if err := processservice.ValidateLogConfig(config.ProcessLogs); err != nil {
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
	if err := server.ValidateConfig(config); err != nil {
		return server.Config{}, err
	}
	return config, nil
}
