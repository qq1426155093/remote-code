package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/pelletier/go-toml/v2"
	"github.com/qq1426155093/remote-code/internal/auth"
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
}

func defaultControllerOptions() controllerOptions {
	return controllerOptions{serverConfig: server.Config{
		ListenAddress:    "127.0.0.1:9443",
		MaxUploadBytes:   1 << 30,
		RuntimeDirectory: "/var/run/remote-code-controller",
		MaxProcesses:     16,
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
		applyControllerFileConfig(&options, fileConfig)
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
		"tls-key": true, "token-file": true,
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

func applyControllerFileConfig(options *controllerOptions, config controllerFileConfig) {
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
