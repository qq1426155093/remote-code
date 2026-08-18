package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestParseControllerOptionsLoadsTOMLAndAppliesFlagOverrides(t *testing.T) {
	workspace := t.TempDir()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("secret-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configFile := writeControllerConfig(t, `
version = 1
workspace = "/from/config"
listen_address = "0.0.0.0:9443"
runtime_directory = "/run/from-config"
max_upload_bytes = 2048
max_processes = 4
allow_insecure_remote = true

[process_logs]
max_bytes_per_process = 1048576
max_total_bytes = 8388608
segment_bytes = 262144
retention_after_exit = "36h"
max_observers_per_process = 3

[tls]
certificate_file = "/tls/server.crt"
key_file = "/tls/server.key"

[auth]
token_file = "`+tokenFile+`"
`)
	options, err := parseControllerOptions([]string{
		"--config", configFile,
		"--workspace", workspace,
		"--listen-addr=127.0.0.1:9555",
		"--max-processes", "9",
		"--allow-insecure-remote=false",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatalf("parseControllerOptions() error = %v", err)
	}
	config := options.serverConfig
	if options.configFile != configFile || config.Workspace != workspace || config.ListenAddress != "127.0.0.1:9555" ||
		config.RuntimeDirectory != "/run/from-config" || config.MaxUploadBytes != 2048 || config.MaxProcesses != 9 ||
		config.AllowInsecureRemote || config.TLSCertificateFile != "/tls/server.crt" || config.TLSKeyFile != "/tls/server.key" ||
		config.ProcessLogs.MaxBytesPerProcess != 1048576 || config.ProcessLogs.MaxTotalBytes != 8388608 ||
		config.ProcessLogs.SegmentBytes != 262144 || config.ProcessLogs.RetentionAfterExit != 36*time.Hour ||
		config.ProcessLogs.MaxObservers != 3 || options.tokenFile != tokenFile {
		t.Fatalf("parsed options = %+v", options)
	}
	validated, err := options.validatedServerConfig()
	if err != nil {
		t.Fatalf("validatedServerConfig() error = %v", err)
	}
	if validated.Token != "secret-token" {
		t.Fatalf("validated token = %q", validated.Token)
	}
}

func TestParseControllerOptionsUsesDefaultsWithoutConfig(t *testing.T) {
	options, err := parseControllerOptions([]string{"--workspace", "/srv/project"}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	config := options.serverConfig
	if config.Workspace != "/srv/project" || config.ListenAddress != "127.0.0.1:9443" ||
		config.RuntimeDirectory != "/var/run/remote-code-controller" || config.MaxUploadBytes != 1<<30 || config.MaxProcesses != 16 ||
		config.ProcessLogs.MaxBytesPerProcess != 64<<20 || config.ProcessLogs.MaxTotalBytes != 4<<30 ||
		config.ProcessLogs.SegmentBytes != 4<<20 || config.ProcessLogs.RetentionAfterExit != 7*24*time.Hour || config.ProcessLogs.MaxObservers != 8 ||
		config.ControllerLogs.MaxBytesPerController != 32<<20 || config.ControllerLogs.MaxTotalBytes != 128<<20 ||
		config.ControllerLogs.SegmentBytes != 4<<20 || config.ControllerLogs.RetentionAfterRestart != 7*24*time.Hour || config.ControllerLogs.MaxObservers != 8 ||
		config.FileTransfers.Disabled || config.FileTransfers.UploadSessionTTL != 24*time.Hour || config.FileTransfers.CompletedSessionTTL != time.Hour ||
		config.FileTransfers.MaxUploadSessions != 64 || config.FileTransfers.MaxStagingBytes != 4<<30 || config.FileTransfers.CheckpointBytes != 4<<20 ||
		config.FileTransfers.CheckpointInterval != time.Second || config.FileTransfers.MaxConcurrentDownloads != 16 {
		t.Fatalf("defaults = %+v", config)
	}
}

func TestLoadControllerConfigRejectsInvalidFiles(t *testing.T) {
	tests := []struct {
		name     string
		contents string
		want     string
	}{
		{name: "missing version", contents: `workspace = "/work"`, want: "version must be 1"},
		{name: "future version", contents: "version = 7\n", want: "version must be 1, 2, 3, 4, 5, or 6"},
		{name: "unknown field", contents: "version = 1\nmax_proceses = 2\n", want: "unknown field"},
		{name: "wrong type", contents: "version = 1\nmax_processes = \"many\"\n", want: "cannot decode"},
		{name: "duplicate key", contents: "version = 1\nmax_processes = 2\nmax_processes = 3\n", want: "already defined"},
		{name: "malformed", contents: "version = [\n", want: "decode controller config"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			name := writeControllerConfig(t, test.contents)
			_, err := loadControllerConfig(name)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("loadControllerConfig() error = %v, want containing %q", err, test.want)
			}
		})
	}
	empty := filepath.Join(t.TempDir(), "empty.toml")
	if err := os.WriteFile(empty, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadControllerConfig(empty); err == nil {
		t.Fatal("loadControllerConfig(empty) succeeded")
	}
}

func TestExampleControllerConfigParses(t *testing.T) {
	if _, err := loadControllerConfig(filepath.Join("..", "..", "configs", "controller.example.toml")); err != nil {
		t.Fatalf("loadControllerConfig(example) error = %v", err)
	}
}

func TestLoadControllerConfigV2MCP(t *testing.T) {
	definition := filepath.Join(t.TempDir(), "file.mcp.yaml")
	configFile := writeControllerConfig(t, `
version = 2
workspace = "/work"
[mcp]
enabled = true
listen_address = "127.0.0.1:9555"
definition_files = ["`+definition+`"]
allowed_host_capabilities = ["files.read"]
default_tool_timeout = "12s"
max_tool_timeout = "2m"
`)
	file, err := loadControllerConfig(configFile)
	if err != nil {
		t.Fatal(err)
	}
	options := defaultControllerOptions()
	if err := applyControllerFileConfig(&options, file); err != nil {
		t.Fatal(err)
	}
	if !options.serverConfig.MCP.Enabled || options.serverConfig.MCP.ListenAddress != "127.0.0.1:9555" ||
		len(options.serverConfig.MCP.DefinitionFiles) != 1 || options.serverConfig.MCP.DefaultToolTimeout != 12*time.Second ||
		options.serverConfig.MCP.MaxToolTimeout != 2*time.Minute {
		t.Fatalf("MCP config = %+v", options.serverConfig.MCP)
	}

	v1WithMCP := writeControllerConfig(t, "version = 1\n[mcp]\nenabled = false\n")
	if _, err := loadControllerConfig(v1WithMCP); err == nil || !strings.Contains(err.Error(), "version 1") {
		t.Fatalf("v1 MCP error = %v", err)
	}
}

func TestLoadControllerConfigV3ProcessTemplates(t *testing.T) {
	definition := filepath.Join(t.TempDir(), "agents.process-template.yaml")
	configFile := writeControllerConfig(t, `
version = 3
workspace = "/work"

[process_templates]
definition_files = ["`+definition+`"]
`)
	file, err := loadControllerConfig(configFile)
	if err != nil {
		t.Fatal(err)
	}
	options := defaultControllerOptions()
	if err := applyControllerFileConfig(&options, file); err != nil {
		t.Fatal(err)
	}
	if len(options.serverConfig.ProcessTemplates.DefinitionFiles) != 1 || options.serverConfig.ProcessTemplates.DefinitionFiles[0] != definition {
		t.Fatalf("process template config = %+v", options.serverConfig.ProcessTemplates)
	}

	for _, version := range []int{1, 2} {
		name := writeControllerConfig(t, fmt.Sprintf("version = %d\n[process_templates]\ndefinition_files = []\n", version))
		if _, err := loadControllerConfig(name); err == nil || !strings.Contains(err.Error(), "does not support the process_templates table") {
			t.Fatalf("loadControllerConfig(version %d with templates) error = %v", version, err)
		}
	}
}

func TestLoadControllerConfigV4ProcessTemplateExtraParameters(t *testing.T) {
	definition := filepath.Join(t.TempDir(), "agents.process-template.yaml")
	configFile := writeControllerConfig(t, `
version = 4
workspace = "/work"

[process_templates]
definition_files = ["`+definition+`"]

[process_templates.extra_parameters]
default_model = "fast"
common_arguments = ["--safe", "true"]
debug = true
retries = 2
environment = { AGENT_MODE = "shared" }
`)
	file, err := loadControllerConfig(configFile)
	if err != nil {
		t.Fatal(err)
	}
	options := defaultControllerOptions()
	if err := applyControllerFileConfig(&options, file); err != nil {
		t.Fatal(err)
	}
	got := options.serverConfig.ProcessTemplates
	if len(got.DefinitionFiles) != 1 || got.DefinitionFiles[0] != definition ||
		got.ExtraParameters["default_model"] != "fast" || got.ExtraParameters["debug"] != true ||
		got.ExtraParameters["retries"] != int64(2) ||
		!reflect.DeepEqual(got.ExtraParameters["common_arguments"], []any{"--safe", "true"}) ||
		!reflect.DeepEqual(got.ExtraParameters["environment"], map[string]any{"AGENT_MODE": "shared"}) {
		t.Fatalf("process template config = %#v", got)
	}

	v3WithExtraParameters := writeControllerConfig(t, `
version = 3
[process_templates.extra_parameters]
default_model = "fast"
`)
	if _, err := loadControllerConfig(v3WithExtraParameters); err == nil || !strings.Contains(err.Error(), "does not support process_templates.extra_parameters") {
		t.Fatalf("v3 extra_parameters error = %v", err)
	}
	v3WithEmptyExtraParameters := writeControllerConfig(t, "version = 3\n[process_templates.extra_parameters]\n")
	if _, err := loadControllerConfig(v3WithEmptyExtraParameters); err == nil || !strings.Contains(err.Error(), "does not support process_templates.extra_parameters") {
		t.Fatalf("v3 empty extra_parameters error = %v", err)
	}
}

func TestLoadControllerConfigV5FileTransfers(t *testing.T) {
	configFile := writeControllerConfig(t, `
version = 5
workspace = "/work"

[file_transfers]
resumable_enabled = true
upload_session_ttl = "12h"
completed_session_ttl = "30m"
max_active_upload_sessions = 7
max_staging_bytes = 8589934592
checkpoint_bytes = 2097152
checkpoint_interval = "2s"
max_concurrent_downloads = 5
`)
	file, err := loadControllerConfig(configFile)
	if err != nil {
		t.Fatal(err)
	}
	options := defaultControllerOptions()
	if err := applyControllerFileConfig(&options, file); err != nil {
		t.Fatal(err)
	}
	got := options.serverConfig.FileTransfers
	if got.Disabled || got.UploadSessionTTL != 12*time.Hour || got.CompletedSessionTTL != 30*time.Minute ||
		got.MaxUploadSessions != 7 || got.MaxStagingBytes != 8<<30 || got.CheckpointBytes != 2<<20 ||
		got.CheckpointInterval != 2*time.Second || got.MaxConcurrentDownloads != 5 {
		t.Fatalf("file transfer config = %+v", got)
	}

	v4WithTransfers := writeControllerConfig(t, "version = 4\n[file_transfers]\nresumable_enabled = true\n")
	if _, err := loadControllerConfig(v4WithTransfers); err == nil || !strings.Contains(err.Error(), "does not support the file_transfers table") {
		t.Fatalf("v4 file_transfers error = %v", err)
	}
}

func TestLoadControllerConfigV6ControllerLogs(t *testing.T) {
	configFile := writeControllerConfig(t, `
version = 6
workspace = "/work"

[controller_logs]
max_bytes_per_controller = 1048576
max_total_bytes = 2097152
segment_bytes = 262144
retention_after_restart = "36h"
max_observers = 3
`)
	file, err := loadControllerConfig(configFile)
	if err != nil {
		t.Fatal(err)
	}
	options := defaultControllerOptions()
	if err := applyControllerFileConfig(&options, file); err != nil {
		t.Fatal(err)
	}
	got := options.serverConfig.ControllerLogs
	if got.MaxBytesPerController != 1048576 || got.MaxTotalBytes != 2097152 || got.SegmentBytes != 262144 || got.RetentionAfterRestart != 36*time.Hour || got.MaxObservers != 3 {
		t.Fatalf("controller log config = %+v", got)
	}

	v5WithControllerLogs := writeControllerConfig(t, "version = 5\n[controller_logs]\nmax_observers = 2\n")
	if _, err := loadControllerConfig(v5WithControllerLogs); err == nil || !strings.Contains(err.Error(), "does not support the controller_logs table") {
		t.Fatalf("v5 controller_logs error = %v", err)
	}
}

func TestFindConfigFile(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		want    string
		wantErr bool
	}{
		{name: "long separate", args: []string{"--config", "one.toml"}, want: "one.toml"},
		{name: "long inline", args: []string{"--config=two.toml"}, want: "two.toml"},
		{name: "single dash", args: []string{"-config", "three.toml"}, want: "three.toml"},
		{name: "skip another value", args: []string{"--workspace", "--config", "positional"}},
		{name: "after normal option", args: []string{"--max-processes", "4", "--config", "four.toml"}, want: "four.toml"},
		{name: "after positional", args: []string{"positional", "--config", "ignored.toml"}},
		{name: "empty", args: []string{"--config="}, wantErr: true},
		{name: "missing", args: []string{"--config"}, wantErr: true},
		{name: "duplicate", args: []string{"--config", "one", "--config=two"}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := findConfigFile(test.args)
			if (err != nil) != test.wantErr || got != test.want {
				t.Fatalf("findConfigFile() = %q, %v; want %q, error=%v", got, err, test.want, test.wantErr)
			}
		})
	}
}

func TestValidatedServerConfigRejectsInvalidValues(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*controllerOptions)
		want   string
	}{
		{name: "workspace", mutate: func(o *controllerOptions) { o.serverConfig.Workspace = "" }, want: "workspace is required"},
		{name: "listen", mutate: func(o *controllerOptions) { o.serverConfig.ListenAddress = "" }, want: "listen address is required"},
		{name: "runtime", mutate: func(o *controllerOptions) { o.serverConfig.RuntimeDirectory = "" }, want: "runtime directory is required"},
		{name: "upload", mutate: func(o *controllerOptions) { o.serverConfig.MaxUploadBytes = 0 }, want: "max upload bytes must be positive"},
		{name: "upload sessions", mutate: func(o *controllerOptions) { o.serverConfig.FileTransfers.MaxUploadSessions = -1 }, want: "maximum upload sessions"},
		{name: "upload staging", mutate: func(o *controllerOptions) { o.serverConfig.FileTransfers.MaxStagingBytes = 1 }, want: "maximum staging bytes"},
		{name: "processes", mutate: func(o *controllerOptions) { o.serverConfig.MaxProcesses = 4097 }, want: "max processes must be between"},
		{name: "process log total", mutate: func(o *controllerOptions) { o.serverConfig.ProcessLogs.MaxTotalBytes = 1 }, want: "total max bytes"},
		{name: "process log observers", mutate: func(o *controllerOptions) { o.serverConfig.ProcessLogs.MaxObservers = -1 }, want: "max observers"},
		{name: "tls pair", mutate: func(o *controllerOptions) { o.serverConfig.TLSCertificateFile = "cert" }, want: "must be provided together"},
		{name: "insecure remote", mutate: func(o *controllerOptions) { o.serverConfig.ListenAddress = "0.0.0.0:9443" }, want: "refusing insecure non-loopback"},
		{name: "token", mutate: func(o *controllerOptions) { o.tokenFile = filepath.Join(t.TempDir(), "missing") }, want: "read token file"},
		{name: "workspace file", mutate: func(o *controllerOptions) {
			name := filepath.Join(t.TempDir(), "file")
			if err := os.WriteFile(name, nil, 0o600); err != nil {
				t.Fatal(err)
			}
			o.serverConfig.Workspace = name
		}, want: "is not a directory"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options := defaultControllerOptions()
			options.serverConfig.Workspace = t.TempDir()
			test.mutate(&options)
			_, err := options.validatedServerConfig()
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("validatedServerConfig() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestRunControllerChecksConfigurationAndHandlesHelp(t *testing.T) {
	configFile := writeControllerConfig(t, "version = 1\nworkspace = \""+t.TempDir()+"\"\nruntime_directory = \""+t.TempDir()+"\"\n")
	var stdout bytes.Buffer
	if err := runController([]string{"--config", configFile, "--check-config"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("runController(check) error = %v", err)
	}
	if stdout.String() != "configuration OK\n" {
		t.Fatalf("check output = %q", stdout.String())
	}
	if err := runController([]string{"--help"}, &bytes.Buffer{}, &bytes.Buffer{}); err != nil {
		t.Fatalf("runController(help) error = %v", err)
	}
	if _, err := parseControllerOptions([]string{"--help"}, &bytes.Buffer{}); !errors.Is(err, flag.ErrHelp) {
		t.Fatalf("parseControllerOptions(help) error = %v", err)
	}
	_, err := parseControllerOptions([]string{"unexpected"}, &bytes.Buffer{})
	if err == nil {
		t.Fatal("parseControllerOptions(positional) succeeded")
	}
}

func TestRunControllerChecksMCPDefinitionsWithoutListening(t *testing.T) {
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	tokenFile := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(tokenFile, []byte("test-token\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	definition := filepath.Join(t.TempDir(), "check.mcp.yaml")
	definitionYAML := `
version: 1
namespace: check
description: Check config tool
language: expr
tools:
  - name: echo
    title: Echo
    description: Return the supplied value without side effects.
    capabilities: []
    annotations:
      read_only: true
      destructive: false
      idempotent: true
      open_world: false
    input_schema:
      type: object
      required: [value]
      additionalProperties: false
      properties:
        value:
          type: string
          description: Value to return.
    output_schema:
      type: string
    script: |-
      args.value
`
	if err := os.WriteFile(definition, []byte(strings.TrimSpace(definitionYAML)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configFile := writeControllerConfig(t, `
version = 2
workspace = "`+workspace+`"
runtime_directory = "`+runtimeDirectory+`"
[auth]
token_file = "`+tokenFile+`"
[mcp]
enabled = true
listen_address = "127.0.0.1:0"
definition_files = ["`+definition+`"]
allowed_host_capabilities = []
`)
	var stdout bytes.Buffer
	if err := runController([]string{"--config", configFile, "--check-config"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("runController() error = %v", err)
	}
	if stdout.String() != "configuration OK\n" {
		t.Fatalf("output = %q", stdout.String())
	}
}

func TestRunControllerChecksProcessTemplatesWithoutListening(t *testing.T) {
	workspace := t.TempDir()
	definition := filepath.Join(t.TempDir(), "check.process-template.yaml")
	definitionYAML := `
version: 1
language: expr
templates:
  - name: check
    description: Check process template configuration.
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      required: [cwd]
      additionalProperties: false
      properties:
        cwd:
          type: string
          description: Workspace-relative directory.
    command: check-command
    io_mode: pipe
    input_mode: disabled
    render: |-
      {"arguments": [extra_parameters.shared_argument], "working_directory": parameters.cwd, "environment": {}}
`
	if err := os.WriteFile(definition, []byte(strings.TrimSpace(definitionYAML)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	configFile := writeControllerConfig(t, `
version = 4
workspace = "`+workspace+`"
runtime_directory = "`+t.TempDir()+`"
[process_templates]
definition_files = ["`+definition+`"]
[process_templates.extra_parameters]
shared_argument = "--checked"
`)
	var stdout bytes.Buffer
	if err := runController([]string{"--config", configFile, "--check-config"}, &stdout, &bytes.Buffer{}); err != nil {
		t.Fatalf("runController() error = %v", err)
	}
	if stdout.String() != "configuration OK\n" {
		t.Fatalf("output = %q", stdout.String())
	}
}

func writeControllerConfig(t *testing.T, contents string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "controller.toml")
	if err := os.WriteFile(name, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}
