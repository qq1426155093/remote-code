package main

import (
	"bytes"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
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
		options.tokenFile != tokenFile {
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
		config.RuntimeDirectory != "/var/run/remote-code-controller" || config.MaxUploadBytes != 1<<30 || config.MaxProcesses != 16 {
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
		{name: "future version", contents: "version = 2\n", want: "version must be 1"},
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
		{name: "processes", mutate: func(o *controllerOptions) { o.serverConfig.MaxProcesses = 4097 }, want: "max processes must be between"},
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

func writeControllerConfig(t *testing.T, contents string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "controller.toml")
	if err := os.WriteFile(name, []byte(strings.TrimSpace(contents)+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}
