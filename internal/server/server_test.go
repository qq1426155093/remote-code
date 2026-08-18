package server

import (
	"path/filepath"
	"strings"
	"testing"
)

func TestIsLoopbackAddress(t *testing.T) {
	tests := map[string]bool{
		"127.0.0.1:9443": true,
		"[::1]:9443":     true,
		"localhost:9443": true,
		"0.0.0.0:9443":   false,
		":9443":          false,
		"invalid":        false,
	}
	for address, want := range tests {
		if got := isLoopbackAddress(address); got != want {
			t.Errorf("isLoopbackAddress(%q) = %v, want %v", address, got, want)
		}
	}
}

func TestNewRejectsInsecureRemoteListener(t *testing.T) {
	_, err := New(Config{ListenAddress: "0.0.0.0:0", Workspace: t.TempDir()})
	if err == nil {
		t.Fatal("New() accepted insecure non-loopback listener")
	}
}

// Prepare is the single place the MCP credential is resolved, so the whole
// resolution matrix is asserted here rather than at each caller.
func TestPrepareResolvesMCPTokenIndependentlyOfGRPCToken(t *testing.T) {
	tests := []struct {
		name      string
		grpcToken string
		mcpToken  string
		wantMCP   string
	}{
		{name: "unset mcp token reuses the grpc token", grpcToken: "grpc-secret", wantMCP: "grpc-secret"},
		{name: "explicit mcp token wins", grpcToken: "grpc-secret", mcpToken: "mcp-secret", wantMCP: "mcp-secret"},
		{name: "mcp token without a grpc token", mcpToken: "mcp-secret", wantMCP: "mcp-secret"},
		{name: "identical values are not special", grpcToken: "same", mcpToken: "same", wantMCP: "same"},
		{name: "neither configured", wantMCP: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := Config{ListenAddress: "127.0.0.1:9443", Workspace: t.TempDir(), Token: test.grpcToken}
			config.MCP.Token = test.mcpToken
			prepared, err := Prepare(config)
			if err != nil {
				t.Fatalf("Prepare() error = %v", err)
			}
			if prepared.Config.MCP.Token != test.wantMCP {
				t.Fatalf("MCP token = %q, want %q", prepared.Config.MCP.Token, test.wantMCP)
			}
			if prepared.Config.Token != test.grpcToken {
				t.Fatalf("gRPC token = %q, want %q", prepared.Config.Token, test.grpcToken)
			}
		})
	}
}

func TestPrepareRejectsEnabledMCPWithoutAnyToken(t *testing.T) {
	config := Config{ListenAddress: "127.0.0.1:9443", Workspace: t.TempDir()}
	config.MCP.Enabled = true
	config.MCP.DefinitionFiles = []string{filepath.Join(t.TempDir(), "tools.mcp.yaml")}
	_, err := Prepare(config)
	if err == nil || !strings.Contains(err.Error(), "mcp.token_file or auth.token_file") {
		t.Fatalf("Prepare() error = %v, want a message naming both token sources", err)
	}
}
