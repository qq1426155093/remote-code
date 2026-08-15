package mcpserver

import (
	"context"
	"net/http"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/qq1426155093/remote-code/internal/files"
	processservice "github.com/qq1426155093/remote-code/internal/process"
)

func TestStreamableHTTPListsAndCallsConfiguredTool(t *testing.T) {
	workspace := t.TempDir()
	definition := writeDefinition(t, validDefinition)
	prepared, err := Prepare(Config{
		Enabled: true, ListenAddress: "127.0.0.1:0", EndpointPath: "/mcp",
		DefinitionFiles: []string{definition}, Token: "test-token",
	}, workspace, "127.0.0.1:9443")
	if err != nil {
		t.Fatal(err)
	}
	fileService, err := files.New(files.Config{Workspace: workspace})
	if err != nil {
		t.Fatal(err)
	}
	defer fileService.Close()
	processService, err := processservice.New(processservice.Config{Workspace: workspace, RuntimeDirectory: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer processService.Close()
	server, err := NewServer(prepared, fileService, processService)
	if err != nil {
		t.Fatal(err)
	}
	serveErrors := make(chan error, 1)
	go func() { serveErrors <- server.Serve() }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = server.Shutdown(ctx)
	}()

	httpClient := &http.Client{Transport: bearerTransport{token: "test-token", base: http.DefaultTransport}}
	client := mcpsdk.NewClient(&mcpsdk.Implementation{Name: "test-client", Version: "1"}, &mcpsdk.ClientOptions{Capabilities: &mcpsdk.ClientCapabilities{}})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := client.Connect(ctx, &mcpsdk.StreamableClientTransport{
		Endpoint: "http://" + server.Address() + "/mcp", HTTPClient: httpClient,
		DisableStandaloneSSE: true, MaxRetries: -1,
	}, nil)
	if err != nil {
		t.Fatalf("Connect() error = %v", err)
	}
	defer session.Close()
	listed, err := session.ListTools(ctx, &mcpsdk.ListToolsParams{})
	if err != nil {
		t.Fatalf("ListTools() error = %v", err)
	}
	if len(listed.Tools) != 1 || listed.Tools[0].Name != "test.echo" {
		t.Fatalf("tools = %#v", listed.Tools)
	}
	called, err := session.CallTool(ctx, &mcpsdk.CallToolParams{Name: "test.echo", Arguments: map[string]any{"path": "src/main.go"}})
	if err != nil {
		t.Fatalf("CallTool() error = %v", err)
	}
	if called.IsError || called.StructuredContent != "src/main.go" {
		t.Fatalf("call result = %#v", called)
	}

	request, _ := http.NewRequestWithContext(ctx, http.MethodPost, "http://"+server.Address()+"/mcp", strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}`))
	request.Header.Set("Content-Type", "application/json")
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != http.StatusUnauthorized {
		t.Fatalf("unauthenticated status = %d", response.StatusCode)
	}
}

type bearerTransport struct {
	token string
	base  http.RoundTripper
}

func (t bearerTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	clone := request.Clone(request.Context())
	clone.Header = request.Header.Clone()
	clone.Header.Set("Authorization", "Bearer "+t.token)
	return t.base.RoundTrip(clone)
}
