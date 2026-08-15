package mcpserver

import (
	"bytes"
	"context"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/qq1426155093/remote-code/internal/files"
	processservice "github.com/qq1426155093/remote-code/internal/process"
)

func TestStreamableHTTPListsAndCallsConfiguredTool(t *testing.T) {
	workspace := t.TempDir()
	resourceData := []byte{0, 1, 2, 255}
	if err := os.WriteFile(filepath.Join(workspace, "artifact.bin"), resourceData, 0o600); err != nil {
		t.Fatal(err)
	}
	definition := writeDefinition(t, validDefinition)
	prepared, err := Prepare(Config{
		Enabled: true, ListenAddress: "127.0.0.1:0", EndpointPath: "/mcp",
		DefinitionFiles: []string{definition}, Token: "test-token", AllowedHostCapabilities: []string{"files.read"},
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
	templates, err := session.ListResourceTemplates(ctx, &mcpsdk.ListResourceTemplatesParams{})
	if err != nil {
		t.Fatalf("ListResourceTemplates() error = %v", err)
	}
	if len(templates.ResourceTemplates) != 1 || templates.ResourceTemplates[0].URITemplate != workspaceResourceTemplate {
		t.Fatalf("resource templates = %#v", templates.ResourceTemplates)
	}
	resource, err := session.ReadResource(ctx, &mcpsdk.ReadResourceParams{URI: "workspace:///artifact.bin"})
	if err != nil {
		t.Fatalf("ReadResource() error = %v", err)
	}
	if len(resource.Contents) != 1 || !bytes.Equal(resource.Contents[0].Blob, resourceData) {
		t.Fatalf("resource = %#v", resource)
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

func TestWorkspaceResourcePathAndBudget(t *testing.T) {
	if got, err := workspaceResourcePath("workspace:///dir/file%20name.bin"); err != nil || got != "dir/file name.bin" {
		t.Fatalf("workspaceResourcePath() = %q, %v", got, err)
	}
	for _, value := range []string{
		"file:///tmp/secret", "workspace://host/file", "workspace:///", "workspace:///file?query=1",
		"workspace:///file?", "workspace:///file#",
	} {
		if _, err := workspaceResourcePath(value); err == nil {
			t.Fatalf("workspaceResourcePath(%q) succeeded", value)
		}
	}
	if got := maximumResourceBytes(16 << 10); got != 6<<10 {
		t.Fatalf("maximumResourceBytes() = %d", got)
	}
}

func TestToolListMiddlewareAcceptsOmittedParams(t *testing.T) {
	codec, err := newCursorCodec([32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	tools := []*mcpsdk.Tool{{Name: "test.echo"}}
	handler := toolListMiddleware(codec, 100, tools)(func(context.Context, string, mcpsdk.Request) (mcpsdk.Result, error) {
		t.Fatal("tools/list unexpectedly reached the next handler")
		return nil, nil
	})

	result, err := handler(context.Background(), "tools/list", &mcpsdk.ListToolsRequest{})
	if err != nil {
		t.Fatalf("tools/list without params returned error: %v", err)
	}
	listed, ok := result.(*mcpsdk.ListToolsResult)
	if !ok || len(listed.Tools) != 1 || listed.Tools[0].Name != "test.echo" {
		t.Fatalf("tools/list result = %#v", result)
	}
}

func TestToolListMiddlewareAdaptsArrayOutputSchemaForLegacyClients(t *testing.T) {
	codec, err := newCursorCodec([32]byte{})
	if err != nil {
		t.Fatal(err)
	}
	tool := &mcpsdk.Tool{Name: "test.list", OutputSchema: map[string]any{"type": "array"}}
	handler := toolListMiddleware(codec, 100, []*mcpsdk.Tool{tool})(func(context.Context, string, mcpsdk.Request) (mcpsdk.Result, error) {
		t.Fatal("tools/list unexpectedly reached the next handler")
		return nil, nil
	})
	tests := []struct {
		name            string
		protocolVersion string
		wantSchema      bool
	}{
		{name: "legacy", protocolVersion: "2025-11-25", wantSchema: false},
		{name: "modern", protocolVersion: "2026-07-28", wantSchema: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := &mcpsdk.ListToolsParams{Meta: mcpsdk.Meta{mcpsdk.MetaKeyProtocolVersion: test.protocolVersion}}
			result, err := handler(context.Background(), "tools/list", &mcpsdk.ListToolsRequest{Params: params})
			if err != nil {
				t.Fatal(err)
			}
			listed := result.(*mcpsdk.ListToolsResult)
			if got := listed.Tools[0].OutputSchema != nil; got != test.wantSchema {
				t.Fatalf("output schema present = %t, want %t", got, test.wantSchema)
			}
			if listed.CacheScope != "public" {
				t.Fatalf("cache scope = %q, want public", listed.CacheScope)
			}
		})
	}
	if tool.OutputSchema == nil {
		t.Fatal("legacy adaptation mutated the registered tool")
	}
}

func TestStructuredContentForProtocol(t *testing.T) {
	array := []any{"one"}
	if got := structuredContentForProtocol(array, "2025-11-25"); got != nil {
		t.Fatalf("legacy array structured content = %#v", got)
	}
	if got := structuredContentForProtocol(array, "2026-07-28"); got == nil {
		t.Fatal("modern array structured content was omitted")
	}
	object := map[string]any{"value": "one"}
	if got := structuredContentForProtocol(object, "2025-11-25"); got == nil {
		t.Fatal("legacy object structured content was omitted")
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
