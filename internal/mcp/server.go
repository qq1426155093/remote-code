package mcpserver

import (
	"bytes"
	"context"
	"crypto/subtle"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/jsonrpc"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/qq1426155093/remote-code/internal/files"
	"github.com/qq1426155093/remote-code/internal/logging"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/version"
)

// Server owns the MCP HTTP listener and immutable registry.
type Server struct {
	httpServer *http.Server
	listener   net.Listener
	closeOnce  sync.Once
}

// NewServer binds the configured MCP listener and connects host functions to
// the existing controller services.
func NewServer(prepared *Prepared, fileService *files.Service, processService *processservice.Service, loggers ...logging.Logger) (*Server, error) {
	if prepared == nil || !prepared.Config.Enabled || prepared.Registry == nil {
		return nil, errors.New("prepared MCP configuration is required")
	}
	config := prepared.Config
	hosts := &controllerHosts{files: fileService, processes: processService}
	runner := newRunner(config, prepared.Registry, hosts, loggers...)
	sdkServer := mcpsdk.NewServer(&mcpsdk.Implementation{Name: "remote-code-controller", Version: version.Version}, &mcpsdk.ServerOptions{
		PageSize:     config.ToolListPageSize,
		Capabilities: &mcpsdk.ServerCapabilities{Tools: &mcpsdk.ToolCapabilities{ListChanged: false}},
	})
	registerWorkspaceResources(sdkServer, fileService, config, runner.globalSlots)
	sdkTools := make([]*mcpsdk.Tool, 0, len(prepared.Registry.ordered))
	for _, compiled := range prepared.Registry.ordered {
		compiled := compiled
		destructive := compiled.Annotations.Destructive
		openWorld := compiled.Annotations.OpenWorld
		tool := &mcpsdk.Tool{
			Name: compiled.Name, Title: compiled.Title, Description: compiled.Description,
			InputSchema: compiled.InputSchemaValue, OutputSchema: compiled.OutputSchemaValue,
			Annotations: &mcpsdk.ToolAnnotations{
				ReadOnlyHint: compiled.Annotations.ReadOnly, DestructiveHint: &destructive,
				IdempotentHint: compiled.Annotations.Idempotent, OpenWorldHint: &openWorld,
			},
		}
		sdkTools = append(sdkTools, tool)
		sdkServer.AddTool(tool, func(ctx context.Context, request *mcpsdk.CallToolRequest) (*mcpsdk.CallToolResult, error) {
			clientName := ""
			if info := request.ClientInfo(); info != nil {
				clientName = info.Name
			}
			return runner.call(ctx, compiled, request.Params.Arguments, clientName, request.ProtocolVersion()), nil
		})
	}
	codec, err := newCursorCodec(prepared.Registry.digest)
	if err != nil {
		return nil, err
	}
	sdkServer.AddReceivingMiddleware(toolListMiddleware(codec, config.ToolListPageSize, sdkTools))
	sdkHandler := mcpsdk.NewStreamableHTTPHandler(func(*http.Request) *mcpsdk.Server { return sdkServer }, &mcpsdk.StreamableHTTPOptions{
		Stateless: true, JSONResponse: true, MaxRequestBodyBytes: config.MaxRequestBytes,
		PropagateRequestCancellation: true,
	})
	var endpoint http.Handler = sdkHandler
	endpoint = responseLimitMiddleware(config.MaxResponseBytes, endpoint)
	endpoint = requestPolicyMiddleware(config, endpoint)
	endpoint = auth.BearerHTTPMiddleware(config.Token, endpoint)
	endpoint = originMiddleware(config.AllowedOrigins, endpoint)
	endpoint = newRateLimiter(config.RequestsPerSecond, config.RequestBurst).middleware(endpoint)
	endpoint = panicRecoveryMiddleware(endpoint)
	mux := http.NewServeMux()
	mux.Handle(config.EndpointPath, endpoint)
	httpServer := &http.Server{
		Handler: mux, ReadHeaderTimeout: 5 * time.Second, IdleTimeout: 60 * time.Second,
		MaxHeaderBytes: 64 << 10,
	}
	listener, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("listen for MCP on %s: %w", config.ListenAddress, err)
	}
	if config.TLSCertificateFile != "" {
		certificate, err := tls.LoadX509KeyPair(config.TLSCertificateFile, config.TLSKeyFile)
		if err != nil {
			_ = listener.Close()
			return nil, fmt.Errorf("load MCP TLS certificate: %w", err)
		}
		listener = tls.NewListener(listener, &tls.Config{Certificates: []tls.Certificate{certificate}, MinVersion: tls.VersionTLS12})
	}
	return &Server{httpServer: httpServer, listener: listener}, nil
}

func toolListMiddleware(codec cursorCodec, pageSize int, tools []*mcpsdk.Tool) mcpsdk.Middleware {
	return func(next mcpsdk.MethodHandler) mcpsdk.MethodHandler {
		return func(ctx context.Context, method string, request mcpsdk.Request) (mcpsdk.Result, error) {
			if method != "tools/list" {
				return next(ctx, method, request)
			}
			typed, ok := request.(*mcpsdk.ListToolsRequest)
			if !ok {
				return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid tools/list parameters"}
			}
			offset := 0
			if typed.Params != nil && typed.Params.Cursor != "" {
				decoded, err := codec.decode(typed.Params.Cursor, len(tools))
				if err != nil {
					return nil, &jsonrpc.Error{Code: jsonrpc.CodeInvalidParams, Message: "invalid cursor"}
				}
				offset = decoded
			}
			end := offset + pageSize
			if end > len(tools) {
				end = len(tools)
			}
			page := make([]*mcpsdk.Tool, end-offset)
			for index, tool := range tools[offset:end] {
				page[index] = toolForProtocol(tool, typed.ProtocolVersion())
			}
			result := &mcpsdk.ListToolsResult{
				Cacheable: mcpsdk.Cacheable{CacheScope: "public"},
				Tools:     page,
			}
			if end < len(tools) {
				cursor, err := codec.encode(end)
				if err != nil {
					return nil, err
				}
				result.NextCursor = cursor
			}
			return result, nil
		}
	}
}

const arbitraryJSONToolOutputProtocol = "2026-07-28"

func toolForProtocol(tool *mcpsdk.Tool, protocolVersion string) *mcpsdk.Tool {
	if supportsArbitraryJSONToolOutput(protocolVersion) || outputSchemaIsObject(tool.OutputSchema) {
		return tool
	}
	clone := *tool
	clone.OutputSchema = nil
	return &clone
}

func supportsArbitraryJSONToolOutput(protocolVersion string) bool {
	return protocolVersion >= arbitraryJSONToolOutputProtocol
}

func outputSchemaIsObject(schema any) bool {
	if schema == nil {
		return true
	}
	root, ok := schema.(map[string]any)
	return ok && root["type"] == "object"
}

func (s *Server) Address() string { return s.listener.Addr().String() }
func (s *Server) Serve() error    { return s.httpServer.Serve(s.listener) }
func (s *Server) Shutdown(ctx context.Context) error {
	err := s.httpServer.Shutdown(ctx)
	if err != nil {
		_ = s.httpServer.Close()
	}
	s.closeOnce.Do(func() {
		closeErr := s.listener.Close()
		if err == nil && !errors.Is(closeErr, net.ErrClosed) {
			err = closeErr
		}
	})
	return err
}

func originMiddleware(allowedValues []string, next http.Handler) http.Handler {
	allowed := make(map[string]struct{}, len(allowedValues))
	for _, value := range allowedValues {
		normalized, _ := normalizeOrigin(value)
		allowed[normalized] = struct{}{}
	}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		values := request.Header.Values("Origin")
		if len(values) > 1 {
			http.Error(response, "origin not allowed", http.StatusForbidden)
			return
		}
		if len(values) == 1 {
			normalized, err := normalizeOrigin(values[0])
			if err != nil {
				http.Error(response, "origin not allowed", http.StatusForbidden)
				return
			}
			permitted := false
			for candidate := range allowed {
				if len(candidate) == len(normalized) && subtle.ConstantTimeCompare([]byte(candidate), []byte(normalized)) == 1 {
					permitted = true
					break
				}
			}
			if !permitted {
				http.Error(response, "origin not allowed", http.StatusForbidden)
				return
			}
		}
		next.ServeHTTP(response, request)
	})
}

func requestPolicyMiddleware(config Config, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost {
			response.Header().Set("Allow", "POST")
			http.Error(response, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mediaType := strings.ToLower(strings.TrimSpace(strings.Split(request.Header.Get("Content-Type"), ";")[0]))
		if mediaType != "application/json" {
			http.Error(response, "content type must be application/json", http.StatusUnsupportedMediaType)
			return
		}
		accept := request.Header.Get("Accept")
		if accept != "" && !strings.Contains(accept, "application/json") && !strings.Contains(accept, "text/event-stream") && !strings.Contains(accept, "*/*") {
			http.Error(response, "application/json response is required", http.StatusNotAcceptable)
			return
		}
		if request.ContentLength > config.MaxRequestBytes {
			http.Error(response, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		next.ServeHTTP(response, request)
	})
}

type limitedResponse struct {
	header   http.Header
	status   int
	body     bytes.Buffer
	maximum  int64
	exceeded bool
}

func (w *limitedResponse) Header() http.Header { return w.header }
func (w *limitedResponse) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
	}
}
func (w *limitedResponse) Write(value []byte) (int, error) {
	if w.status == 0 {
		w.status = http.StatusOK
	}
	if int64(w.body.Len()+len(value)) > w.maximum {
		w.exceeded = true
		return len(value), nil
	}
	return w.body.Write(value)
}
func responseLimitMiddleware(maximum int64, next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		buffered := &limitedResponse{header: make(http.Header), maximum: maximum}
		next.ServeHTTP(buffered, request)
		if buffered.exceeded {
			http.Error(response, "response exceeded size limit", http.StatusInternalServerError)
			return
		}
		for key, values := range buffered.header {
			response.Header()[key] = append([]string(nil), values...)
		}
		status := buffered.status
		if status == 0 {
			status = http.StatusOK
		}
		response.Header().Set("Content-Length", strconv.Itoa(buffered.body.Len()))
		response.WriteHeader(status)
		_, _ = response.Write(buffered.body.Bytes())
	})
}

type tokenBucket struct {
	mu       sync.Mutex
	rate     float64
	capacity float64
	tokens   float64
	updated  time.Time
}

func newRateLimiter(rate float64, burst int) *tokenBucket {
	return &tokenBucket{rate: rate, capacity: float64(burst), tokens: float64(burst), updated: time.Now()}
}

func panicRecoveryMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		defer func() {
			if recover() != nil {
				http.Error(response, "internal server error", http.StatusInternalServerError)
			}
		}()
		next.ServeHTTP(response, request)
	})
}
func (b *tokenBucket) middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !b.allow(time.Now()) {
			w.Header().Set("Retry-After", "1")
			http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
			return
		}
		next.ServeHTTP(w, r)
	})
}
func (b *tokenBucket) allow(now time.Time) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.tokens += now.Sub(b.updated).Seconds() * b.rate
	if b.tokens > b.capacity {
		b.tokens = b.capacity
	}
	b.updated = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
