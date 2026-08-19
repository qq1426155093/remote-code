package mcpserver

import (
	"errors"
	"fmt"
	"math"
	"net"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	defaultListenAddress      = "127.0.0.1:9444"
	defaultEndpointPath       = "/mcp"
	defaultMaxRequestBytes    = int64(1 << 20)
	defaultMaxResponseBytes   = int64(4 << 20)
	minimumResponseBytes      = int64(16 << 10)
	defaultMaxConcurrentCalls = 16
	defaultRequestsPerSecond  = 20
	defaultRequestBurst       = 40
	defaultToolTimeout        = 30 * time.Second
	defaultMaxToolTimeout     = 5 * time.Minute
	defaultToolListPageSize   = 100
	maxDefinitionFiles        = 128
)

// Config controls the optional Streamable HTTP MCP endpoint.
type Config struct {
	Enabled                 bool
	ListenAddress           string
	EndpointPath            string
	DefinitionFiles         []string
	AllowedOrigins          []string
	AllowedHostCapabilities []string
	MaxRequestBytes         int64
	MaxResponseBytes        int64
	MaxConcurrentCalls      int
	RequestsPerSecond       float64
	RequestBurst            int
	DefaultToolTimeout      time.Duration
	MaxToolTimeout          time.Duration
	ToolListPageSize        int
	TLSCertificateFile      string
	TLSKeyFile              string
	Token                   string
}

// ApplyDefaults fills optional limits without enabling MCP.
func (c *Config) ApplyDefaults() {
	if c.ListenAddress == "" {
		c.ListenAddress = defaultListenAddress
	}
	if c.EndpointPath == "" {
		c.EndpointPath = defaultEndpointPath
	}
	if c.MaxRequestBytes == 0 {
		c.MaxRequestBytes = defaultMaxRequestBytes
	}
	if c.MaxResponseBytes == 0 {
		c.MaxResponseBytes = defaultMaxResponseBytes
	}
	if c.MaxConcurrentCalls == 0 {
		c.MaxConcurrentCalls = defaultMaxConcurrentCalls
	}
	if c.RequestsPerSecond == 0 {
		c.RequestsPerSecond = defaultRequestsPerSecond
	}
	if c.RequestBurst == 0 {
		c.RequestBurst = defaultRequestBurst
	}
	if c.DefaultToolTimeout == 0 {
		c.DefaultToolTimeout = defaultToolTimeout
	}
	if c.MaxToolTimeout == 0 {
		c.MaxToolTimeout = defaultMaxToolTimeout
	}
	if c.ToolListPageSize == 0 {
		c.ToolListPageSize = defaultToolListPageSize
	}
}

// ValidateConfig checks transport and resource-limit invariants.
func ValidateConfig(config Config, grpcAddress string) error {
	if !config.Enabled {
		return nil
	}
	config.ApplyDefaults()
	if len(config.DefinitionFiles) == 0 {
		return errors.New("mcp.definition_files must contain at least one file")
	}
	if len(config.DefinitionFiles) > maxDefinitionFiles {
		return fmt.Errorf("mcp.definition_files may contain at most %d files", maxDefinitionFiles)
	}
	if config.Token == "" {
		return errors.New("mcp requires a non-empty bearer token; configure mcp.token_file or auth.token_file")
	}
	if config.ListenAddress == grpcAddress {
		return errors.New("mcp and gRPC listen addresses must differ")
	}
	if _, _, err := net.SplitHostPort(config.ListenAddress); err != nil {
		return fmt.Errorf("invalid mcp.listen_address: %w", err)
	}
	if !strings.HasPrefix(config.EndpointPath, "/") || config.EndpointPath == "/" || strings.ContainsAny(config.EndpointPath, "?#") || path.Clean(config.EndpointPath) != config.EndpointPath {
		return errors.New("mcp.endpoint_path must be a clean absolute HTTP path other than /")
	}
	if config.MaxRequestBytes <= 0 || config.MaxRequestBytes > 64<<20 {
		return errors.New("mcp.max_request_bytes must be between 1 and 67108864")
	}
	if config.MaxResponseBytes < minimumResponseBytes || config.MaxResponseBytes > 64<<20 {
		return errors.New("mcp.max_response_bytes must be between 16384 and 67108864")
	}
	if config.MaxConcurrentCalls <= 0 || config.MaxConcurrentCalls > 4096 {
		return errors.New("mcp.max_concurrent_calls must be between 1 and 4096")
	}
	if config.RequestsPerSecond <= 0 || config.RequestsPerSecond > 100000 || math.IsNaN(config.RequestsPerSecond) || math.IsInf(config.RequestsPerSecond, 0) {
		return errors.New("mcp.requests_per_second must be greater than 0 and at most 100000")
	}
	if config.RequestBurst <= 0 || config.RequestBurst > 100000 {
		return errors.New("mcp.request_burst must be between 1 and 100000")
	}
	if config.DefaultToolTimeout <= 0 || config.MaxToolTimeout <= 0 || config.DefaultToolTimeout > config.MaxToolTimeout {
		return errors.New("mcp tool timeouts must be positive and default_tool_timeout must not exceed max_tool_timeout")
	}
	if config.MaxToolTimeout > time.Hour {
		return errors.New("mcp.max_tool_timeout must not exceed 1h")
	}
	if config.ToolListPageSize <= 0 || config.ToolListPageSize > 500 {
		return errors.New("mcp.tool_list_page_size must be between 1 and 500")
	}
	if (config.TLSCertificateFile == "") != (config.TLSKeyFile == "") {
		return errors.New("mcp TLS certificate and key must be provided together")
	}
	seenOrigins := make(map[string]struct{}, len(config.AllowedOrigins))
	for _, origin := range config.AllowedOrigins {
		normalized, err := normalizeOrigin(origin)
		if err != nil {
			return fmt.Errorf("invalid mcp.allowed_origins entry %q: %w", origin, err)
		}
		if _, exists := seenOrigins[normalized]; exists {
			return fmt.Errorf("duplicate mcp.allowed_origins entry %q", origin)
		}
		seenOrigins[normalized] = struct{}{}
	}
	seenCapabilities := make(map[string]struct{}, len(config.AllowedHostCapabilities))
	for _, capability := range config.AllowedHostCapabilities {
		if _, ok := capabilityCatalog[capability]; !ok {
			return fmt.Errorf("unknown mcp host capability %q", capability)
		}
		if _, exists := seenCapabilities[capability]; exists {
			return fmt.Errorf("duplicate mcp host capability %q", capability)
		}
		seenCapabilities[capability] = struct{}{}
	}
	return nil
}

func normalizeOrigin(value string) (string, error) {
	parsed, err := url.Parse(value)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" || parsed.User != nil || parsed.RawQuery != "" || parsed.Fragment != "" || (parsed.Path != "" && parsed.Path != "/") {
		return "", errors.New("origin must contain only scheme and authority")
	}
	scheme := strings.ToLower(parsed.Scheme)
	if scheme != "http" && scheme != "https" {
		return "", errors.New("origin scheme must be http or https")
	}
	host := strings.ToLower(parsed.Hostname())
	if host == "" || strings.Contains(host, "*") || strings.EqualFold(value, "null") {
		return "", errors.New("wildcard and null origins are not allowed")
	}
	port := parsed.Port()
	if (scheme == "http" && port == "80") || (scheme == "https" && port == "443") {
		port = ""
	}
	if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	if port != "" {
		host = net.JoinHostPort(strings.Trim(host, "[]"), port)
	}
	return scheme + "://" + host, nil
}
