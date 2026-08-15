package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"mime"
	"net/http"
	"net/url"
	"path/filepath"
	"strings"

	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/qq1426155093/remote-code/internal/files"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	workspaceResourceTemplate = "workspace:///{+path}"
	responseEnvelopeReserve   = int64(8 << 10)
)

func registerWorkspaceResources(server *mcpsdk.Server, fileService *files.Service, config Config, slots chan struct{}) {
	if !containsCapability(config.AllowedHostCapabilities, "files.read") {
		return
	}
	server.AddResourceTemplate(&mcpsdk.ResourceTemplate{
		Name: "workspace-file", Title: "Workspace file",
		Description: "Read a bounded regular workspace file as binary content. Paths are workspace-relative and cannot escape through symbolic links.",
		URITemplate: workspaceResourceTemplate,
	}, func(ctx context.Context, request *mcpsdk.ReadResourceRequest) (*mcpsdk.ReadResourceResult, error) {
		if request == nil || request.Params == nil || !acquireSlot(ctx, slots) {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			return nil, mcpsdk.ResourceNotFoundError("")
		}
		defer releaseSlot(slots)
		name, err := workspaceResourcePath(request.Params.URI)
		if err != nil {
			return nil, mcpsdk.ResourceNotFoundError(request.Params.URI)
		}
		encodedURI, err := json.Marshal(request.Params.URI)
		if err != nil {
			return nil, mcpsdk.ResourceNotFoundError(request.Params.URI)
		}
		maximum := maximumResourceBytes(config.MaxResponseBytes - int64(len(encodedURI)))
		if uploadMaximum := fileService.MaxUploadBytes(); maximum > uploadMaximum {
			maximum = uploadMaximum
		}
		if maximum <= 0 {
			return nil, errors.New("workspace resource URI exceeds its response budget")
		}
		readCtx, cancel := context.WithTimeout(ctx, config.DefaultToolTimeout)
		defer cancel()
		read, err := fileService.ReadBytes(readCtx, name, maximum)
		if err != nil {
			switch status.Code(err) {
			case codes.InvalidArgument, codes.NotFound, codes.FailedPrecondition:
				return nil, mcpsdk.ResourceNotFoundError(request.Params.URI)
			case codes.ResourceExhausted:
				return nil, fmt.Errorf("workspace resource exceeds its response budget")
			case codes.Canceled, codes.DeadlineExceeded:
				if contextErr := readCtx.Err(); contextErr != nil {
					return nil, contextErr
				}
				return nil, errors.New("workspace resource read was interrupted")
			default:
				return nil, errors.New("workspace resource could not be read")
			}
		}
		mimeType := mime.TypeByExtension(filepath.Ext(name))
		if mimeType == "" {
			mimeType = http.DetectContentType(read.Data)
		}
		return &mcpsdk.ReadResourceResult{
			Cacheable: mcpsdk.Cacheable{CacheScope: "private"},
			Contents:  []*mcpsdk.ResourceContents{{URI: request.Params.URI, MIMEType: mimeType, Blob: read.Data}},
		}, nil
	})
}

func workspaceResourcePath(rawURI string) (string, error) {
	parsed, err := url.Parse(rawURI)
	if err != nil || parsed.Scheme != "workspace" || parsed.Host != "" || parsed.User != nil || parsed.RawQuery != "" || parsed.ForceQuery ||
		parsed.Fragment != "" || strings.Contains(rawURI, "#") || parsed.Opaque != "" {
		return "", errors.New("invalid workspace resource URI")
	}
	decoded, err := url.PathUnescape(parsed.EscapedPath())
	if err != nil || !strings.HasPrefix(decoded, "/") || decoded == "/" {
		return "", errors.New("invalid workspace resource path")
	}
	return strings.TrimPrefix(decoded, "/"), nil
}

func maximumResourceBytes(maxResponse int64) int64 {
	if maxResponse <= responseEnvelopeReserve {
		return 0
	}
	// Blob content is base64 encoded on the wire. Reserve space for the JSON-RPC
	// envelope, URI, MIME type, and response metadata.
	return (maxResponse - responseEnvelopeReserve) * 3 / 4
}

func containsCapability(values []string, target string) bool {
	for _, value := range values {
		if value == target {
			return true
		}
	}
	return false
}
