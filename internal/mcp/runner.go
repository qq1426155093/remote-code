package mcpserver

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"runtime/debug"
	"sort"
	"strings"
	"time"

	"github.com/expr-lang/expr"
	mcpsdk "github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/qq1426155093/remote-code/internal/auth"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type runner struct {
	hosts       HostDispatcher
	globalSlots chan struct{}
	toolSlots   map[string]chan struct{}
	maxResponse int64
}

func newRunner(config Config, registry *Registry, hosts HostDispatcher) *runner {
	result := &runner{
		hosts: hosts, globalSlots: make(chan struct{}, config.MaxConcurrentCalls),
		toolSlots: make(map[string]chan struct{}), maxResponse: config.MaxResponseBytes,
	}
	for _, tool := range registry.ordered {
		if tool.MaxConcurrency > 0 {
			result.toolSlots[tool.Name] = make(chan struct{}, tool.MaxConcurrency)
		}
	}
	return result
}

func (r *runner) call(ctx context.Context, tool *CompiledTool, raw json.RawMessage, clientName string) (result *mcpsdk.CallToolResult) {
	started := time.Now()
	callID := invocationID()
	resultIsError := true
	defer func() {
		if recover() != nil {
			log.Printf("MCP tool panic id=%s tool=%q stack=%q", callID, tool.Name, debug.Stack())
			result = toolError("tool execution failed")
			resultIsError = true
		}
		principal, _ := auth.PrincipalFromContext(ctx)
		log.Printf("MCP tool call id=%s tool=%q client=%q principal=%q error=%t duration_ms=%d", callID, tool.Name, clientName, principal.ID, resultIsError, time.Since(started).Milliseconds())
	}()
	arguments, err := decodeJSONValue(raw)
	if err != nil {
		return toolError("arguments must contain one valid JSON object")
	}
	argumentMap, ok := arguments.(map[string]any)
	if !ok {
		return toolError("arguments must be a JSON object")
	}
	if err := tool.InputValidator.Validate(arguments); err != nil {
		return toolError(safeSchemaError(err))
	}
	if !acquireSlot(ctx, r.globalSlots) {
		return toolError(contextMessage(ctx.Err()))
	}
	defer releaseSlot(r.globalSlots)
	if slots := r.toolSlots[tool.Name]; slots != nil {
		if !acquireSlot(ctx, slots) {
			return toolError(contextMessage(ctx.Err()))
		}
		defer releaseSlot(slots)
	}
	callCtx, cancel := context.WithTimeout(ctx, tool.Timeout)
	defer cancel()
	callCtx = withInvocation(callCtx, r.hosts, tool.Capabilities)
	environment := scriptEnvironment{
		Context: callCtx, Args: argumentMap,
		Call: scriptCallMetadata{ID: callID, Tool: tool.Name, ClientName: clientName},
	}
	output, err := expr.Run(tool.Program, environment)
	if err != nil {
		return toolError(safeExecutionError(err, callCtx))
	}
	nodes := 0
	normalized, err := normalizeJSONValue(output, 0, &nodes)
	if err != nil {
		return toolError("tool returned an unsupported or oversized result")
	}
	if tool.OutputValidator != nil {
		if err := tool.OutputValidator.Validate(normalized); err != nil {
			return toolError("tool result did not match its output schema")
		}
	}
	encoded, err := json.Marshal(normalized)
	if err != nil || int64(len(encoded)) > r.maxResponse {
		return toolError("tool result exceeded the response size limit")
	}
	resultIsError = false
	return &mcpsdk.CallToolResult{
		Content:           []mcpsdk.Content{&mcpsdk.TextContent{Text: string(encoded)}},
		StructuredContent: normalized,
	}
}

func safeSchemaError(err error) string {
	var validation *jsonschema.ValidationError
	if !errors.As(err, &validation) {
		return "arguments do not match the tool input schema"
	}
	locations := make(map[string]struct{})
	var collect func(*jsonschema.ValidationError)
	collect = func(current *jsonschema.ValidationError) {
		if len(current.Causes) == 0 {
			parts := make([]string, len(current.InstanceLocation))
			for index, part := range current.InstanceLocation {
				parts[index] = escapeJSONPointer(part)
			}
			location := "/" + strings.Join(parts, "/")
			if len(parts) == 0 {
				location = "/"
			}
			locations[location] = struct{}{}
			return
		}
		for _, cause := range current.Causes {
			collect(cause)
		}
	}
	collect(validation)
	ordered := make([]string, 0, len(locations))
	for location := range locations {
		ordered = append(ordered, location)
	}
	sort.Strings(ordered)
	if len(ordered) > 8 {
		ordered = ordered[:8]
	}
	if len(ordered) == 0 {
		return "arguments do not match the tool input schema"
	}
	return "arguments do not match the tool input schema at " + strings.Join(ordered, ", ")
}

func acquireSlot(ctx context.Context, slots chan struct{}) bool {
	select {
	case slots <- struct{}{}:
		return true
	case <-ctx.Done():
		return false
	}
}

func releaseSlot(slots chan struct{}) { <-slots }

func toolError(message string) *mcpsdk.CallToolResult {
	return &mcpsdk.CallToolResult{Content: []mcpsdk.Content{&mcpsdk.TextContent{Text: message}}, IsError: true}
}

func safeExecutionError(err error, ctx context.Context) string {
	if ctx.Err() != nil {
		return contextMessage(ctx.Err())
	}
	switch status.Code(err) {
	case codes.InvalidArgument, codes.NotFound, codes.AlreadyExists, codes.FailedPrecondition,
		codes.ResourceExhausted, codes.PermissionDenied, codes.OutOfRange, codes.Unimplemented:
		return boundedError(err, 768)
	case codes.Canceled, codes.DeadlineExceeded:
		return contextMessage(err)
	default:
		return "tool execution failed"
	}
}

func contextMessage(err error) string {
	if errors.Is(err, context.DeadlineExceeded) || status.Code(err) == codes.DeadlineExceeded {
		return "tool execution timed out"
	}
	return "tool execution was canceled"
}

func boundedError(err error, maximum int) string {
	message := err.Error()
	if len(message) > maximum {
		message = message[:maximum] + "..."
	}
	return message
}

func invocationID() string {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return fmt.Sprintf("fallback-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(value)
}
