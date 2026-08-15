package mcpserver

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/expr-lang/expr/vm"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	maxToolsPerModule           = 128
	maxRegistryTools            = 512
	maxDescription              = 16 << 10
	maxRegistryDescriptionBytes = 2 << 20
)

var (
	namespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,62}$`)
	toolNamePattern  = regexp.MustCompile(`^[A-Za-z0-9_.-]+$`)
)

// Prepared is a fully validated, immutable MCP definition registry.
type Prepared struct {
	Config   Config
	Registry *Registry
}

// Registry contains tools sorted by fully-qualified name.
type Registry struct {
	byName  map[string]*CompiledTool
	ordered []*CompiledTool
	digest  [sha256.Size]byte
}

// CompiledTool is an immutable tool definition and Expr program.
type CompiledTool struct {
	Name              string
	Title             string
	Description       string
	Capabilities      []string
	Annotations       ToolAnnotations
	InputSchemaJSON   json.RawMessage
	OutputSchemaJSON  json.RawMessage
	InputSchemaValue  any
	OutputSchemaValue any
	InputValidator    *jsonschema.Schema
	OutputValidator   *jsonschema.Schema
	Program           *vm.Program
	Policy            ScriptPolicy
	Timeout           time.Duration
	MaxConcurrency    int
}

// Prepare validates definition files, schemas and scripts without binding a listener.
func Prepare(config Config, workspace, grpcAddress string) (*Prepared, error) {
	if !config.Enabled {
		return &Prepared{Config: config}, nil
	}
	config.ApplyDefaults()
	if err := ValidateConfig(config, grpcAddress); err != nil {
		return nil, err
	}
	documents, err := loadDefinitionFiles(config.DefinitionFiles, workspace)
	if err != nil {
		return nil, err
	}
	allowed := make(map[string]struct{}, len(config.AllowedHostCapabilities))
	for _, capability := range config.AllowedHostCapabilities {
		allowed[capability] = struct{}{}
	}
	registry := &Registry{byName: make(map[string]*CompiledTool)}
	descriptionBytes := 0
	for moduleIndex, document := range documents {
		if document.Version != 1 {
			return nil, fmt.Errorf("MCP definition %q version must be 1", config.DefinitionFiles[moduleIndex])
		}
		if !namespacePattern.MatchString(document.Namespace) {
			return nil, fmt.Errorf("MCP definition %q has invalid namespace", config.DefinitionFiles[moduleIndex])
		}
		if document.Language != "expr" {
			return nil, fmt.Errorf("MCP definition %q language must be expr", config.DefinitionFiles[moduleIndex])
		}
		if len(document.Tools) == 0 || len(document.Tools) > maxToolsPerModule {
			return nil, fmt.Errorf("MCP definition %q must contain between 1 and %d tools", config.DefinitionFiles[moduleIndex], maxToolsPerModule)
		}
		for toolIndex, definition := range document.Tools {
			tool, err := compileTool(config, document.Namespace, definition)
			if err != nil {
				return nil, fmt.Errorf("MCP definition %q tool %d: %w", config.DefinitionFiles[moduleIndex], toolIndex, err)
			}
			for _, capability := range tool.Capabilities {
				if _, ok := allowed[capability]; !ok {
					return nil, fmt.Errorf("tool %q requires capability %q outside mcp.allowed_host_capabilities", tool.Name, capability)
				}
			}
			if _, exists := registry.byName[tool.Name]; exists {
				return nil, fmt.Errorf("duplicate MCP tool name %q", tool.Name)
			}
			registry.byName[tool.Name] = tool
			registry.ordered = append(registry.ordered, tool)
			descriptionBytes += len(tool.Title) + len(tool.Description)
			if descriptionBytes > maxRegistryDescriptionBytes {
				return nil, fmt.Errorf("MCP registry descriptions exceed %d bytes", maxRegistryDescriptionBytes)
			}
			if len(registry.ordered) > maxRegistryTools {
				return nil, fmt.Errorf("MCP registry contains more than %d tools", maxRegistryTools)
			}
		}
	}
	sort.Slice(registry.ordered, func(i, j int) bool { return registry.ordered[i].Name < registry.ordered[j].Name })
	hash := sha256.New()
	for _, tool := range registry.ordered {
		encoded, _ := json.Marshal([]any{tool.Name, tool.Title, tool.Description, tool.Capabilities, tool.InputSchemaJSON, tool.OutputSchemaJSON})
		hash.Write(encoded)
	}
	copy(registry.digest[:], hash.Sum(nil))
	return &Prepared{Config: config, Registry: registry}, nil
}

func compileTool(config Config, namespace string, definition toolDocument) (*CompiledTool, error) {
	name := namespace + "." + definition.Name
	if definition.Name == "" || len(name) > 128 || !toolNamePattern.MatchString(name) {
		return nil, errors.New("tool name is invalid")
	}
	if strings.TrimSpace(definition.Title) == "" || strings.TrimSpace(definition.Description) == "" {
		return nil, errors.New("title and description are required")
	}
	if len(definition.Title) > 1024 || len(definition.Description) > maxDescription {
		return nil, errors.New("title or description exceeds its size limit")
	}
	if definition.Capabilities == nil {
		return nil, errors.New("capabilities must be explicitly provided")
	}
	seenCapabilities := make(map[string]struct{}, len(*definition.Capabilities))
	for _, capability := range *definition.Capabilities {
		if _, ok := capabilityCatalog[capability]; !ok {
			return nil, fmt.Errorf("unknown capability %q", capability)
		}
		if _, duplicate := seenCapabilities[capability]; duplicate {
			return nil, fmt.Errorf("duplicate capability %q", capability)
		}
		seenCapabilities[capability] = struct{}{}
	}
	if definition.Annotations == nil || definition.Annotations.ReadOnly == nil || definition.Annotations.Destructive == nil || definition.Annotations.Idempotent == nil || definition.Annotations.OpenWorld == nil {
		return nil, errors.New("all four annotations must be explicitly provided")
	}
	timeout := config.DefaultToolTimeout
	if definition.Timeout != "" {
		parsed, err := time.ParseDuration(definition.Timeout)
		if err != nil || parsed <= 0 {
			return nil, errors.New("timeout must be a positive Go duration")
		}
		timeout = parsed
	}
	if timeout > config.MaxToolTimeout {
		return nil, fmt.Errorf("timeout exceeds the global maximum %s", config.MaxToolTimeout)
	}
	if definition.MaxConcurrency < 0 || definition.MaxConcurrency > config.MaxConcurrentCalls {
		return nil, fmt.Errorf("max_concurrency must be between 0 and %d", config.MaxConcurrentCalls)
	}
	inputValidator, inputValue, err := compileSchema(name+"-input", definition.InputSchema, true)
	if err != nil {
		return nil, fmt.Errorf("input_schema: %w", err)
	}
	outputValidator, outputValue, err := compileSchema(name+"-output", definition.OutputSchema, false)
	if err != nil {
		return nil, fmt.Errorf("output_schema: %w", err)
	}
	program, policy, err := compileScript(definition.Script, *definition.Capabilities)
	if err != nil {
		return nil, err
	}
	annotations := ToolAnnotations{ReadOnly: *definition.Annotations.ReadOnly, Destructive: *definition.Annotations.Destructive, Idempotent: *definition.Annotations.Idempotent, OpenWorld: *definition.Annotations.OpenWorld}
	if policy.Strongest == effectRead && (!annotations.ReadOnly || annotations.Destructive) {
		return nil, errors.New("read-only script requires read_only=true and destructive=false")
	}
	if policy.Strongest != effectRead && annotations.ReadOnly {
		return nil, errors.New("mutating script cannot declare read_only=true")
	}
	if policy.Strongest == effectDestructive && !annotations.Destructive {
		return nil, errors.New("destructive script requires destructive=true")
	}
	if policy.Strongest == effectMutate && annotations.Destructive {
		return nil, errors.New("non-destructive mutation requires destructive=false")
	}
	_, startsDirectProcess := seenCapabilities["processes.start"]
	_, startsTemplateProcess := seenCapabilities["process_templates.start"]
	startsProcess := startsDirectProcess || startsTemplateProcess
	if startsProcess && annotations.Idempotent {
		return nil, errors.New("process start capabilities require idempotent=false")
	}
	if annotations.OpenWorld != startsProcess {
		return nil, fmt.Errorf("open_world must be %t for this script", startsProcess)
	}
	return &CompiledTool{
		Name: name, Title: definition.Title, Description: definition.Description,
		Capabilities: append([]string(nil), (*definition.Capabilities)...), Annotations: annotations,
		InputSchemaJSON: append(json.RawMessage(nil), definition.InputSchema...), OutputSchemaJSON: append(json.RawMessage(nil), definition.OutputSchema...),
		InputSchemaValue: inputValue, OutputSchemaValue: outputValue,
		InputValidator: inputValidator, OutputValidator: outputValidator, Program: program, Policy: policy,
		Timeout: timeout, MaxConcurrency: definition.MaxConcurrency,
	}, nil
}

func (r *Registry) tool(name string) (*CompiledTool, bool) {
	tool, ok := r.byName[name]
	return tool, ok
}

func compactRaw(raw json.RawMessage) json.RawMessage {
	var buffer bytes.Buffer
	if json.Compact(&buffer, raw) == nil {
		return buffer.Bytes()
	}
	return append(json.RawMessage(nil), raw...)
}
