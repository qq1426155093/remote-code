package mcpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"

	"go.yaml.in/yaml/v4"
)

const (
	maxDefinitionBytes      = int64(1 << 20)
	maxDefinitionTotalBytes = int64(8 << 20)
	maxYAMLNodes            = 100_000
	maxYAMLDepth            = 64
	maxYAMLScalarBytes      = 256 << 10
	maxYAMLCollectionItems  = 10_000
	maxScriptBytes          = 64 << 10
)

var jsonNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

type physicalFile struct {
	path string
	dev  uint64
	ino  uint64
}

func loadDefinitionFiles(paths []string, workspace string) ([]definitionDocument, error) {
	workspacePath, err := filepath.EvalSymlinks(workspace)
	if err != nil {
		return nil, fmt.Errorf("resolve workspace: %w", err)
	}
	workspacePath, err = filepath.Abs(workspacePath)
	if err != nil {
		return nil, fmt.Errorf("make workspace path absolute: %w", err)
	}
	var total int64
	seen := make(map[[2]uint64]string, len(paths))
	documents := make([]definitionDocument, 0, len(paths))
	for _, configuredPath := range paths {
		if !strings.HasSuffix(configuredPath, ".mcp.yaml") {
			return nil, fmt.Errorf("MCP definition %q must end in .mcp.yaml", configuredPath)
		}
		contents, physical, err := readDefinitionFile(configuredPath)
		if err != nil {
			return nil, err
		}
		total += int64(len(contents))
		if total > maxDefinitionTotalBytes {
			return nil, fmt.Errorf("MCP definitions exceed the %d byte total limit", maxDefinitionTotalBytes)
		}
		key := [2]uint64{physical.dev, physical.ino}
		if earlier, exists := seen[key]; exists {
			return nil, fmt.Errorf("MCP definitions %q and %q refer to the same file", earlier, configuredPath)
		}
		seen[key] = configuredPath
		if pathWithin(workspacePath, physical.path) {
			return nil, fmt.Errorf("MCP definition %q must be outside the workspace", configuredPath)
		}
		document, err := decodeDefinition(configuredPath, contents)
		if err != nil {
			return nil, err
		}
		documents = append(documents, document)
	}
	return documents, nil
}

func pathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func decodeDefinition(name string, contents []byte) (definitionDocument, error) {
	if !utf8.Valid(contents) {
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: input is not valid UTF-8", name)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "%") {
			return definitionDocument{}, fmt.Errorf("decode MCP definition %q: YAML directives are not allowed", name)
		}
		break
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: %w", name, err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return definitionDocument{}, fmt.Errorf("decode MCP definition %q: exactly one YAML document is required", name)
		}
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: %w", name, err)
	}
	state := yamlValidationState{name: name}
	value, err := state.convert(&root, 0, "")
	if err != nil {
		return definitionDocument{}, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: root must be a mapping", name)
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return definitionDocument{}, fmt.Errorf("normalize MCP definition %q: %w", name, err)
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(canonical))
	jsonDecoder.DisallowUnknownFields()
	var document definitionDocument
	if err := jsonDecoder.Decode(&document); err != nil {
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: %w", name, err)
	}
	if err := ensureJSONEOF(jsonDecoder); err != nil {
		return definitionDocument{}, fmt.Errorf("decode MCP definition %q: %w", name, err)
	}
	return document, nil
}

type yamlValidationState struct {
	name  string
	nodes int
}

func (s *yamlValidationState) convert(node *yaml.Node, depth int, pointer string) (any, error) {
	if node == nil {
		return nil, s.nodeError(node, "nil YAML node")
	}
	s.nodes++
	if s.nodes > maxYAMLNodes {
		return nil, s.nodeError(node, fmt.Sprintf("document contains more than %d nodes", maxYAMLNodes))
	}
	if depth > maxYAMLDepth {
		return nil, s.nodeError(node, fmt.Sprintf("document exceeds maximum depth %d", maxYAMLDepth))
	}
	if node.Anchor != "" || node.Alias != nil || node.Kind == yaml.AliasNode {
		return nil, s.nodeError(node, "anchors and aliases are not allowed")
	}
	if node.Style&yaml.TaggedStyle != 0 {
		return nil, s.nodeError(node, "explicit YAML tags are not allowed")
	}
	switch node.Kind {
	case yaml.DocumentNode:
		if len(node.Content) != 1 {
			return nil, s.nodeError(node, "document must contain exactly one root value")
		}
		return s.convert(node.Content[0], depth, pointer)
	case yaml.MappingNode:
		if len(node.Content)%2 != 0 || len(node.Content)/2 > maxYAMLCollectionItems {
			return nil, s.nodeError(node, fmt.Sprintf("mapping may contain at most %d entries", maxYAMLCollectionItems))
		}
		result := make(map[string]any, len(node.Content)/2)
		for index := 0; index < len(node.Content); index += 2 {
			key := node.Content[index]
			if key.Kind != yaml.ScalarNode || key.Tag != "!!str" {
				return nil, s.nodeError(key, "mapping keys must be strings")
			}
			if key.Value == "<<" {
				return nil, s.nodeError(key, "YAML merge keys are not allowed")
			}
			if _, exists := result[key.Value]; exists {
				return nil, s.nodeError(key, fmt.Sprintf("duplicate mapping key %q", key.Value))
			}
			childPointer := pointer + "/" + escapeJSONPointer(key.Value)
			if key.Value == "script" && (node.Content[index+1].Style&yaml.FoldedStyle != 0 ||
				(strings.Contains(node.Content[index+1].Value, "\n") && node.Content[index+1].Style&yaml.LiteralStyle == 0)) {
				return nil, s.nodeError(node.Content[index+1], "multi-line script must use a literal block scalar")
			}
			value, err := s.convert(node.Content[index+1], depth+1, childPointer)
			if err != nil {
				return nil, err
			}
			result[key.Value] = value
		}
		return result, nil
	case yaml.SequenceNode:
		if len(node.Content) > maxYAMLCollectionItems {
			return nil, s.nodeError(node, fmt.Sprintf("sequence may contain at most %d items", maxYAMLCollectionItems))
		}
		result := make([]any, len(node.Content))
		for index, child := range node.Content {
			value, err := s.convert(child, depth+1, pointer+"/"+strconv.Itoa(index))
			if err != nil {
				return nil, err
			}
			result[index] = value
		}
		return result, nil
	case yaml.ScalarNode:
		return s.convertScalar(node, pointer)
	default:
		return nil, s.nodeError(node, "unsupported YAML node")
	}
}

func (s *yamlValidationState) convertScalar(node *yaml.Node, pointer string) (any, error) {
	if len(node.Value) > maxYAMLScalarBytes {
		return nil, s.nodeError(node, fmt.Sprintf("scalar exceeds the %d byte limit", maxYAMLScalarBytes))
	}
	if strings.HasSuffix(pointer, "/script") && len(node.Value) > maxScriptBytes {
		return nil, s.nodeError(node, fmt.Sprintf("script exceeds the %d byte limit", maxScriptBytes))
	}
	switch node.Tag {
	case "!!str":
		return node.Value, nil
	case "!!null":
		if node.Value != "null" {
			return nil, s.nodeError(node, "null must use the canonical JSON spelling null")
		}
		return nil, nil
	case "!!bool":
		if node.Value == "true" {
			return true, nil
		}
		if node.Value == "false" {
			return false, nil
		}
		return nil, s.nodeError(node, "boolean must use true or false")
	case "!!int":
		if !jsonNumberPattern.MatchString(node.Value) || strings.ContainsAny(node.Value, ".eE") {
			return nil, s.nodeError(node, "integer must be a canonical decimal JSON integer")
		}
		value, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return nil, s.nodeError(node, "integer is outside the signed 64-bit range")
		}
		return value, nil
	case "!!float":
		if !jsonNumberPattern.MatchString(node.Value) {
			return nil, s.nodeError(node, "number must use JSON number syntax")
		}
		value, err := strconv.ParseFloat(node.Value, 64)
		if err != nil || math.IsInf(value, 0) || math.IsNaN(value) {
			return nil, s.nodeError(node, "number must be finite")
		}
		return value, nil
	default:
		return nil, s.nodeError(node, fmt.Sprintf("YAML type %q is not allowed", node.Tag))
	}
}

func (s *yamlValidationState) nodeError(node *yaml.Node, message string) error {
	if node != nil && node.Line > 0 {
		return fmt.Errorf("decode MCP definition %q at line %d, column %d: %s", s.name, node.Line, node.Column, message)
	}
	return fmt.Errorf("decode MCP definition %q: %s", s.name, message)
}

func escapeJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
