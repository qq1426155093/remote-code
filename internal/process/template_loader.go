package process

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
	maxProcessTemplateDefinitionBytes      = int64(1 << 20)
	maxProcessTemplateDefinitionTotalBytes = int64(8 << 20)
	maxProcessTemplateYAMLNodes            = 100_000
	maxProcessTemplateYAMLDepth            = 64
	maxProcessTemplateYAMLScalarBytes      = 256 << 10
	maxProcessTemplateYAMLCollectionItems  = 10_000
)

var processTemplateJSONNumberPattern = regexp.MustCompile(`^-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$`)

type processTemplatePhysicalFile struct {
	path string
	dev  uint64
	ino  uint64
}

func loadProcessTemplateDefinitionFiles(paths []string, workspace string) ([]processTemplateDocument, error) {
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
	documents := make([]processTemplateDocument, 0, len(paths))
	for _, configuredPath := range paths {
		if !strings.HasSuffix(configuredPath, ".process-template.yaml") {
			return nil, fmt.Errorf("process template definition %q must end in .process-template.yaml", configuredPath)
		}
		contents, physical, err := readProcessTemplateDefinitionFile(configuredPath)
		if err != nil {
			return nil, err
		}
		total += int64(len(contents))
		if total > maxProcessTemplateDefinitionTotalBytes {
			return nil, fmt.Errorf("process template definitions exceed the %d byte total limit", maxProcessTemplateDefinitionTotalBytes)
		}
		key := [2]uint64{physical.dev, physical.ino}
		if earlier, duplicate := seen[key]; duplicate {
			return nil, fmt.Errorf("process template definitions %q and %q refer to the same file", earlier, configuredPath)
		}
		seen[key] = configuredPath
		if processTemplatePathWithin(workspacePath, physical.path) {
			return nil, fmt.Errorf("process template definition %q must be outside the workspace", configuredPath)
		}
		document, err := decodeProcessTemplateDefinition(configuredPath, contents)
		if err != nil {
			return nil, err
		}
		documents = append(documents, document)
	}
	return documents, nil
}

func processTemplatePathWithin(parent, candidate string) bool {
	relative, err := filepath.Rel(parent, candidate)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func decodeProcessTemplateDefinition(name string, contents []byte) (processTemplateDocument, error) {
	if !utf8.Valid(contents) {
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: input is not valid UTF-8", name)
	}
	for _, line := range strings.Split(string(contents), "\n") {
		trimmed := strings.TrimSpace(line)
		if trimmed == "" || strings.HasPrefix(trimmed, "#") {
			continue
		}
		if strings.HasPrefix(trimmed, "%") {
			return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: YAML directives are not allowed", name)
		}
		break
	}
	decoder := yaml.NewDecoder(bytes.NewReader(contents))
	var root yaml.Node
	if err := decoder.Decode(&root); err != nil {
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: %w", name, err)
	}
	var trailing yaml.Node
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: exactly one YAML document is required", name)
		}
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: %w", name, err)
	}
	state := processTemplateYAMLValidationState{name: name}
	value, err := state.convert(&root, 0, "")
	if err != nil {
		return processTemplateDocument{}, err
	}
	object, ok := value.(map[string]any)
	if !ok {
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: root must be a mapping", name)
	}
	canonical, err := json.Marshal(object)
	if err != nil {
		return processTemplateDocument{}, fmt.Errorf("normalize process template definition %q: %w", name, err)
	}
	jsonDecoder := json.NewDecoder(bytes.NewReader(canonical))
	jsonDecoder.DisallowUnknownFields()
	var document processTemplateDocument
	if err := jsonDecoder.Decode(&document); err != nil {
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: %w", name, err)
	}
	if err := ensureTemplateJSONEOF(jsonDecoder); err != nil {
		return processTemplateDocument{}, fmt.Errorf("decode process template definition %q: %w", name, err)
	}
	return document, nil
}

type processTemplateYAMLValidationState struct {
	name  string
	nodes int
}

func (s *processTemplateYAMLValidationState) convert(node *yaml.Node, depth int, pointer string) (any, error) {
	if node == nil {
		return nil, s.nodeError(node, "nil YAML node")
	}
	s.nodes++
	if s.nodes > maxProcessTemplateYAMLNodes {
		return nil, s.nodeError(node, fmt.Sprintf("document contains more than %d nodes", maxProcessTemplateYAMLNodes))
	}
	if depth > maxProcessTemplateYAMLDepth {
		return nil, s.nodeError(node, fmt.Sprintf("document exceeds maximum depth %d", maxProcessTemplateYAMLDepth))
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
		if len(node.Content)%2 != 0 || len(node.Content)/2 > maxProcessTemplateYAMLCollectionItems {
			return nil, s.nodeError(node, fmt.Sprintf("mapping may contain at most %d entries", maxProcessTemplateYAMLCollectionItems))
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
			if _, duplicate := result[key.Value]; duplicate {
				return nil, s.nodeError(key, fmt.Sprintf("duplicate mapping key %q", key.Value))
			}
			childPointer := pointer + "/" + escapeProcessTemplateJSONPointer(key.Value)
			child := node.Content[index+1]
			if key.Value == "render" && (child.Style&yaml.FoldedStyle != 0 ||
				(strings.Contains(child.Value, "\n") && child.Style&yaml.LiteralStyle == 0)) {
				return nil, s.nodeError(child, "multi-line render expression must use a literal block scalar")
			}
			value, err := s.convert(child, depth+1, childPointer)
			if err != nil {
				return nil, err
			}
			result[key.Value] = value
		}
		return result, nil
	case yaml.SequenceNode:
		if len(node.Content) > maxProcessTemplateYAMLCollectionItems {
			return nil, s.nodeError(node, fmt.Sprintf("sequence may contain at most %d items", maxProcessTemplateYAMLCollectionItems))
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

func (s *processTemplateYAMLValidationState) convertScalar(node *yaml.Node, pointer string) (any, error) {
	if len(node.Value) > maxProcessTemplateYAMLScalarBytes {
		return nil, s.nodeError(node, fmt.Sprintf("scalar exceeds the %d byte limit", maxProcessTemplateYAMLScalarBytes))
	}
	if strings.HasSuffix(pointer, "/render") && len(node.Value) > maxTemplateRenderBytes {
		return nil, s.nodeError(node, fmt.Sprintf("render expression exceeds the %d byte limit", maxTemplateRenderBytes))
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
		if !processTemplateJSONNumberPattern.MatchString(node.Value) || strings.ContainsAny(node.Value, ".eE") {
			return nil, s.nodeError(node, "integer must be a canonical decimal JSON integer")
		}
		value, err := strconv.ParseInt(node.Value, 10, 64)
		if err != nil {
			return nil, s.nodeError(node, "integer is outside the signed 64-bit range")
		}
		return value, nil
	case "!!float":
		if !processTemplateJSONNumberPattern.MatchString(node.Value) {
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

func (s *processTemplateYAMLValidationState) nodeError(node *yaml.Node, message string) error {
	if node != nil && node.Line > 0 {
		return fmt.Errorf("decode process template definition %q at line %d, column %d: %s", s.name, node.Line, node.Column, message)
	}
	return fmt.Errorf("decode process template definition %q: %s", s.name, message)
}
