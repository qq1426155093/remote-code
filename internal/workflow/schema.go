package workflow

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	maxSchemaDepth      = 64
	maxSchemaNodes      = 20_000
	maxSchemaRegexBytes = 4096
)

func compileParameterSchema(name string, raw json.RawMessage) (*jsonschema.Schema, json.RawMessage, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil, errors.New("parameters_schema is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, fmt.Errorf("decode schema: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, nil, fmt.Errorf("decode schema: %w", err)
	}
	root, ok := document.(map[string]any)
	if !ok {
		return nil, nil, errors.New("root must be an object")
	}
	if root["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		return nil, nil, errors.New("$schema must be https://json-schema.org/draft/2020-12/schema")
	}
	if root["type"] != "object" {
		return nil, nil, errors.New("root type must be object")
	}
	if additional, exists := root["additionalProperties"]; !exists || additional != false {
		return nil, nil, errors.New("additionalProperties must explicitly be false")
	}
	scan := schemaScanState{}
	if err := scan.scan(document, 0); err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(denySchemaLoader{})
	resourceURL := "https://remote-code.invalid/workflow-schemas/" + name
	if err := compiler.AddResource(resourceURL, document); err != nil {
		return nil, nil, fmt.Errorf("add schema resource: %w", err)
	}
	validator, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, nil, fmt.Errorf("compile schema: %w", err)
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return nil, nil, fmt.Errorf("normalize schema: %w", err)
	}
	return validator, canonical, nil
}

type denySchemaLoader struct{}

func (denySchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external schema resources are disabled")
}

type schemaScanState struct {
	nodes int
}

func (s *schemaScanState) scan(value any, depth int) error {
	s.nodes++
	if s.nodes > maxSchemaNodes {
		return fmt.Errorf("schema contains more than %d values", maxSchemaNodes)
	}
	if depth > maxSchemaDepth {
		return fmt.Errorf("schema exceeds maximum depth %d", maxSchemaDepth)
	}
	switch typed := value.(type) {
	case map[string]any:
		for key, child := range typed {
			switch key {
			case "$schema":
				if uri, ok := child.(string); !ok || uri != "https://json-schema.org/draft/2020-12/schema" {
					return errors.New("$schema must be https://json-schema.org/draft/2020-12/schema")
				}
			case "$ref", "$dynamicRef":
				uri, ok := child.(string)
				if !ok || !strings.HasPrefix(uri, "#") {
					return fmt.Errorf("%s may only contain a local fragment reference", key)
				}
			case "pattern":
				if pattern, ok := child.(string); ok && len(pattern) > maxSchemaRegexBytes {
					return fmt.Errorf("schema pattern exceeds %d bytes", maxSchemaRegexBytes)
				}
			}
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		for _, child := range typed {
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	case nil, bool, string, json.Number:
		return nil
	default:
		return fmt.Errorf("schema contains unsupported value type %T", value)
	}
	return nil
}
