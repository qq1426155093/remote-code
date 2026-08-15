package process

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxTemplateSchemaDepth      = 64
	maxTemplateSchemaNodes      = 20_000
	maxTemplateSchemaRegexBytes = 4096
)

func compileProcessTemplateSchema(name string, raw json.RawMessage) (*jsonschema.Schema, any, *structpb.Struct, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		return nil, nil, nil, errors.New("parameters_schema is required")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var document any
	if err := decoder.Decode(&document); err != nil {
		return nil, nil, nil, fmt.Errorf("decode schema: %w", err)
	}
	if err := ensureTemplateJSONEOF(decoder); err != nil {
		return nil, nil, nil, fmt.Errorf("decode schema: %w", err)
	}
	root, ok := document.(map[string]any)
	if !ok {
		return nil, nil, nil, errors.New("parameters_schema root must be an object")
	}
	if root["$schema"] != "https://json-schema.org/draft/2020-12/schema" {
		return nil, nil, nil, errors.New("parameters_schema $schema must be https://json-schema.org/draft/2020-12/schema")
	}
	if root["type"] != "object" {
		return nil, nil, nil, errors.New("parameters_schema root type must be object")
	}
	if additional, exists := root["additionalProperties"]; !exists || additional != false {
		return nil, nil, nil, errors.New("parameters_schema must explicitly set additionalProperties to false")
	}
	if err := validateTemplatePropertyDescriptions(root, ""); err != nil {
		return nil, nil, nil, err
	}
	scan := templateSchemaScanState{}
	if err := scan.scan(document, 0); err != nil {
		return nil, nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(denyTemplateSchemaLoader{})
	resourceURL := "https://remote-code.invalid/process-template-schemas/" + name
	if err := compiler.AddResource(resourceURL, document); err != nil {
		return nil, nil, nil, fmt.Errorf("add schema resource: %w", err)
	}
	validator, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("compile schema: %w", err)
	}
	publicSchema := &structpb.Struct{}
	if err := protojson.Unmarshal(raw, publicSchema); err != nil {
		return nil, nil, nil, fmt.Errorf("convert schema to protobuf: %w", err)
	}
	return validator, document, publicSchema, nil
}

type denyTemplateSchemaLoader struct{}

func (denyTemplateSchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external schema resources are disabled")
}

type templateSchemaScanState struct {
	nodes int
}

func (s *templateSchemaScanState) scan(value any, depth int) error {
	s.nodes++
	if s.nodes > maxTemplateSchemaNodes {
		return fmt.Errorf("schema contains more than %d values", maxTemplateSchemaNodes)
	}
	if depth > maxTemplateSchemaDepth {
		return fmt.Errorf("schema exceeds maximum depth %d", maxTemplateSchemaDepth)
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
				if pattern, ok := child.(string); ok && len(pattern) > maxTemplateSchemaRegexBytes {
					return fmt.Errorf("schema pattern exceeds %d bytes", maxTemplateSchemaRegexBytes)
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

func validateTemplatePropertyDescriptions(value any, location string) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if properties, ok := object["properties"].(map[string]any); ok {
		for name, raw := range properties {
			property, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("parameters_schema property %q must be an object", location+name)
			}
			description, ok := property["description"].(string)
			if !ok || strings.TrimSpace(description) == "" {
				return fmt.Errorf("parameters_schema property %q requires a non-empty description", location+name)
			}
			if err := validateTemplatePropertyDescriptions(property, location+name+"."); err != nil {
				return err
			}
		}
	}
	for key, child := range object {
		if key == "properties" {
			continue
		}
		switch typed := child.(type) {
		case map[string]any:
			if err := validateTemplatePropertyDescriptions(typed, location); err != nil {
				return err
			}
		case []any:
			for _, item := range typed {
				if err := validateTemplatePropertyDescriptions(item, location); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func ensureTemplateJSONEOF(decoder *json.Decoder) error {
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return errors.New("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}
