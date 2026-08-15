package mcpserver

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	maxSchemaDepth      = 64
	maxSchemaNodes      = 20_000
	maxSchemaRegexBytes = 4096
)

func compileSchema(name string, raw json.RawMessage, input bool) (*jsonschema.Schema, any, error) {
	if len(raw) == 0 || bytes.Equal(raw, []byte("null")) {
		if input {
			return nil, nil, errors.New("input_schema is required")
		}
		return nil, nil, nil
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
	if input {
		root, ok := document.(map[string]any)
		if !ok {
			return nil, nil, errors.New("input_schema root must be an object")
		}
		if root["type"] != "object" {
			return nil, nil, errors.New("input_schema root type must be object")
		}
		additional, exists := root["additionalProperties"]
		if !exists || additional != false {
			return nil, nil, errors.New("input_schema must explicitly set additionalProperties to false")
		}
		if err := validatePropertyDescriptions(root, ""); err != nil {
			return nil, nil, err
		}
	}
	state := schemaScanState{}
	if err := state.scan(document, 0); err != nil {
		return nil, nil, err
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(denySchemaLoader{})
	resourceURL := "https://remote-code.invalid/schemas/" + name
	if err := compiler.AddResource(resourceURL, document); err != nil {
		return nil, nil, fmt.Errorf("add schema resource: %w", err)
	}
	schema, err := compiler.Compile(resourceURL)
	if err != nil {
		return nil, nil, fmt.Errorf("compile schema: %w", err)
	}
	return schema, document, nil
}

type denySchemaLoader struct{}

func (denySchemaLoader) Load(string) (any, error) {
	return nil, errors.New("external schema resources are disabled")
}

type schemaScanState struct{ nodes int }

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
			case "x-mcp-header":
				return errors.New("x-mcp-header is not supported")
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

func decodeJSONValue(raw []byte) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = []byte("{}")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	value, err := decodeStrictJSONToken(decoder)
	if err != nil {
		return nil, err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return nil, errors.New("unexpected trailing JSON value")
		}
		return nil, err
	}
	return normalizeJSONValue(value, 0, new(int))
}

func decodeStrictJSONToken(decoder *json.Decoder) (any, error) {
	token, err := decoder.Token()
	if err != nil {
		return nil, err
	}
	delimiter, compound := token.(json.Delim)
	if !compound {
		return token, nil
	}
	switch delimiter {
	case '{':
		result := make(map[string]any)
		seen := make(map[string]struct{})
		for decoder.More() {
			keyToken, err := decoder.Token()
			if err != nil {
				return nil, err
			}
			key, ok := keyToken.(string)
			if !ok {
				return nil, errors.New("JSON object key must be a string")
			}
			if _, duplicate := seen[key]; duplicate {
				return nil, fmt.Errorf("duplicate JSON object key %q", key)
			}
			seen[key] = struct{}{}
			value, err := decodeStrictJSONToken(decoder)
			if err != nil {
				return nil, err
			}
			result[key] = value
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim('}') {
			return nil, errors.New("invalid JSON object")
		}
		return result, nil
	case '[':
		result := make([]any, 0)
		for decoder.More() {
			value, err := decodeStrictJSONToken(decoder)
			if err != nil {
				return nil, err
			}
			result = append(result, value)
		}
		closing, err := decoder.Token()
		if err != nil || closing != json.Delim(']') {
			return nil, errors.New("invalid JSON array")
		}
		return result, nil
	default:
		return nil, errors.New("invalid JSON delimiter")
	}
}

func validatePropertyDescriptions(value any, location string) error {
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if properties, ok := object["properties"].(map[string]any); ok {
		for name, raw := range properties {
			property, ok := raw.(map[string]any)
			if !ok {
				return fmt.Errorf("input_schema property %q must be an object", location+name)
			}
			description, ok := property["description"].(string)
			if !ok || strings.TrimSpace(description) == "" {
				return fmt.Errorf("input_schema property %q requires a non-empty description", location+name)
			}
			if err := validatePropertyDescriptions(property, location+name+"."); err != nil {
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
			if err := validatePropertyDescriptions(typed, location); err != nil {
				return err
			}
		case []any:
			for _, item := range typed {
				if err := validatePropertyDescriptions(item, location); err != nil {
					return err
				}
			}
		}
	}
	return nil
}
