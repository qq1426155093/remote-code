package mcpserver

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
)

const maxJSONNodes = 100_000

func normalizeJSONValue(value any, depth int, nodes *int) (any, error) {
	*nodes++
	if *nodes > maxJSONNodes {
		return nil, fmt.Errorf("JSON value contains more than %d nodes", maxJSONNodes)
	}
	if depth > maxYAMLDepth {
		return nil, fmt.Errorf("JSON value exceeds maximum depth %d", maxYAMLDepth)
	}
	switch typed := value.(type) {
	case nil, bool, string:
		return typed, nil
	case json.Number:
		text := typed.String()
		if !jsonNumberPattern.MatchString(text) {
			return nil, errors.New("invalid JSON number")
		}
		if !containsFloatMarker(text) {
			integer, err := strconv.ParseInt(text, 10, 64)
			if err != nil {
				return nil, errors.New("JSON integer is outside the signed 64-bit range")
			}
			return integer, nil
		}
		float, err := strconv.ParseFloat(text, 64)
		if err != nil || math.IsInf(float, 0) || math.IsNaN(float) {
			return nil, errors.New("JSON number must be finite")
		}
		return float, nil
	case float64:
		if math.IsInf(typed, 0) || math.IsNaN(typed) {
			return nil, errors.New("number must be finite")
		}
		return typed, nil
	case int:
		return int64(typed), nil
	case int8:
		return int64(typed), nil
	case int16:
		return int64(typed), nil
	case int32:
		return int64(typed), nil
	case int64:
		return typed, nil
	case uint:
		if uint64(typed) > math.MaxInt64 {
			return nil, errors.New("unsigned integer exceeds the signed 64-bit range")
		}
		return int64(typed), nil
	case uint8:
		return int64(typed), nil
	case uint16:
		return int64(typed), nil
	case uint32:
		return int64(typed), nil
	case uint64:
		if typed > math.MaxInt64 {
			return nil, errors.New("unsigned integer exceeds the signed 64-bit range")
		}
		return int64(typed), nil
	case []any:
		if len(typed) > maxYAMLCollectionItems {
			return nil, fmt.Errorf("array contains more than %d items", maxYAMLCollectionItems)
		}
		result := make([]any, len(typed))
		for index, child := range typed {
			normalized, err := normalizeJSONValue(child, depth+1, nodes)
			if err != nil {
				return nil, fmt.Errorf("array item %d: %w", index, err)
			}
			result[index] = normalized
		}
		return result, nil
	case map[string]any:
		if len(typed) > maxYAMLCollectionItems {
			return nil, fmt.Errorf("object contains more than %d properties", maxYAMLCollectionItems)
		}
		result := make(map[string]any, len(typed))
		for key, child := range typed {
			if len(key) > maxYAMLScalarBytes {
				return nil, errors.New("object key exceeds the size limit")
			}
			normalized, err := normalizeJSONValue(child, depth+1, nodes)
			if err != nil {
				return nil, fmt.Errorf("object property %q: %w", key, err)
			}
			result[key] = normalized
		}
		return result, nil
	default:
		// Expr may preserve concrete slices/maps. Convert those containers while
		// still rejecting structs, pointers, functions and arbitrary values.
		reflected := reflect.ValueOf(value)
		switch reflected.Kind() {
		case reflect.Slice, reflect.Array:
			if reflected.Len() > maxYAMLCollectionItems {
				return nil, errors.New("array exceeds the item limit")
			}
			items := make([]any, reflected.Len())
			for index := range items {
				normalized, err := normalizeJSONValue(reflected.Index(index).Interface(), depth+1, nodes)
				if err != nil {
					return nil, err
				}
				items[index] = normalized
			}
			return items, nil
		case reflect.Map:
			if reflected.Type().Key().Kind() != reflect.String {
				return nil, errors.New("object keys must be strings")
			}
			object := make(map[string]any, reflected.Len())
			iterator := reflected.MapRange()
			for iterator.Next() {
				normalized, err := normalizeJSONValue(iterator.Value().Interface(), depth+1, nodes)
				if err != nil {
					return nil, err
				}
				object[iterator.Key().String()] = normalized
			}
			return object, nil
		}
		return nil, fmt.Errorf("unsupported JSON value type %T", value)
	}
}

func containsFloatMarker(value string) bool {
	for _, char := range value {
		if char == '.' || char == 'e' || char == 'E' {
			return true
		}
	}
	return false
}
