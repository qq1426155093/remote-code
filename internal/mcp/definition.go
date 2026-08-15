package mcpserver

import "encoding/json"

type definitionDocument struct {
	Version     int            `json:"version"`
	Namespace   string         `json:"namespace"`
	Description string         `json:"description"`
	Language    string         `json:"language"`
	Tools       []toolDocument `json:"tools"`
}

type toolDocument struct {
	Name           string          `json:"name"`
	Title          string          `json:"title"`
	Description    string          `json:"description"`
	Capabilities   *[]string       `json:"capabilities"`
	Timeout        string          `json:"timeout"`
	MaxConcurrency int             `json:"max_concurrency"`
	Annotations    *annotationsDoc `json:"annotations"`
	InputSchema    json.RawMessage `json:"input_schema"`
	OutputSchema   json.RawMessage `json:"output_schema"`
	Script         string          `json:"script"`
}

type annotationsDoc struct {
	ReadOnly    *bool `json:"read_only"`
	Destructive *bool `json:"destructive"`
	Idempotent  *bool `json:"idempotent"`
	OpenWorld   *bool `json:"open_world"`
}

type ToolAnnotations struct {
	ReadOnly    bool
	Destructive bool
	Idempotent  bool
	OpenWorld   bool
}
