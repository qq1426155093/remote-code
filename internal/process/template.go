package process

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/santhosh-tekuri/jsonschema/v6"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	maxTemplateDefinitionFiles = 64
	maxTemplatesPerDocument    = 128
	maxProcessTemplates        = 256
	maxTemplateDescription     = 16 << 10
	maxTemplateRenderBytes     = 64 << 10
	maxTemplateExprNodes       = 20_000
	maxTemplateValueNodes      = 4096
	maxTemplateValueDepth      = 32
	maxTemplateValueBytes      = 256 << 10
	maxTemplateCollectionItems = 4096
)

var templateRevisionPattern = regexp.MustCompile(`^[0-9a-f]{64}$`)

// TemplateConfig identifies operator-controlled process template definitions.
type TemplateConfig struct {
	DefinitionFiles []string
}

// TemplateRegistry is an immutable collection of compiled process templates.
// It is safe for concurrent reads and renders.
type TemplateRegistry struct {
	byName  map[string]*compiledProcessTemplate
	ordered []*compiledProcessTemplate
}

type compiledProcessTemplate struct {
	summary          *codev1.ProcessTemplateSummary
	parametersSchema *structpb.Struct
	validator        *jsonschema.Schema
	program          *vm.Program
	command          string
}

type processTemplateDocument struct {
	Version   int                         `json:"version"`
	Language  string                      `json:"language"`
	Templates []processTemplateDefinition `json:"templates"`
}

type processTemplateDefinition struct {
	Name             string          `json:"name"`
	Description      string          `json:"description"`
	ParametersSchema json.RawMessage `json:"parameters_schema"`
	Command          string          `json:"command"`
	IOMode           string          `json:"io_mode"`
	InputMode        string          `json:"input_mode"`
	Render           string          `json:"render"`
}

type templateEnvironment struct {
	Parameters map[string]any `expr:"parameters"`
}

// PrepareTemplates safely loads and compiles all configured definitions. It
// performs no process start and does not bind a listener.
func PrepareTemplates(config TemplateConfig, workspace string) (*TemplateRegistry, error) {
	registry := &TemplateRegistry{byName: make(map[string]*compiledProcessTemplate)}
	if len(config.DefinitionFiles) == 0 {
		return registry, nil
	}
	if len(config.DefinitionFiles) > maxTemplateDefinitionFiles {
		return nil, fmt.Errorf("process templates accept at most %d definition files", maxTemplateDefinitionFiles)
	}
	documents, err := loadProcessTemplateDefinitionFiles(config.DefinitionFiles, workspace)
	if err != nil {
		return nil, err
	}
	for documentIndex, document := range documents {
		definitionFile := config.DefinitionFiles[documentIndex]
		if document.Version != 1 {
			return nil, fmt.Errorf("process template definition %q version must be 1", definitionFile)
		}
		if document.Language != "expr" {
			return nil, fmt.Errorf("process template definition %q language must be expr", definitionFile)
		}
		if len(document.Templates) == 0 || len(document.Templates) > maxTemplatesPerDocument {
			return nil, fmt.Errorf("process template definition %q must contain between 1 and %d templates", definitionFile, maxTemplatesPerDocument)
		}
		for templateIndex, definition := range document.Templates {
			compiled, compileErr := compileProcessTemplate(definition)
			if compileErr != nil {
				return nil, fmt.Errorf("process template definition %q template %d: %w", definitionFile, templateIndex, compileErr)
			}
			name := compiled.summary.GetName()
			if _, duplicate := registry.byName[name]; duplicate {
				return nil, fmt.Errorf("duplicate process template name %q", name)
			}
			registry.byName[name] = compiled
			registry.ordered = append(registry.ordered, compiled)
			if len(registry.ordered) > maxProcessTemplates {
				return nil, fmt.Errorf("process template registry contains more than %d templates", maxProcessTemplates)
			}
		}
	}
	sort.Slice(registry.ordered, func(i, j int) bool {
		return registry.ordered[i].summary.GetName() < registry.ordered[j].summary.GetName()
	})
	return registry, nil
}

func compileProcessTemplate(definition processTemplateDefinition) (*compiledProcessTemplate, error) {
	if !identifierPattern.MatchString(definition.Name) {
		return nil, fmt.Errorf("template name must match %s", identifierPattern)
	}
	if strings.TrimSpace(definition.Description) == "" {
		return nil, errors.New("template description is required")
	}
	if len(definition.Description) > maxTemplateDescription || !utf8.ValidString(definition.Description) || strings.ContainsRune(definition.Description, '\x00') {
		return nil, fmt.Errorf("template description must be valid UTF-8 without NUL and at most %d bytes", maxTemplateDescription)
	}
	if definition.Command == "" {
		return nil, errors.New("template command is required")
	}
	if len(definition.Command) > maxCommandBytes || strings.ContainsRune(definition.Command, '\x00') {
		return nil, fmt.Errorf("template command must not contain NUL and must be at most %d bytes", maxCommandBytes)
	}
	ioMode, err := parseTemplateIOMode(definition.IOMode)
	if err != nil {
		return nil, err
	}
	inputMode, err := parseTemplateInputMode(definition.InputMode)
	if err != nil {
		return nil, err
	}
	validator, schemaValue, schemaProto, err := compileProcessTemplateSchema(definition.Name, definition.ParametersSchema)
	if err != nil {
		return nil, fmt.Errorf("parameters_schema: %w", err)
	}
	program, err := compileProcessTemplateExpr(definition.Render)
	if err != nil {
		return nil, err
	}
	canonicalSchema, err := json.Marshal(schemaValue)
	if err != nil {
		return nil, fmt.Errorf("normalize parameters schema: %w", err)
	}
	digestInput, err := json.Marshal([]any{
		definition.Name, definition.Description, json.RawMessage(canonicalSchema), definition.Command,
		definition.IOMode, definition.InputMode, definition.Render,
	})
	if err != nil {
		return nil, fmt.Errorf("calculate template revision: %w", err)
	}
	digest := sha256.Sum256(digestInput)
	return &compiledProcessTemplate{
		summary: &codev1.ProcessTemplateSummary{
			Name: definition.Name, Description: definition.Description, Revision: hex.EncodeToString(digest[:]),
			IoMode: ioMode, InputMode: inputMode,
		},
		parametersSchema: schemaProto,
		validator:        validator,
		program:          program,
		command:          definition.Command,
	}, nil
}

func parseTemplateIOMode(value string) (codev1.ProcessIOMode, error) {
	switch value {
	case "pipe":
		return codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE, nil
	case "pty":
		return codev1.ProcessIOMode_PROCESS_IO_MODE_PTY, nil
	default:
		return codev1.ProcessIOMode_PROCESS_IO_MODE_UNSPECIFIED, errors.New("io_mode must be pipe or pty")
	}
}

func parseTemplateInputMode(value string) (codev1.ProcessInputMode, error) {
	switch value {
	case "disabled":
		return codev1.ProcessInputMode_PROCESS_INPUT_MODE_DISABLED, nil
	case "managed":
		return codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED, nil
	default:
		return codev1.ProcessInputMode_PROCESS_INPUT_MODE_UNSPECIFIED, errors.New("input_mode must be disabled or managed")
	}
}

func compileProcessTemplateExpr(source string) (*vm.Program, error) {
	if source == "" {
		return nil, errors.New("render expression is required")
	}
	if len(source) > maxTemplateRenderBytes || !utf8.ValidString(source) || strings.ContainsRune(source, '\x00') {
		return nil, fmt.Errorf("render expression must be valid UTF-8 without NUL and at most %d bytes", maxTemplateRenderBytes)
	}
	tree, err := parser.Parse(source)
	if err != nil {
		return nil, fmt.Errorf("parse Expr render expression: %w", err)
	}
	analysis := templateExprAnalysis{}
	analysis.walk(tree.Node, 0, 0)
	if analysis.err != nil {
		return nil, analysis.err
	}
	if analysis.nodes > maxTemplateExprNodes {
		return nil, fmt.Errorf("render expression contains more than %d nodes", maxTemplateExprNodes)
	}
	if analysis.collectionCalls > 32 {
		return nil, errors.New("render expression may contain at most 32 collection call sites")
	}
	environment := templateEnvironment{Parameters: map[string]any{}}
	program, err := expr.Compile(source,
		expr.Env(environment),
		expr.AsAny(),
		expr.DisableBuiltin("now"),
		expr.DisableBuiltin("date"),
		expr.DisableBuiltin("duration"),
		expr.DisableBuiltin("timezone"),
		expr.DisableBuiltin("repeat"),
		expr.DisableBuiltin("reduce"),
	)
	if err != nil {
		return nil, fmt.Errorf("compile Expr render expression: %w", err)
	}
	return program, nil
}

type templateExprAnalysis struct {
	nodes           int
	collectionCalls int
	err             error
}

func (s *templateExprAnalysis) walk(node ast.Node, depth, predicateDepth int) {
	if node == nil || s.err != nil {
		return
	}
	s.nodes++
	if depth > maxTemplateValueDepth {
		s.err = fmt.Errorf("render expression exceeds maximum depth %d", maxTemplateValueDepth)
		return
	}
	switch n := node.(type) {
	case *ast.UnaryNode:
		s.walk(n.Node, depth+1, predicateDepth)
	case *ast.BinaryNode:
		if n.Operator == ".." {
			s.err = errors.New("Expr range operator is not allowed in process templates")
			return
		}
		s.walk(n.Left, depth+1, predicateDepth)
		s.walk(n.Right, depth+1, predicateDepth)
	case *ast.ChainNode:
		s.walk(n.Node, depth+1, predicateDepth)
	case *ast.MemberNode:
		s.walk(n.Node, depth+1, predicateDepth)
		s.walk(n.Property, depth+1, predicateDepth)
	case *ast.SliceNode:
		s.walk(n.Node, depth+1, predicateDepth)
		s.walk(n.From, depth+1, predicateDepth)
		s.walk(n.To, depth+1, predicateDepth)
	case *ast.CallNode:
		if identifier, ok := n.Callee.(*ast.IdentifierNode); ok {
			switch identifier.Value {
			case "now", "date", "duration", "timezone", "repeat", "reduce":
				s.err = fmt.Errorf("Expr function %s is not allowed in process templates", identifier.Value)
				return
			}
		}
		s.walk(n.Callee, depth+1, predicateDepth)
		for _, argument := range n.Arguments {
			s.walk(argument, depth+1, predicateDepth)
		}
	case *ast.BuiltinNode:
		switch n.Name {
		case "now", "date", "duration", "timezone", "repeat", "reduce":
			s.err = fmt.Errorf("Expr builtin %s is not allowed in process templates", n.Name)
			return
		}
		s.collectionCalls++
		for _, argument := range n.Arguments {
			s.walk(argument, depth+1, predicateDepth)
		}
	case *ast.PredicateNode:
		if predicateDepth >= 2 {
			s.err = errors.New("collection predicates may be nested at most twice")
			return
		}
		s.walk(n.Node, depth+1, predicateDepth+1)
	case *ast.VariableDeclaratorNode:
		s.walk(n.Value, depth+1, predicateDepth)
		s.walk(n.Expr, depth+1, predicateDepth)
	case *ast.SequenceNode:
		for _, child := range n.Nodes {
			s.walk(child, depth+1, predicateDepth)
		}
	case *ast.ConditionalNode:
		s.walk(n.Cond, depth+1, predicateDepth)
		s.walk(n.Exp1, depth+1, predicateDepth)
		s.walk(n.Exp2, depth+1, predicateDepth)
	case *ast.ArrayNode:
		for _, child := range n.Nodes {
			s.walk(child, depth+1, predicateDepth)
		}
	case *ast.MapNode:
		for _, pair := range n.Pairs {
			s.walk(pair, depth+1, predicateDepth)
		}
	case *ast.PairNode:
		s.walk(n.Key, depth+1, predicateDepth)
		s.walk(n.Value, depth+1, predicateDepth)
	}
}

// Count returns the number of configured templates.
func (r *TemplateRegistry) Count() int {
	if r == nil {
		return 0
	}
	return len(r.ordered)
}

func (r *TemplateRegistry) lookup(name string) (*compiledProcessTemplate, bool) {
	if r == nil {
		return nil, false
	}
	template, ok := r.byName[name]
	return template, ok
}

func (r *TemplateRegistry) summaries() []*codev1.ProcessTemplateSummary {
	if r == nil {
		return nil
	}
	result := make([]*codev1.ProcessTemplateSummary, 0, len(r.ordered))
	for _, template := range r.ordered {
		result = append(result, proto.Clone(template.summary).(*codev1.ProcessTemplateSummary))
	}
	return result
}

func (t *compiledProcessTemplate) publicDefinition() *codev1.ProcessTemplate {
	return &codev1.ProcessTemplate{
		Summary:          proto.Clone(t.summary).(*codev1.ProcessTemplateSummary),
		ParametersSchema: proto.Clone(t.parametersSchema).(*structpb.Struct),
	}
}

func (t *compiledProcessTemplate) render(ctx context.Context, parameters *structpb.Struct) (*codev1.StartProcessRequest, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if parameters == nil {
		parameters = &structpb.Struct{Fields: map[string]*structpb.Value{}}
	}
	parameterMap := parameters.AsMap()
	if err := validateProcessTemplateValue(parameterMap); err != nil {
		return nil, status.Error(codes.InvalidArgument, "template parameters are too deeply nested or exceed their size limit")
	}
	if err := t.validator.Validate(parameterMap); err != nil {
		return nil, status.Error(codes.InvalidArgument, safeProcessTemplateSchemaError(err))
	}
	output, err := expr.Run(t.program, templateEnvironment{Parameters: parameterMap})
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "process template %q could not render a valid process specification", t.summary.GetName())
	}
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if err := validateProcessTemplateValue(output); err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "process template %q rendered an unsupported or oversized result", t.summary.GetName())
	}
	arguments, workingDirectory, environment, err := normalizeRenderedProcessTemplate(output)
	if err != nil {
		return nil, status.Errorf(codes.FailedPrecondition, "process template %q rendered an invalid process specification", t.summary.GetName())
	}
	return &codev1.StartProcessRequest{
		Command: t.command, Arguments: arguments, WorkingDirectory: workingDirectory,
		Environment: environment, IoMode: t.summary.GetIoMode(), InputMode: t.summary.GetInputMode(),
	}, nil
}

func normalizeRenderedProcessTemplate(value any) ([]string, string, map[string]string, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, "", nil, errors.New("render result must be an object")
	}
	for key := range object {
		if key != "arguments" && key != "working_directory" && key != "environment" {
			return nil, "", nil, fmt.Errorf("unknown rendered field %q", key)
		}
	}
	var arguments []string
	if raw, exists := object["arguments"]; exists {
		var err error
		arguments, err = renderedStringSlice(raw)
		if err != nil {
			return nil, "", nil, fmt.Errorf("arguments: %w", err)
		}
	}
	workingDirectory := "."
	if raw, exists := object["working_directory"]; exists {
		var ok bool
		workingDirectory, ok = raw.(string)
		if !ok {
			return nil, "", nil, errors.New("working_directory must be a string")
		}
	}
	environment := make(map[string]string)
	if raw, exists := object["environment"]; exists {
		var err error
		environment, err = renderedStringMap(raw)
		if err != nil {
			return nil, "", nil, fmt.Errorf("environment: %w", err)
		}
	}
	return arguments, workingDirectory, environment, nil
}

func renderedStringSlice(value any) ([]string, error) {
	switch values := value.(type) {
	case []string:
		return append([]string(nil), values...), nil
	case []any:
		result := make([]string, len(values))
		for index, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("item %d must be a string", index)
			}
			result[index] = text
		}
		return result, nil
	default:
		return nil, errors.New("must be an array of strings")
	}
}

func renderedStringMap(value any) (map[string]string, error) {
	switch values := value.(type) {
	case map[string]string:
		result := make(map[string]string, len(values))
		for key, item := range values {
			result[key] = item
		}
		return result, nil
	case map[string]any:
		result := make(map[string]string, len(values))
		for key, item := range values {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("value for %q must be a string", key)
			}
			result[key] = text
		}
		return result, nil
	default:
		return nil, errors.New("must be an object with string values")
	}
}

func validateProcessTemplateValue(value any) error {
	state := templateValueState{}
	return state.scan(value, 0)
}

type templateValueState struct {
	nodes int
	bytes int
}

func (s *templateValueState) scan(value any, depth int) error {
	s.nodes++
	if s.nodes > maxTemplateValueNodes {
		return fmt.Errorf("value contains more than %d nodes", maxTemplateValueNodes)
	}
	if depth > maxTemplateValueDepth {
		return fmt.Errorf("value exceeds maximum depth %d", maxTemplateValueDepth)
	}
	switch typed := value.(type) {
	case nil, bool:
	case string:
		s.bytes += len(typed)
	case float64:
		if math.IsNaN(typed) || math.IsInf(typed, 0) {
			return errors.New("value contains a non-finite number")
		}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, json.Number:
	case []string:
		if len(typed) > maxTemplateCollectionItems {
			return errors.New("value collection is too large")
		}
		for _, child := range typed {
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	case []any:
		if len(typed) > maxTemplateCollectionItems {
			return errors.New("value collection is too large")
		}
		for _, child := range typed {
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	case map[string]string:
		if len(typed) > maxTemplateCollectionItems {
			return errors.New("value collection is too large")
		}
		for key, child := range typed {
			s.bytes += len(key)
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	case map[string]any:
		if len(typed) > maxTemplateCollectionItems {
			return errors.New("value collection is too large")
		}
		for key, child := range typed {
			s.bytes += len(key)
			if err := s.scan(child, depth+1); err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("unsupported value type %T", value)
	}
	if s.bytes > maxTemplateValueBytes {
		return fmt.Errorf("value exceeds %d bytes", maxTemplateValueBytes)
	}
	return nil
}

func safeProcessTemplateSchemaError(err error) string {
	var validation *jsonschema.ValidationError
	if !errors.As(err, &validation) {
		return "template parameters do not match the configured schema"
	}
	locations := make(map[string]struct{})
	var collect func(*jsonschema.ValidationError)
	collect = func(current *jsonschema.ValidationError) {
		if len(current.Causes) == 0 {
			parts := make([]string, len(current.InstanceLocation))
			for index, part := range current.InstanceLocation {
				parts[index] = escapeProcessTemplateJSONPointer(part)
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
		return "template parameters do not match the configured schema"
	}
	return "template parameters do not match the configured schema at " + strings.Join(ordered, ", ")
}

func escapeProcessTemplateJSONPointer(value string) string {
	return strings.ReplaceAll(strings.ReplaceAll(value, "~", "~0"), "/", "~1")
}
