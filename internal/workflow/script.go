package workflow

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"
)

const (
	maxScriptBytes = 64 << 10
	maxScriptNodes = 20_000
	maxScriptDepth = 64
	maxHostCalls   = 64
)

type scriptEnvironment struct {
	ExecutionContext context.Context   `expr:"__workflow_context"`
	Parameters       map[string]any    `expr:"parameters"`
	WorkflowContext  map[string]string `expr:"context"`
	Nodes            map[string]any    `expr:"nodes"`
	Activities       map[string]any    `expr:"activities"`
}

type scriptResult struct {
	Route          string
	ContextWrites  map[string]string
	ContextDeletes map[string]struct{}
}

type suspendActivity struct {
	OperationID  string
	ExecutorKind string
	Input        any
	InputHash    string
	Existing     bool
}

func (e *suspendActivity) Error() string { return "workflow activity is not complete" }

type replayError struct {
	message string
}

func (e *replayError) Error() string { return e.message }
func (e *replayError) Unwrap() error { return ErrNonDeterminism }

type scriptInvocation struct {
	node           *NodeRun
	contextWrites  map[string]string
	contextDeletes map[string]struct{}
}

type invocationContextKey struct{}

func compileScript(source string, declaredRoutes map[string][]string) (*vm.Program, map[string]string, map[string]struct{}, error) {
	if len(source) == 0 {
		return nil, nil, nil, errors.New("script is required")
	}
	if len(source) > maxScriptBytes || !utf8.ValidString(source) || strings.ContainsRune(source, '\x00') {
		return nil, nil, nil, fmt.Errorf("script must be valid UTF-8 without NUL and at most %d bytes", maxScriptBytes)
	}
	tree, err := parser.Parse(source)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("parse Expr script: %w", err)
	}
	routes := make(map[string]int)
	if err := collectRouteLiterals(source, tree.Node, routes); err != nil {
		return nil, nil, nil, fmt.Errorf("route analysis: %w", err)
	}
	if err := compareRoutes(source, routes, declaredRoutes); err != nil {
		return nil, nil, nil, err
	}
	analysis := scriptAnalysis{
		source: source, operations: make(map[string]string), contextKeys: make(map[string]struct{}),
	}
	analysis.walk(tree.Node, 0, 0)
	if analysis.err != nil {
		return nil, nil, nil, analysis.err
	}
	if analysis.nodes > maxScriptNodes {
		return nil, nil, nil, fmt.Errorf("script contains more than %d AST nodes", maxScriptNodes)
	}
	if analysis.hostCalls > maxHostCalls {
		return nil, nil, nil, fmt.Errorf("script contains more than %d workflow host call sites", maxHostCalls)
	}
	environment := scriptEnvironment{
		ExecutionContext: context.Background(), Parameters: map[string]any{}, WorkflowContext: map[string]string{},
		Nodes: map[string]any{}, Activities: map[string]any{},
	}
	program, err := expr.Compile(source,
		expr.Env(environment),
		expr.AsKind(reflect.String),
		expr.MaxNodes(maxScriptNodes),
		expr.Function("activity", runActivityHost, new(func(context.Context, string, string, any) any)),
		expr.Function("context_set", runContextSetHost, new(func(context.Context, string, string) any)),
		expr.Function("context_delete", runContextDeleteHost, new(func(context.Context, string) any)),
		expr.WithContext("__workflow_context"),
		expr.DisableBuiltin("now"),
		expr.DisableBuiltin("date"),
		expr.DisableBuiltin("duration"),
		expr.DisableBuiltin("timezone"),
		expr.DisableBuiltin("repeat"),
		expr.DisableBuiltin("reduce"),
	)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("compile Expr script: %w", err)
	}
	return program, analysis.operations, analysis.contextKeys, nil
}

func collectRouteLiterals(source string, node ast.Node, result map[string]int) error {
	switch n := node.(type) {
	case *ast.StringNode:
		if _, exists := result[n.Value]; !exists {
			result[n.Value] = n.Location().From
		}
		return nil
	case *ast.ConditionalNode:
		if err := collectRouteLiterals(source, n.Exp1, result); err != nil {
			return err
		}
		return collectRouteLiterals(source, n.Exp2, result)
	case *ast.SequenceNode:
		if len(n.Nodes) == 0 {
			return errors.New("empty expression sequence has no route")
		}
		return collectRouteLiterals(source, n.Nodes[len(n.Nodes)-1], result)
	case *ast.VariableDeclaratorNode:
		return collectRouteLiterals(source, n.Expr, result)
	default:
		line, column := sourcePosition(source, node.Location().From)
		return fmt.Errorf("normal exit at Expr line %d, column %d must be a direct string literal, not %T", line, column, node)
	}
}

func compareRoutes(source string, routes map[string]int, declared map[string][]string) error {
	var undeclared, unreachable []string
	for route := range routes {
		if _, exists := declared[route]; !exists {
			undeclared = append(undeclared, route)
		}
	}
	for route := range declared {
		if _, exists := routes[route]; !exists {
			unreachable = append(unreachable, route)
		}
	}
	sort.Strings(undeclared)
	sort.Strings(unreachable)
	if len(undeclared) > 0 || len(unreachable) > 0 {
		location := ""
		if len(undeclared) > 0 {
			line, column := sourcePosition(source, routes[undeclared[0]])
			location = fmt.Sprintf("; first undeclared route is at Expr line %d, column %d", line, column)
		}
		return fmt.Errorf("script routes do not exactly match declarations: undeclared=%v unreachable=%v%s", undeclared, unreachable, location)
	}
	return nil
}

type scriptAnalysis struct {
	source      string
	nodes       int
	hostCalls   int
	operations  map[string]string
	contextKeys map[string]struct{}
	err         error
}

func (s *scriptAnalysis) walk(node ast.Node, depth, guarded int) {
	if node == nil || s.err != nil {
		return
	}
	s.nodes++
	if depth > maxScriptDepth {
		s.err = fmt.Errorf("script exceeds maximum AST depth %d", maxScriptDepth)
		return
	}
	switch n := node.(type) {
	case *ast.IdentifierNode:
		if n.Value == "$env" {
			s.err = s.at(node, "Expr $env is not allowed")
		} else if n.Value == "__workflow_context" {
			s.err = s.at(node, "internal workflow execution context is not script-visible")
		}
	case *ast.UnaryNode:
		s.walk(n.Node, depth+1, guarded)
	case *ast.BinaryNode:
		nextGuarded := guarded
		if n.Operator == "&&" || n.Operator == "||" {
			nextGuarded++
		}
		if n.Operator == ".." {
			s.err = s.at(node, "Expr range operator is not allowed")
			return
		}
		s.walk(n.Left, depth+1, nextGuarded)
		s.walk(n.Right, depth+1, nextGuarded)
	case *ast.ChainNode:
		s.walk(n.Node, depth+1, guarded)
	case *ast.MemberNode:
		s.walk(n.Node, depth+1, guarded)
		s.walk(n.Property, depth+1, guarded)
	case *ast.SliceNode:
		s.walk(n.Node, depth+1, guarded)
		s.walk(n.From, depth+1, guarded)
		s.walk(n.To, depth+1, guarded)
	case *ast.CallNode:
		if identifier, ok := n.Callee.(*ast.IdentifierNode); ok {
			switch identifier.Value {
			case "activity":
				s.analyzeActivity(n, guarded)
				return
			case "context_set", "context_delete":
				s.analyzeContextMutation(n, guarded)
				return
			case "now", "date", "duration", "timezone", "repeat", "reduce":
				s.err = s.at(node, fmt.Sprintf("Expr function %s is not allowed", identifier.Value))
				return
			}
		}
		s.walk(n.Callee, depth+1, guarded)
		for _, argument := range n.Arguments {
			s.walk(argument, depth+1, guarded)
		}
	case *ast.BuiltinNode:
		switch n.Name {
		case "now", "date", "duration", "timezone", "repeat", "reduce":
			s.err = s.at(node, fmt.Sprintf("Expr builtin %s is not allowed", n.Name))
			return
		}
		for _, argument := range n.Arguments {
			s.walk(argument, depth+1, guarded)
		}
	case *ast.PredicateNode:
		s.walk(n.Node, depth+1, guarded+1)
	case *ast.VariableDeclaratorNode:
		if n.Name == "parameters" || n.Name == "context" || n.Name == "nodes" || n.Name == "activities" || n.Name == "__workflow_context" {
			s.err = s.at(node, fmt.Sprintf("%s is reserved", n.Name))
			return
		}
		s.walk(n.Value, depth+1, guarded)
		s.walk(n.Expr, depth+1, guarded)
	case *ast.SequenceNode:
		for _, child := range n.Nodes {
			s.walk(child, depth+1, guarded)
		}
	case *ast.ConditionalNode:
		s.walk(n.Cond, depth+1, guarded+1)
		s.walk(n.Exp1, depth+1, guarded)
		s.walk(n.Exp2, depth+1, guarded)
	case *ast.ArrayNode:
		for _, child := range n.Nodes {
			s.walk(child, depth+1, guarded)
		}
	case *ast.MapNode:
		for _, pair := range n.Pairs {
			s.walk(pair, depth+1, guarded)
		}
	case *ast.PairNode:
		s.walk(n.Key, depth+1, guarded)
		s.walk(n.Value, depth+1, guarded)
	}
}

func (s *scriptAnalysis) analyzeActivity(call *ast.CallNode, guarded int) {
	s.hostCalls++
	if guarded > 0 {
		s.err = s.at(call, "activity cannot be called from a conditional, predicate, or short-circuit condition")
		return
	}
	if len(call.Arguments) != 3 {
		s.err = s.at(call, "activity requires operation_id, executor_kind, and input")
		return
	}
	operation, ok := call.Arguments[0].(*ast.StringNode)
	if !ok || !identifierPattern.MatchString(operation.Value) {
		s.err = s.at(call.Arguments[0], fmt.Sprintf("activity operation_id must be a string literal matching %s", identifierPattern))
		return
	}
	executor, ok := call.Arguments[1].(*ast.StringNode)
	if !ok || !identifierPattern.MatchString(executor.Value) {
		s.err = s.at(call.Arguments[1], fmt.Sprintf("activity executor_kind must be a string literal matching %s", identifierPattern))
		return
	}
	if earlier, duplicate := s.operations[operation.Value]; duplicate {
		s.err = s.at(call, fmt.Sprintf("activity operation_id %q is already used with executor %q", operation.Value, earlier))
		return
	}
	s.operations[operation.Value] = executor.Value
	s.walk(call.Arguments[2], 1, guarded)
}

func (s *scriptAnalysis) analyzeContextMutation(call *ast.CallNode, guarded int) {
	s.hostCalls++
	identifier := call.Callee.(*ast.IdentifierNode).Value
	if guarded > 0 {
		s.err = s.at(call, fmt.Sprintf("%s cannot be called from a predicate, condition, or short-circuit expression", identifier))
		return
	}
	expected := 1
	if identifier == "context_set" {
		expected = 2
	}
	if len(call.Arguments) != expected {
		s.err = s.at(call, fmt.Sprintf("%s requires %d argument(s)", identifier, expected))
		return
	}
	key, ok := call.Arguments[0].(*ast.StringNode)
	if !ok || !contextKeyPattern.MatchString(key.Value) {
		s.err = s.at(call.Arguments[0], fmt.Sprintf("%s key must be a string literal matching %s", identifier, contextKeyPattern))
		return
	}
	s.contextKeys[key.Value] = struct{}{}
	if identifier == "context_set" {
		s.walk(call.Arguments[1], 1, guarded)
	}
}

func (s *scriptAnalysis) at(node ast.Node, message string) error {
	line, column := sourcePosition(s.source, node.Location().From)
	return fmt.Errorf("Expr line %d, column %d: %s", line, column, message)
}

func sourcePosition(source string, offset int) (int, int) {
	line, column := 1, 1
	if offset > len(source) {
		offset = len(source)
	}
	for _, r := range source[:offset] {
		if r == '\n' {
			line++
			column = 1
		} else {
			column++
		}
	}
	return line, column
}

func runActivityHost(params ...any) (any, error) {
	if len(params) != 4 {
		return nil, errors.New("invalid workflow activity invocation")
	}
	ctx, ok := params[0].(context.Context)
	if !ok {
		return nil, errors.New("workflow activity context is missing")
	}
	invocation, ok := ctx.Value(invocationContextKey{}).(*scriptInvocation)
	if !ok || invocation.node == nil {
		return nil, errors.New("workflow activity invocation is unavailable")
	}
	operationID, operationOK := params[1].(string)
	executorKind, executorOK := params[2].(string)
	if !operationOK || !executorOK {
		return nil, errors.New("workflow activity identifiers must be strings")
	}
	input, err := cloneJSONValue(params[3])
	if err != nil {
		return nil, fmt.Errorf("normalize workflow activity input: %w", err)
	}
	encoded, _ := json.Marshal(input)
	digest := sha256.Sum256(encoded)
	inputHash := hex.EncodeToString(digest[:])
	activity := invocation.node.Activities[operationID]
	if activity == nil {
		return nil, &suspendActivity{OperationID: operationID, ExecutorKind: executorKind, Input: input, InputHash: inputHash}
	}
	if activity.ExecutorKind != executorKind || activity.InputHash != inputHash {
		return nil, &replayError{message: fmt.Sprintf("activity %q changed executor kind or input", operationID)}
	}
	if activity.State == ActivitySucceeded && activity.Result != nil {
		return map[string]any{
			"status": activity.Result.Status, "code": activity.Result.Code, "message": activity.Result.Message,
			"output": activity.Result.Output, "external_ref": activity.Result.ExternalRef,
		}, nil
	}
	if activity.State == ActivityFailed || activity.State == ActivityCancelled {
		return nil, fmt.Errorf("activity %q ended in system state %s", operationID, activity.State)
	}
	return nil, &suspendActivity{OperationID: operationID, ExecutorKind: executorKind, InputHash: inputHash, Existing: true}
}

func runContextSetHost(params ...any) (any, error) {
	if len(params) != 3 {
		return nil, errors.New("invalid workflow context_set invocation")
	}
	invocation, err := contextInvocation(params[0])
	if err != nil {
		return nil, err
	}
	key, keyOK := params[1].(string)
	value, valueOK := params[2].(string)
	if !keyOK || !valueOK {
		return nil, errors.New("workflow context_set key and value must be strings")
	}
	if err := validateContextEntry(key, value); err != nil {
		return nil, err
	}
	invocation.contextWrites[key] = value
	delete(invocation.contextDeletes, key)
	return nil, nil
}

func runContextDeleteHost(params ...any) (any, error) {
	if len(params) != 2 {
		return nil, errors.New("invalid workflow context_delete invocation")
	}
	invocation, err := contextInvocation(params[0])
	if err != nil {
		return nil, err
	}
	key, ok := params[1].(string)
	if !ok || !contextKeyPattern.MatchString(key) {
		return nil, fmt.Errorf("workflow context_delete key must match %s", contextKeyPattern)
	}
	delete(invocation.contextWrites, key)
	invocation.contextDeletes[key] = struct{}{}
	return nil, nil
}

func contextInvocation(value any) (*scriptInvocation, error) {
	ctx, ok := value.(context.Context)
	if !ok {
		return nil, errors.New("workflow execution context is missing")
	}
	invocation, ok := ctx.Value(invocationContextKey{}).(*scriptInvocation)
	if !ok || invocation == nil {
		return nil, errors.New("workflow script invocation is unavailable")
	}
	return invocation, nil
}

func runScript(
	ctx context.Context,
	node *NodeRun,
	definition *compiledNode,
	parameters map[string]any,
	workflowContext map[string]string,
	nodes map[string]any,
) (scriptResult, error) {
	activities := make(map[string]any, len(node.Activities))
	for operation, activity := range node.Activities {
		activities[operation] = map[string]any{"id": activity.ID, "state": activity.State}
	}
	invocation := &scriptInvocation{
		node: node, contextWrites: make(map[string]string), contextDeletes: make(map[string]struct{}),
	}
	ctx = context.WithValue(ctx, invocationContextKey{}, invocation)
	value, err := expr.Run(definition.program, scriptEnvironment{
		ExecutionContext: ctx, Parameters: parameters, WorkflowContext: cloneWorkflowContext(workflowContext),
		Nodes: nodes, Activities: activities,
	})
	if err != nil {
		return scriptResult{}, err
	}
	route, ok := value.(string)
	if !ok {
		return scriptResult{}, fmt.Errorf("Expr returned %T instead of string", value)
	}
	if _, declared := definition.definition.Routes[route]; !declared {
		return scriptResult{}, fmt.Errorf("Expr returned undeclared route %q", route)
	}
	if err := validateContextChanges(workflowContext, invocation.contextWrites, invocation.contextDeletes); err != nil {
		return scriptResult{}, err
	}
	return scriptResult{
		Route: route, ContextWrites: invocation.contextWrites, ContextDeletes: invocation.contextDeletes,
	}, nil
}

func validateContextEntry(key, value string) error {
	if !contextKeyPattern.MatchString(key) {
		return fmt.Errorf("workflow context key must match %s", contextKeyPattern)
	}
	if len(value) > maxContextValueBytes || !utf8.ValidString(value) || strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("workflow context value for %q must be valid UTF-8 without NUL and at most %d bytes", key, maxContextValueBytes)
	}
	return nil
}

func validateWorkflowContext(values map[string]string) error {
	if len(values) > maxContextEntries {
		return fmt.Errorf("workflow context contains more than %d entries", maxContextEntries)
	}
	for key, value := range values {
		if err := validateContextEntry(key, value); err != nil {
			return err
		}
	}
	encoded, err := json.Marshal(values)
	if err != nil {
		return fmt.Errorf("encode workflow context: %w", err)
	}
	if len(encoded) > maxContextBytes {
		return fmt.Errorf("workflow context exceeds %d bytes", maxContextBytes)
	}
	return nil
}

func validateContextChanges(current, writes map[string]string, deletes map[string]struct{}) error {
	updated := cloneWorkflowContext(current)
	for key := range deletes {
		delete(updated, key)
	}
	for key, value := range writes {
		updated[key] = value
	}
	return validateWorkflowContext(updated)
}

func cloneWorkflowContext(values map[string]string) map[string]string {
	result := make(map[string]string, len(values))
	for key, value := range values {
		result[key] = value
	}
	return result
}
