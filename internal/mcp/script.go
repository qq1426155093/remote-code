package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/ast"
	"github.com/expr-lang/expr/parser"
	"github.com/expr-lang/expr/vm"
)

const maxExprNodes = 20_000

type ScriptPolicy struct {
	Capabilities []string
	Strongest    hostEffect
	Mutation     string
}

type scriptEnvironment struct {
	Context context.Context    `expr:"__ctx"`
	Args    map[string]any     `expr:"args"`
	Call    scriptCallMetadata `expr:"call"`
}

type scriptCallMetadata struct {
	ID         string `expr:"id"`
	Tool       string `expr:"tool"`
	ClientName string `expr:"client_name"`
}

func compileScript(source string, declared []string) (*vm.Program, ScriptPolicy, error) {
	if source == "" {
		return nil, ScriptPolicy{}, errors.New("script is required")
	}
	if len(source) > maxScriptBytes || !utf8.ValidString(source) || strings.ContainsRune(source, '\x00') {
		return nil, ScriptPolicy{}, errors.New("script must be valid UTF-8 without NUL and at most 65536 bytes")
	}
	tree, err := parser.Parse(source)
	if err != nil {
		return nil, ScriptPolicy{}, fmt.Errorf("parse Expr script: %w", err)
	}
	analysis := scriptAnalysis{required: make(map[string]struct{}), strongest: effectRead}
	analysis.walk(tree.Node, 0, 0)
	if analysis.err != nil {
		return nil, ScriptPolicy{}, analysis.err
	}
	if analysis.nodes > maxExprNodes {
		return nil, ScriptPolicy{}, fmt.Errorf("Expr script contains more than %d nodes", maxExprNodes)
	}
	if analysis.hostCalls > 16 {
		return nil, ScriptPolicy{}, errors.New("Expr script may contain at most 16 host call sites")
	}
	if analysis.collectionCalls > 32 {
		return nil, ScriptPolicy{}, errors.New("Expr script may contain at most 32 collection call sites")
	}
	if analysis.mutations > 1 {
		return nil, ScriptPolicy{}, errors.New("Expr script may contain at most one mutating host call")
	}
	if analysis.mutations == 1 && finalHostCall(tree.Node) != analysis.mutationName {
		return nil, ScriptPolicy{}, errors.New("the mutating host call must be the script's final expression")
	}
	required := make([]string, 0, len(analysis.required))
	for capability := range analysis.required {
		required = append(required, capability)
	}
	sort.Strings(required)
	declaredCopy := append([]string(nil), declared...)
	sort.Strings(declaredCopy)
	if strings.Join(required, "\x00") != strings.Join(declaredCopy, "\x00") {
		return nil, ScriptPolicy{}, fmt.Errorf("declared capabilities %v do not exactly match script capabilities %v", declared, required)
	}
	hostNames := make(map[string]struct{}, len(analysis.hostNames))
	for name := range analysis.hostNames {
		hostNames[name] = struct{}{}
	}
	environment := scriptEnvironment{Context: context.Background(), Args: map[string]any{}, Call: scriptCallMetadata{}}
	options := []expr.Option{expr.Env(environment), expr.WithContext("__ctx"), expr.AsAny()}
	options = append(options, hostOptions(hostNames)...)
	program, err := expr.Compile(source, options...)
	if err != nil {
		return nil, ScriptPolicy{}, fmt.Errorf("compile Expr script: %w", err)
	}
	return program, ScriptPolicy{Capabilities: required, Strongest: analysis.strongest, Mutation: analysis.mutationName}, nil
}

type scriptAnalysis struct {
	nodes           int
	mutations       int
	hostCalls       int
	collectionCalls int
	mutationName    string
	strongest       hostEffect
	required        map[string]struct{}
	hostNames       map[string]struct{}
	err             error
}

func (s *scriptAnalysis) walk(node ast.Node, depth int, predicateDepth int) {
	if node == nil || s.err != nil {
		return
	}
	s.nodes++
	if depth > maxYAMLDepth {
		s.err = fmt.Errorf("Expr script exceeds maximum depth %d", maxYAMLDepth)
		return
	}
	if s.hostNames == nil {
		s.hostNames = make(map[string]struct{})
	}
	if call, ok := node.(*ast.CallNode); ok {
		identifier, ok := call.Callee.(*ast.IdentifierNode)
		if ok {
			if definition, host := hostCatalog[identifier.Value]; host {
				s.hostCalls++
				if len(call.Arguments) != definition.arity {
					s.err = fmt.Errorf("host function %s expects %d arguments", definition.name, definition.arity)
					return
				}
				s.required[definition.capability] = struct{}{}
				s.hostNames[definition.name] = struct{}{}
				if definition.effect > s.strongest {
					s.strongest = definition.effect
				}
				if predicateDepth > 0 {
					s.err = fmt.Errorf("host function %s is not allowed in a collection predicate", definition.name)
					return
				}
				if definition.effect != effectRead {
					s.mutations++
					s.mutationName = definition.name
				}
			}
		}
	}
	switch n := node.(type) {
	case *ast.UnaryNode:
		s.walk(n.Node, depth+1, predicateDepth)
	case *ast.BinaryNode:
		if n.Operator == ".." {
			s.err = errors.New("Expr range operator is not allowed")
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
		if identifier, ok := n.Callee.(*ast.IdentifierNode); ok && identifier.Value == "repeat" {
			s.err = errors.New("repeat is not allowed")
			return
		}
		s.walk(n.Callee, depth+1, predicateDepth)
		for _, argument := range n.Arguments {
			s.walk(argument, depth+1, predicateDepth)
		}
	case *ast.BuiltinNode:
		if n.Name == "repeat" {
			s.err = errors.New("repeat is not allowed")
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

func finalHostCall(node ast.Node) string {
	for {
		switch n := node.(type) {
		case *ast.VariableDeclaratorNode:
			node = n.Expr
		case *ast.SequenceNode:
			if len(n.Nodes) == 0 {
				return ""
			}
			node = n.Nodes[len(n.Nodes)-1]
		default:
			call, ok := node.(*ast.CallNode)
			if !ok {
				return ""
			}
			identifier, ok := call.Callee.(*ast.IdentifierNode)
			if !ok {
				return ""
			}
			return identifier.Value
		}
	}
}
