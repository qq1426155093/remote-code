package mcpserver

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/expr-lang/expr"
)

type hostEffect uint8

const (
	effectRead hostEffect = iota
	effectMutate
	effectDestructive
)

type hostDefinition struct {
	name       string
	capability string
	effect     hostEffect
	arity      int
}

var hostCatalog = map[string]hostDefinition{
	"file_stat":       {"file_stat", "files.read", effectRead, 1},
	"file_list":       {"file_list", "files.read", effectRead, 1},
	"file_tree":       {"file_tree", "files.read", effectRead, 1},
	"file_read_text":  {"file_read_text", "files.read", effectRead, 2},
	"file_write_text": {"file_write_text", "files.write", effectDestructive, 4},
	"file_mkdir":      {"file_mkdir", "files.write", effectMutate, 3},
	"file_move":       {"file_move", "files.write", effectDestructive, 3},
	"file_chmod":      {"file_chmod", "files.write", effectMutate, 2},
	"file_remove":     {"file_remove", "files.write", effectDestructive, 2},
	"process_start":   {"process_start", "processes.start", effectMutate, 1},
	"process_list":    {"process_list", "processes.read", effectRead, 1},
	"process_signal":  {"process_signal", "processes.signal", effectDestructive, 3},
	"process_delete":  {"process_delete", "processes.signal", effectDestructive, 1},
	"process_logs":    {"process_logs", "processes.read", effectRead, 4},
}

var capabilityCatalog = func() map[string]struct{} {
	result := make(map[string]struct{})
	for _, definition := range hostCatalog {
		result[definition.capability] = struct{}{}
	}
	return result
}()

// HostDispatcher invokes controller capabilities after compile-time and
// runtime authorization checks.
type HostDispatcher interface {
	Call(context.Context, string, []any) (any, error)
}

type invocation struct {
	dispatcher   HostDispatcher
	capabilities map[string]struct{}
	mutations    int
	calls        int
	resultBytes  int64
}

type invocationContextKey struct{}

func withInvocation(ctx context.Context, dispatcher HostDispatcher, capabilities []string) context.Context {
	allowed := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		allowed[capability] = struct{}{}
	}
	return context.WithValue(ctx, invocationContextKey{}, &invocation{dispatcher: dispatcher, capabilities: allowed})
}

func hostOptions(names map[string]struct{}) []expr.Option {
	options := make([]expr.Option, 0, len(names))
	for name := range names {
		definition := hostCatalog[name]
		options = append(options, bindHostOption(definition))
	}
	return options
}

func bindHostOption(definition hostDefinition) expr.Option {
	callback := func(values ...any) (any, error) {
		if len(values) == 0 {
			return nil, errors.New("host context is unavailable")
		}
		ctx, ok := values[0].(context.Context)
		if !ok {
			return nil, errors.New("host context is unavailable")
		}
		call, ok := ctx.Value(invocationContextKey{}).(*invocation)
		if !ok || call.dispatcher == nil {
			return nil, errors.New("host dispatcher is unavailable")
		}
		if _, ok := call.capabilities[definition.capability]; !ok {
			return nil, fmt.Errorf("host capability %q is not authorized", definition.capability)
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		call.calls++
		if call.calls > 16 {
			return nil, errors.New("tool invocation exceeded its host call budget")
		}
		if len(values)-1 != definition.arity {
			return nil, fmt.Errorf("host function %s received an invalid argument count", definition.name)
		}
		if definition.effect != effectRead {
			call.mutations++
			if call.mutations > 1 {
				return nil, errors.New("tool invocation exceeded its mutation budget")
			}
		}
		result, err := call.dispatcher.Call(ctx, definition.name, values[1:])
		if err != nil {
			return nil, err
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(result)
		if err != nil {
			return nil, errors.New("host function returned an invalid result")
		}
		call.resultBytes += int64(len(encoded))
		if call.resultBytes > 16<<20 {
			return nil, errors.New("tool invocation exceeded its host result budget")
		}
		return result, nil
	}
	return expr.Function(definition.name, callback, hostSignature(definition.arity))
}

func hostSignature(arity int) any {
	switch arity {
	case 1:
		return new(func(context.Context, any) any)
	case 2:
		return new(func(context.Context, any, any) any)
	case 3:
		return new(func(context.Context, any, any, any) any)
	case 4:
		return new(func(context.Context, any, any, any, any) any)
	default:
		panic("unsupported host function arity")
	}
}
