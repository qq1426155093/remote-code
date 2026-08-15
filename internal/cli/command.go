package cli

import (
	"errors"
	"fmt"
	"sort"
	"strings"
)

type commandAction uint8

const (
	commandContinue commandAction = iota
	commandExit
)

type commandHandler func(*REPL, []string) error

type commandCompletion func(*commandCompleter, []string, string) []completionCandidate

type commandSpec struct {
	name       string
	aliases    []string
	arguments  string
	listSuffix string
	details    string
	handler    commandHandler
	complete   commandCompletion
	action     commandAction
}

func (c *commandSpec) synopsis() string {
	names := make([]string, 0, 1+len(c.aliases))
	names = append(names, c.name)
	names = append(names, c.aliases...)
	synopsis := strings.Join(names, " | ")
	if c.arguments != "" {
		synopsis += " " + c.arguments
	}
	if c.listSuffix != "" {
		synopsis += " " + c.listSuffix
	}
	return synopsis
}

func (c *commandSpec) usage(invokedName string) string {
	usage := "usage: " + invokedName
	if c.arguments != "" {
		usage += " " + c.arguments
	}
	if c.details != "" {
		usage += "\n" + c.details
	}
	return usage
}

type commandRegistry struct {
	ordered         []commandSpec
	byName          map[string]*commandSpec
	completionNames []string
}

func newCommandRegistry(specs []commandSpec) (*commandRegistry, error) {
	registry := &commandRegistry{
		ordered: append([]commandSpec(nil), specs...),
		byName:  make(map[string]*commandSpec),
	}
	for index := range registry.ordered {
		spec := &registry.ordered[index]
		spec.aliases = append([]string(nil), spec.aliases...)
		if err := validateCommandName(spec.name); err != nil {
			return nil, fmt.Errorf("command %d: %w", index, err)
		}
		if spec.handler == nil {
			return nil, fmt.Errorf("command %q has no handler", spec.name)
		}
		if spec.action != commandContinue && spec.action != commandExit {
			return nil, fmt.Errorf("command %q has invalid action %d", spec.name, spec.action)
		}
		for _, name := range append([]string{spec.name}, spec.aliases...) {
			if err := validateCommandName(name); err != nil {
				return nil, fmt.Errorf("command %q alias: %w", spec.name, err)
			}
			if existing, ok := registry.byName[name]; ok {
				return nil, fmt.Errorf("command name %q is already registered by %q", name, existing.name)
			}
			registry.byName[name] = spec
			registry.completionNames = append(registry.completionNames, name)
		}
	}
	sort.Strings(registry.completionNames)
	return registry, nil
}

func validateCommandName(name string) error {
	if name == "" {
		return errors.New("command name is empty")
	}
	if strings.IndexAny(name, " \t\r\n") >= 0 {
		return fmt.Errorf("command name %q contains whitespace", name)
	}
	return nil
}

func (r *commandRegistry) lookup(name string) (*commandSpec, bool) {
	spec, ok := r.byName[name]
	return spec, ok
}

func (r *commandRegistry) commandCandidates() []completionCandidate {
	candidates := make([]completionCandidate, 0, len(r.completionNames))
	for _, name := range r.completionNames {
		candidates = append(candidates, completionCandidate{value: name, finish: true})
	}
	return candidates
}

type commandUsageError struct {
	reason string
}

func (e *commandUsageError) Error() string {
	if e.reason != "" {
		return e.reason
	}
	return "invalid command usage"
}

func usageError() error {
	return &commandUsageError{}
}

func usageErrorf(format string, arguments ...any) error {
	return &commandUsageError{reason: fmt.Sprintf(format, arguments...)}
}

var defaultCommandRegistry = mustCommandRegistry([]commandSpec{
	{name: "help", arguments: "[command]", handler: (*REPL).help, complete: (*commandCompleter).completeHelp},
	{name: "pwd", handler: (*REPL).printWorkingDirectory},
	{name: "cd", arguments: "[REMOTE_DIR]", handler: (*REPL).changeDirectory, complete: completeFirstRemotePath(completeDirectories)},
	{name: "ls", arguments: "[-l] [REMOTE_PATH]", handler: (*REPL).list, complete: completeRemotePathWithOption("-l", 1, completeAnyPath)},
	{name: "tree", arguments: "[REMOTE_PATH]", handler: (*REPL).tree, complete: completeFirstRemotePath(completeDirectories)},
	{name: "stat", arguments: "REMOTE_PATH", handler: (*REPL).stat, complete: completeFirstRemotePath(completeAnyPath)},
	{name: "cat", arguments: "REMOTE_FILE", handler: (*REPL).cat, complete: completeFirstRemotePath(completeFiles)},
	{name: "upload", arguments: "LOCAL_FILE [REMOTE_FILE]", handler: (*REPL).upload, complete: (*commandCompleter).completeUpload},
	{name: "download", arguments: "REMOTE_FILE [LOCAL_FILE]", handler: (*REPL).download, complete: (*commandCompleter).completeDownload},
	{name: "mkdir", arguments: "[-p] REMOTE_DIR", handler: (*REPL).mkdir, complete: completeRemotePathWithOption("-p", 1, completeDirectories)},
	{name: "rm", arguments: "[-r] REMOTE_PATH", handler: (*REPL).remove, complete: completeRemotePathWithOption("-r", 1, completeAnyPath)},
	{name: "mv", arguments: "[-f] SOURCE DESTINATION", handler: (*REPL).move, complete: completeRemotePathWithOption("-f", 2, completeAnyPath)},
	{name: "chmod", arguments: "OCTAL_MODE REMOTE_PATH", handler: (*REPL).chmod, complete: (*commandCompleter).completeChmod},
	{name: "exec", arguments: "[--name NAME] [--pipe|--pty] [--stdin|--attach] [--cwd REMOTE_DIR] [-e KEY=VALUE ...] [--] CMD [ARG ...]", handler: (*REPL).startProcess, complete: (*commandCompleter).completeExec},
	{name: "ps", arguments: "[-a]", handler: (*REPL).listProcesses, complete: (*commandCompleter).completePS},
	{name: "kill", arguments: "[-s SIGNAL] [-w] PROCESS", handler: (*REPL).signalProcess, complete: (*commandCompleter).completeKill},
	{name: "stdin", arguments: "PROCESS", handler: (*REPL).writeProcessInput, complete: (*commandCompleter).completeProcessInput},
	{name: "attach", arguments: "PROCESS", handler: (*REPL).attachProcess, complete: (*commandCompleter).completeProcessAttach},
	{name: "forget", arguments: "PROCESS_OR_GLOB [PROCESS_OR_GLOB ...]", handler: (*REPL).forgetProcess, complete: (*commandCompleter).completeForget},
	{
		name: "logs", arguments: "[-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID",
		listSuffix: "(Ctrl-C stops following; process continues)",
		details:    "Ctrl-C stops --follow; the process continues",
		handler:    (*REPL).observeProcessLogs,
		complete:   (*commandCompleter).completeLogs,
	},
	{name: "clear", handler: (*REPL).clearScreen},
	{name: "exit", aliases: []string{"quit"}, handler: (*REPL).exitSession, action: commandExit},
})

func mustCommandRegistry(specs []commandSpec) *commandRegistry {
	registry, err := newCommandRegistry(specs)
	if err != nil {
		panic(fmt.Sprintf("initialize CLI commands: %v", err))
	}
	return registry
}

func (r *REPL) execute(arguments []string) (commandAction, error) {
	if len(arguments) == 0 {
		return commandContinue, nil
	}
	invokedName := arguments[0]
	spec, ok := r.commands.lookup(invokedName)
	if !ok {
		return commandContinue, fmt.Errorf("unknown command %q; type 'help' for available commands", invokedName)
	}
	if err := spec.handler(r, arguments[1:]); err != nil {
		var invalidUsage *commandUsageError
		if errors.As(err, &invalidUsage) {
			usage := spec.usage(invokedName)
			if invalidUsage.reason != "" {
				return commandContinue, fmt.Errorf("%s; %s", invalidUsage.reason, usage)
			}
			return commandContinue, errors.New(usage)
		}
		return commandContinue, err
	}
	return spec.action, nil
}

func (r *REPL) help(arguments []string) error {
	if len(arguments) > 1 {
		return usageError()
	}
	if len(arguments) == 1 {
		spec, ok := r.commands.lookup(arguments[0])
		if !ok {
			return fmt.Errorf("unknown command %q", arguments[0])
		}
		fmt.Fprintln(r.stdout, spec.usage(arguments[0]))
		return nil
	}
	for index := range r.commands.ordered {
		fmt.Fprintln(r.stdout, r.commands.ordered[index].synopsis())
	}
	return nil
}

func (r *REPL) printWorkingDirectory(arguments []string) error {
	if len(arguments) != 0 {
		return usageError()
	}
	fmt.Fprintln(r.stdout, displayCWD(r.cwd))
	return nil
}

func (r *REPL) clearScreen(arguments []string) error {
	if len(arguments) != 0 {
		return usageError()
	}
	fmt.Fprint(r.stdout, "\033[H\033[2J")
	return nil
}

func (r *REPL) exitSession(arguments []string) error {
	if len(arguments) != 0 {
		return usageError()
	}
	return nil
}
