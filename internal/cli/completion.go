package cli

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

const maxCompletionTimeout = 2 * time.Second

type completionFileClient interface {
	List(context.Context, string) ([]*codev1.FileInfo, error)
}

type completionProcessClient interface {
	ListProcesses(context.Context, ...bool) ([]*codev1.ProcessInfo, error)
}

type completionTemplateClient interface {
	ListProcessTemplates(context.Context) ([]*codev1.ProcessTemplateSummary, error)
}

type commandCompleter struct {
	client   completionFileClient
	cwd      func() string
	timeout  time.Duration
	commands *commandRegistry
}

type completionCandidate struct {
	value  string
	finish bool
}

type completionInput struct {
	values []string
	raw    string
	value  string
	quote  rune
}

type completionPathKind int

const (
	completeAnyPath completionPathKind = iota
	completeDirectories
	completeFiles
)

func newCompleter(client completionFileClient, cwd func() string, timeout time.Duration, commands *commandRegistry) *commandCompleter {
	if timeout <= 0 || timeout > maxCompletionTimeout {
		timeout = maxCompletionTimeout
	}
	return &commandCompleter{client: client, cwd: cwd, timeout: timeout, commands: commands}
}

// Do implements readline.AutoCompleter. Candidates are generated only for the
// text to the left of the cursor so editing in the middle of a line is safe.
func (c *commandCompleter) Do(line []rune, pos int) ([][]rune, int) {
	if pos < 0 {
		pos = 0
	}
	if pos > len(line) {
		pos = len(line)
	}
	input := parseCompletionInput(line[:pos])
	if len(input.values) == 1 {
		return formatCompletions(input, c.commands.commandCandidates())
	}

	spec, ok := c.commands.lookup(input.values[0])
	if !ok || spec.complete == nil {
		return formatCompletions(input, nil)
	}
	arguments := input.values[1:]
	current := arguments[len(arguments)-1]
	previous := arguments[:len(arguments)-1]
	candidates := spec.complete(c, previous, current)
	return formatCompletions(input, candidates)
}

func (c *commandCompleter) completeHelp(previous []string, _ string) []completionCandidate {
	if len(previous) != 0 {
		return nil
	}
	return c.commands.commandCandidates()
}

func completeFirstRemotePath(kind completionPathKind) commandCompletion {
	return func(c *commandCompleter, previous []string, current string) []completionCandidate {
		if len(previous) != 0 {
			return nil
		}
		return c.completeRemotePath(current, kind)
	}
}

func completeRemotePathWithOption(option string, maxValues int, kind completionPathKind) commandCompletion {
	return func(c *commandCompleter, previous []string, current string) []completionCandidate {
		return c.completeWithOption(previous, current, option, maxValues, kind)
	}
}

func (c *commandCompleter) completeUpload(previous []string, current string) []completionCandidate {
	switch len(previous) {
	case 0:
		return completeLocalPath(current, completeFiles)
	case 1:
		return c.completeRemotePath(current, completeAnyPath)
	default:
		return nil
	}
}

func (c *commandCompleter) completeDownload(previous []string, current string) []completionCandidate {
	switch len(previous) {
	case 0:
		return c.completeRemotePath(current, completeFiles)
	case 1:
		return completeLocalPath(current, completeAnyPath)
	default:
		return nil
	}
}

func (c *commandCompleter) completeChmod(previous []string, current string) []completionCandidate {
	switch len(previous) {
	case 0:
		candidates := make([]completionCandidate, 0, 5)
		for _, mode := range []string{"0600", "0640", "0644", "0700", "0755"} {
			candidates = append(candidates, completionCandidate{value: mode, finish: true})
		}
		return candidates
	case 1:
		return c.completeRemotePath(current, completeAnyPath)
	default:
		return nil
	}
}

func (c *commandCompleter) completePS(previous []string, current string) []completionCandidate {
	if len(previous) == 0 && (current == "" || strings.HasPrefix(current, "-")) {
		return []completionCandidate{{value: "-a", finish: true}}
	}
	return nil
}

func (c *commandCompleter) completeForget(previous []string, current string) []completionCandidate {
	if strings.HasPrefix(current, "-") || strings.ContainsAny(current, "*?[") {
		return nil
	}
	processClient, ok := c.client.(completionProcessClient)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	processes, err := processClient.ListProcesses(ctx, true)
	if err != nil {
		return nil
	}
	selectedNames := make(map[string]bool, len(previous))
	for _, argument := range previous {
		selector, err := parseProcessSelector(argument)
		if err == nil && selector.GetReference() != nil && selector.GetReference().GetName() != "" {
			selectedNames[selector.GetReference().GetName()] = true
		}
	}
	seenNames := make(map[string]bool, len(processes))
	candidates := make([]completionCandidate, 0, len(processes))
	for _, process := range processes {
		if process != nil && !isActiveProcessState(process.GetState()) && !selectedNames[process.GetName()] && !seenNames[process.GetName()] {
			candidates = append(candidates, completionCandidate{value: process.GetName(), finish: true})
			seenNames[process.GetName()] = true
		}
	}
	return candidates
}

func isActiveProcessState(state codev1.ProcessState) bool {
	return state == codev1.ProcessState_PROCESS_STATE_STARTING || state == codev1.ProcessState_PROCESS_STATE_RUNNING
}

func (c *commandCompleter) completeLogs(previous []string, current string) []completionCandidate {
	used := make(map[string]bool)
	expectValue := false
	targetSeen := false
	for _, argument := range previous {
		if expectValue {
			expectValue = false
			continue
		}
		switch argument {
		case "-n", "--tail":
			used["start"] = true
			expectValue = true
		case "--offset":
			used["start"] = true
			expectValue = true
		case "-f", "--follow":
			used["follow"] = true
		case "--stdout":
			used["stdout"] = true
		case "--stderr":
			used["stderr"] = true
		default:
			targetSeen = true
		}
	}
	if expectValue || targetSeen || (current != "" && !strings.HasPrefix(current, "-")) {
		return nil
	}
	var candidates []completionCandidate
	for _, option := range []struct {
		value string
		key   string
	}{
		{value: "-f", key: "follow"},
		{value: "-n", key: "start"},
		{value: "--offset", key: "start"},
		{value: "--stdout", key: "stdout"},
		{value: "--stderr", key: "stderr"},
	} {
		if !used[option.key] {
			candidates = append(candidates, completionCandidate{value: option.value, finish: true})
		}
	}
	return candidates
}

func (c *commandCompleter) completeExec(previous []string, current string) []completionCandidate {
	if len(previous) > 0 {
		switch previous[len(previous)-1] {
		case "--cwd":
			return c.completeRemotePath(current, completeDirectories)
		case "--name", "-e", "--env":
			return nil
		}
	}
	used := make(map[string]bool)
	expectValue := false
	commandStarted := false
	optionsEnded := false
	for _, argument := range previous {
		if expectValue {
			expectValue = false
			continue
		}
		switch {
		case optionsEnded:
			commandStarted = true
		case argument == "--name" || argument == "--cwd" || argument == "-e" || argument == "--env":
			if argument == "--name" || argument == "--cwd" {
				used[argument] = true
			}
			expectValue = true
		case argument == "--pipe" || argument == "--pty" || argument == "--stdin" || argument == "--attach":
			used[argument] = true
		case argument == "--":
			optionsEnded = true
		default:
			commandStarted = true
		}
		if commandStarted {
			break
		}
	}
	if commandStarted {
		return nil
	}
	candidates := make([]completionCandidate, 0)
	if !optionsEnded && (current == "" || strings.HasPrefix(current, "-")) {
		for _, option := range []string{"--cwd", "--name", "--pipe", "--pty", "--stdin", "--attach", "-e"} {
			if option == "-e" || !used[option] {
				candidates = append(candidates, completionCandidate{value: option, finish: true})
			}
		}
	}
	return candidates
}

func (c *commandCompleter) completeProcessTemplateName(previous []string, current string) []completionCandidate {
	if len(previous) != 0 || strings.HasPrefix(current, "-") {
		return nil
	}
	return c.processTemplateCandidates()
}

func (c *commandCompleter) completeExecTemplate(previous []string, current string) []completionCandidate {
	if len(previous) > 0 {
		switch previous[len(previous)-1] {
		case "--params-file":
			return completeLocalPath(current, completeFiles)
		case "--name", "--params":
			return nil
		}
	}
	used := make(map[string]bool)
	expectValue := false
	templateSeen := false
	optionsEnded := false
	for _, argument := range previous {
		if expectValue {
			expectValue = false
			continue
		}
		switch {
		case optionsEnded:
			templateSeen = true
		case argument == "--name" || argument == "--params" || argument == "--params-file":
			if argument != "--name" {
				used["parameters"] = true
			} else {
				used[argument] = true
			}
			expectValue = true
		case argument == "--attach":
			used[argument] = true
		case argument == "--":
			optionsEnded = true
		default:
			templateSeen = true
		}
	}
	if templateSeen {
		return nil
	}
	var candidates []completionCandidate
	if !optionsEnded && (current == "" || strings.HasPrefix(current, "-")) {
		for _, option := range []struct {
			value string
			key   string
		}{
			{value: "--name", key: "--name"},
			{value: "--attach", key: "--attach"},
			{value: "--params", key: "parameters"},
			{value: "--params-file", key: "parameters"},
		} {
			if !used[option.key] {
				candidates = append(candidates, completionCandidate{value: option.value, finish: true})
			}
		}
	}
	if current == "" || !strings.HasPrefix(current, "-") {
		candidates = append(candidates, c.processTemplateCandidates()...)
	}
	return candidates
}

func (c *commandCompleter) processTemplateCandidates() []completionCandidate {
	client, ok := c.client.(completionTemplateClient)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	templates, err := client.ListProcessTemplates(ctx)
	if err != nil {
		return nil
	}
	result := make([]completionCandidate, 0, len(templates))
	for _, template := range templates {
		if template != nil && template.GetName() != "" {
			result = append(result, completionCandidate{value: template.GetName(), finish: true})
		}
	}
	return result
}

func (c *commandCompleter) completeProcessAttach(previous []string, current string) []completionCandidate {
	if len(previous) > 0 || strings.HasPrefix(current, "-") {
		return nil
	}
	return c.processAttachCandidates(nil)
}

func (c *commandCompleter) completeProcessWindows(previous []string, current string) []completionCandidate {
	usedTailLines := false
	expectTailLines := false
	optionsEnded := false
	usedProcesses := make(map[string]bool)
	for _, argument := range previous {
		if expectTailLines {
			expectTailLines = false
			continue
		}
		switch {
		case optionsEnded:
			usedProcesses[argument] = true
		case argument == "-n" || argument == "--tail-lines":
			usedTailLines = true
			expectTailLines = true
		case argument == "--":
			optionsEnded = true
		default:
			usedProcesses[argument] = true
		}
	}
	if expectTailLines {
		return nil
	}
	candidates := make([]completionCandidate, 0)
	if !optionsEnded && (current == "" || strings.HasPrefix(current, "-")) && !usedTailLines {
		candidates = append(candidates,
			completionCandidate{value: "-n", finish: true},
			completionCandidate{value: "--tail-lines", finish: true},
		)
	}
	if optionsEnded || current == "" || !strings.HasPrefix(current, "-") {
		candidates = append(candidates, c.processAttachCandidates(usedProcesses)...)
	}
	return candidates
}

func (c *commandCompleter) processAttachCandidates(excluded map[string]bool) []completionCandidate {
	processClient, ok := c.client.(completionProcessClient)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	processes, err := processClient.ListProcesses(ctx)
	if err != nil {
		return nil
	}
	var candidates []completionCandidate
	for _, process := range processes {
		if process != nil && process.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING &&
			process.GetIoMode() == codev1.ProcessIOMode_PROCESS_IO_MODE_PTY &&
			process.GetInputMode() == codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED &&
			process.GetInputState() != codev1.ProcessInputState_PROCESS_INPUT_STATE_CLOSED &&
			!excluded[process.GetName()] {
			candidates = append(candidates, completionCandidate{value: process.GetName(), finish: true})
		}
	}
	return candidates
}

func (c *commandCompleter) completeProcessInput(previous []string, current string) []completionCandidate {
	if len(previous) > 0 || strings.HasPrefix(current, "-") {
		return nil
	}
	processClient, ok := c.client.(completionProcessClient)
	if !ok {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	processes, err := processClient.ListProcesses(ctx)
	if err != nil {
		return nil
	}
	var candidates []completionCandidate
	for _, process := range processes {
		if process != nil && process.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING &&
			process.GetInputMode() == codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED &&
			process.GetInputState() != codev1.ProcessInputState_PROCESS_INPUT_STATE_CLOSED {
			candidates = append(candidates, completionCandidate{value: process.GetName(), finish: true})
		}
	}
	return candidates
}

func (c *commandCompleter) completeKill(previous []string, current string) []completionCandidate {
	if len(previous) > 0 && (previous[len(previous)-1] == "-s" || previous[len(previous)-1] == "--signal") {
		candidates := make([]completionCandidate, 0, len(processSignalCompletions))
		for _, signal := range processSignalCompletions {
			candidates = append(candidates, completionCandidate{value: signal, finish: true})
		}
		return candidates
	}
	targetSeen := false
	expectSignal := false
	usedWait := false
	usedSignal := false
	for _, argument := range previous {
		if expectSignal {
			expectSignal = false
			continue
		}
		switch argument {
		case "-s", "--signal":
			usedSignal = true
			expectSignal = true
		case "-w", "--wait":
			usedWait = true
		case "--":
		default:
			targetSeen = true
		}
	}
	candidates := make([]completionCandidate, 0)
	if current == "" || strings.HasPrefix(current, "-") {
		if !usedSignal {
			candidates = append(candidates, completionCandidate{value: "-s", finish: true})
		}
		if !usedWait {
			candidates = append(candidates, completionCandidate{value: "-w", finish: true})
		}
	}
	if !targetSeen && (current == "" || !strings.HasPrefix(current, "-")) {
		processClient, ok := c.client.(completionProcessClient)
		if !ok {
			return candidates
		}
		ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
		defer cancel()
		processes, err := processClient.ListProcesses(ctx)
		if err != nil {
			return candidates
		}
		for _, process := range processes {
			if process != nil && process.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING {
				candidates = append(candidates, completionCandidate{value: process.GetName(), finish: true})
			}
		}
	}
	return candidates
}

var processSignalCompletions = []string{"CONT", "HUP", "INT", "KILL", "QUIT", "STOP", "TERM", "USR1", "USR2"}

func (c *commandCompleter) completeWithOption(previous []string, current, option string, maxValues int, kind completionPathKind) []completionCandidate {
	usedOption := false
	valueCount := 0
	for _, argument := range previous {
		if argument == option {
			usedOption = true
		} else {
			valueCount++
		}
	}
	if strings.HasPrefix(current, "-") {
		if !usedOption && valueCount < maxValues {
			return []completionCandidate{{value: option, finish: true}}
		}
		return nil
	}
	candidates := make([]completionCandidate, 0)
	if current == "" && !usedOption && valueCount < maxValues {
		candidates = append(candidates, completionCandidate{value: option, finish: true})
	}
	if valueCount < maxValues {
		candidates = append(candidates, c.completeRemotePath(current, kind)...)
	}
	return candidates
}

func (c *commandCompleter) completeRemotePath(partial string, kind completionPathKind) []completionCandidate {
	directoryInput, namePrefix, resultPrefix := splitRemoteCompletionPath(partial)
	directory, err := resolveRemotePath(c.cwd(), directoryInput)
	if err != nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), c.timeout)
	defer cancel()
	files, err := c.client.List(ctx, directory)
	if err != nil {
		return nil
	}
	candidates := make([]completionCandidate, 0, len(files))
	for _, file := range files {
		if file == nil || !strings.HasPrefix(file.GetName(), namePrefix) || hideDotFile(file.GetName(), namePrefix) {
			continue
		}
		isDirectory := file.GetType() == codev1.FileType_FILE_TYPE_DIRECTORY
		if (kind == completeDirectories && !isDirectory) || (kind == completeFiles && isDirectory) {
			continue
		}
		value := resultPrefix + file.GetName()
		if isDirectory {
			value += "/"
		}
		candidates = append(candidates, completionCandidate{value: value, finish: !isDirectory})
	}
	return candidates
}

func splitRemoteCompletionPath(partial string) (directory, namePrefix, resultPrefix string) {
	separator := strings.LastIndex(partial, "/")
	if separator < 0 {
		return "", partial, ""
	}
	resultPrefix = partial[:separator+1]
	directory = partial[:separator]
	if directory == "" && strings.HasPrefix(partial, "/") {
		directory = "/"
	}
	return directory, partial[separator+1:], resultPrefix
}

func completeLocalPath(partial string, kind completionPathKind) []completionCandidate {
	directory, namePrefix := filepath.Split(partial)
	readDirectory := directory
	if readDirectory == "" {
		readDirectory = "."
	}
	entries, err := os.ReadDir(readDirectory)
	if err != nil {
		return nil
	}
	candidates := make([]completionCandidate, 0, len(entries))
	for _, entry := range entries {
		if !strings.HasPrefix(entry.Name(), namePrefix) || hideDotFile(entry.Name(), namePrefix) {
			continue
		}
		isDirectory, isRegular := localEntryType(readDirectory, entry)
		if (kind == completeDirectories && !isDirectory) || (kind == completeFiles && !isDirectory && !isRegular) {
			continue
		}
		value := directory + entry.Name()
		if isDirectory {
			value += string(filepath.Separator)
		}
		candidates = append(candidates, completionCandidate{value: value, finish: !isDirectory})
	}
	return candidates
}

func localEntryType(directory string, entry os.DirEntry) (isDirectory, isRegular bool) {
	info, err := entry.Info()
	if err != nil {
		return false, false
	}
	if info.Mode()&os.ModeSymlink != 0 {
		info, err = os.Stat(filepath.Join(directory, entry.Name()))
		if err != nil {
			return false, false
		}
	}
	return info.IsDir(), info.Mode().IsRegular()
}

func hideDotFile(name, prefix string) bool {
	return strings.HasPrefix(name, ".") && !strings.HasPrefix(prefix, ".")
}

func formatCompletions(input completionInput, candidates []completionCandidate) ([][]rune, int) {
	result := make([][]rune, 0, len(candidates))
	seen := make(map[string]struct{}, len(candidates))
	for _, candidate := range candidates {
		suffix, ok := strings.CutPrefix(candidate.value, input.value)
		if !ok {
			continue
		}
		encoded := encodeCompletionFragment(suffix, input.quote)
		if candidate.finish {
			if input.quote != 0 {
				encoded += string(input.quote)
			}
			encoded += " "
		}
		if _, ok := seen[encoded]; ok {
			continue
		}
		seen[encoded] = struct{}{}
		result = append(result, []rune(encoded))
	}
	return result, len([]rune(input.raw))
}

func encodeCompletionFragment(value string, quote rune) string {
	var result strings.Builder
	for _, character := range value {
		switch quote {
		case '\'':
			if character == '\'' {
				result.WriteString("'\\''")
			} else {
				result.WriteRune(character)
			}
		case '"':
			if strings.ContainsRune("\\\"$`", character) {
				result.WriteRune('\\')
			}
			result.WriteRune(character)
		default:
			if unicode.IsSpace(character) || strings.ContainsRune("\\\"'", character) {
				result.WriteRune('\\')
			}
			result.WriteRune(character)
		}
	}
	return result.String()
}

func parseCompletionInput(line []rune) completionInput {
	values := make([]string, 0)
	var value strings.Builder
	tokenStart := 0
	tokenStarted := false
	var quote rune
	escaped := false
	for index, character := range line {
		if escaped {
			value.WriteRune(character)
			escaped = false
			tokenStarted = true
			continue
		}
		switch quote {
		case '\'':
			if character == '\'' {
				quote = 0
			} else {
				value.WriteRune(character)
			}
			tokenStarted = true
		case '"':
			switch character {
			case '"':
				quote = 0
			case '\\':
				escaped = true
			default:
				value.WriteRune(character)
			}
			tokenStarted = true
		default:
			switch {
			case unicode.IsSpace(character):
				if tokenStarted {
					values = append(values, value.String())
					value.Reset()
					tokenStarted = false
				}
				tokenStart = index + 1
			case character == '\\':
				escaped = true
				tokenStarted = true
			case character == '\'' || character == '"':
				quote = character
				tokenStarted = true
			default:
				value.WriteRune(character)
				tokenStarted = true
			}
		}
	}
	values = append(values, value.String())
	return completionInput{
		values: values,
		raw:    string(line[tokenStart:]),
		value:  value.String(),
		quote:  quote,
	}
}
