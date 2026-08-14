package cli

import (
	"context"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

const maxCompletionTimeout = 2 * time.Second

type completionFileClient interface {
	List(context.Context, string) ([]*codev1.FileInfo, error)
}

type commandCompleter struct {
	client  completionFileClient
	cwd     func() string
	timeout time.Duration
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

func newCompleter(client completionFileClient, cwd func() string, timeout time.Duration) *commandCompleter {
	if timeout <= 0 || timeout > maxCompletionTimeout {
		timeout = maxCompletionTimeout
	}
	return &commandCompleter{client: client, cwd: cwd, timeout: timeout}
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
		return formatCompletions(input, commandCompletionCandidates())
	}

	command := input.values[0]
	arguments := input.values[1:]
	current := arguments[len(arguments)-1]
	previous := arguments[:len(arguments)-1]
	var candidates []completionCandidate
	switch command {
	case "help":
		if len(previous) == 0 {
			candidates = commandCompletionCandidates()
		}
	case "cd":
		if len(previous) == 0 {
			candidates = c.completeRemotePath(current, completeDirectories)
		}
	case "ls":
		candidates = c.completeWithOption(previous, current, "-l", 1, completeAnyPath)
	case "tree":
		if len(previous) == 0 {
			candidates = c.completeRemotePath(current, completeDirectories)
		}
	case "stat":
		if len(previous) == 0 {
			candidates = c.completeRemotePath(current, completeAnyPath)
		}
	case "cat":
		if len(previous) == 0 {
			candidates = c.completeRemotePath(current, completeFiles)
		}
	case "upload":
		switch len(previous) {
		case 0:
			candidates = completeLocalPath(current, completeFiles)
		case 1:
			candidates = c.completeRemotePath(current, completeAnyPath)
		}
	case "download":
		switch len(previous) {
		case 0:
			candidates = c.completeRemotePath(current, completeFiles)
		case 1:
			candidates = completeLocalPath(current, completeAnyPath)
		}
	case "mkdir":
		candidates = c.completeWithOption(previous, current, "-p", 1, completeDirectories)
	case "rm":
		candidates = c.completeWithOption(previous, current, "-r", 1, completeAnyPath)
	case "mv":
		candidates = c.completeWithOption(previous, current, "-f", 2, completeAnyPath)
	case "chmod":
		switch len(previous) {
		case 0:
			for _, mode := range []string{"0600", "0640", "0644", "0700", "0755"} {
				candidates = append(candidates, completionCandidate{value: mode, finish: true})
			}
		case 1:
			candidates = c.completeRemotePath(current, completeAnyPath)
		}
	}
	return formatCompletions(input, candidates)
}

func commandCompletionCandidates() []completionCandidate {
	names := make([]string, 0, len(commandUsage))
	for name := range commandUsage {
		names = append(names, name)
	}
	sort.Strings(names)
	candidates := make([]completionCandidate, 0, len(names))
	for _, name := range names {
		candidates = append(candidates, completionCandidate{value: name, finish: true})
	}
	return candidates
}

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
