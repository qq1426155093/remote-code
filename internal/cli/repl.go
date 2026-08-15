package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/chzyer/readline"
	"github.com/google/shlex"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errCatLimit = errors.New("cat output limit exceeded")

// Config controls one interactive session.
type Config struct {
	Timeout     time.Duration
	CatMaxBytes int64
	Stdout      io.Writer
	Stderr      io.Writer
}

// REPL implements the long-running terminal command loop.
type REPL struct {
	client      *remoteclient.Client
	line        *readline.Instance
	stdout      io.Writer
	stderr      io.Writer
	timeout     time.Duration
	catMaxBytes int64
	cwd         string
}

// New creates an interactive REPL around an established client and terminal.
func New(client *remoteclient.Client, line *readline.Instance, config Config) *REPL {
	stdout := config.Stdout
	if stdout == nil {
		stdout = os.Stdout
	}
	stderr := config.Stderr
	if stderr == nil {
		stderr = os.Stderr
	}
	repl := &REPL{
		client: client, line: line, stdout: stdout, stderr: stderr,
		timeout: config.Timeout, catMaxBytes: config.CatMaxBytes, cwd: ".",
	}
	if line != nil && line.Config != nil {
		line.Config.AutoComplete = newCompleter(client, func() string { return repl.cwd }, config.Timeout)
	}
	return repl
}

// Run reads commands until exit, quit, or terminal EOF.
func (r *REPL) Run() error {
	info := r.client.Info()
	fmt.Fprintf(r.stdout, "Connected to remote-code-controller v%s (API %s, workspace %s)\n",
		info.GetControllerVersion(), info.GetApiVersion(), info.GetWorkspaceName())
	fmt.Fprintln(r.stdout, "Type 'help' for available commands; press Tab for completion.")
	for {
		r.line.SetPrompt("remote-code:" + displayCWD(r.cwd) + "> ")
		line, err := r.line.Readline()
		switch {
		case errors.Is(err, readline.ErrInterrupt):
			continue
		case errors.Is(err, io.EOF):
			return nil
		case err != nil:
			return fmt.Errorf("read terminal input: %w", err)
		}
		arguments, err := parseCommand(line)
		if err != nil {
			r.printError(fmt.Errorf("parse command: %w", err))
			continue
		}
		if len(arguments) == 0 {
			continue
		}
		if arguments[0] == "exit" || arguments[0] == "quit" {
			if len(arguments) != 1 {
				r.printError(fmt.Errorf("usage: %s", arguments[0]))
				continue
			}
			return nil
		}
		if err := r.execute(arguments); err != nil {
			r.printError(err)
		}
	}
}

func (r *REPL) execute(arguments []string) error {
	switch arguments[0] {
	case "help":
		return r.help(arguments[1:])
	case "pwd":
		if len(arguments) != 1 {
			return errors.New("usage: pwd")
		}
		fmt.Fprintln(r.stdout, displayCWD(r.cwd))
		return nil
	case "cd":
		return r.changeDirectory(arguments[1:])
	case "ls":
		return r.list(arguments[1:])
	case "tree":
		return r.tree(arguments[1:])
	case "exec":
		return r.startProcess(arguments[1:])
	case "ps":
		return r.listProcesses(arguments[1:])
	case "kill":
		return r.signalProcess(arguments[1:])
	case "forget":
		return r.forgetProcess(arguments[1:])
	case "logs":
		return r.observeProcessLogs(arguments[1:])
	case "stat":
		return r.stat(arguments[1:])
	case "cat":
		return r.cat(arguments[1:])
	case "upload":
		return r.upload(arguments[1:])
	case "download":
		return r.download(arguments[1:])
	case "mkdir":
		return r.mkdir(arguments[1:])
	case "rm":
		return r.remove(arguments[1:])
	case "mv":
		return r.move(arguments[1:])
	case "chmod":
		return r.chmod(arguments[1:])
	case "clear":
		if len(arguments) != 1 {
			return errors.New("usage: clear")
		}
		fmt.Fprint(r.stdout, "\033[H\033[2J")
		return nil
	default:
		return fmt.Errorf("unknown command %q; type 'help' for available commands", arguments[0])
	}
}

func (r *REPL) help(arguments []string) error {
	if len(arguments) > 1 {
		return errors.New("usage: help [command]")
	}
	if len(arguments) == 1 {
		usage, ok := commandUsage[arguments[0]]
		if !ok {
			return fmt.Errorf("unknown command %q", arguments[0])
		}
		fmt.Fprintln(r.stdout, usage)
		return nil
	}
	commands := []string{"help [command]", "pwd", "cd [REMOTE_DIR]", "ls [-l] [REMOTE_PATH]", "tree [REMOTE_PATH]", "stat REMOTE_PATH", "cat REMOTE_FILE", "upload LOCAL_FILE [REMOTE_FILE]", "download REMOTE_FILE [LOCAL_FILE]", "mkdir [-p] REMOTE_DIR", "rm [-r] REMOTE_PATH", "mv [-f] SOURCE DESTINATION", "chmod OCTAL_MODE REMOTE_PATH", "exec [--name NAME] [--pipe|--pty] [--cwd REMOTE_DIR] [-e KEY=VALUE ...] [--] CMD [ARG ...]", "ps [-a]", "kill [-s SIGNAL] [-w] PROCESS", "forget PROCESS", "logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID", "clear", "exit | quit"}
	for _, command := range commands {
		fmt.Fprintln(r.stdout, command)
	}
	return nil
}

var commandUsage = map[string]string{
	"help":     "usage: help [command]",
	"pwd":      "usage: pwd",
	"cd":       "usage: cd [REMOTE_DIR]",
	"ls":       "usage: ls [-l] [REMOTE_PATH]",
	"tree":     "usage: tree [REMOTE_PATH]",
	"stat":     "usage: stat REMOTE_PATH",
	"cat":      "usage: cat REMOTE_FILE",
	"upload":   "usage: upload LOCAL_FILE [REMOTE_FILE]",
	"download": "usage: download REMOTE_FILE [LOCAL_FILE]",
	"mkdir":    "usage: mkdir [-p] REMOTE_DIR",
	"rm":       "usage: rm [-r] REMOTE_PATH",
	"mv":       "usage: mv [-f] SOURCE DESTINATION",
	"chmod":    "usage: chmod OCTAL_MODE REMOTE_PATH",
	"exec":     "usage: exec [--name NAME] [--pipe|--pty] [--cwd REMOTE_DIR] [-e KEY=VALUE ...] [--] CMD [ARG ...]",
	"ps":       "usage: ps [-a]",
	"kill":     "usage: kill [-s SIGNAL] [-w] PROCESS",
	"forget":   "usage: forget PROCESS",
	"logs":     "usage: logs [-f] [-n LINES|--offset OFFSET] [--stdout|--stderr] PROCESS_ID",
	"clear":    "usage: clear",
	"exit":     "usage: exit",
	"quit":     "usage: quit",
}

func (r *REPL) changeDirectory(arguments []string) error {
	if len(arguments) > 1 {
		return errors.New("usage: cd [REMOTE_DIR]")
	}
	target := "/"
	if len(arguments) == 1 {
		target = arguments[0]
	}
	remotePath, err := resolveRemotePath(r.cwd, target)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.Stat(ctx, remotePath)
	if err != nil {
		return err
	}
	if info.GetType() != codev1.FileType_FILE_TYPE_DIRECTORY {
		return fmt.Errorf("%s is not a directory", displayCWD(remotePath))
	}
	r.cwd = remotePath
	return nil
}

func (r *REPL) list(arguments []string) error {
	long := false
	filtered := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		if argument == "-l" {
			long = true
			continue
		}
		if strings.HasPrefix(argument, "-") {
			return fmt.Errorf("unknown ls option %q", argument)
		}
		filtered = append(filtered, argument)
	}
	if len(filtered) > 1 {
		return errors.New("usage: ls [-l] [REMOTE_PATH]")
	}
	target := ""
	if len(filtered) == 1 {
		target = filtered[0]
	}
	remotePath, err := resolveRemotePath(r.cwd, target)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	files, err := r.client.List(ctx, remotePath)
	if err != nil {
		return err
	}
	if !long {
		for _, file := range files {
			fmt.Fprintln(r.stdout, displayFileName(file))
		}
		return nil
	}
	writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
	for _, file := range files {
		fmt.Fprintf(writer, "%s\t%d\t%s\t%s\n", modeString(file), file.GetSize(), formatTime(file), displayFileName(file))
	}
	return writer.Flush()
}

func (r *REPL) tree(arguments []string) error {
	if len(arguments) > 1 {
		return errors.New("usage: tree [REMOTE_PATH]")
	}
	target := ""
	if len(arguments) == 1 {
		target = arguments[0]
	}
	remotePath, err := resolveRemotePath(r.cwd, target)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	root, err := r.client.Tree(ctx, remotePath)
	if err != nil {
		return err
	}
	printTree(r.stdout, root)
	return nil
}

func printTree(writer io.Writer, root *codev1.TreeNode) {
	if root == nil || root.GetFile() == nil {
		return
	}
	fmt.Fprintln(writer, root.GetFile().GetPath())
	printTreeChildren(writer, root.GetChildren(), "")
}

func printTreeChildren(writer io.Writer, children []*codev1.TreeNode, prefix string) {
	for index, child := range children {
		if child == nil || child.GetFile() == nil {
			continue
		}
		last := index == len(children)-1
		connector := "├── "
		childPrefix := prefix + "│   "
		if last {
			connector = "└── "
			childPrefix = prefix + "    "
		}
		fmt.Fprintln(writer, prefix+connector+displayFileName(child.GetFile()))
		printTreeChildren(writer, child.GetChildren(), childPrefix)
	}
}

func (r *REPL) stat(arguments []string) error {
	if len(arguments) != 1 {
		return errors.New("usage: stat REMOTE_PATH")
	}
	remotePath, err := resolveRemotePath(r.cwd, arguments[0])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.Stat(ctx, remotePath)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "Path: %s\nType: %s\nSize: %d\nMode: %04o (%s)\nModified: %s\n",
		info.GetPath(), fileTypeName(info.GetType()), info.GetSize(), info.GetMode(), modeString(info), formatTime(info))
	if info.GetSymlinkTarget() != "" {
		fmt.Fprintf(r.stdout, "Link target: %s\n", info.GetSymlinkTarget())
	}
	return nil
}

func (r *REPL) cat(arguments []string) error {
	if len(arguments) != 1 {
		return errors.New("usage: cat REMOTE_FILE")
	}
	remotePath, err := resolveRemotePath(r.cwd, arguments[0])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	writer := &limitWriter{writer: r.stdout, remaining: r.catMaxBytes}
	_, err = r.client.Download(ctx, remotePath, writer)
	if errors.Is(err, errCatLimit) {
		return fmt.Errorf("cat output exceeds %d bytes; use download instead", r.catMaxBytes)
	}
	if err == nil {
		fmt.Fprintln(r.stdout)
	}
	return err
}

func (r *REPL) upload(arguments []string) error {
	if len(arguments) < 1 || len(arguments) > 2 {
		return errors.New("usage: upload LOCAL_FILE [REMOTE_FILE]")
	}
	target := filepath.Base(arguments[0])
	if len(arguments) == 2 {
		target = arguments[1]
	}
	remotePath, err := resolveRemotePath(r.cwd, target)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	response, err := r.client.UploadFile(ctx, arguments[0], remotePath, true)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "uploaded %d bytes to %s\n", response.GetSize(), response.GetFile().GetPath())
	return nil
}

func (r *REPL) download(arguments []string) error {
	if len(arguments) < 1 || len(arguments) > 2 {
		return errors.New("usage: download REMOTE_FILE [LOCAL_FILE]")
	}
	remotePath, err := resolveRemotePath(r.cwd, arguments[0])
	if err != nil {
		return err
	}
	localPath := path.Base(remotePath)
	if len(arguments) == 2 {
		localPath = arguments[1]
	}
	if localPath == "." || localPath == "/" {
		return errors.New("a local download filename is required")
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	result, err := r.client.DownloadFile(ctx, remotePath, localPath)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "downloaded %d bytes to %s\n", result.Size, localPath)
	return nil
}

func (r *REPL) mkdir(arguments []string) error {
	parents, values, err := parseBooleanOption(arguments, "-p")
	if err != nil || len(values) != 1 {
		return errors.New("usage: mkdir [-p] REMOTE_DIR")
	}
	remotePath, err := resolveRemotePath(r.cwd, values[0])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.Mkdir(ctx, remotePath, 0o755, parents)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "created %s\n", info.GetPath())
	return nil
}

func (r *REPL) remove(arguments []string) error {
	recursive, values, err := parseBooleanOption(arguments, "-r")
	if err != nil || len(values) != 1 {
		return errors.New("usage: rm [-r] REMOTE_PATH")
	}
	remotePath, err := resolveRemotePath(r.cwd, values[0])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	if err := r.client.Remove(ctx, remotePath, recursive); err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "removed %s\n", displayCWD(remotePath))
	return nil
}

func (r *REPL) move(arguments []string) error {
	overwrite, values, err := parseBooleanOption(arguments, "-f")
	if err != nil || len(values) != 2 {
		return errors.New("usage: mv [-f] SOURCE DESTINATION")
	}
	source, err := resolveRemotePath(r.cwd, values[0])
	if err != nil {
		return err
	}
	destination, err := resolveRemotePath(r.cwd, values[1])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.Move(ctx, source, destination, overwrite)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "moved to %s\n", info.GetPath())
	return nil
}

func (r *REPL) chmod(arguments []string) error {
	if len(arguments) != 2 {
		return errors.New("usage: chmod OCTAL_MODE REMOTE_PATH")
	}
	mode, err := parseMode(arguments[0])
	if err != nil {
		return err
	}
	remotePath, err := resolveRemotePath(r.cwd, arguments[1])
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.Chmod(ctx, remotePath, os.FileMode(mode))
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "mode of %s changed to %04o\n", info.GetPath(), info.GetMode())
	return nil
}

func (r *REPL) commandContext() (context.Context, context.CancelFunc) {
	if r.timeout <= 0 {
		return context.WithCancel(context.Background())
	}
	return context.WithTimeout(context.Background(), r.timeout)
}

func (r *REPL) printError(err error) {
	code := status.Code(err)
	if code != codes.Unknown {
		fmt.Fprintf(r.stderr, "error: %s (%s)\n", status.Convert(err).Message(), code)
		return
	}
	fmt.Fprintf(r.stderr, "error: %s\n", err)
}

func resolveRemotePath(cwd, input string) (string, error) {
	base := cwd
	if strings.HasPrefix(input, "/") {
		base = "."
		input = strings.TrimLeft(input, "/")
	}
	clean := path.Clean(path.Join(base, input))
	if clean == ".." || strings.HasPrefix(clean, "../") {
		return "", errors.New("remote path cannot go above workspace root")
	}
	return clean, nil
}

func displayCWD(cwd string) string {
	if cwd == "." {
		return "/"
	}
	return "/" + cwd
}

func parseBooleanOption(arguments []string, option string) (bool, []string, error) {
	enabled := false
	values := make([]string, 0, len(arguments))
	for _, argument := range arguments {
		if argument == option {
			enabled = true
			continue
		}
		if strings.HasPrefix(argument, "-") {
			return false, nil, fmt.Errorf("unknown option %q", argument)
		}
		values = append(values, argument)
	}
	return enabled, values, nil
}

func parseMode(value string) (uint32, error) {
	if value == "" || len(value) > 4 {
		return 0, errors.New("mode must be an octal value between 0000 and 0777")
	}
	mode, err := strconv.ParseUint(value, 8, 32)
	if err != nil || mode > 0o777 {
		return 0, errors.New("mode must be an octal value between 0000 and 0777")
	}
	return uint32(mode), nil
}

func parseCommand(line string) ([]string, error) {
	return shlex.Split(line)
}

func modeString(info *codev1.FileInfo) string {
	mode := os.FileMode(info.GetMode())
	switch info.GetType() {
	case codev1.FileType_FILE_TYPE_DIRECTORY:
		mode |= os.ModeDir
	case codev1.FileType_FILE_TYPE_SYMLINK:
		mode |= os.ModeSymlink
	}
	return mode.String()
}

func displayFileName(info *codev1.FileInfo) string {
	if info.GetSymlinkTarget() != "" {
		return info.GetName() + " -> " + info.GetSymlinkTarget()
	}
	return info.GetName()
}

func fileTypeName(fileType codev1.FileType) string {
	switch fileType {
	case codev1.FileType_FILE_TYPE_REGULAR:
		return "regular"
	case codev1.FileType_FILE_TYPE_DIRECTORY:
		return "directory"
	case codev1.FileType_FILE_TYPE_SYMLINK:
		return "symlink"
	case codev1.FileType_FILE_TYPE_OTHER:
		return "other"
	default:
		return "unspecified"
	}
}

func formatTime(info *codev1.FileInfo) string {
	if info.GetModifiedAt() == nil || !info.GetModifiedAt().IsValid() {
		return "-"
	}
	return info.GetModifiedAt().AsTime().Local().Format(time.RFC3339)
}

type limitWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *limitWriter) Write(data []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, errCatLimit
	}
	if int64(len(data)) > w.remaining {
		data = data[:w.remaining]
		n, err := w.writer.Write(data)
		w.remaining -= int64(n)
		if err != nil {
			return n, err
		}
		return n, errCatLimit
	}
	n, err := w.writer.Write(data)
	w.remaining -= int64(n)
	return n, err
}
