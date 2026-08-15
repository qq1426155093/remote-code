package mcpserver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/files"
	processservice "github.com/qq1426155093/remote-code/internal/process"
)

type controllerHosts struct {
	files     *files.Service
	processes *processservice.Service
}

func (h *controllerHosts) Call(ctx context.Context, name string, arguments []any) (any, error) {
	switch name {
	case "file_stat":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		response, err := h.files.Stat(ctx, &codev1.StatRequest{Path: path})
		if err != nil {
			return nil, err
		}
		return fileInfoJSON(response.GetFile()), nil
	case "file_list":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		response, err := h.files.List(ctx, &codev1.ListRequest{Path: path})
		if err != nil {
			return nil, err
		}
		result := make([]any, len(response.GetFiles()))
		for index, file := range response.GetFiles() {
			result[index] = fileInfoJSON(file)
		}
		return result, nil
	case "file_tree":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		response, err := h.files.Tree(ctx, &codev1.TreeRequest{Path: path})
		if err != nil {
			return nil, err
		}
		return treeNodeJSON(response.GetRoot()), nil
	case "file_read_text":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		maximum, err := intArgument(arguments[1], "max_bytes", 1, 16<<20)
		if err != nil {
			return nil, err
		}
		result, err := h.files.ReadText(ctx, path, maximum)
		if err != nil {
			return nil, err
		}
		return map[string]any{"path": result.File.GetPath(), "content": result.Text, "size": result.Size, "sha256": hex.EncodeToString(result.SHA256)}, nil
	case "file_write_text":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		content, err := stringArgument(arguments[1], "content")
		if err != nil {
			return nil, err
		}
		overwrite, err := boolArgument(arguments[2], "overwrite")
		if err != nil {
			return nil, err
		}
		mode, err := intArgument(arguments[3], "mode", 0, 0o777)
		if err != nil {
			return nil, err
		}
		response, err := h.files.WriteText(ctx, path, content, overwrite, uint32(mode))
		if err != nil {
			return nil, err
		}
		return map[string]any{"file": fileInfoJSON(response.GetFile()), "size": response.GetSize(), "sha256": hex.EncodeToString(response.GetSha256())}, nil
	case "file_mkdir":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		parents, err := boolArgument(arguments[1], "parents")
		if err != nil {
			return nil, err
		}
		mode, err := intArgument(arguments[2], "mode", 0, 0o777)
		if err != nil {
			return nil, err
		}
		response, err := h.files.Mkdir(ctx, &codev1.MkdirRequest{Path: path, Parents: parents, Mode: uint32(mode)})
		if err != nil {
			return nil, err
		}
		return fileInfoJSON(response.GetFile()), nil
	case "file_move":
		source, err := stringArgument(arguments[0], "source")
		if err != nil {
			return nil, err
		}
		destination, err := stringArgument(arguments[1], "destination")
		if err != nil {
			return nil, err
		}
		overwrite, err := boolArgument(arguments[2], "overwrite")
		if err != nil {
			return nil, err
		}
		response, err := h.files.Move(ctx, &codev1.MoveRequest{Source: source, Destination: destination, Overwrite: overwrite})
		if err != nil {
			return nil, err
		}
		return fileInfoJSON(response.GetFile()), nil
	case "file_chmod":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		mode, err := intArgument(arguments[1], "mode", 0, 0o777)
		if err != nil {
			return nil, err
		}
		response, err := h.files.Chmod(ctx, &codev1.ChmodRequest{Path: path, Mode: uint32(mode)})
		if err != nil {
			return nil, err
		}
		return fileInfoJSON(response.GetFile()), nil
	case "file_remove":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		recursive, err := boolArgument(arguments[1], "recursive")
		if err != nil {
			return nil, err
		}
		response, err := h.files.Remove(ctx, &codev1.RemoveRequest{Path: path, Recursive: recursive})
		if err != nil {
			return nil, err
		}
		return map[string]any{"path": response.GetPath()}, nil
	case "process_start":
		request, err := startProcessArgument(arguments[0])
		if err != nil {
			return nil, err
		}
		response, err := h.processes.StartProcess(ctx, request)
		if err != nil {
			return nil, err
		}
		return processInfoJSON(response.GetProcess()), nil
	case "process_list":
		all, err := boolArgument(arguments[0], "all")
		if err != nil {
			return nil, err
		}
		response, err := h.processes.ListProcesses(ctx, &codev1.ListProcessesRequest{All: all})
		if err != nil {
			return nil, err
		}
		result := make([]any, len(response.GetProcesses()))
		for index, process := range response.GetProcesses() {
			result[index] = processInfoJSON(process)
		}
		return result, nil
	case "process_signal":
		reference, err := processReferenceArgument(arguments[0])
		if err != nil {
			return nil, err
		}
		signal, err := processSignalArgument(arguments[1])
		if err != nil {
			return nil, err
		}
		wait, err := boolArgument(arguments[2], "wait")
		if err != nil {
			return nil, err
		}
		response, err := h.processes.SignalProcess(ctx, &codev1.SignalProcessRequest{Process: reference, Signal: signal, Wait: wait})
		if err != nil {
			return nil, err
		}
		return processInfoJSON(response.GetProcess()), nil
	case "process_delete":
		reference, err := processReferenceArgument(arguments[0])
		if err != nil {
			return nil, err
		}
		response, err := h.processes.DeleteProcess(ctx, &codev1.DeleteProcessRequest{Process: reference})
		if err != nil {
			return nil, err
		}
		return processInfoJSON(response.GetProcess()), nil
	case "process_logs":
		return h.processLogs(ctx, arguments)
	default:
		return nil, errors.New("unknown host function")
	}
}

func (h *controllerHosts) processLogs(ctx context.Context, arguments []any) (any, error) {
	processID, err := stringArgument(arguments[0], "process_id")
	if err != nil {
		return nil, err
	}
	streamNames, err := stringSliceArgument(arguments[1], "streams")
	if err != nil {
		return nil, err
	}
	streams := make([]codev1.ProcessLogStream, 0, len(streamNames))
	for _, stream := range streamNames {
		switch strings.ToLower(stream) {
		case "stdout":
			streams = append(streams, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT)
		case "stderr":
			streams = append(streams, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR)
		default:
			return nil, fmt.Errorf("unsupported log stream %q", stream)
		}
	}
	tailLines, err := intArgument(arguments[2], "tail_lines", 0, 100000)
	if err != nil {
		return nil, err
	}
	maxBytes, err := intArgument(arguments[3], "max_bytes", 1, 16<<20)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.processes.SnapshotLogs(ctx, processID, streams, uint64(tailLines), maxBytes)
	if err != nil {
		return nil, err
	}
	chunks := make([]any, 0, len(snapshot.Chunks))
	for _, chunk := range snapshot.Chunks {
		item := map[string]any{
			"offset": int64(chunk.Offset), "line_offset": int64(chunk.LineOffset), "timestamp": chunk.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"),
			"stream":     strings.ToLower(strings.TrimPrefix(chunk.Stream.String(), "PROCESS_LOG_STREAM_")),
			"line_start": chunk.LineStart, "line_end": chunk.LineEnd, "line_truncated": chunk.LineTruncated,
		}
		if utf8.Valid(chunk.Data) {
			item["data"] = string(chunk.Data)
		} else {
			item["data_base64"] = base64.StdEncoding.EncodeToString(chunk.Data)
		}
		chunks = append(chunks, item)
	}
	return map[string]any{
		"process_id": snapshot.ProcessID, "io_mode": strings.ToLower(strings.TrimPrefix(snapshot.IOMode.String(), "PROCESS_IO_MODE_")),
		"earliest_offset": int64(snapshot.EarliestOffset), "snapshot_end_offset": int64(snapshot.SnapshotEnd),
		"resolved_start_offset": int64(snapshot.ResolvedStart), "history_truncated": snapshot.HistoryTruncated,
		"tail_truncated": snapshot.TailTruncated, "bytes_truncated": snapshot.BytesTruncated,
		"logs_complete": snapshot.LogsComplete, "chunks": chunks,
	}, nil
}

func fileInfoJSON(file *codev1.FileInfo) any {
	if file == nil {
		return nil
	}
	result := map[string]any{
		"path": file.GetPath(), "name": file.GetName(), "type": strings.ToLower(strings.TrimPrefix(file.GetType().String(), "FILE_TYPE_")),
		"size": file.GetSize(), "mode": int64(file.GetMode()), "symlink_target": file.GetSymlinkTarget(),
	}
	if file.GetModifiedAt() != nil {
		result["modified_at"] = file.GetModifiedAt().AsTime().UTC().Format("2006-01-02T15:04:05.000000000Z")
	}
	return result
}

func treeNodeJSON(node *codev1.TreeNode) any {
	if node == nil {
		return nil
	}
	children := make([]any, len(node.GetChildren()))
	for index, child := range node.GetChildren() {
		children[index] = treeNodeJSON(child)
	}
	return map[string]any{"file": fileInfoJSON(node.GetFile()), "children": children}
}

func processInfoJSON(process *codev1.ProcessInfo) any {
	if process == nil {
		return nil
	}
	result := map[string]any{
		"id": process.GetId(), "name": process.GetName(), "pid": process.GetPid(),
		"io_mode": strings.ToLower(strings.TrimPrefix(process.GetIoMode().String(), "PROCESS_IO_MODE_")),
		"state":   strings.ToLower(strings.TrimPrefix(process.GetState().String(), "PROCESS_STATE_")),
		"command": process.GetCommand(), "arguments": append([]string(nil), process.GetArguments()...),
		"working_directory": process.GetWorkingDirectory(), "environment_keys": append([]string(nil), process.GetEnvironmentKeys()...),
		"input_mode":    strings.ToLower(strings.TrimPrefix(process.GetInputMode().String(), "PROCESS_INPUT_MODE_")),
		"input_state":   strings.ToLower(strings.TrimPrefix(process.GetInputState().String(), "PROCESS_INPUT_STATE_")),
		"template_name": process.GetTemplateName(), "template_revision": process.GetTemplateRevision(),
		"arguments_redacted": process.GetArgumentsRedacted(),
	}
	if process.GetCreatedAt() != nil {
		result["created_at"] = process.GetCreatedAt().AsTime().UTC().Format("2006-01-02T15:04:05.000000000Z")
	}
	if process.GetStartedAt() != nil {
		result["started_at"] = process.GetStartedAt().AsTime().UTC().Format("2006-01-02T15:04:05.000000000Z")
	}
	if process.GetExitedAt() != nil {
		result["exited_at"] = process.GetExitedAt().AsTime().UTC().Format("2006-01-02T15:04:05.000000000Z")
	}
	if process.ExitCode != nil {
		result["exit_code"] = int64(process.GetExitCode())
	}
	if process.ExitSignal != nil {
		result["exit_signal"] = int64(process.GetExitSignal())
	}
	return result
}

func startProcessArgument(value any) (*codev1.StartProcessRequest, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("process options must be an object")
	}
	allowed := map[string]bool{"name": true, "command": true, "arguments": true, "working_directory": true, "io_mode": true, "environment": true, "input_mode": true, "terminal_size": true}
	for key := range object {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown process option %q", key)
		}
	}
	request := &codev1.StartProcessRequest{}
	var err error
	if value, exists := object["name"]; exists {
		request.Name, err = stringArgument(value, "name")
		if err != nil {
			return nil, err
		}
	}
	command, exists := object["command"]
	if !exists {
		return nil, errors.New("process option command is required")
	}
	request.Command, err = stringArgument(command, "command")
	if err != nil {
		return nil, err
	}
	if value, exists := object["arguments"]; exists {
		request.Arguments, err = stringSliceArgument(value, "arguments")
		if err != nil {
			return nil, err
		}
	}
	if value, exists := object["working_directory"]; exists {
		request.WorkingDirectory, err = stringArgument(value, "working_directory")
		if err != nil {
			return nil, err
		}
	}
	if value, exists := object["environment"]; exists {
		request.Environment, err = stringMapArgument(value, "environment")
		if err != nil {
			return nil, err
		}
	}
	if value, exists := object["io_mode"]; exists {
		mode, modeErr := stringArgument(value, "io_mode")
		if modeErr != nil {
			return nil, modeErr
		}
		switch strings.ToLower(mode) {
		case "pipe":
			request.IoMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE
		case "pty":
			request.IoMode = codev1.ProcessIOMode_PROCESS_IO_MODE_PTY
		default:
			return nil, errors.New("io_mode must be pipe or pty")
		}
	}
	if value, exists := object["input_mode"]; exists {
		mode, modeErr := stringArgument(value, "input_mode")
		if modeErr != nil {
			return nil, modeErr
		}
		switch strings.ToLower(mode) {
		case "disabled":
			request.InputMode = codev1.ProcessInputMode_PROCESS_INPUT_MODE_DISABLED
		case "managed":
			request.InputMode = codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED
		default:
			return nil, errors.New("input_mode must be disabled or managed")
		}
	}
	if value, exists := object["terminal_size"]; exists {
		size, ok := value.(map[string]any)
		if !ok {
			return nil, errors.New("terminal_size must be an object")
		}
		for key := range size {
			if key != "rows" && key != "columns" {
				return nil, fmt.Errorf("unknown terminal_size option %q", key)
			}
		}
		rows, rowErr := intArgument(size["rows"], "terminal_size.rows", 1, 65535)
		if rowErr != nil {
			return nil, rowErr
		}
		columns, columnErr := intArgument(size["columns"], "terminal_size.columns", 1, 65535)
		if columnErr != nil {
			return nil, columnErr
		}
		request.TerminalSize = &codev1.TerminalSize{Rows: uint32(rows), Columns: uint32(columns)}
	}
	return request, nil
}

func processReferenceArgument(value any) (*codev1.ProcessReference, error) {
	object, ok := value.(map[string]any)
	if !ok || len(object) != 1 {
		return nil, errors.New("process reference must contain exactly one of id, name, or pid")
	}
	if raw, exists := object["id"]; exists {
		value, err := stringArgument(raw, "id")
		if err != nil {
			return nil, err
		}
		return &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: value}}, nil
	}
	if raw, exists := object["name"]; exists {
		value, err := stringArgument(raw, "name")
		if err != nil {
			return nil, err
		}
		return &codev1.ProcessReference{Value: &codev1.ProcessReference_Name{Name: value}}, nil
	}
	if raw, exists := object["pid"]; exists {
		value, err := intArgument(raw, "pid", 1, 1<<53)
		if err != nil {
			return nil, err
		}
		return &codev1.ProcessReference{Value: &codev1.ProcessReference_Pid{Pid: value}}, nil
	}
	return nil, errors.New("process reference must contain id, name, or pid")
}

func processSignalArgument(value any) (codev1.ProcessSignal, error) {
	name, err := stringArgument(value, "signal")
	if err != nil {
		return 0, err
	}
	signals := map[string]codev1.ProcessSignal{"hup": 1, "int": 2, "quit": 3, "term": 4, "kill": 5, "usr1": 6, "usr2": 7, "stop": 8, "cont": 9}
	signal, ok := signals[strings.ToLower(name)]
	if !ok {
		return 0, fmt.Errorf("unsupported process signal %q", name)
	}
	return signal, nil
}

func stringArgument(value any, name string) (string, error) {
	result, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("%s must be a string", name)
	}
	return result, nil
}
func boolArgument(value any, name string) (bool, error) {
	result, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("%s must be a boolean", name)
	}
	return result, nil
}
func intArgument(value any, name string, minimum, maximum int64) (int64, error) {
	var result int64
	switch typed := value.(type) {
	case int:
		result = int64(typed)
	case int64:
		result = typed
	case float64:
		if typed != float64(int64(typed)) {
			return 0, fmt.Errorf("%s must be an integer", name)
		}
		result = int64(typed)
	default:
		return 0, fmt.Errorf("%s must be an integer", name)
	}
	if result < minimum || result > maximum {
		return 0, fmt.Errorf("%s must be between %d and %d", name, minimum, maximum)
	}
	return result, nil
}
func stringSliceArgument(value any, name string) ([]string, error) {
	items, ok := value.([]any)
	if !ok {
		if typed, ok := value.([]string); ok {
			return append([]string(nil), typed...), nil
		}
		return nil, fmt.Errorf("%s must be an array", name)
	}
	result := make([]string, len(items))
	for index, item := range items {
		text, err := stringArgument(item, name)
		if err != nil {
			return nil, err
		}
		result[index] = text
	}
	return result, nil
}
func stringMapArgument(value any, name string) (map[string]string, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("%s must be an object", name)
	}
	result := make(map[string]string, len(object))
	for key, item := range object {
		text, err := stringArgument(item, name+"."+key)
		if err != nil {
			return nil, err
		}
		result[key] = text
	}
	return result, nil
}
