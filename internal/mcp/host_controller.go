package mcpserver

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/files"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/version"
	"google.golang.org/protobuf/types/known/structpb"
)

type controllerHosts struct {
	files     *files.Service
	processes *processservice.Service
}

func (h *controllerHosts) Call(ctx context.Context, name string, arguments []any) (any, error) {
	switch name {
	case "controller_info":
		return map[string]any{
			"controller_version":     version.Version,
			"api_version":            version.APIVersion,
			"workspace_name":         h.files.WorkspaceName(),
			"max_upload_bytes":       h.files.MaxUploadBytes(),
			"max_processes":          int64(h.processes.MaxProcesses()),
			"process_template_count": int64(h.processes.ProcessTemplateCount()),
		}, nil
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
	case "file_read_range":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		startLine, err := intArgument(arguments[1], "start_line", 1, 1<<53)
		if err != nil {
			return nil, err
		}
		maxLines, err := intArgument(arguments[2], "max_lines", 1, 10_000)
		if err != nil {
			return nil, err
		}
		maxBytes, err := intArgument(arguments[3], "max_bytes", 1, 16<<20)
		if err != nil {
			return nil, err
		}
		result, err := h.files.ReadTextRange(ctx, path, uint64(startLine), int(maxLines), maxBytes)
		if err != nil {
			return nil, err
		}
		nextLine := any(nil)
		if result.NextLine > 0 {
			nextLine = int64(result.NextLine)
		}
		return map[string]any{
			"path": result.Path, "content": result.Content, "size": result.Size,
			"start_line": int64(result.StartLine), "next_line": nextLine, "line_count": int64(result.LineCount),
			"eof": result.EOF, "truncated": result.Truncated,
		}, nil
	case "file_search":
		return h.fileSearch(ctx, arguments[0])
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
	case "file_apply_patch":
		path, err := stringArgument(arguments[0], "path")
		if err != nil {
			return nil, err
		}
		expected, err := stringArgument(arguments[1], "expected_sha256")
		if err != nil {
			return nil, err
		}
		patch, err := stringArgument(arguments[2], "patch")
		if err != nil {
			return nil, err
		}
		response, err := h.files.ApplyTextPatch(ctx, path, expected, patch)
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
	case "process_get":
		reference, err := processReferenceArgument(arguments[0])
		if err != nil {
			return nil, err
		}
		process, err := h.processes.GetProcessInfo(ctx, reference)
		if err != nil {
			return nil, err
		}
		return processInfoJSON(process), nil
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
	case "process_logs_since":
		return h.processLogsSince(ctx, arguments)
	case "process_template_list":
		response, err := h.processes.ListProcessTemplates(ctx, &codev1.ListProcessTemplatesRequest{})
		if err != nil {
			return nil, err
		}
		result := make([]any, len(response.GetTemplates()))
		for index, template := range response.GetTemplates() {
			result[index] = processTemplateSummaryJSON(template)
		}
		return result, nil
	case "process_template_get":
		templateName, err := stringArgument(arguments[0], "name")
		if err != nil {
			return nil, err
		}
		response, err := h.processes.GetProcessTemplate(ctx, &codev1.GetProcessTemplateRequest{Name: templateName})
		if err != nil {
			return nil, err
		}
		return processTemplateJSON(response.GetTemplate()), nil
	case "process_template_start":
		request, err := startProcessTemplateArgument(arguments[0])
		if err != nil {
			return nil, err
		}
		response, err := h.processes.StartProcessFromTemplate(ctx, request)
		if err != nil {
			return nil, err
		}
		return processInfoJSON(response.GetProcess()), nil
	default:
		return nil, errors.New("unknown host function")
	}
}

func (h *controllerHosts) processLogs(ctx context.Context, arguments []any) (any, error) {
	processID, err := stringArgument(arguments[0], "process_id")
	if err != nil {
		return nil, err
	}
	streams, err := logStreamsArgument(arguments[1])
	if err != nil {
		return nil, err
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
	return logSnapshotJSON(snapshot, false), nil
}

func (h *controllerHosts) processLogsSince(ctx context.Context, arguments []any) (any, error) {
	processID, err := stringArgument(arguments[0], "process_id")
	if err != nil {
		return nil, err
	}
	streams, err := logStreamsArgument(arguments[1])
	if err != nil {
		return nil, err
	}
	offsetText, err := stringArgument(arguments[2], "offset")
	if err != nil {
		return nil, err
	}
	offset, err := strconv.ParseUint(offsetText, 10, 64)
	if err != nil || strconv.FormatUint(offset, 10) != offsetText {
		return nil, errors.New("offset must be a canonical unsigned decimal string")
	}
	maxBytes, err := intArgument(arguments[3], "max_bytes", 1, 16<<20)
	if err != nil {
		return nil, err
	}
	snapshot, err := h.processes.SnapshotLogsFromOffset(ctx, processID, streams, offset, maxBytes)
	if err != nil {
		return nil, err
	}
	return logSnapshotJSON(snapshot, true), nil
}

func logStreamsArgument(value any) ([]codev1.ProcessLogStream, error) {
	streamNames, err := stringSliceArgument(value, "streams")
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
	return streams, nil
}

func logSnapshotJSON(snapshot *processservice.LogSnapshot, stringOffsets bool) any {
	offsetValue := func(value uint64) any {
		if stringOffsets {
			return strconv.FormatUint(value, 10)
		}
		return int64(value)
	}
	chunks := make([]any, 0, len(snapshot.Chunks))
	for _, chunk := range snapshot.Chunks {
		item := map[string]any{
			"offset": offsetValue(chunk.Offset), "line_offset": offsetValue(chunk.LineOffset), "timestamp": chunk.Timestamp.UTC().Format("2006-01-02T15:04:05.000000000Z"),
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
		"earliest_offset": offsetValue(snapshot.EarliestOffset), "snapshot_end_offset": offsetValue(snapshot.SnapshotEnd),
		"resolved_start_offset": offsetValue(snapshot.ResolvedStart), "next_offset": offsetValue(snapshot.NextOffset), "history_truncated": snapshot.HistoryTruncated,
		"tail_truncated": snapshot.TailTruncated, "bytes_truncated": snapshot.BytesTruncated,
		"logs_complete": snapshot.LogsComplete, "chunks": chunks,
	}
}

func (h *controllerHosts) fileSearch(ctx context.Context, value any) (any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("search options must be an object")
	}
	allowed := map[string]bool{
		"path": true, "glob": true, "query": true, "case_sensitive": true,
		"max_results": true, "max_bytes": true,
	}
	for key := range object {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown search option %q", key)
		}
	}
	pathValue, err := stringArgument(object["path"], "path")
	if err != nil {
		return nil, err
	}
	glob, err := stringArgument(object["glob"], "glob")
	if err != nil {
		return nil, err
	}
	query, err := stringArgument(object["query"], "query")
	if err != nil {
		return nil, err
	}
	caseSensitive, err := boolArgument(object["case_sensitive"], "case_sensitive")
	if err != nil {
		return nil, err
	}
	maxResults, err := intArgument(object["max_results"], "max_results", 1, 1000)
	if err != nil {
		return nil, err
	}
	maxBytes, err := intArgument(object["max_bytes"], "max_bytes", 1, 64<<20)
	if err != nil {
		return nil, err
	}
	search, err := h.files.SearchText(ctx, files.SearchOptions{
		Path: pathValue, Glob: glob, Query: query, CaseSensitive: caseSensitive,
		MaxResults: int(maxResults), MaxBytes: maxBytes,
	})
	if err != nil {
		return nil, err
	}
	matches := make([]any, len(search.Matches))
	for index, match := range search.Matches {
		matches[index] = map[string]any{
			"path": match.Path, "line": int64(match.Line), "column": int64(match.Column),
			"text": match.Text, "text_truncated": match.TextTruncated,
		}
	}
	return map[string]any{
		"matches": matches, "files_scanned": int64(search.FilesScanned), "bytes_scanned": search.BytesScanned,
		"skipped_files": int64(search.SkippedFiles), "result_truncated": search.ResultTruncated, "scan_truncated": search.ScanTruncated,
	}, nil
}

func processTemplateSummaryJSON(summary *codev1.ProcessTemplateSummary) any {
	if summary == nil {
		return nil
	}
	return map[string]any{
		"name": summary.GetName(), "description": summary.GetDescription(), "revision": summary.GetRevision(),
		"io_mode":    strings.ToLower(strings.TrimPrefix(summary.GetIoMode().String(), "PROCESS_IO_MODE_")),
		"input_mode": strings.ToLower(strings.TrimPrefix(summary.GetInputMode().String(), "PROCESS_INPUT_MODE_")),
	}
}

func processTemplateJSON(template *codev1.ProcessTemplate) any {
	if template == nil {
		return nil
	}
	parametersSchema := map[string]any{}
	if template.GetParametersSchema() != nil {
		parametersSchema = template.GetParametersSchema().AsMap()
	}
	return map[string]any{"summary": processTemplateSummaryJSON(template.GetSummary()), "parameters_schema": parametersSchema}
}

func startProcessTemplateArgument(value any) (*codev1.StartProcessFromTemplateRequest, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("process template options must be an object")
	}
	allowed := map[string]bool{
		"template_name": true, "parameters": true, "process_name": true,
		"terminal_size": true, "expected_template_revision": true,
	}
	for key := range object {
		if !allowed[key] {
			return nil, fmt.Errorf("unknown process template option %q", key)
		}
	}
	templateName, err := stringArgument(object["template_name"], "template_name")
	if err != nil {
		return nil, err
	}
	request := &codev1.StartProcessFromTemplateRequest{TemplateName: templateName}
	if raw, exists := object["parameters"]; exists {
		parameters, ok := raw.(map[string]any)
		if !ok {
			return nil, errors.New("parameters must be an object")
		}
		request.Parameters, err = structpb.NewStruct(parameters)
		if err != nil {
			return nil, errors.New("parameters contain an unsupported value")
		}
	}
	if raw, exists := object["process_name"]; exists {
		request.ProcessName, err = stringArgument(raw, "process_name")
		if err != nil {
			return nil, err
		}
	}
	if raw, exists := object["expected_template_revision"]; exists {
		request.ExpectedTemplateRevision, err = stringArgument(raw, "expected_template_revision")
		if err != nil {
			return nil, err
		}
	}
	if raw, exists := object["terminal_size"]; exists {
		request.TerminalSize, err = terminalSizeArgument(raw)
		if err != nil {
			return nil, err
		}
	}
	return request, nil
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
		request.TerminalSize, err = terminalSizeArgument(value)
		if err != nil {
			return nil, err
		}
	}
	return request, nil
}

func terminalSizeArgument(value any) (*codev1.TerminalSize, error) {
	size, ok := value.(map[string]any)
	if !ok {
		return nil, errors.New("terminal_size must be an object")
	}
	for key := range size {
		if key != "rows" && key != "columns" {
			return nil, fmt.Errorf("unknown terminal_size option %q", key)
		}
	}
	rows, err := intArgument(size["rows"], "terminal_size.rows", 1, 65535)
	if err != nil {
		return nil, err
	}
	columns, err := intArgument(size["columns"], "terminal_size.columns", 1, 65535)
	if err != nil {
		return nil, err
	}
	return &codev1.TerminalSize{Rows: uint32(rows), Columns: uint32(columns)}, nil
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
