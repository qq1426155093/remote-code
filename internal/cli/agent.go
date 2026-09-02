package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

type agentQueryOptions struct {
	sessionID        string
	workingDirectory string
	environment      map[string]string
	prompt           string
}

// parseAgentQueryOptions splits `agent` arguments into options and prompt
// words; the prompt is the remaining words joined by spaces, so quoted input
// keeps its spacing and a leading-dash word needs the `--` separator.
func parseAgentQueryOptions(arguments []string) (agentQueryOptions, error) {
	var options agentQueryOptions
	sessionSet := false
	words := make([]string, 0, len(arguments))
	for index := 0; index < len(arguments); index++ {
		switch argument := arguments[index]; argument {
		case "--session":
			if index+1 >= len(arguments) || sessionSet {
				return agentQueryOptions{}, usageError()
			}
			index++
			sessionSet = true
			options.sessionID = arguments[index]
		case "--cwd":
			if index+1 >= len(arguments) || options.workingDirectory != "" {
				return agentQueryOptions{}, usageError()
			}
			index++
			options.workingDirectory = arguments[index]
		case "--env":
			if index+1 >= len(arguments) {
				return agentQueryOptions{}, usageError()
			}
			index++
			key, value, found := strings.Cut(arguments[index], "=")
			if !found || key == "" {
				return agentQueryOptions{}, usageErrorf("agent --env must be KEY=VALUE, got %q", arguments[index])
			}
			if _, duplicate := options.environment[key]; duplicate {
				return agentQueryOptions{}, usageErrorf("agent --env %q given twice", key)
			}
			if options.environment == nil {
				options.environment = make(map[string]string)
			}
			options.environment[key] = value
		case "--":
			words = append(words, arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(argument, "-") {
				return agentQueryOptions{}, usageErrorf("unknown agent option %q", argument)
			}
			words = append(words, argument)
		}
	}
	if sessionSet && options.sessionID == "" {
		return agentQueryOptions{}, errors.New("agent session id must not be empty")
	}
	options.prompt = strings.Join(words, " ")
	return options, nil
}

// agentQuery sends one prompt to the shared code agent and renders the turn
// events as they stream. The REPL remembers the last session id so a
// follow-up `agent` command continues that conversation; `agent-close`
// forgets it.
func (r *REPL) agentQuery(arguments []string) error {
	options, err := parseAgentQueryOptions(arguments)
	if err != nil {
		return err
	}
	if options.prompt == "" {
		return usageError()
	}
	sessionID := options.sessionID
	if sessionID == "" {
		sessionID = r.agentSession
	}
	workingDirectory := options.workingDirectory
	if workingDirectory != "" && sessionID == "" {
		resolved, err := resolveRemotePath(r.cwd, workingDirectory)
		if err != nil {
			return err
		}
		workingDirectory = resolved
	}

	commandContext, cancelCommand := r.commandContext()
	defer cancelCommand()
	// A turn is always interruptible: Ctrl-C stops this stream and cancels the
	// turn on the controller, which prints the query id to replay later.
	streamContext, stopInterrupt := r.interruptContext(commandContext)
	defer stopInterrupt()
	interrupted := func() bool {
		return streamContext.Err() != nil && commandContext.Err() == nil
	}

	stream, err := r.client.AgentQuery(streamContext, options.prompt, remoteclient.AgentQueryOptions{
		SessionID: sessionID, WorkingDirectory: workingDirectory, Environment: options.environment,
	})
	if err != nil {
		if interrupted() {
			return nil
		}
		return err
	}
	renderer := &agentEventRenderer{output: r.stdout}
	queryID := ""
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if interrupted() {
				return r.cancelInterruptedTurn(commandContext, queryID)
			}
			return err
		}
		if queryID == "" {
			queryID = response.GetQueryId()
			if queryID != "" {
				fmt.Fprintf(r.stdout, "query: %s\n", queryID)
			}
		}
		if started := response.GetSessionStarted(); started != nil {
			r.agentSession = started.GetSessionId()
		}
		if err := renderer.write(response); err != nil {
			return err
		}
	}
}

// cancelInterruptedTurn stops a turn whose stream was interrupted and points
// at the replay command, since the record outlives the stream.
func (r *REPL) cancelInterruptedTurn(commandContext context.Context, queryID string) error {
	if queryID == "" {
		return nil
	}
	if err := r.client.CancelAgentQuery(commandContext, queryID); err != nil {
		fmt.Fprintf(r.stdout, "turn %s keeps running (cancel failed: %v)\n", queryID, err)
		return nil
	}
	fmt.Fprintf(r.stdout, "cancelled turn %s; replay with 'agent-observe %s'\n", queryID, queryID)
	return nil
}

// agentObserveOptions selects the replay window of `agent-observe`. A running
// query is followed by default; --no-follow drains the retained events only.
type agentObserveOptions struct {
	queryID      string
	fromSequence uint64
	follow       bool
}

// parseAgentObserveOptions splits `agent-observe` arguments into the replay
// flags and the positional query id.
func parseAgentObserveOptions(arguments []string) (agentObserveOptions, error) {
	options := agentObserveOptions{follow: true}
	fromSet := false
	noFollowSet := false
	words := make([]string, 0, 1)
	for index := 0; index < len(arguments); index++ {
		switch argument := arguments[index]; argument {
		case "--from":
			if index+1 >= len(arguments) || fromSet {
				return agentObserveOptions{}, usageError()
			}
			index++
			from, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return agentObserveOptions{}, usageErrorf("invalid --from sequence %q", arguments[index])
			}
			fromSet = true
			options.fromSequence = from
		case "--no-follow":
			if noFollowSet {
				return agentObserveOptions{}, usageError()
			}
			noFollowSet = true
			options.follow = false
		case "--":
			words = append(words, arguments[index+1:]...)
			index = len(arguments)
		default:
			if strings.HasPrefix(argument, "-") {
				return agentObserveOptions{}, usageErrorf("unknown agent-observe option %q", argument)
			}
			words = append(words, argument)
		}
	}
	if len(words) != 1 {
		return agentObserveOptions{}, usageError()
	}
	options.queryID = words[0]
	return options, nil
}

// agentObserve replays a query's frames — a finished turn's answer, the tail
// a disconnected stream missed (--from), or a running turn's live output
// (followed by default; --no-follow drains the snapshot only). Ctrl-C stops
// observing; the turn itself keeps running.
func (r *REPL) agentObserve(arguments []string) error {
	options, err := parseAgentObserveOptions(arguments)
	if err != nil {
		return err
	}
	commandContext, cancelCommand := r.commandContext()
	defer cancelCommand()
	streamContext, stopInterrupt := r.interruptContext(commandContext)
	defer stopInterrupt()
	interrupted := func() bool {
		return streamContext.Err() != nil && commandContext.Err() == nil
	}

	stream, err := r.client.ObserveAgentQuery(streamContext, options.queryID, remoteclient.AgentObserveOptions{
		FromSequence: options.fromSequence, Follow: options.follow,
	})
	if err != nil {
		if interrupted() {
			return nil
		}
		return err
	}
	renderer := &agentEventRenderer{output: r.stdout}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if interrupted() {
				return nil
			}
			return err
		}
		switch payload := response.GetPayload().(type) {
		case *codev1.ObserveQueryResponse_Header:
			if err := writeAgentQueryHeader(r.stdout, payload.Header); err != nil {
				return err
			}
		case *codev1.ObserveQueryResponse_Event:
			if err := renderer.write(payload.Event); err != nil {
				return err
			}
		case *codev1.ObserveQueryResponse_End:
			return writeAgentQueryEnd(r.stdout, payload.End)
		}
	}
}

// agentCancel stops a running query; its stream settles with stop reason
// cancelled and stays replayable.
func (r *REPL) agentCancel(arguments []string) error {
	if len(arguments) != 1 {
		return usageError()
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	if err := r.client.CancelAgentQuery(ctx, arguments[0]); err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "cancelled agent query %s\n", arguments[0])
	return nil
}

// writeAgentQueryHeader summarizes the replay window before its frames.
func writeAgentQueryHeader(output io.Writer, header *codev1.AgentQueryHeader) error {
	if header == nil {
		return nil
	}
	line := fmt.Sprintf("query %s: %s, session %s, sequences %d..%d",
		header.GetQueryId(), agentQueryStateText(header.GetState()), header.GetSessionId(),
		header.GetResolvedStartSequence(), header.GetSnapshotEndSequence())
	if header.GetHistoryTruncated() {
		line += fmt.Sprintf(", truncated (earliest %d)", header.GetEarliestSequence())
	}
	if header.GetFollow() {
		line += ", following"
	}
	_, err := fmt.Fprintln(output, line)
	return err
}

// writeAgentQueryEnd reports how the observation ended.
func writeAgentQueryEnd(output io.Writer, end *codev1.AgentQueryEnd) error {
	if end == nil {
		return nil
	}
	_, err := fmt.Fprintf(output, "end: %s (next sequence %d)\n",
		agentQueryEndReasonText(end.GetReason()), end.GetNextSequence())
	return err
}

func agentQueryStateText(state codev1.AgentQueryState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "AGENT_QUERY_STATE_"))
}

func agentQueryEndReasonText(reason codev1.AgentQueryEndReason) string {
	return strings.ToLower(strings.TrimPrefix(reason.String(), "AGENT_QUERY_END_REASON_"))
}

// agentCloseSession ends the remembered agent session, or the named one.
func (r *REPL) agentCloseSession(arguments []string) error {
	if len(arguments) > 1 {
		return usageError()
	}
	sessionID := r.agentSession
	if len(arguments) == 1 {
		sessionID = arguments[0]
	}
	if sessionID == "" {
		return errors.New("no agent session to close; start one with 'agent' or pass a session id")
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	if err := r.client.CloseAgentSession(ctx, sessionID); err != nil {
		return err
	}
	if r.agentSession == sessionID {
		r.agentSession = ""
	}
	fmt.Fprintf(r.stdout, "closed agent session %s\n", sessionID)
	return nil
}

// agentEventRenderer turns turn events into terminal lines. Message and
// thought chunks are plain text streams, so consecutive chunks of one kind are
// written inline while any other event starts a new line first.
type agentEventRenderer struct {
	output io.Writer
	// textKind is the kind currently streaming inline, empty at line start.
	textKind string
	midLine  bool
}

func (a *agentEventRenderer) write(response *codev1.QueryResponse) error {
	switch payload := response.GetEvent().(type) {
	case *codev1.QueryResponse_SessionStarted:
		if err := a.breakText(); err != nil {
			return err
		}
		return a.printf("session: %s\n", payload.SessionStarted.GetSessionId())
	case *codev1.QueryResponse_Message:
		return a.writeText("message", payload.Message.GetText())
	case *codev1.QueryResponse_Thought:
		return a.writeText("thought", payload.Thought.GetText())
	case *codev1.QueryResponse_ToolCall:
		if err := a.breakText(); err != nil {
			return err
		}
		return a.printf("%s\n", formatAgentToolCall(payload.ToolCall))
	case *codev1.QueryResponse_Plan:
		if err := a.breakText(); err != nil {
			return err
		}
		return a.writePlan(payload.Plan)
	case *codev1.QueryResponse_Usage:
		if err := a.breakText(); err != nil {
			return err
		}
		return a.printf("%s\n", formatAgentUsage(payload.Usage))
	case *codev1.QueryResponse_Completed:
		if err := a.breakText(); err != nil {
			return err
		}
		return a.printf("stop: %s\n", payload.Completed.GetStopReason())
	}
	return nil
}

// writeText streams one text chunk inline, prefixing only the first chunk of
// a thought run so streaming output stays readable.
func (a *agentEventRenderer) writeText(kind, text string) error {
	if text == "" {
		return nil
	}
	if a.textKind != kind {
		if err := a.breakText(); err != nil {
			return err
		}
		if kind == "thought" {
			if _, err := io.WriteString(a.output, "· "); err != nil {
				return err
			}
			a.midLine = true
		}
		a.textKind = kind
	}
	if _, err := io.WriteString(a.output, text); err != nil {
		return err
	}
	a.midLine = !strings.HasSuffix(text, "\n")
	return nil
}

// breakText ends an inline text run so the next event starts on its own line.
func (a *agentEventRenderer) breakText() error {
	a.textKind = ""
	if !a.midLine {
		return nil
	}
	a.midLine = false
	_, err := fmt.Fprintln(a.output)
	return err
}

// printf writes a complete line; every format ends in a newline so the
// renderer always stays at line start afterwards.
func (a *agentEventRenderer) printf(format string, arguments ...any) error {
	_, err := fmt.Fprintf(a.output, format, arguments...)
	return err
}

func (a *agentEventRenderer) writePlan(plan *codev1.AgentPlan) error {
	if plan == nil {
		return nil
	}
	if err := a.printf("plan:\n"); err != nil {
		return err
	}
	for _, entry := range plan.GetEntries() {
		if err := a.printf("  %s\n", formatAgentPlanEntry(entry)); err != nil {
			return err
		}
	}
	return nil
}

// formatAgentToolCall renders one tool call event: the arrow marks a creation,
// the tilde a later update of the same call.
func formatAgentToolCall(call *codev1.AgentToolCall) string {
	if call == nil {
		return ""
	}
	marker := "→"
	if call.GetUpdate() {
		marker = "~"
	}
	parts := []string{marker}
	if kind := call.GetKind(); kind != "" {
		parts = append(parts, "["+kind+"]")
	}
	if title := call.GetTitle(); title != "" {
		parts = append(parts, title)
	}
	if status := call.GetStatus(); status != "" {
		parts = append(parts, "("+status+")")
	}
	for _, location := range call.GetLocations() {
		parts = append(parts, formatAgentToolCallLocation(location))
	}
	return strings.Join(parts, " ")
}

func formatAgentToolCallLocation(location *codev1.AgentToolCallLocation) string {
	if location == nil {
		return ""
	}
	if location.Line != nil {
		return fmt.Sprintf("%s:%d", location.GetPath(), location.GetLine())
	}
	return location.GetPath()
}

func formatAgentPlanEntry(entry *codev1.AgentPlanEntry) string {
	if entry == nil {
		return ""
	}
	line := entry.GetContent()
	if status := entry.GetStatus(); status != "" {
		line = "[" + status + "] " + line
	}
	if priority := entry.GetPriority(); priority != "" {
		line += " (" + priority + ")"
	}
	return line
}

func formatAgentUsage(usage *codev1.AgentUsage) string {
	if usage == nil {
		return ""
	}
	line := fmt.Sprintf("usage: %d/%d context", usage.GetContextUsed(), usage.GetContextSize())
	if cost := usage.GetCost(); cost != nil {
		line += fmt.Sprintf(", cost %.2f %s", cost.GetAmount(), cost.GetCurrency())
	}
	return line
}
