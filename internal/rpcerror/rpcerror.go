// Package rpcerror attaches machine-readable reasons to gRPC status errors.
//
// A status code alone does not identify a condition. FailedPrecondition covers
// more than twenty distinct situations in the process service alone, so a
// client that must react differently to "the process input is closed" and "the
// process input was never enabled" would otherwise have to match the
// human-readable message. Every condition a caller may reasonably act on
// carries a google.rpc.ErrorInfo detail whose Reason is a constant declared
// here and whose Domain is DomainName.
//
// Messages remain free-form and are for humans. Reasons are API surface.
package rpcerror

import (
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// DomainName scopes every reason this controller reports.
const DomainName = "remote.code.v1"

// Reason identifies one condition. Values are stable API surface: a reason may
// be added, but an existing value must never be renamed or reused for a
// different condition. Callers must tolerate unknown reasons.
type Reason string

const (
	// Process lifecycle.
	ProcessServiceShuttingDown   Reason = "PROCESS_SERVICE_SHUTTING_DOWN"
	ActiveProcessLimitReached    Reason = "ACTIVE_PROCESS_LIMIT_REACHED"
	ProcessNameInUse             Reason = "PROCESS_NAME_IN_USE"
	ProcessHistoryLimitReached   Reason = "PROCESS_HISTORY_LIMIT_REACHED"
	ProcessNotRunning            Reason = "PROCESS_NOT_RUNNING"
	ProcessNotTerminal           Reason = "PROCESS_NOT_TERMINAL"
	ProcessAlreadyExited         Reason = "PROCESS_ALREADY_EXITED"
	WorkingDirectoryOpenFailed   Reason = "WORKING_DIRECTORY_OPEN_FAILED"
	WorkingDirectoryNotDirectory Reason = "WORKING_DIRECTORY_NOT_DIRECTORY"

	// Process input.
	ProcessNotPTY            Reason = "PROCESS_NOT_PTY"
	PTYInputCloseUnsupported Reason = "PTY_INPUT_CLOSE_UNSUPPORTED"
	ProcessInputDisabled     Reason = "PROCESS_INPUT_DISABLED"
	ProcessInputClosed       Reason = "PROCESS_INPUT_CLOSED"
	ProcessInputAttached     Reason = "PROCESS_INPUT_ATTACHED"
	ProcessInputRaw          Reason = "PROCESS_INPUT_RAW"

	// Process logs. The two offset reasons predate this package and are already
	// on the wire, so their values are kept verbatim.
	ProcessLogsUnavailable         Reason = "PROCESS_LOGS_UNAVAILABLE"
	ProcessLogsObserved            Reason = "PROCESS_LOGS_OBSERVED"
	ProcessLogObserverLimitReached Reason = "PROCESS_LOG_OBSERVER_LIMIT_REACHED"
	LogOffsetOutOfRange            Reason = "LOG_OFFSET_OUT_OF_RANGE"
	ControllerLogOffsetOutOfRange  Reason = "CONTROLLER_LOG_OFFSET_OUT_OF_RANGE"
	ControllerLogsUnavailable      Reason = "CONTROLLER_LOGS_UNAVAILABLE"

	// Process templates.
	TemplateRenderFailed     Reason = "TEMPLATE_RENDER_FAILED"
	TemplateRevisionMismatch Reason = "TEMPLATE_REVISION_MISMATCH"

	// Agent service. The agent bridges gRPC queries to an ACP agent child
	// process; these reasons describe that bridge, not the child's own errors.
	AgentDisabled    Reason = "AGENT_DISABLED"
	AgentStartFailed Reason = "AGENT_START_FAILED"
	// Retained for API stability: turn-scoped sessions (2026-09) stopped
	// emitting both — unknown resume ids surface the agent's own error, and a
	// crash ends the in-flight turn with AgentProcessLost instead.
	AgentSessionNotFound Reason = "AGENT_SESSION_NOT_FOUND"
	AgentSessionLost     Reason = "AGENT_SESSION_LOST"
	// AgentSessionNotResumable marks a Query carrying session_id while the
	// agent child never advertised the session/resume capability.
	AgentSessionNotResumable Reason = "AGENT_SESSION_NOT_RESUMABLE"
	AgentTurnActive          Reason = "AGENT_TURN_ACTIVE"
	AgentProcessLost         Reason = "AGENT_PROCESS_LOST"
	AgentWorkingDirectory    Reason = "AGENT_WORKING_DIRECTORY"
	// AgentEnvironment marks a request environment rejected on validation:
	// malformed keys or a size-budget overflow.
	AgentEnvironment Reason = "AGENT_ENVIRONMENT"
	// AgentEnvConflict marks a request whose environment requires restarting
	// the agent child while the current generation still has sessions or
	// running turns; the caller retries after they end.
	AgentEnvConflict Reason = "AGENT_ENV_CONFLICT"
	// AgentRequestError wraps a JSON-RPC error the agent itself returned; the
	// metadata carries the raw jsonrpc_code.
	AgentRequestError Reason = "AGENT_REQUEST_ERROR"
	// AgentSelectionUnsupported marks a Query carrying agent while the agent
	// child exposes no agent picker among its session config options (no
	// custom agents configured, or a non-claude-agent-acp ACP agent).
	AgentSelectionUnsupported Reason = "AGENT_SELECTION_UNSUPPORTED"
	// AgentNameInvalid marks a Query whose agent is not among the personas the
	// agent child offered the session it created or resumed.
	AgentNameInvalid Reason = "AGENT_NAME_INVALID"

	// Query replay: addressing a retained turn's event stream.
	AgentQueryNotFound        Reason = "AGENT_QUERY_NOT_FOUND"
	AgentQueryReplayDisabled  Reason = "AGENT_QUERY_REPLAY_DISABLED"
	AgentQuerySequenceInvalid Reason = "AGENT_QUERY_SEQUENCE_INVALID"
	AgentQueryEventsPruned    Reason = "AGENT_QUERY_EVENTS_PRUNED"
	// AgentQueryObserverLimit marks a live observation refused because the
	// record already carries the configured observer count.
	AgentQueryObserverLimit Reason = "AGENT_QUERY_OBSERVER_LIMIT"
	// AgentQueryObserverLag marks a live observation kicked for reading too
	// slowly; the client re-observes from the last sequence it received.
	AgentQueryObserverLag Reason = "AGENT_QUERY_OBSERVER_LAG"

	// File transfers. These mirror FileTransferErrorReason, which stays on the
	// wire for clients that already read it.
	TransferOffsetMismatch Reason = "TRANSFER_OFFSET_MISMATCH"
	TransferFileChanged    Reason = "TRANSFER_FILE_CHANGED"
	TransferPrefixMismatch Reason = "TRANSFER_PREFIX_MISMATCH"
	TransferSessionState   Reason = "TRANSFER_SESSION_STATE"
	TransferActiveTransfer Reason = "TRANSFER_ACTIVE_TRANSFER"
)

// Info builds the detail carried by every reasoned error. Metadata is optional
// and must never contain secrets: details cross the same boundary as the
// message.
func Info(reason Reason, metadata map[string]string) *errdetails.ErrorInfo {
	return &errdetails.ErrorInfo{Reason: string(reason), Domain: DomainName, Metadata: metadata}
}

// Errorf returns a status error carrying reason.
func Errorf(code codes.Code, reason Reason, format string, args ...any) error {
	return withInfo(status.Newf(code, format, args...), reason, nil)
}

// ErrorfWithMetadata returns a status error carrying reason and structured
// values a caller needs to recover, such as the offsets still readable.
func ErrorfWithMetadata(code codes.Code, reason Reason, metadata map[string]string, format string, args ...any) error {
	return withInfo(status.Newf(code, format, args...), reason, metadata)
}

// ReasonOf returns the reason carried by err, or an empty Reason when err is
// nil, is not a status error, or carries no reason from this domain.
func ReasonOf(err error) Reason {
	if err == nil {
		return ""
	}
	for _, detail := range status.Convert(err).Details() {
		info, ok := detail.(*errdetails.ErrorInfo)
		if ok && info.GetDomain() == DomainName {
			return Reason(info.GetReason())
		}
	}
	return ""
}

// withInfo degrades to the undetailed status rather than failing the call when
// the detail cannot be marshalled; the code and message still reach the client.
func withInfo(base *status.Status, reason Reason, metadata map[string]string) error {
	detailed, err := base.WithDetails(Info(reason, metadata))
	if err != nil {
		return base.Err()
	}
	return detailed.Err()
}
