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
