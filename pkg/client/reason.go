package client

import "github.com/qq1426155093/remote-code/internal/rpcerror"

// Reason returns the machine-readable reason the controller attached to err, or
// an empty string when it carries none.
//
// A gRPC code identifies the class of failure; several distinct conditions
// share one code. FailedPrecondition alone covers a process that is not
// running, input that was never enabled, input that is closed, and logs that
// are being observed. Callers that must tell those apart should switch on the
// reason rather than on the message, which is free-form and may change.
//
// Callers must tolerate an unknown or empty reason: a newer controller may
// report reasons this client does not know, and not every error carries one.
func Reason(err error) string { return string(rpcerror.ReasonOf(err)) }

// Reasons the controller reports. The set grows over time, so treat an
// unrecognized value as an unclassified failure rather than an error.
const (
	ReasonProcessServiceShuttingDown   = string(rpcerror.ProcessServiceShuttingDown)
	ReasonActiveProcessLimitReached    = string(rpcerror.ActiveProcessLimitReached)
	ReasonProcessNameInUse             = string(rpcerror.ProcessNameInUse)
	ReasonProcessHistoryLimitReached   = string(rpcerror.ProcessHistoryLimitReached)
	ReasonProcessNotRunning            = string(rpcerror.ProcessNotRunning)
	ReasonProcessNotTerminal           = string(rpcerror.ProcessNotTerminal)
	ReasonProcessAlreadyExited         = string(rpcerror.ProcessAlreadyExited)
	ReasonWorkingDirectoryOpenFailed   = string(rpcerror.WorkingDirectoryOpenFailed)
	ReasonWorkingDirectoryNotDirectory = string(rpcerror.WorkingDirectoryNotDirectory)

	ReasonProcessNotPTY            = string(rpcerror.ProcessNotPTY)
	ReasonPTYInputCloseUnsupported = string(rpcerror.PTYInputCloseUnsupported)
	ReasonProcessInputDisabled     = string(rpcerror.ProcessInputDisabled)
	ReasonProcessInputClosed       = string(rpcerror.ProcessInputClosed)
	ReasonProcessInputAttached     = string(rpcerror.ProcessInputAttached)

	ReasonProcessLogsUnavailable         = string(rpcerror.ProcessLogsUnavailable)
	ReasonProcessLogsObserved            = string(rpcerror.ProcessLogsObserved)
	ReasonProcessLogObserverLimitReached = string(rpcerror.ProcessLogObserverLimitReached)
	ReasonLogOffsetOutOfRange            = string(rpcerror.LogOffsetOutOfRange)
	ReasonControllerLogOffsetOutOfRange  = string(rpcerror.ControllerLogOffsetOutOfRange)
	ReasonControllerLogsUnavailable      = string(rpcerror.ControllerLogsUnavailable)

	ReasonTemplateRenderFailed     = string(rpcerror.TemplateRenderFailed)
	ReasonTemplateRevisionMismatch = string(rpcerror.TemplateRevisionMismatch)

	ReasonTransferOffsetMismatch = string(rpcerror.TransferOffsetMismatch)
	ReasonTransferFileChanged    = string(rpcerror.TransferFileChanged)
	ReasonTransferPrefixMismatch = string(rpcerror.TransferPrefixMismatch)
	ReasonTransferSessionState   = string(rpcerror.TransferSessionState)
	ReasonTransferActiveTransfer = string(rpcerror.TransferActiveTransfer)
)
