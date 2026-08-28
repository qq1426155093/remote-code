package agent

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/coder/acp-go-sdk"
)

// updateSink receives session events keyed by the agent-side session id. The
// acpClient calls it from the SDK's notification-processing goroutine, so
// implementations must not block: the SDK drains notifications sequentially
// and a stalled sink would freeze every session on the connection.
type updateSink interface {
	dispatchUpdate(sessionID acp.SessionId, event Event)
}

// acpClient implements the ACP client role for the bridge. remote-code
// advertises no fs, terminal, or elicitation capabilities at initialize time,
// so the agent is not expected to call those methods; they return errors
// rather than pretending to work.
type acpClient struct {
	sink   updateSink
	logger *slog.Logger
}

var _ acp.Client = (*acpClient)(nil)

// RequestPermission auto-approves. The gRPC caller already holds a controller
// token, which is equivalent to remote code execution via StartProcess, so an
// approval round-trip would be ceremony rather than isolation. Selection
// prefers an allow_once option, then allow_always, then the first option;
// without any option the request is cancelled, which is the protocol's way of
// saying "no answer". Options are chosen by kind only — optionIds are
// agent-defined strings (claude-agent-acp uses permission-mode names for
// ExitPlanMode) and must never be hardcoded.
func (c *acpClient) RequestPermission(_ context.Context, params acp.RequestPermissionRequest) (acp.RequestPermissionResponse, error) {
	option, ok := selectPermissionOption(params.Options)
	if !ok {
		return acp.RequestPermissionResponse{
			Outcome: acp.RequestPermissionOutcome{Cancelled: &acp.RequestPermissionOutcomeCancelled{}},
		}, nil
	}
	return acp.RequestPermissionResponse{
		Outcome: acp.RequestPermissionOutcome{Selected: &acp.RequestPermissionOutcomeSelected{OptionId: option.OptionId}},
	}, nil
}

// selectPermissionOption picks the option to auto-approve with: allow_once
// first (least standing effect), allow_always second, otherwise the first
// offered option. ok is false when the agent offered no options at all.
func selectPermissionOption(options []acp.PermissionOption) (acp.PermissionOption, bool) {
	var allowAlways *acp.PermissionOption
	for i := range options {
		switch options[i].Kind {
		case acp.PermissionOptionKindAllowOnce:
			return options[i], true
		case acp.PermissionOptionKindAllowAlways:
			if allowAlways == nil {
				allowAlways = &options[i]
			}
		}
	}
	if allowAlways != nil {
		return *allowAlways, true
	}
	if len(options) > 0 {
		return options[0], true
	}
	return acp.PermissionOption{}, false
}

// SessionUpdate maps the notification to an Event and fans it out to the
// session's active turn. Updates that arrive with no listener (late chunks
// after a turn settled, or a dropped kind) are consumed and logged.
func (c *acpClient) SessionUpdate(_ context.Context, params acp.SessionNotification) error {
	event, ok := eventFromSessionUpdate(params)
	if !ok {
		return nil
	}
	c.sink.dispatchUpdate(params.SessionId, event)
	return nil
}

func (c *acpClient) ReadTextFile(context.Context, acp.ReadTextFileRequest) (acp.ReadTextFileResponse, error) {
	return acp.ReadTextFileResponse{}, errUnadvertised("fs/readTextFile")
}

func (c *acpClient) WriteTextFile(context.Context, acp.WriteTextFileRequest) (acp.WriteTextFileResponse, error) {
	return acp.WriteTextFileResponse{}, errUnadvertised("fs/writeTextFile")
}

func (c *acpClient) CreateTerminal(context.Context, acp.CreateTerminalRequest) (acp.CreateTerminalResponse, error) {
	return acp.CreateTerminalResponse{}, errUnadvertised("terminal/create")
}

func (c *acpClient) KillTerminal(context.Context, acp.KillTerminalRequest) (acp.KillTerminalResponse, error) {
	return acp.KillTerminalResponse{}, errUnadvertised("terminal/kill")
}

func (c *acpClient) TerminalOutput(context.Context, acp.TerminalOutputRequest) (acp.TerminalOutputResponse, error) {
	return acp.TerminalOutputResponse{}, errUnadvertised("terminal/output")
}

func (c *acpClient) ReleaseTerminal(context.Context, acp.ReleaseTerminalRequest) (acp.ReleaseTerminalResponse, error) {
	return acp.ReleaseTerminalResponse{}, errUnadvertised("terminal/release")
}

func (c *acpClient) WaitForTerminalExit(context.Context, acp.WaitForTerminalExitRequest) (acp.WaitForTerminalExitResponse, error) {
	return acp.WaitForTerminalExitResponse{}, errUnadvertised("terminal/waitForExit")
}

// errUnadvertised explains that the method belongs to a client capability this
// bridge never advertised, so a well-behaved agent should not have called it.
func errUnadvertised(method string) error {
	return fmt.Errorf("remote-code does not implement the ACP client method %q because it advertises no such capability", method)
}
