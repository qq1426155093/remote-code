package client

import (
	"context"
	"errors"
	"strings"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// AgentQueryOptions selects the conversation one agent turn runs in. An empty
// SessionID starts a new session at the workspace root, or in
// WorkingDirectory when set; a non-empty SessionID reuses an existing session
// and ignores WorkingDirectory.
type AgentQueryOptions struct {
	SessionID        string
	WorkingDirectory string
}

// AgentQuery starts one agent turn and returns its server-streamed events.
// Cancelling ctx cancels the turn on the controller. The stream ends with a
// completed event when the turn settled and with a status error when it
// failed; unknown-session refusals surface before the first event.
func (c *Client) AgentQuery(ctx context.Context, prompt string, options AgentQueryOptions) (codev1.AgentService_QueryClient, error) {
	if err := c.requireAgentService(); err != nil {
		return nil, err
	}
	if strings.TrimSpace(prompt) == "" {
		return nil, errors.New("agent prompt must not be empty")
	}
	request := &codev1.QueryRequest{Prompt: prompt}
	if options.SessionID != "" {
		request.SessionId = &options.SessionID
	}
	if options.WorkingDirectory != "" {
		request.WorkingDirectory = &options.WorkingDirectory
	}
	return c.agent.Query(ctx, request)
}

// CloseAgentSession ends one agent session on the controller. It fails while
// the session still has a running turn.
func (c *Client) CloseAgentSession(ctx context.Context, sessionID string) error {
	if err := c.requireAgentService(); err != nil {
		return err
	}
	if sessionID == "" {
		return errors.New("agent session id must not be empty")
	}
	_, err := c.agent.CloseSession(ctx, &codev1.CloseSessionRequest{SessionId: sessionID})
	return err
}

// requireAgentService gates the agent wrappers on the connection-time
// capability report so callers get one clear client-side error instead of
// Unimplemented failures from every agent RPC.
func (c *Client) requireAgentService() error {
	if c.info.GetAgent() == nil {
		return status.Error(codes.FailedPrecondition, "agent service is disabled on this controller")
	}
	return nil
}
