package agent

import (
	"context"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RPC adapts the agent bridge to the gRPC AgentService. A nil bridge answers
// every call with AGENT_DISABLED, so the surface stays registered and a client
// can tell a disabled agent from a controller that predates the service.
type RPC struct {
	codev1.UnimplementedAgentServiceServer
	bridge *Service
}

// NewRPC wraps one bridge; service may be nil for the disabled surface.
func NewRPC(service *Service) *RPC { return &RPC{bridge: service} }

// Query runs one turn and forwards its events until the turn settles. The
// stream context drives cancellation: a caller that goes away cancels the turn
// on the agent instead of abandoning it.
func (r *RPC) Query(request *codev1.QueryRequest, stream codev1.AgentService_QueryServer) error {
	if r.bridge == nil {
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentDisabled, "the agent service is disabled in controller configuration")
	}
	turn, err := r.bridge.StartTurn(stream.Context(), TurnRequest{
		Prompt:           request.GetPrompt(),
		SessionID:        request.GetSessionId(),
		WorkingDirectory: request.GetWorkingDirectory(),
	})
	if err != nil {
		return err
	}
	for {
		select {
		case event, ok := <-turn.Events():
			if !ok {
				// Events closed means the pump finished; Wait never blocks here.
				return turn.Wait()
			}
			if err := stream.Send(queryResponseOf(event)); err != nil {
				return err
			}
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}

// CloseSession ends a session on the bridge; see Service.CloseSession for the
// capability negotiation with the agent.
func (r *RPC) CloseSession(ctx context.Context, request *codev1.CloseSessionRequest) (*codev1.CloseSessionResponse, error) {
	if r.bridge == nil {
		return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentDisabled, "the agent service is disabled in controller configuration")
	}
	if err := r.bridge.CloseSession(ctx, request.GetSessionId()); err != nil {
		return nil, err
	}
	return &codev1.CloseSessionResponse{}, nil
}

// Info reports the bridge status for GetInfo, or nil when the agent service is
// disabled so clients can feature-detect the field's presence.
func (r *RPC) Info() *codev1.AgentInfo {
	if r.bridge == nil {
		return nil
	}
	snapshot := r.bridge.Snapshot()
	info := &codev1.AgentInfo{
		Enabled:        true,
		Started:        snapshot.Started,
		ProcessId:      snapshot.ProcessID,
		Generation:     snapshot.Generation,
		CloseSupported: snapshot.CloseSupported,
		Sessions:       uint32(snapshot.Sessions),
	}
	return info
}

// queryResponseOf maps one turn event to its wire variant.
func queryResponseOf(event Event) *codev1.QueryResponse {
	response := &codev1.QueryResponse{}
	switch event.Kind {
	case EventKindSessionStarted:
		response.Event = &codev1.QueryResponse_SessionStarted{
			SessionStarted: &codev1.AgentSessionStarted{SessionId: event.SessionStarted.SessionID},
		}
	case EventKindMessage:
		response.Event = &codev1.QueryResponse_Message{
			Message: &codev1.AgentMessage{Text: event.Message.Text},
		}
	case EventKindThought:
		response.Event = &codev1.QueryResponse_Thought{
			Thought: &codev1.AgentThought{Text: event.Thought.Text},
		}
	case EventKindToolCall:
		call := &codev1.AgentToolCall{
			ToolCallId: event.ToolCall.ToolCallID,
			Title:      event.ToolCall.Title,
			Kind:       event.ToolCall.Kind,
			Status:     event.ToolCall.Status,
			Update:     event.ToolCall.Update,
		}
		for _, location := range event.ToolCall.Locations {
			wire := &codev1.AgentToolCallLocation{Path: location.Path}
			if location.HasLine {
				line := location.Line
				wire.Line = &line
			}
			call.Locations = append(call.Locations, wire)
		}
		response.Event = &codev1.QueryResponse_ToolCall{ToolCall: call}
	case EventKindPlan:
		plan := &codev1.AgentPlan{}
		for _, entry := range event.Plan.Entries {
			plan.Entries = append(plan.Entries, &codev1.AgentPlanEntry{
				Content:  entry.Content,
				Priority: entry.Priority,
				Status:   entry.Status,
			})
		}
		response.Event = &codev1.QueryResponse_Plan{Plan: plan}
	case EventKindUsage:
		usage := &codev1.AgentUsage{
			ContextSize: event.Usage.ContextSize,
			ContextUsed: event.Usage.ContextUsed,
		}
		if event.Usage.Cost != nil {
			usage.Cost = &codev1.AgentCost{Amount: event.Usage.Cost.Amount, Currency: event.Usage.Cost.Currency}
		}
		response.Event = &codev1.QueryResponse_Usage{Usage: usage}
	case EventKindCompleted:
		response.Event = &codev1.QueryResponse_Completed{
			Completed: &codev1.AgentTurnCompleted{StopReason: event.Completed.StopReason},
		}
	}
	return response
}

// Shutdown stops the bridge and its agent child; see Service.Shutdown. It is a
// no-op on the disabled surface.
func (r *RPC) Shutdown(ctx context.Context) error {
	if r.bridge == nil {
		return nil
	}
	return r.bridge.Shutdown(ctx)
}
