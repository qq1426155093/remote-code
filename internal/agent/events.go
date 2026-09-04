package agent

import (
	"github.com/coder/acp-go-sdk"
)

// EventKind discriminates the event variants a turn streams to its caller.
// The values mirror the QueryResponse oneof in the gRPC surface.
type EventKind string

const (
	EventKindSessionStarted EventKind = "session_started"
	EventKindMessage        EventKind = "message"
	EventKindThought        EventKind = "thought"
	EventKindToolCall       EventKind = "tool_call"
	EventKindPlan           EventKind = "plan"
	EventKindUsage          EventKind = "usage"
	EventKindCompleted      EventKind = "completed"
)

// Event is one turn observation. Exactly one variant is populated, selected by
// Kind. Events are value types: they are copied into caller channels and must
// never carry io handles or locks.
type Event struct {
	Kind EventKind

	SessionStarted SessionStarted
	Message        TextChunk
	Thought        TextChunk
	ToolCall       ToolCall
	Plan           Plan
	Usage          Usage
	Completed      Completed
}

// SessionStarted is the first frame of a turn that created a new session. It
// carries the ACP session id the caller reuses for follow-up turns.
type SessionStarted struct {
	SessionID string
}

// TextChunk is one streamed agent message or thought fragment.
type TextChunk struct {
	Text string
}

// ToolCall reports a tool call creation or a later status/content update. A
// fresh call has Update false; subsequent updates reference the same ToolCallID.
type ToolCall struct {
	ToolCallID string
	Title      string
	Kind       string
	Status     string
	Update     bool
	Locations  []ToolLocation
	// RawInput and RawOutput carry the JSON values the agent attached to the
	// call (ACP rawInput/rawOutput); Content carries its structured output
	// blocks (diffs for edits). Each is nil/empty on frames that omit it.
	RawInput  any
	RawOutput any
	Content   []acp.ToolCallContent
}

// ToolLocation points at a file (and optional line) a tool call touched.
type ToolLocation struct {
	Path    string
	Line    int32
	HasLine bool
}

// Plan is a full-plan replacement: the agent sends the complete entry list on
// every update and the client swaps its copy wholesale.
type Plan struct {
	Entries []PlanEntry
}

// PlanEntry is one plan task with its current status.
type PlanEntry struct {
	Content  string
	Priority string
	Status   string
}

// Usage reports context-window consumption for the session.
type Usage struct {
	ContextSize int64
	ContextUsed int64
	Cost        *Cost
}

// Cost is the monetary cost accumulated by the session, when the agent
// reports one.
type Cost struct {
	Amount   float64
	Currency string
}

// Completed is the last frame of a settled turn. StopReason carries the ACP
// stop reason verbatim (end_turn, max_tokens, refusal, cancelled, ...).
type Completed struct {
	StopReason string
}

// eventFromSessionUpdate maps one ACP session/update notification to an Event.
// The second return is false for update kinds this bridge deliberately drops:
// user_message_chunk (the caller already knows what it sent) and the
// unavailable-command surface kinds (available_commands_update,
// current_mode_update, config_option_update, session_info_update) whose
// features are not exposed in v1.
func eventFromSessionUpdate(notification acp.SessionNotification) (Event, bool) {
	update := notification.Update
	switch {
	case update.AgentMessageChunk != nil:
		return Event{Kind: EventKindMessage, Message: TextChunk{textOf(update.AgentMessageChunk.Content)}}, true
	case update.AgentThoughtChunk != nil:
		return Event{Kind: EventKindThought, Thought: TextChunk{textOf(update.AgentThoughtChunk.Content)}}, true
	case update.ToolCall != nil:
		call := update.ToolCall
		return Event{Kind: EventKindToolCall, ToolCall: ToolCall{
			ToolCallID: string(call.ToolCallId),
			Title:      call.Title,
			Kind:       string(call.Kind),
			Status:     string(call.Status),
			Locations:  locationsOf(call.Locations),
			RawInput:   call.RawInput,
			RawOutput:  call.RawOutput,
			Content:    call.Content,
		}}, true
	case update.ToolCallUpdate != nil:
		call := update.ToolCallUpdate
		event := ToolCall{
			ToolCallID: string(call.ToolCallId),
			Update:     true,
			Locations:  locationsOf(call.Locations),
			RawInput:   call.RawInput,
			RawOutput:  call.RawOutput,
			Content:    call.Content,
		}
		if call.Title != nil {
			event.Title = *call.Title
		}
		if call.Kind != nil {
			event.Kind = string(*call.Kind)
		}
		if call.Status != nil {
			event.Status = string(*call.Status)
		}
		return Event{Kind: EventKindToolCall, ToolCall: event}, true
	case update.Plan != nil:
		entries := make([]PlanEntry, 0, len(update.Plan.Entries))
		for _, entry := range update.Plan.Entries {
			entries = append(entries, PlanEntry{
				Content:  entry.Content,
				Priority: string(entry.Priority),
				Status:   string(entry.Status),
			})
		}
		return Event{Kind: EventKindPlan, Plan: Plan{Entries: entries}}, true
	case update.UsageUpdate != nil:
		usage := Usage{
			ContextSize: int64(update.UsageUpdate.Size),
			ContextUsed: int64(update.UsageUpdate.Used),
		}
		if cost := update.UsageUpdate.Cost; cost != nil {
			usage.Cost = &Cost{Amount: cost.Amount, Currency: cost.Currency}
		}
		return Event{Kind: EventKindUsage, Usage: usage}, true
	default:
		return Event{}, false
	}
}

// textOf extracts the text of a text content block; non-text blocks render as
// an empty chunk because this bridge only prompts with plain text.
func textOf(block acp.ContentBlock) string {
	if block.Text != nil {
		return block.Text.Text
	}
	return ""
}

func locationsOf(locations []acp.ToolCallLocation) []ToolLocation {
	if len(locations) == 0 {
		return nil
	}
	mapped := make([]ToolLocation, 0, len(locations))
	for _, location := range locations {
		toolLocation := ToolLocation{Path: location.Path}
		if location.Line != nil {
			toolLocation.Line = int32(*location.Line)
			toolLocation.HasLine = true
		}
		mapped = append(mapped, toolLocation)
	}
	return mapped
}
