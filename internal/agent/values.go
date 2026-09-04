package agent

import (
	"encoding/json"

	"github.com/coder/acp-go-sdk"
	"google.golang.org/protobuf/types/known/structpb"
)

// toolCallValue converts one ACP raw payload (rawInput/rawOutput) to its wire
// form. The values are already JSON-decoded by the RPC layer, so a marshal
// round-trip is lossless without enumerating every JSON shape here. A nil
// value means the frame leaves the payload unchanged and stays absent.
func toolCallValue(value any) *structpb.Value {
	if value == nil {
		return nil
	}
	data, err := json.Marshal(value)
	if err != nil {
		// JSON-decoded values cannot fail to re-marshal; silence is to keep
		// the mapping total rather than drop the whole frame.
		return nil
	}
	wire := &structpb.Value{}
	if err := wire.UnmarshalJSON(data); err != nil {
		return nil
	}
	return wire
}

// toolCallContentValue serializes one ACP tool call content variant. The SDK's
// union wrappers marshal as {} because their variant fields carry json:"-",
// so the populated variant is picked explicitly; embedded content blocks get
// the same treatment one level down.
func toolCallContentValue(block acp.ToolCallContent) *structpb.Value {
	var payload any
	switch {
	case block.Diff != nil:
		payload = block.Diff
	case block.Terminal != nil:
		payload = block.Terminal
	case block.Content != nil:
		payload = map[string]any{
			"type":    block.Content.Type,
			"content": contentBlockPayload(block.Content.Content),
		}
	default:
		return nil
	}
	return toolCallValue(payload)
}

// contentBlockPayload picks a content block's variant, whose struct carries
// the json tags the union wrapper lacks.
func contentBlockPayload(block acp.ContentBlock) any {
	switch {
	case block.Text != nil:
		return block.Text
	case block.Image != nil:
		return block.Image
	case block.Audio != nil:
		return block.Audio
	case block.ResourceLink != nil:
		return block.ResourceLink
	case block.Resource != nil:
		return block.Resource
	}
	return nil
}
