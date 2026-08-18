package client

import (
	"context"
	"errors"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

// ControllerLogOptions selects a replay start and whether the stream follows
// new controller events. Offset and TailLines are mutually exclusive.
type ControllerLogOptions struct {
	Offset    *uint64
	TailLines *uint64
	Follow    bool
}

// ObserveControllerLogs opens the controller runtime-log stream. Checkpoints
// expose the logical NextOffset used as a stable resume token across segment
// files and controller restarts.
func (c *Client) ObserveControllerLogs(ctx context.Context, options ControllerLogOptions) (codev1.ControllerService_ObserveControllerLogsClient, error) {
	if options.Offset != nil && options.TailLines != nil {
		return nil, errors.New("controller log offset and tail lines are mutually exclusive")
	}
	request := &codev1.ObserveControllerLogsRequest{Follow: options.Follow}
	if options.Offset != nil {
		request.Start = &codev1.ObserveControllerLogsRequest_Offset{Offset: *options.Offset}
	}
	if options.TailLines != nil {
		request.Start = &codev1.ObserveControllerLogsRequest_TailLines{TailLines: *options.TailLines}
	}
	return c.controller.ObserveControllerLogs(ctx, request)
}
