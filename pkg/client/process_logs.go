package client

import (
	"context"
	"errors"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

// ProcessLogOptions selects a replay start and whether the stream should
// continue following new records. A nil Offset and nil TailLines starts at
// logical offset zero. Offset and TailLines are mutually exclusive.
type ProcessLogOptions struct {
	Streams   []codev1.ProcessLogStream
	Offset    *uint64
	TailLines *uint64
	Follow    bool
}

// ObserveProcessLogs opens the server stream. Callers should persist the
// NextOffset from checkpoints (or chunks) as the resume token; it is a logical
// record offset and is intentionally independent of segment file positions.
func (c *Client) ObserveProcessLogs(ctx context.Context, processID string, options ProcessLogOptions) (codev1.ProcessService_ObserveProcessLogsClient, error) {
	if options.Offset != nil && options.TailLines != nil {
		return nil, errors.New("process log offset and tail lines are mutually exclusive")
	}
	request := &codev1.ObserveProcessLogsRequest{
		ProcessId: processID,
		Streams:   append([]codev1.ProcessLogStream(nil), options.Streams...),
		Follow:    options.Follow,
	}
	if options.Offset != nil {
		request.Start = &codev1.ObserveProcessLogsRequest_Offset{Offset: *options.Offset}
	}
	if options.TailLines != nil {
		request.Start = &codev1.ObserveProcessLogsRequest_TailLines{TailLines: *options.TailLines}
	}
	return c.processes.ObserveProcessLogs(ctx, request)
}
