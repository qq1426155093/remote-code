package process

import (
	"context"
	"errors"
	"io"
	"sync"
)

var errProcessInputClosed = errors.New("process input is closed")

type processInputOperation struct {
	data   []byte
	close  bool
	result chan processInputResult
}

type processInputResult struct {
	written int
	err     error
}

// processInput serializes writes to one child input endpoint. The pump may be
// blocked in an operating-system write, but callers can still observe context
// cancellation; aborting the endpoint during process teardown unblocks it.
type processInput struct {
	writer    io.WriteCloser
	requests  chan processInputOperation
	closed    chan struct{}
	done      chan struct{}
	closeOnce sync.Once
	closeErr  error
}

func newProcessInput(writer io.WriteCloser) *processInput {
	input := &processInput{
		writer: writer, requests: make(chan processInputOperation),
		closed: make(chan struct{}), done: make(chan struct{}),
	}
	go input.run()
	return input
}

func (i *processInput) write(ctx context.Context, data []byte) (int, error) {
	if len(data) == 0 {
		return 0, nil
	}
	operation := processInputOperation{
		data: append([]byte(nil), data...), result: make(chan processInputResult, 1),
	}
	return i.submit(ctx, operation)
}

func (i *processInput) closeInput(ctx context.Context) error {
	operation := processInputOperation{close: true, result: make(chan processInputResult, 1)}
	_, err := i.submit(ctx, operation)
	return err
}

func (i *processInput) submit(ctx context.Context, operation processInputOperation) (int, error) {
	select {
	case i.requests <- operation:
	case <-i.closed:
		return 0, errProcessInputClosed
	case <-ctx.Done():
		return 0, ctx.Err()
	}
	// Once accepted by the pump, the operation may complete even if the caller
	// cancels. This preserves ordering for a later attachment; the caller must
	// treat an unacknowledged write as having an unknown delivery outcome.
	select {
	case result := <-operation.result:
		return result.written, result.err
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (i *processInput) abort() {
	i.closeEndpoint()
	<-i.done
}

func (i *processInput) isClosed() bool {
	select {
	case <-i.closed:
		return true
	default:
		return false
	}
}

func (i *processInput) run() {
	defer close(i.done)
	for {
		select {
		case operation := <-i.requests:
			if operation.close {
				err := i.closeEndpoint()
				operation.result <- processInputResult{err: err}
				return
			}
			written, err := writeProcessInput(i.writer, operation.data)
			operation.result <- processInputResult{written: written, err: err}
			if err != nil && i.isClosed() {
				return
			}
		case <-i.closed:
			return
		}
	}
}

func (i *processInput) closeEndpoint() error {
	i.closeOnce.Do(func() {
		close(i.closed)
		i.closeErr = i.writer.Close()
	})
	return i.closeErr
}

func writeProcessInput(writer io.Writer, data []byte) (int, error) {
	written := 0
	for written < len(data) {
		count, err := writer.Write(data[written:])
		written += count
		if err != nil {
			return written, err
		}
		if count == 0 {
			return written, io.ErrNoProgress
		}
	}
	return written, nil
}
