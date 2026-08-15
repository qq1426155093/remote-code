package client

import (
	"context"
	"errors"
	"io"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const processAttachPendingOperations = 32

type processAttachAckKind uint8

const (
	processAttachDataAck processAttachAckKind = iota + 1
	processAttachResizeAck
)

type processAttachExpectedAck struct {
	kind     processAttachAckKind
	sequence uint64
	bytes    int
	rows     uint32
	columns  uint32
}

// ProcessAttachOptions controls one interactive PTY attachment. Rows and
// Columns must either both be zero or both describe the current local terminal.
type ProcessAttachOptions struct {
	Rows    uint32
	Columns uint32
}

// ProcessAttachment combines the existing process-input and process-log RPCs
// into one interactive PTY session. Close and Detach release the writer but do
// not stop the remote process or close its input endpoint.
type ProcessAttachment struct {
	ctx        context.Context
	cancel     context.CancelFunc
	input      codev1.ProcessService_StreamProcessInputClient
	logs       codev1.ProcessService_ObserveProcessLogsClient
	process    *codev1.ProcessInfo
	output     chan []byte
	done       chan struct{}
	events     chan error
	pending    chan processAttachExpectedAck
	finishOnce sync.Once

	sendMu  sync.Mutex
	next    uint64
	closing bool

	resultMu sync.Mutex
	err      error
	last     *codev1.ProcessInfo
	logEnd   *codev1.ProcessLogEnd
	offset   uint64
}

// OpenProcessAttachment acquires the exclusive managed-input writer and then
// follows PTY output from the current log boundary. It does not introduce a
// separate attach RPC; both streams remain independently observable on wire.
func (c *Client) OpenProcessAttachment(ctx context.Context, process *codev1.ProcessReference, options ProcessAttachOptions) (*ProcessAttachment, error) {
	if process == nil {
		return nil, status.Error(codes.InvalidArgument, "process reference is required")
	}
	if (options.Rows == 0) != (options.Columns == 0) || options.Rows > 65535 || options.Columns > 65535 {
		return nil, status.Error(codes.InvalidArgument, "terminal size rows and columns must both be between 1 and 65535")
	}
	attachmentContext, cancel := context.WithCancel(ctx)
	input, err := c.processes.StreamProcessInput(attachmentContext)
	if err != nil {
		cancel()
		return nil, err
	}
	cleanupInput := func() {
		_ = input.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Detach{Detach: &codev1.ProcessInputDetach{}}})
		_ = input.CloseSend()
	}
	if err := input.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Open{
		Open: &codev1.ProcessInputOpen{Process: process},
	}}); err != nil {
		cleanupInput()
		cancel()
		return nil, err
	}
	response, err := input.Recv()
	if err != nil {
		cleanupInput()
		cancel()
		return nil, err
	}
	opened := response.GetOpened()
	if opened == nil || opened.GetProcess() == nil {
		cleanupInput()
		cancel()
		return nil, status.Error(codes.DataLoss, "process input stream did not return an opened frame")
	}
	if opened.GetProcess().GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
		cleanupInput()
		cancel()
		return nil, status.Error(codes.FailedPrecondition, "interactive attachment requires a PTY process")
	}

	tail := uint64(0)
	logs, err := c.ObserveProcessLogs(attachmentContext, opened.GetProcess().GetId(), ProcessLogOptions{
		Streams:   []codev1.ProcessLogStream{codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT},
		TailLines: &tail,
		Follow:    true,
	})
	if err != nil {
		cleanupInput()
		cancel()
		return nil, err
	}
	offset, err := prepareProcessAttachmentLogs(logs, opened.GetProcess().GetId())
	if err != nil {
		cleanupInput()
		cancel()
		return nil, err
	}

	attachment := &ProcessAttachment{
		ctx: attachmentContext, cancel: cancel, input: input, logs: logs,
		process: opened.GetProcess(), output: make(chan []byte, 64), done: make(chan struct{}),
		events: make(chan error, 1), pending: make(chan processAttachExpectedAck, processAttachPendingOperations),
		next: 1, offset: offset,
	}
	var receivers sync.WaitGroup
	receivers.Add(2)
	go func() {
		defer receivers.Done()
		attachment.receiveInput()
	}()
	go func() {
		defer receivers.Done()
		defer close(attachment.output)
		attachment.receiveLogs()
	}()
	go attachment.finish(&receivers)

	if options.Rows != 0 {
		if err := attachment.Resize(options.Rows, options.Columns); err != nil {
			_ = attachment.Detach()
			return nil, err
		}
	}
	return attachment, nil
}

func prepareProcessAttachmentLogs(stream codev1.ProcessService_ObserveProcessLogsClient, processID string) (uint64, error) {
	var sawHeader bool
	for {
		response, err := stream.Recv()
		if err != nil {
			return 0, err
		}
		if header := response.GetHeader(); header != nil {
			if sawHeader || header.GetProcessId() != processID || header.GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
				return 0, status.Error(codes.DataLoss, "invalid process attachment log header")
			}
			sawHeader = true
			continue
		}
		if checkpoint := response.GetCheckpoint(); checkpoint != nil && checkpoint.GetReplayComplete() {
			if !sawHeader {
				return 0, status.Error(codes.DataLoss, "process attachment log checkpoint preceded its header")
			}
			return checkpoint.GetNextOffset(), nil
		}
		if response.GetChunk() != nil || response.GetEnd() != nil {
			return 0, status.Error(codes.DataLoss, "process attachment log stream ended during setup")
		}
		return 0, status.Error(codes.DataLoss, "process attachment log stream returned an empty frame")
	}
}

// Process returns the stable process snapshot obtained while opening.
func (a *ProcessAttachment) Process() *codev1.ProcessInfo { return a.process }

// Output returns exact PTY output byte chunks. Callers must consume it while
// attached so that output backpressure remains bounded.
func (a *ProcessAttachment) Output() <-chan []byte { return a.output }

// Done is closed when either stream ends or the attachment is detached.
func (a *ProcessAttachment) Done() <-chan struct{} { return a.done }

// Offset returns the latest observed process-log checkpoint.
func (a *ProcessAttachment) Offset() uint64 {
	a.resultMu.Lock()
	defer a.resultMu.Unlock()
	return a.offset
}

// Write pipelines exact input bytes with a bounded number of unacknowledged
// operations. Delivery errors that arrive later are returned by Wait.
func (a *ProcessAttachment) Write(data []byte) (int, error) {
	written := 0
	for written < len(data) {
		end := min(written+processInputChunkBytes, len(data))
		chunk := append([]byte(nil), data[written:end]...)
		a.sendMu.Lock()
		if a.closing {
			a.sendMu.Unlock()
			return written, io.ErrClosedPipe
		}
		sequence := a.next
		expected := processAttachExpectedAck{kind: processAttachDataAck, sequence: sequence, bytes: len(chunk)}
		if err := a.reserveAck(expected); err != nil {
			a.sendMu.Unlock()
			return written, err
		}
		err := a.input.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Data{
			Data: &codev1.ProcessInputData{Sequence: sequence, Data: chunk},
		}})
		if err == nil {
			a.next++
		}
		a.sendMu.Unlock()
		if err != nil {
			a.report(err)
			return written, err
		}
		written = end
	}
	return written, nil
}

// Resize pipelines a PTY resize in the same ordered operation stream as Write.
func (a *ProcessAttachment) Resize(rows, columns uint32) error {
	if rows == 0 || columns == 0 || rows > 65535 || columns > 65535 {
		return status.Error(codes.InvalidArgument, "terminal size rows and columns must be between 1 and 65535")
	}
	a.sendMu.Lock()
	defer a.sendMu.Unlock()
	if a.closing {
		return io.ErrClosedPipe
	}
	sequence := a.next
	if err := a.reserveAck(processAttachExpectedAck{
		kind: processAttachResizeAck, sequence: sequence, rows: rows, columns: columns,
	}); err != nil {
		return err
	}
	err := a.input.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Resize{
		Resize: &codev1.ProcessTerminalResize{
			Sequence: sequence,
			Size:     &codev1.TerminalSize{Rows: rows, Columns: columns},
		},
	}})
	if err != nil {
		a.report(err)
		return err
	}
	a.next++
	return nil
}

func (a *ProcessAttachment) reserveAck(expected processAttachExpectedAck) error {
	select {
	case a.pending <- expected:
		return nil
	case <-a.ctx.Done():
		return a.ctx.Err()
	}
}

// Detach releases the exclusive input writer and stops observing output. It
// waits until both local stream receivers have stopped.
func (a *ProcessAttachment) Detach() error {
	a.sendMu.Lock()
	if !a.closing {
		a.closing = true
		err := a.input.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Detach{
			Detach: &codev1.ProcessInputDetach{},
		}})
		closeErr := a.input.CloseSend()
		if err == nil {
			err = closeErr
		}
		if err != nil {
			a.report(err)
		}
	}
	a.sendMu.Unlock()
	return a.Wait()
}

// Close implements io.Closer as a non-destructive detach.
func (a *ProcessAttachment) Close() error { return a.Detach() }

// Wait waits for attachment completion and returns its first transport or
// protocol error. A clean detach or process exit returns nil.
func (a *ProcessAttachment) Wait() error {
	<-a.done
	a.resultMu.Lock()
	defer a.resultMu.Unlock()
	return a.err
}

func (a *ProcessAttachment) receiveInput() {
	for {
		response, err := a.input.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = status.Error(codes.DataLoss, "process input stream ended without an end frame")
			}
			a.report(err)
			return
		}
		if end := response.GetEnd(); end != nil {
			a.resultMu.Lock()
			a.last = end.GetProcess()
			a.resultMu.Unlock()
			if end.GetReason() != codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_PROCESS_EXITED {
				a.report(nil)
			}
			return
		}
		select {
		case expected := <-a.pending:
			if err := validateProcessAttachmentAck(response, expected); err != nil {
				a.report(err)
				return
			}
		default:
			a.report(status.Error(codes.DataLoss, "process input stream returned an unexpected acknowledgement"))
			return
		}
	}
}

func validateProcessAttachmentAck(response *codev1.StreamProcessInputResponse, expected processAttachExpectedAck) error {
	switch expected.kind {
	case processAttachDataAck:
		ack := response.GetAck()
		if ack == nil || ack.GetSequence() != expected.sequence || int(ack.GetBytesWritten()) != expected.bytes {
			return status.Error(codes.DataLoss, "invalid process input acknowledgement")
		}
	case processAttachResizeAck:
		ack := response.GetResizeAck()
		if ack == nil || ack.GetSequence() != expected.sequence || ack.GetSize().GetRows() != expected.rows || ack.GetSize().GetColumns() != expected.columns {
			return status.Error(codes.DataLoss, "invalid process terminal resize acknowledgement")
		}
	default:
		return status.Error(codes.DataLoss, "unknown process attachment acknowledgement")
	}
	return nil
}

func (a *ProcessAttachment) receiveLogs() {
	for {
		response, err := a.logs.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = status.Error(codes.DataLoss, "process log stream ended without an end frame")
			}
			a.report(err)
			return
		}
		if chunk := response.GetChunk(); chunk != nil {
			data := append([]byte(nil), chunk.GetData()...)
			a.resultMu.Lock()
			a.offset = chunk.GetNextOffset()
			a.resultMu.Unlock()
			if len(data) != 0 {
				select {
				case a.output <- data:
				case <-a.ctx.Done():
					return
				}
			}
			continue
		}
		if checkpoint := response.GetCheckpoint(); checkpoint != nil {
			a.resultMu.Lock()
			a.offset = checkpoint.GetNextOffset()
			a.resultMu.Unlock()
			continue
		}
		if end := response.GetEnd(); end != nil {
			a.resultMu.Lock()
			a.logEnd = end
			a.offset = end.GetNextOffset()
			a.resultMu.Unlock()
			a.report(nil)
			return
		}
		a.report(status.Error(codes.DataLoss, "process log stream returned an unexpected frame"))
		return
	}
}

func (a *ProcessAttachment) report(err error) {
	a.finishOnce.Do(func() { a.events <- err })
}

func (a *ProcessAttachment) finish(receivers *sync.WaitGroup) {
	err := <-a.events
	a.resultMu.Lock()
	if err != nil {
		if a.ctx.Err() != nil && status.Code(err) == codes.Canceled {
			a.err = a.ctx.Err()
		} else {
			a.err = err
		}
	}
	a.resultMu.Unlock()
	a.cancel()
	receivers.Wait()
	close(a.done)
}

var _ io.WriteCloser = (*ProcessAttachment)(nil)
