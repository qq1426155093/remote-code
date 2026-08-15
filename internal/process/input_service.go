package process

import (
	"context"
	"errors"
	"io"
	"syscall"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const maxProcessInputChunkBytes = 64 << 10

var errProcessNotPTY = errors.New("process does not have a PTY")

type processInputReceive struct {
	request *codev1.StreamProcessInputRequest
	err     error
}

// StreamProcessInput attaches one ordered writer to a managed input endpoint.
// Output remains available independently through ObserveProcessLogs.
func (s *Service) StreamProcessInput(stream codev1.ProcessService_StreamProcessInputServer) error {
	first, err := stream.Recv()
	if errors.Is(err, io.EOF) {
		return status.Error(codes.InvalidArgument, "process input stream requires an open frame")
	}
	if err != nil {
		return processInputContextError(stream.Context(), err)
	}
	open := first.GetOpen()
	if open == nil {
		return status.Error(codes.InvalidArgument, "the first process input frame must be open")
	}

	record, input, err := s.acquireProcessInput(open.GetProcess())
	if err != nil {
		return err
	}
	attached := true
	release := func() {
		if attached {
			s.releaseProcessInput(record, input)
			attached = false
		}
	}
	defer release()

	if err := stream.Send(&codev1.StreamProcessInputResponse{Payload: &codev1.StreamProcessInputResponse_Opened{
		Opened: &codev1.ProcessInputOpened{Process: s.snapshot(record)},
	}}); err != nil {
		return err
	}

	received := make(chan processInputReceive, 1)
	go receiveProcessInput(stream, received)
	expectedSequence := uint64(1)
	for {
		select {
		case <-record.done:
			release()
			return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_PROCESS_EXITED, s.snapshot(record))
		case result := <-received:
			if errors.Is(result.err, io.EOF) {
				release()
				return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_DETACHED, s.snapshot(record))
			}
			if result.err != nil {
				return processInputContextError(stream.Context(), result.err)
			}
			request := result.request
			switch payload := request.GetPayload().(type) {
			case *codev1.StreamProcessInputRequest_Data:
				data := payload.Data
				if data.GetSequence() != expectedSequence {
					return status.Errorf(codes.InvalidArgument, "process input sequence must be %d", expectedSequence)
				}
				if len(data.GetData()) == 0 {
					return status.Error(codes.InvalidArgument, "process input data must not be empty")
				}
				if len(data.GetData()) > maxProcessInputChunkBytes {
					return status.Errorf(codes.InvalidArgument, "process input data exceeds %d bytes", maxProcessInputChunkBytes)
				}
				written, writeErr := input.write(stream.Context(), data.GetData())
				if writeErr != nil {
					if input.isClosed() {
						select {
						case <-record.done:
							release()
							return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_PROCESS_EXITED, s.snapshot(record))
						case <-stream.Context().Done():
							return status.FromContextError(stream.Context().Err()).Err()
						}
					}
					return mapProcessInputWriteError(writeErr)
				}
				if written != len(data.GetData()) {
					return status.Error(codes.Internal, "process input write was incomplete")
				}
				if err := stream.Send(&codev1.StreamProcessInputResponse{Payload: &codev1.StreamProcessInputResponse_Ack{
					Ack: &codev1.ProcessInputAck{Sequence: data.GetSequence(), BytesWritten: uint32(written)},
				}}); err != nil {
					return err
				}
				expectedSequence++
			case *codev1.StreamProcessInputRequest_Resize:
				resize := payload.Resize
				if resize.GetSequence() != expectedSequence {
					return status.Errorf(codes.InvalidArgument, "process input sequence must be %d", expectedSequence)
				}
				if err := validateTerminalSize(resize.GetSize()); err != nil {
					return err
				}
				if s.snapshot(record).GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
					return status.Error(codes.FailedPrecondition, "process does not use a PTY")
				}
				if err := record.command.resize(resize.GetSize().GetRows(), resize.GetSize().GetColumns()); err != nil {
					if input.isClosed() {
						select {
						case <-record.done:
							release()
							return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_PROCESS_EXITED, s.snapshot(record))
						case <-stream.Context().Done():
							return status.FromContextError(stream.Context().Err()).Err()
						}
					}
					select {
					case <-record.done:
						release()
						return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_PROCESS_EXITED, s.snapshot(record))
					default:
					}
					if errors.Is(err, errProcessNotPTY) {
						return status.Error(codes.FailedPrecondition, err.Error())
					}
					return status.Error(codes.Internal, "resize process terminal failed")
				}
				if err := stream.Send(&codev1.StreamProcessInputResponse{Payload: &codev1.StreamProcessInputResponse_ResizeAck{
					ResizeAck: &codev1.ProcessTerminalResizeAck{
						Sequence: resize.GetSequence(),
						Size:     &codev1.TerminalSize{Rows: resize.GetSize().GetRows(), Columns: resize.GetSize().GetColumns()},
					},
				}}); err != nil {
					return err
				}
				expectedSequence++
			case *codev1.StreamProcessInputRequest_CloseInput:
				if s.snapshot(record).GetIoMode() == codev1.ProcessIOMode_PROCESS_IO_MODE_PTY {
					return status.Error(codes.FailedPrecondition, "PTY input cannot be closed independently; send terminal EOF data or signal the process")
				}
				if err := input.closeInput(stream.Context()); err != nil {
					return mapProcessInputWriteError(err)
				}
				s.closeProcessInputState(record)
				attached = false
				return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_INPUT_CLOSED, s.snapshot(record))
			case *codev1.StreamProcessInputRequest_Detach:
				release()
				return sendProcessInputEnd(stream, codev1.ProcessInputEndReason_PROCESS_INPUT_END_REASON_DETACHED, s.snapshot(record))
			case *codev1.StreamProcessInputRequest_Open:
				return status.Error(codes.InvalidArgument, "process input open frame may only appear first")
			default:
				return status.Error(codes.InvalidArgument, "process input frame payload is required")
			}
		}
	}
}

func (s *Service) acquireProcessInput(reference *codev1.ProcessReference) (*managedProcess, *processInput, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, nil, status.Error(codes.Unavailable, "process service is shutting down")
	}
	record, err := s.lookupLocked(reference)
	if err != nil {
		return nil, nil, err
	}
	if record.info.GetState() != codev1.ProcessState_PROCESS_STATE_RUNNING || record.command == nil {
		return nil, nil, status.Errorf(codes.FailedPrecondition, "process %q is %s", record.info.GetName(), processStateName(record.info.GetState()))
	}
	if record.info.GetInputMode() != codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED || record.command.input == nil {
		return nil, nil, status.Error(codes.FailedPrecondition, "process input was not enabled at startup")
	}
	if record.info.GetInputState() == codev1.ProcessInputState_PROCESS_INPUT_STATE_CLOSED || record.command.input.isClosed() {
		return nil, nil, status.Error(codes.FailedPrecondition, "process input is closed")
	}
	if record.inputAttached {
		return nil, nil, status.Error(codes.AlreadyExists, "process input already has an attached writer")
	}
	record.inputAttached = true
	record.info.InputState = codev1.ProcessInputState_PROCESS_INPUT_STATE_ATTACHED
	return record, record.command.input, nil
}

func (s *Service) releaseProcessInput(record *managedProcess, input *processInput) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record.inputAttached = false
	if record.info.GetInputState() != codev1.ProcessInputState_PROCESS_INPUT_STATE_ATTACHED {
		return
	}
	if record.info.GetState() == codev1.ProcessState_PROCESS_STATE_RUNNING && !input.isClosed() {
		record.info.InputState = codev1.ProcessInputState_PROCESS_INPUT_STATE_OPEN
	} else {
		record.info.InputState = codev1.ProcessInputState_PROCESS_INPUT_STATE_CLOSED
	}
}

func (s *Service) closeProcessInputState(record *managedProcess) {
	s.mu.Lock()
	record.inputAttached = false
	record.info.InputState = codev1.ProcessInputState_PROCESS_INPUT_STATE_CLOSED
	s.mu.Unlock()
}

func receiveProcessInput(stream codev1.ProcessService_StreamProcessInputServer, destination chan<- processInputReceive) {
	for {
		request, err := stream.Recv()
		select {
		case destination <- processInputReceive{request: request, err: err}:
		case <-stream.Context().Done():
			return
		}
		if err != nil {
			return
		}
	}
}

func sendProcessInputEnd(stream codev1.ProcessService_StreamProcessInputServer, reason codev1.ProcessInputEndReason, info *codev1.ProcessInfo) error {
	return stream.Send(&codev1.StreamProcessInputResponse{Payload: &codev1.StreamProcessInputResponse_End{
		End: &codev1.ProcessInputEnd{Reason: reason, Process: info},
	}})
}

func processInputContextError(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return status.FromContextError(ctxErr).Err()
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return status.FromContextError(err).Err()
	}
	return err
}

func mapProcessInputWriteError(err error) error {
	switch {
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return status.FromContextError(err).Err()
	case errors.Is(err, errProcessInputClosed), errors.Is(err, io.ErrClosedPipe), errors.Is(err, syscall.EPIPE):
		return status.Error(codes.FailedPrecondition, "process input is closed")
	default:
		return status.Error(codes.Internal, "write process input failed")
	}
}
