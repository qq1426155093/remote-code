package client

import (
	"context"
	"io"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const processInputChunkBytes = 64 << 10

// ProcessInputSession is one exclusive attachment to a running process input.
// Close detaches and deliberately does not close the child stdin endpoint.
type ProcessInputSession struct {
	mu       sync.Mutex
	stream   codev1.ProcessService_StreamProcessInputClient
	opened   *codev1.ProcessInfo
	last     *codev1.ProcessInfo
	next     uint64
	finished bool
}

// OpenProcessInput attaches to input retained by a process that was started in
// PROCESS_INPUT_MODE_MANAGED mode.
func (c *Client) OpenProcessInput(ctx context.Context, process *codev1.ProcessReference) (*ProcessInputSession, error) {
	stream, err := c.processes.StreamProcessInput(ctx)
	if err != nil {
		return nil, err
	}
	if err := stream.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Open{
		Open: &codev1.ProcessInputOpen{Process: process},
	}}); err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	response, err := stream.Recv()
	if err != nil {
		_ = stream.CloseSend()
		return nil, err
	}
	opened := response.GetOpened()
	if opened == nil || opened.GetProcess() == nil {
		_ = stream.CloseSend()
		return nil, status.Error(codes.DataLoss, "process input stream did not return an opened frame")
	}
	return &ProcessInputSession{stream: stream, opened: opened.GetProcess(), next: 1}, nil
}

// Process returns the process snapshot captured when this input session opened.
func (s *ProcessInputSession) Process() *codev1.ProcessInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.opened
}

// Write sends and confirms p in ordered chunks. If the context is canceled
// before an acknowledgement, the last chunk's delivery outcome is unknown and
// callers must not retry it automatically.
func (s *ProcessInputSession) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return 0, io.ErrClosedPipe
	}
	written := 0
	for written < len(p) {
		end := min(written+processInputChunkBytes, len(p))
		chunk := p[written:end]
		sequence := s.next
		if err := s.stream.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Data{
			Data: &codev1.ProcessInputData{Sequence: sequence, Data: chunk},
		}}); err != nil {
			s.finished = true
			return written, err
		}
		response, err := s.stream.Recv()
		if err != nil {
			s.finished = true
			return written, err
		}
		if endFrame := response.GetEnd(); endFrame != nil {
			s.finishLocked(endFrame.GetProcess())
			return written, processInputEndedError(endFrame.GetReason())
		}
		ack := response.GetAck()
		if ack == nil || ack.GetSequence() != sequence || int(ack.GetBytesWritten()) != len(chunk) {
			s.finished = true
			return written, status.Error(codes.DataLoss, "invalid process input acknowledgement")
		}
		written = end
		s.next++
	}
	return written, nil
}

// Resize changes the dimensions of an attached PTY and waits for the server to
// confirm that the resize was applied. It shares ordering with Write calls.
func (s *ProcessInputSession) Resize(rows, columns uint32) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return io.ErrClosedPipe
	}
	sequence := s.next
	if err := s.stream.Send(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Resize{
		Resize: &codev1.ProcessTerminalResize{
			Sequence: sequence,
			Size:     &codev1.TerminalSize{Rows: rows, Columns: columns},
		},
	}}); err != nil {
		s.finished = true
		return err
	}
	response, err := s.stream.Recv()
	if err != nil {
		s.finished = true
		return err
	}
	if endFrame := response.GetEnd(); endFrame != nil {
		s.finishLocked(endFrame.GetProcess())
		return processInputEndedError(endFrame.GetReason())
	}
	ack := response.GetResizeAck()
	if ack == nil || ack.GetSequence() != sequence || ack.GetSize().GetRows() != rows || ack.GetSize().GetColumns() != columns {
		s.finished = true
		return status.Error(codes.DataLoss, "invalid process terminal resize acknowledgement")
	}
	s.next++
	return nil
}

// CloseInput permanently closes PIPE stdin after all acknowledged writes. PTY
// sessions reject this operation because a PTY has no independent write side.
func (s *ProcessInputSession) CloseInput() (*codev1.ProcessInfo, error) {
	return s.finish(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_CloseInput{
		CloseInput: &codev1.ProcessInputClose{},
	}})
}

// Detach releases the exclusive writer while keeping the remote input open.
func (s *ProcessInputSession) Detach() (*codev1.ProcessInfo, error) {
	return s.finish(&codev1.StreamProcessInputRequest{Payload: &codev1.StreamProcessInputRequest_Detach{
		Detach: &codev1.ProcessInputDetach{},
	}})
}

// Close implements io.Closer as a non-destructive detach.
func (s *ProcessInputSession) Close() error {
	_, err := s.Detach()
	return err
}

func (s *ProcessInputSession) finish(request *codev1.StreamProcessInputRequest) (*codev1.ProcessInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finished {
		return s.last, nil
	}
	if err := s.stream.Send(request); err != nil {
		s.finished = true
		return nil, err
	}
	if err := s.stream.CloseSend(); err != nil {
		s.finished = true
		return nil, err
	}
	response, err := s.stream.Recv()
	if err != nil {
		s.finished = true
		return nil, err
	}
	end := response.GetEnd()
	if end == nil || end.GetProcess() == nil {
		s.finished = true
		return nil, status.Error(codes.DataLoss, "process input stream did not return an end frame")
	}
	s.finishLocked(end.GetProcess())
	return end.GetProcess(), nil
}

func (s *ProcessInputSession) finishLocked(info *codev1.ProcessInfo) {
	s.finished = true
	s.last = info
	_ = s.stream.CloseSend()
}

func processInputEndedError(reason codev1.ProcessInputEndReason) error {
	name := reason.String()
	if name == "" {
		name = "unknown"
	}
	return status.Errorf(codes.FailedPrecondition, "process input ended: %s", name)
}

var _ io.WriteCloser = (*ProcessInputSession)(nil)
