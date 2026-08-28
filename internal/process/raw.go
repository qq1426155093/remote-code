package process

import (
	"context"
	"io"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// RawProcessSpec describes an internally managed protocol subprocess. Raw
// processes exist for controllers that need to drive a child over a structured
// stdio protocol (for example the ACP agent): unlike StartProcess they keep
// the child's stdout in an exclusive pipe instead of the segmented log, so
// protocol frames are never persisted. Everything else — validation, limits,
// process groups, reaping, records, and restart LOST semantics — is identical
// to an ordinary managed process.
type RawProcessSpec struct {
	Name             string
	Command          string
	Arguments        []string
	WorkingDirectory string            // workspace-relative, validated like any start
	Environment      map[string]string // values live only in the child's environment
}

// RawProcess is the caller's handle to a raw subprocess. Stdin and Stdout are
// the parent ends of the child's pipes; the registry closes them after the
// process exits, and Done closes when the exit has been recorded. Stderr keeps
// flowing into the segmented log and stays observable via ObserveProcessLogs.
type RawProcess struct {
	Process *codev1.ProcessInfo
	Stdin   io.WriteCloser
	Stdout  io.ReadCloser
	Done    <-chan struct{}
}

// StartRawProcess launches one registry-managed subprocess whose stdout is
// reserved for the caller. The gRPC surface is unchanged: StartProcess behaves
// exactly as before, and StreamProcessInput refuses to attach to a raw process
// because an unsynchronized writer would corrupt the protocol stream.
func (s *Service) StartRawProcess(ctx context.Context, spec RawProcessSpec) (*RawProcess, error) {
	if spec.Command == "" {
		return nil, status.Error(codes.InvalidArgument, "raw process command is required")
	}
	request := &codev1.StartProcessRequest{
		Name:             spec.Name,
		Command:          spec.Command,
		Arguments:        spec.Arguments,
		WorkingDirectory: spec.WorkingDirectory,
		Environment:      spec.Environment,
		IoMode:           codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
		InputMode:        codev1.ProcessInputMode_PROCESS_INPUT_MODE_DISABLED,
	}
	record, err := s.launchProcess(ctx, request, startOrigin{}, true)
	if err != nil {
		return nil, err
	}
	return &RawProcess{
		Process: s.snapshot(record),
		Stdin:   record.rawStdin,
		Stdout:  record.rawStdout,
		Done:    record.done,
	}, nil
}
