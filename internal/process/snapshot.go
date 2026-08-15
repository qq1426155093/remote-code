package process

import (
	"context"
	"errors"
	"fmt"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errStopLogSnapshot = errors.New("stop bounded log snapshot")

// LogSnapshot is a bounded, non-following process log view for in-process adapters.
type LogSnapshot struct {
	ProcessID        string
	IOMode           codev1.ProcessIOMode
	EarliestOffset   uint64
	SnapshotEnd      uint64
	ResolvedStart    uint64
	NextOffset       uint64
	HistoryTruncated bool
	TailTruncated    bool
	BytesTruncated   bool
	LogsComplete     bool
	Chunks           []LogSnapshotChunk
}

type LogSnapshotChunk struct {
	Offset        uint64
	LineOffset    uint64
	Timestamp     time.Time
	Stream        codev1.ProcessLogStream
	Data          []byte
	LineStart     bool
	LineEnd       bool
	LineTruncated bool
}

// SnapshotLogs reuses the process log store to create a bounded snapshot.
func (s *Service) SnapshotLogs(ctx context.Context, processID string, streams []codev1.ProcessLogStream, tailLines uint64, maxBytes int64) (*LogSnapshot, error) {
	if tailLines > maxProcessLogTailLines {
		return nil, status.Errorf(codes.InvalidArgument, "tail_lines exceeds %d", maxProcessLogTailLines)
	}
	return s.snapshotLogs(ctx, processID, streams, maxBytes, func(logs *processLog, selected map[codev1.ProcessLogStream]bool) (preparedProcessLogRead, error) {
		return logs.prepareTail(tailLines, selected)
	})
}

// SnapshotLogsFromOffset returns a bounded non-following snapshot beginning at
// an exact logical record offset. NextOffset can be supplied to a later call.
func (s *Service) SnapshotLogsFromOffset(ctx context.Context, processID string, streams []codev1.ProcessLogStream, offset uint64, maxBytes int64) (*LogSnapshot, error) {
	return s.snapshotLogs(ctx, processID, streams, maxBytes, func(logs *processLog, _ map[codev1.ProcessLogStream]bool) (preparedProcessLogRead, error) {
		return logs.prepareOffset(offset)
	})
}

func (s *Service) snapshotLogs(
	ctx context.Context,
	processID string,
	streams []codev1.ProcessLogStream,
	maxBytes int64,
	prepare func(*processLog, map[codev1.ProcessLogStream]bool) (preparedProcessLogRead, error),
) (*LogSnapshot, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if !uuidPattern.MatchString(processID) {
		return nil, status.Error(codes.InvalidArgument, "process_id must be a lowercase UUID")
	}
	if maxBytes <= 0 || maxBytes > 16<<20 {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be between 1 and 16777216")
	}
	selected, _, err := validateLogStreams(streams)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	record := s.processes[processID]
	if record == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.NotFound, "process not found")
	}
	logs := record.logs
	info := cloneProcessInfo(record.info)
	if logs == nil {
		s.mu.Unlock()
		return nil, status.Error(codes.FailedPrecondition, "process logs are unavailable")
	}
	if err := logs.acquireObserver(); err != nil {
		s.mu.Unlock()
		return nil, status.Error(codes.ResourceExhausted, err.Error())
	}
	s.mu.Unlock()
	defer logs.releaseObserver()
	prepared, err := prepare(logs, selected)
	if err != nil {
		return nil, processLogRangeError(processID, logs, err)
	}
	result := &LogSnapshot{
		ProcessID: processID, IOMode: info.GetIoMode(), EarliestOffset: prepared.earliest,
		SnapshotEnd: prepared.end, ResolvedStart: prepared.start, NextOffset: prepared.start, HistoryTruncated: prepared.historyTruncated,
		TailTruncated: prepared.tailTruncated, LogsComplete: prepared.complete,
	}
	var total int64
	readStreams := map[codev1.ProcessLogStream]bool{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: true,
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR: true,
	}
	err = logs.readRange(prepared.start, prepared.end, prepared.earliest, readStreams, prepared.selectedLines, func(stored storedProcessLogRecord, truncated bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if !selected[stored.stream] {
			return nil
		}
		if total+int64(len(stored.payload)) > maxBytes {
			result.BytesTruncated = true
			return errStopLogSnapshot
		}
		total += int64(len(stored.payload))
		result.Chunks = append(result.Chunks, LogSnapshotChunk{
			Offset: stored.offset, LineOffset: stored.lineOffset, Timestamp: time.Unix(0, stored.timestamp),
			Stream: stored.stream, Data: append([]byte(nil), stored.payload...), LineStart: stored.lineStart(),
			LineEnd: stored.lineEnd(), LineTruncated: truncated,
		})
		result.NextOffset = stored.offset + 1
		return nil
	})
	if errors.Is(err, errStopLogSnapshot) {
		err = nil
	}
	if contextErr := ctx.Err(); contextErr != nil {
		return nil, status.FromContextError(contextErr).Err()
	}
	if err != nil {
		return nil, status.Errorf(codes.DataLoss, "read process logs: %s", fmt.Sprint(err))
	}
	if !result.BytesTruncated {
		result.NextOffset = prepared.end
	}
	return result, nil
}
