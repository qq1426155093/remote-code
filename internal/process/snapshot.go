package process

import (
	"context"
	"fmt"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// LogSnapshot is a bounded, non-following process log view for in-process adapters.
type LogSnapshot struct {
	ProcessID        string
	IOMode           codev1.ProcessIOMode
	EarliestOffset   uint64
	SnapshotEnd      uint64
	ResolvedStart    uint64
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
	if !uuidPattern.MatchString(processID) {
		return nil, status.Error(codes.InvalidArgument, "process_id must be a lowercase UUID")
	}
	if maxBytes <= 0 || maxBytes > 16<<20 {
		return nil, status.Error(codes.InvalidArgument, "max_bytes must be between 1 and 16777216")
	}
	if tailLines > maxProcessLogTailLines {
		return nil, status.Errorf(codes.InvalidArgument, "tail_lines exceeds %d", maxProcessLogTailLines)
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
	prepared, err := logs.prepareTail(tailLines, selected)
	if err != nil {
		return nil, processLogRangeError(processID, logs, err)
	}
	result := &LogSnapshot{
		ProcessID: processID, IOMode: info.GetIoMode(), EarliestOffset: prepared.earliest,
		SnapshotEnd: prepared.end, ResolvedStart: prepared.start, HistoryTruncated: prepared.historyTruncated,
		TailTruncated: prepared.tailTruncated, LogsComplete: prepared.complete,
	}
	var total int64
	err = logs.readRange(prepared.start, prepared.end, prepared.earliest, selected, prepared.selectedLines, func(stored storedProcessLogRecord, truncated bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		if total+int64(len(stored.payload)) > maxBytes {
			result.BytesTruncated = true
			return nil
		}
		total += int64(len(stored.payload))
		result.Chunks = append(result.Chunks, LogSnapshotChunk{
			Offset: stored.offset, LineOffset: stored.lineOffset, Timestamp: time.Unix(0, stored.timestamp),
			Stream: stored.stream, Data: append([]byte(nil), stored.payload...), LineStart: stored.lineStart(),
			LineEnd: stored.lineEnd(), LineTruncated: truncated,
		})
		return nil
	})
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Errorf(codes.DataLoss, "read process logs: %s", fmt.Sprint(err))
	}
	return result, nil
}
