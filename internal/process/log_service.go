package process

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// ObserveProcessLogs replays a stable snapshot and optionally follows output
// from the exact snapshot boundary until the process log is finalized.
func (s *Service) ObserveProcessLogs(request *codev1.ObserveProcessLogsRequest, stream codev1.ProcessService_ObserveProcessLogsServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "process log request is required")
	}
	id := request.GetProcessId()
	if !uuidPattern.MatchString(id) {
		return status.Error(codes.InvalidArgument, "process_id must be a lowercase UUID")
	}
	selected, selectedList, err := validateLogStreams(request.GetStreams())
	if err != nil {
		return err
	}

	s.mu.Lock()
	record := s.processes[id]
	if record == nil {
		s.mu.Unlock()
		return status.Error(codes.NotFound, "process not found")
	}
	logs := record.logs
	info := cloneProcessInfo(record.info)
	if logs == nil {
		s.mu.Unlock()
		return status.Error(codes.FailedPrecondition, "process logs are unavailable")
	}
	if err := logs.acquireObserver(); err != nil {
		s.mu.Unlock()
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	s.mu.Unlock()
	defer logs.releaseObserver()

	var prepared preparedProcessLogRead
	switch start := request.GetStart().(type) {
	case *codev1.ObserveProcessLogsRequest_TailLines:
		if start.TailLines > maxProcessLogTailLines {
			return status.Errorf(codes.InvalidArgument, "tail_lines exceeds %d", maxProcessLogTailLines)
		}
		prepared, err = logs.prepareTail(start.TailLines, selected)
	case *codev1.ObserveProcessLogsRequest_Offset:
		prepared, err = logs.prepareOffset(start.Offset)
	case nil:
		prepared, err = logs.prepareOffset(0)
	default:
		return status.Error(codes.InvalidArgument, "unsupported process log start position")
	}
	if err != nil {
		return processLogRangeError(id, logs, err)
	}
	if err := stream.Send(&codev1.ObserveProcessLogsResponse{Payload: &codev1.ObserveProcessLogsResponse_Header{Header: &codev1.ProcessLogHeader{
		ProcessId:           id,
		IoMode:              info.GetIoMode(),
		Streams:             selectedList,
		EarliestOffset:      prepared.earliest,
		SnapshotEndOffset:   prepared.end,
		ResolvedStartOffset: prepared.start,
		HistoryTruncated:    prepared.historyTruncated,
		TailTruncated:       prepared.tailTruncated,
		Follow:              request.GetFollow(),
		FormatVersion:       processLogFormatVersion,
	}}}); err != nil {
		return err
	}

	sendRecord := func(record storedProcessLogRecord, truncated bool) error {
		return stream.Send(&codev1.ObserveProcessLogsResponse{Payload: &codev1.ObserveProcessLogsResponse_Chunk{Chunk: &codev1.ProcessLogChunk{
			Offset:        record.offset,
			NextOffset:    record.offset + 1,
			LineOffset:    record.lineOffset,
			Timestamp:     timestamppb.New(time.Unix(0, record.timestamp)),
			Stream:        record.stream,
			Data:          append([]byte(nil), record.payload...),
			LineStart:     record.lineStart(),
			LineEnd:       record.lineEnd(),
			LineTruncated: truncated,
		}}})
	}
	if err := logs.readRange(prepared.start, prepared.end, prepared.earliest, selected, prepared.selectedLines, sendRecord); err != nil {
		return status.Errorf(codes.DataLoss, "read process logs: %v", err)
	}
	if err := sendLogCheckpoint(stream, prepared.end, true); err != nil {
		return err
	}

	if !request.GetFollow() {
		return s.finishLogStream(record, stream, prepared.end, codev1.ProcessLogEndReason_PROCESS_LOG_END_REASON_SNAPSHOT_COMPLETE, prepared.complete)
	}
	cursor := prepared.end
	for {
		end, earliest, finalized, complete, notify := logs.current(cursor)
		if cursor < earliest {
			return processLogRangeError(id, logs, fmt.Errorf("offset %d expired while following", cursor))
		}
		if end > cursor {
			if err := logs.readRange(cursor, end, earliest, selected, nil, sendRecord); err != nil {
				return status.Errorf(codes.DataLoss, "follow process logs: %v", err)
			}
			cursor = end
			if err := sendLogCheckpoint(stream, cursor, false); err != nil {
				return err
			}
			continue
		}
		if finalized {
			select {
			case <-record.done:
			case <-stream.Context().Done():
				return status.FromContextError(stream.Context().Err()).Err()
			}
			return s.finishLogStream(record, stream, cursor, codev1.ProcessLogEndReason_PROCESS_LOG_END_REASON_PROCESS_EXITED, complete)
		}
		select {
		case <-notify:
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}

func validateLogStreams(values []codev1.ProcessLogStream) (map[codev1.ProcessLogStream]bool, []codev1.ProcessLogStream, error) {
	if len(values) == 0 {
		values = []codev1.ProcessLogStream{
			codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT,
			codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR,
		}
	}
	selected := make(map[codev1.ProcessLogStream]bool, len(values))
	ordered := make([]codev1.ProcessLogStream, 0, len(values))
	for _, value := range values {
		if value != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT && value != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
			return nil, nil, status.Errorf(codes.InvalidArgument, "unsupported process log stream %q", value)
		}
		if selected[value] {
			return nil, nil, status.Errorf(codes.InvalidArgument, "process log stream %q is duplicated", value)
		}
		selected[value] = true
		ordered = append(ordered, value)
	}
	sort.Slice(ordered, func(i, j int) bool { return ordered[i] < ordered[j] })
	return selected, ordered, nil
}

func sendLogCheckpoint(stream codev1.ProcessService_ObserveProcessLogsServer, offset uint64, replayComplete bool) error {
	return stream.Send(&codev1.ObserveProcessLogsResponse{Payload: &codev1.ObserveProcessLogsResponse_Checkpoint{Checkpoint: &codev1.ProcessLogCheckpoint{
		NextOffset:     offset,
		ReplayComplete: replayComplete,
	}}})
}

func (s *Service) finishLogStream(record *managedProcess, stream codev1.ProcessService_ObserveProcessLogsServer, offset uint64, reason codev1.ProcessLogEndReason, complete bool) error {
	info := s.snapshot(record)
	if err := stream.Send(&codev1.ObserveProcessLogsResponse{Payload: &codev1.ObserveProcessLogsResponse_End{End: &codev1.ProcessLogEnd{
		NextOffset:   offset,
		Reason:       reason,
		State:        info.GetState(),
		ExitCode:     info.ExitCode,
		ExitSignal:   info.ExitSignal,
		LogsComplete: complete,
	}}}); err != nil {
		return err
	}
	if !complete {
		return status.Error(codes.DataLoss, "process output log is incomplete")
	}
	return nil
}

func processLogRangeError(id string, logs *processLog, cause error) error {
	end, earliest, _, _, _ := logs.current(0)
	base := status.New(codes.OutOfRange, cause.Error())
	detail := &errdetails.ErrorInfo{
		Reason: "LOG_OFFSET_OUT_OF_RANGE",
		Domain: "remote.code.v1",
		Metadata: map[string]string{
			"process_id":      id,
			"earliest_offset": strconv.FormatUint(earliest, 10),
			"next_offset":     strconv.FormatUint(end, 10),
		},
	}
	withDetails, err := base.WithDetails(detail)
	if err != nil {
		return base.Err()
	}
	return withDetails.Err()
}

func (s *Service) runLogJanitor() {
	defer close(s.janitorDone)
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.enforceLogRetention()
		case <-s.janitorStop:
			return
		}
	}
}

func (s *Service) enforceLogRetention() {
	type candidate struct {
		logs *processLog
		info *codev1.ProcessInfo
	}
	s.mu.Lock()
	values := make([]candidate, 0, len(s.processes))
	for _, record := range s.processes {
		if record.logs != nil {
			values = append(values, candidate{logs: record.logs, info: cloneProcessInfo(record.info)})
		}
	}
	s.mu.Unlock()
	now := time.Now()
	for _, value := range values {
		if s.logConfig.RetentionAfterExit <= 0 || isActiveState(value.info.GetState()) || value.info.GetExitedAt() == nil {
			continue
		}
		if now.Sub(value.info.GetExitedAt().AsTime()) >= s.logConfig.RetentionAfterExit {
			value.logs.expire()
		}
	}
	var total int64
	for _, value := range values {
		total += value.logs.diskBytes()
	}
	if total <= s.logConfig.MaxTotalBytes {
		return
	}
	sort.Slice(values, func(i, j int) bool {
		leftActive := isActiveState(values[i].info.GetState())
		rightActive := isActiveState(values[j].info.GetState())
		if leftActive != rightActive {
			return !leftActive
		}
		left := values[i].info.GetCreatedAt().AsTime()
		right := values[j].info.GetCreatedAt().AsTime()
		return left.Before(right)
	})
	for total > s.logConfig.MaxTotalBytes {
		removed := false
		for _, value := range values {
			before := value.logs.diskBytes()
			if value.logs.trimOldestSegment() {
				total -= before - value.logs.diskBytes()
				removed = true
				if total <= s.logConfig.MaxTotalBytes {
					break
				}
			}
		}
		if !removed {
			break
		}
	}
}
