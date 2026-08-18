package controllerlog

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	processservice "github.com/qq1426155093/remote-code/internal/process"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// Service exposes the controller runtime log over the versioned gRPC API.
type Service struct {
	logger *Logger
}

// NewService creates a controller-log RPC adapter. A fallback logger is valid;
// in that case Observe returns FailedPrecondition instead of exposing files.
func NewService(logger *Logger) *Service { return &Service{logger: logger} }

// Capabilities returns the public limits of the underlying durable logger.
func (s *Service) Capabilities() *codev1.ControllerLogCapabilities {
	if s == nil || s.logger == nil {
		return &codev1.ControllerLogCapabilities{}
	}
	return s.logger.Capabilities()
}

// ObserveControllerLogs replays a stable snapshot and optionally follows new
// controller events until shutdown, cancellation, or log failure.
func (s *Service) ObserveControllerLogs(request *codev1.ObserveControllerLogsRequest, stream codev1.ControllerService_ObserveControllerLogsServer) error {
	if request == nil {
		return status.Error(codes.InvalidArgument, "controller log request is required")
	}
	if stream == nil {
		return status.Error(codes.InvalidArgument, "controller log stream is required")
	}
	if s == nil || s.logger == nil {
		return status.Error(codes.FailedPrecondition, "controller runtime logs are unavailable")
	}
	logger := s.logger
	store := logger.Store()
	if store == nil {
		return status.Error(codes.FailedPrecondition, "controller runtime logs are unavailable")
	}
	if err := store.AcquireObserver(); err != nil {
		return status.Error(codes.ResourceExhausted, err.Error())
	}
	defer store.ReleaseObserver()

	prepared, err := prepareRequest(store, request)
	if err != nil {
		if status.Code(err) == codes.InvalidArgument {
			return err
		}
		if _, offsetStart := request.GetStart().(*codev1.ObserveControllerLogsRequest_Offset); offsetStart || request.GetStart() == nil {
			return controllerLogRangeError(store, err)
		}
		return status.Errorf(codes.DataLoss, "prepare controller logs: %v", err)
	}
	if err := stream.Send(&codev1.ObserveControllerLogsResponse{Payload: &codev1.ObserveControllerLogsResponse_Header{Header: &codev1.ControllerLogHeader{
		BootId: logger.BootID(), EarliestOffset: prepared.Earliest, SnapshotEndOffset: prepared.End,
		ResolvedStartOffset: prepared.Start, HistoryTruncated: prepared.HistoryTruncated,
		TailTruncated: prepared.TailTruncated, Follow: request.GetFollow(), FormatVersion: FormatVersion,
	}}}); err != nil {
		return err
	}

	sendRecord := func(record processservice.RuntimeLogRecord) error {
		event, err := decodeEvent(record.Data, record.Timestamp)
		if err != nil {
			return err
		}
		timestamp := timestamppb.New(event.Timestamp)
		if err := timestamp.CheckValid(); err != nil {
			return fmt.Errorf("controller log event timestamp is invalid: %w", err)
		}
		return stream.Send(&codev1.ObserveControllerLogsResponse{Payload: &codev1.ObserveControllerLogsResponse_Entry{Entry: &codev1.ControllerLogEntry{
			Offset: record.Offset, Timestamp: timestamp, Level: eventLevel(string(event.Level)),
			Component: event.Component, Event: event.Name, Message: event.Message,
			Fields: cloneFields(event.Fields), LineTruncated: record.LineTruncated, BootId: event.BootID,
			NextOffset: record.NextOffset,
		}}})
	}
	if err := store.ReadRange(stream.Context(), prepared, sendRecord); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return status.FromContextError(err).Err()
		}
		return status.Errorf(codes.DataLoss, "read controller logs: %v", err)
	}
	if err := sendCheckpoint(stream, prepared.End, true); err != nil {
		return err
	}
	if !request.GetFollow() {
		return finish(stream, prepared.End, codev1.ControllerLogEndReason_CONTROLLER_LOG_END_REASON_SNAPSHOT_COMPLETE, prepared.Complete)
	}

	cursor := prepared.End
	for {
		current := store.Current()
		if cursor < current.Earliest {
			return controllerLogRangeError(store, fmt.Errorf("offset %d expired while following", cursor))
		}
		if current.End > cursor {
			read := processservice.RuntimeLogRead{
				Earliest: current.Earliest, End: current.End, Start: cursor, Complete: current.Complete,
			}
			if err := store.ReadRange(stream.Context(), read, sendRecord); err != nil {
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return status.FromContextError(err).Err()
				}
				return status.Errorf(codes.DataLoss, "follow controller logs: %v", err)
			}
			cursor = current.End
			if err := sendCheckpoint(stream, cursor, false); err != nil {
				return err
			}
			continue
		}
		if current.Finalized {
			reason := codev1.ControllerLogEndReason_CONTROLLER_LOG_END_REASON_CONTROLLER_SHUTDOWN
			if !current.Complete {
				reason = codev1.ControllerLogEndReason_CONTROLLER_LOG_END_REASON_LOG_UNAVAILABLE
			}
			return finish(stream, cursor, reason, current.Complete)
		}
		notify := current.Notify
		if notify == nil {
			return status.Error(codes.Unavailable, "controller runtime log notification is unavailable")
		}
		select {
		case <-notify:
		case <-stream.Context().Done():
			return status.FromContextError(stream.Context().Err()).Err()
		}
	}
}

func prepareRequest(store *processservice.RuntimeLog, request *codev1.ObserveControllerLogsRequest) (processservice.RuntimeLogRead, error) {
	switch start := request.GetStart().(type) {
	case *codev1.ObserveControllerLogsRequest_TailLines:
		if start.TailLines > processservice.MaxRuntimeLogTailLines {
			return processservice.RuntimeLogRead{}, status.Errorf(codes.InvalidArgument, "tail_lines exceeds %d", processservice.MaxRuntimeLogTailLines)
		}
		return store.PrepareTail(start.TailLines)
	case *codev1.ObserveControllerLogsRequest_Offset:
		return store.PrepareOffset(start.Offset)
	case nil:
		return store.PrepareOffset(0)
	default:
		return processservice.RuntimeLogRead{}, status.Error(codes.InvalidArgument, "unsupported controller log start position")
	}
}

func sendCheckpoint(stream codev1.ControllerService_ObserveControllerLogsServer, offset uint64, replayComplete bool) error {
	return stream.Send(&codev1.ObserveControllerLogsResponse{Payload: &codev1.ObserveControllerLogsResponse_Checkpoint{Checkpoint: &codev1.ControllerLogCheckpoint{
		NextOffset: offset, ReplayComplete: replayComplete,
	}}})
}

func finish(stream codev1.ControllerService_ObserveControllerLogsServer, offset uint64, reason codev1.ControllerLogEndReason, complete bool) error {
	if err := stream.Send(&codev1.ObserveControllerLogsResponse{Payload: &codev1.ObserveControllerLogsResponse_End{End: &codev1.ControllerLogEnd{
		NextOffset: offset, Reason: reason, LogsComplete: complete,
	}}}); err != nil {
		return err
	}
	if !complete {
		return status.Error(codes.DataLoss, "controller runtime log is incomplete")
	}
	return nil
}

func controllerLogRangeError(store *processservice.RuntimeLog, cause error) error {
	current := store.Current()
	base := status.New(codes.OutOfRange, cause.Error())
	detail := &errdetails.ErrorInfo{
		Reason: "CONTROLLER_LOG_OFFSET_OUT_OF_RANGE", Domain: "remote.code.v1",
		Metadata: map[string]string{
			"earliest_offset": strconv.FormatUint(current.Earliest, 10),
			"next_offset":     strconv.FormatUint(current.End, 10),
		},
	}
	withDetails, err := base.WithDetails(detail)
	if err != nil {
		return base.Err()
	}
	return withDetails.Err()
}

func decodeEvent(data []byte, fallbackTimestamp time.Time) (persistedEvent, error) {
	data = trimEventLine(data)
	var event persistedEvent
	if err := json.Unmarshal(data, &event); err != nil {
		return persistedEvent{}, fmt.Errorf("decode controller log event: %w", err)
	}
	if event.Timestamp.IsZero() {
		event.Timestamp = fallbackTimestamp.UTC()
	}
	if event.Component == "" || event.Name == "" {
		return persistedEvent{}, errors.New("controller log event is missing component or event")
	}
	return event, nil
}

func trimEventLine(data []byte) []byte {
	for len(data) > 0 && (data[len(data)-1] == '\n' || data[len(data)-1] == '\r') {
		data = data[:len(data)-1]
	}
	return data
}

func cloneFields(fields map[string]string) map[string]string {
	if len(fields) == 0 {
		return nil
	}
	result := make(map[string]string, len(fields))
	for key, value := range fields {
		result[key] = value
	}
	return result
}

func eventLevel(level string) codev1.ControllerLogLevel {
	switch level {
	case "DEBUG":
		return codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_DEBUG
	case "WARN":
		return codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_WARN
	case "ERROR":
		return codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_ERROR
	default:
		return codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_INFO
	}
}
