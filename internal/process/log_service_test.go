package process

import (
	"bytes"
	"context"
	"errors"
	"io"
	"path/filepath"
	"reflect"
	"sync"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const observedProcessID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

type captureProcessLogStream struct {
	ctx      context.Context
	messages chan *codev1.ObserveProcessLogsResponse
	mu       sync.Mutex
	values   []*codev1.ObserveProcessLogsResponse
}

func newCaptureProcessLogStream(ctx context.Context) *captureProcessLogStream {
	return &captureProcessLogStream{ctx: ctx, messages: make(chan *codev1.ObserveProcessLogsResponse, 64)}
}

func (s *captureProcessLogStream) Send(response *codev1.ObserveProcessLogsResponse) error {
	s.mu.Lock()
	s.values = append(s.values, response)
	s.mu.Unlock()
	select {
	case s.messages <- response:
		return nil
	case <-s.ctx.Done():
		return s.ctx.Err()
	}
}

func (s *captureProcessLogStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureProcessLogStream) SendHeader(metadata.MD) error { return nil }
func (s *captureProcessLogStream) SetTrailer(metadata.MD)       {}
func (s *captureProcessLogStream) Context() context.Context     { return s.ctx }
func (s *captureProcessLogStream) SendMsg(value any) error {
	response, ok := value.(*codev1.ObserveProcessLogsResponse)
	if !ok {
		return errors.New("unexpected stream message type")
	}
	return s.Send(response)
}
func (s *captureProcessLogStream) RecvMsg(any) error { return io.EOF }

func (s *captureProcessLogStream) snapshot() []*codev1.ObserveProcessLogsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*codev1.ObserveProcessLogsResponse(nil), s.values...)
}

func TestObserveProcessLogsReplaysOffsetAndAdvancesAcrossFilteredStreams(t *testing.T) {
	service, record := newObservedProcess(t, false)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "out-0\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR, "err-1\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "out-2\n")
	if err := record.logs.finalize(); err != nil {
		t.Fatal(err)
	}

	stream := newCaptureProcessLogStream(context.Background())
	err := service.ObserveProcessLogs(&codev1.ObserveProcessLogsRequest{
		ProcessId: observedProcessID,
		Streams:   []codev1.ProcessLogStream{codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT},
		Start:     &codev1.ObserveProcessLogsRequest_Offset{Offset: 1},
	}, stream)
	if err != nil {
		t.Fatalf("ObserveProcessLogs() error = %v", err)
	}
	messages := stream.snapshot()
	if len(messages) != 4 {
		t.Fatalf("messages = %d, want header, chunk, checkpoint, end", len(messages))
	}
	header := messages[0].GetHeader()
	chunk := messages[1].GetChunk()
	checkpoint := messages[2].GetCheckpoint()
	end := messages[3].GetEnd()
	if header.GetResolvedStartOffset() != 1 || header.GetSnapshotEndOffset() != 3 || chunk.GetOffset() != 2 || string(chunk.GetData()) != "out-2\n" {
		t.Fatalf("header=%+v chunk=%+v", header, chunk)
	}
	if checkpoint.GetNextOffset() != 3 || !checkpoint.GetReplayComplete() || end.GetNextOffset() != 3 || end.GetReason() != codev1.ProcessLogEndReason_PROCESS_LOG_END_REASON_SNAPSHOT_COMPLETE {
		t.Fatalf("checkpoint=%+v end=%+v", checkpoint, end)
	}
}

func TestObserveProcessLogsTailUsesLogicalLines(t *testing.T) {
	service, record := newObservedProcess(t, false)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "one\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR, "ignored\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "two-")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "part\nthree\n")
	if err := record.logs.finalize(); err != nil {
		t.Fatal(err)
	}

	stream := newCaptureProcessLogStream(context.Background())
	err := service.ObserveProcessLogs(&codev1.ObserveProcessLogsRequest{
		ProcessId: observedProcessID,
		Streams:   []codev1.ProcessLogStream{codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT},
		Start:     &codev1.ObserveProcessLogsRequest_TailLines{TailLines: 2},
	}, stream)
	if err != nil {
		t.Fatal(err)
	}
	var output []byte
	var offsets []uint64
	for _, response := range stream.snapshot() {
		if chunk := response.GetChunk(); chunk != nil {
			output = append(output, chunk.GetData()...)
			offsets = append(offsets, chunk.GetOffset())
		}
	}
	if string(output) != "two-part\nthree\n" || !reflect.DeepEqual(offsets, []uint64{2, 3, 4}) {
		t.Fatalf("tail output=%q offsets=%v", output, offsets)
	}
}

func TestObserveProcessLogsFollowsWithoutReplayGap(t *testing.T) {
	service, record := newObservedProcess(t, true)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "before\n")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	stream := newCaptureProcessLogStream(ctx)
	result := make(chan error, 1)
	go func() {
		result <- service.ObserveProcessLogs(&codev1.ObserveProcessLogsRequest{
			ProcessId: observedProcessID,
			Start:     &codev1.ObserveProcessLogsRequest_Offset{Offset: 0},
			Follow:    true,
		}, stream)
	}()

	waitForLogCheckpoint(t, stream, true)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR, "after\n")
	waitForLogCheckpoint(t, stream, false)
	if err := record.logs.finalize(); err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	record.info.State = codev1.ProcessState_PROCESS_STATE_EXITED
	record.info.ExitedAt = timestamppb.Now()
	code := int32(0)
	record.info.ExitCode = &code
	close(record.done)
	service.mu.Unlock()
	if err := <-result; err != nil {
		t.Fatalf("ObserveProcessLogs(follow) error = %v", err)
	}

	var output []byte
	var offsets []uint64
	var end *codev1.ProcessLogEnd
	for _, response := range stream.snapshot() {
		if chunk := response.GetChunk(); chunk != nil {
			output = append(output, chunk.GetData()...)
			offsets = append(offsets, chunk.GetOffset())
		}
		if response.GetEnd() != nil {
			end = response.GetEnd()
		}
	}
	if string(output) != "before\nafter\n" || !reflect.DeepEqual(offsets, []uint64{0, 1}) {
		t.Fatalf("follow output=%q offsets=%v", output, offsets)
	}
	if end == nil || end.GetReason() != codev1.ProcessLogEndReason_PROCESS_LOG_END_REASON_PROCESS_EXITED || end.GetState() != codev1.ProcessState_PROCESS_STATE_EXITED || end.GetExitCode() != 0 {
		t.Fatalf("end = %+v", end)
	}
}

func TestObserveProcessLogsReportsExpiredOffsetBounds(t *testing.T) {
	service, record := newObservedProcess(t, false)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "gone\n")
	if err := record.logs.finalize(); err != nil {
		t.Fatal(err)
	}
	if !record.logs.expire() {
		t.Fatal("expire() = false")
	}
	err := service.ObserveProcessLogs(&codev1.ObserveProcessLogsRequest{
		ProcessId: observedProcessID,
		Start:     &codev1.ObserveProcessLogsRequest_Offset{Offset: 0},
	}, newCaptureProcessLogStream(context.Background()))
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("ObserveProcessLogs(expired) error = %v", err)
	}
	values := status.Convert(err).Details()
	if len(values) != 1 {
		t.Fatalf("error details = %#v", values)
	}
	detail, ok := values[0].(*errdetails.ErrorInfo)
	if !ok || detail.GetMetadata()["earliest_offset"] != "1" || detail.GetMetadata()["next_offset"] != "1" {
		t.Fatalf("range detail = %#v", values[0])
	}
}

func TestServiceEnforcesGlobalLogLimitAndExitRetention(t *testing.T) {
	config, err := normalizeLogConfig(LogConfig{
		MaxBytesPerProcess: minProcessLogSegmentBytes,
		MaxTotalBytes:      minProcessLogSegmentBytes,
		SegmentBytes:       minProcessLogSegmentBytes,
		RetentionAfterExit: 24 * time.Hour,
		MaxObservers:       2,
	})
	if err != nil {
		t.Fatal(err)
	}
	service := &Service{processes: make(map[string]*managedProcess), logConfig: config}
	created := time.Now().Add(-2 * time.Hour)
	var records []*managedProcess
	for index, id := range []string{
		"cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		"dddddddd-dddd-4ddd-8ddd-dddddddddddd",
	} {
		logs, err := newProcessLog(filepath.Join(t.TempDir(), processLogDirectoryName), config)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := logs.write(codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, bytes.Repeat([]byte("x"), 170<<10)); err != nil {
			t.Fatal(err)
		}
		if err := logs.finalize(); err != nil {
			t.Fatal(err)
		}
		exited := created.Add(time.Duration(index) * time.Hour)
		record := &managedProcess{info: &codev1.ProcessInfo{
			Id: id, Name: "retained", State: codev1.ProcessState_PROCESS_STATE_EXITED,
			CreatedAt: timestamppb.New(exited), ExitedAt: timestamppb.New(time.Now()),
		}, logs: logs, done: closedChannel()}
		service.processes[id] = record
		records = append(records, record)
	}
	service.enforceLogRetention()
	if len(records[0].logs.segments) != 0 || len(records[1].logs.segments) == 0 {
		t.Fatalf("segments after global limit = %d, %d; want oldest removed", len(records[0].logs.segments), len(records[1].logs.segments))
	}
	service.logConfig.RetentionAfterExit = time.Nanosecond
	service.enforceLogRetention()
	if len(records[1].logs.segments) != 0 || !records[1].logs.expired {
		t.Fatalf("newer log was not expired: segments=%d expired=%v", len(records[1].logs.segments), records[1].logs.expired)
	}
}

func newObservedProcess(t *testing.T, running bool) (*Service, *managedProcess) {
	t.Helper()
	config, err := normalizeLogConfig(LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	logs, err := newProcessLog(filepath.Join(t.TempDir(), processLogDirectoryName), config)
	if err != nil {
		t.Fatal(err)
	}
	done := closedChannel()
	state := codev1.ProcessState_PROCESS_STATE_EXITED
	info := &codev1.ProcessInfo{
		Id: observedProcessID, Name: "observed", IoMode: codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE,
		State: state, CreatedAt: timestamppb.Now(), ExitedAt: timestamppb.Now(),
	}
	if running {
		done = make(chan struct{})
		info.State = codev1.ProcessState_PROCESS_STATE_RUNNING
		info.ExitedAt = nil
	}
	record := &managedProcess{info: info, logs: logs, done: done}
	service := &Service{processes: map[string]*managedProcess{observedProcessID: record}, logConfig: config}
	t.Cleanup(func() { _ = logs.finalize() })
	return service, record
}

func mustWriteProcessLog(t *testing.T, logs *processLog, stream codev1.ProcessLogStream, value string) {
	t.Helper()
	if _, err := logs.write(stream, []byte(value)); err != nil {
		t.Fatal(err)
	}
}

func waitForLogCheckpoint(t *testing.T, stream *captureProcessLogStream, replay bool) {
	t.Helper()
	for {
		select {
		case response := <-stream.messages:
			if checkpoint := response.GetCheckpoint(); checkpoint != nil && checkpoint.GetReplayComplete() == replay {
				return
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("timed out waiting for replay=%v checkpoint", replay)
		}
	}
}
