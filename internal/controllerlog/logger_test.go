package controllerlog

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/logging"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type captureControllerLogStream struct {
	mu         sync.Mutex
	ctx        context.Context
	responses  []*codev1.ObserveControllerLogsResponse
	closedWith error
}

func (s *captureControllerLogStream) Send(response *codev1.ObserveControllerLogsResponse) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.responses = append(s.responses, response)
	return s.closedWith
}

func (s *captureControllerLogStream) snapshot() []*codev1.ObserveControllerLogsResponse {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*codev1.ObserveControllerLogsResponse(nil), s.responses...)
}

func (s *captureControllerLogStream) responseCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.responses)
}
func (s *captureControllerLogStream) SetHeader(metadata.MD) error  { return nil }
func (s *captureControllerLogStream) SendHeader(metadata.MD) error { return nil }
func (s *captureControllerLogStream) SetTrailer(metadata.MD)       {}
func (s *captureControllerLogStream) Context() context.Context     { return s.ctx }
func (s *captureControllerLogStream) SendMsg(any) error            { return nil }
func (s *captureControllerLogStream) RecvMsg(any) error            { return io.EOF }

func testControllerLogConfig() Config {
	return Config{
		MaxBytesPerController: 1 << 20, MaxTotalBytes: 2 << 20, SegmentBytes: 256 << 10,
		RetentionAfterRestart: time.Hour, MaxObservers: 2,
	}
}

func TestZeroLoggerEmitIsSafe(t *testing.T) {
	var logger Logger
	logger.Emit(logging.Event{Component: "test", Name: "zero_value"})
	if logger.Available() {
		t.Fatal("zero logger reported durable availability")
	}
}

func TestLoggerPersistsRedactsAndObservesEvents(t *testing.T) {
	directory := t.TempDir()
	var stderr bytes.Buffer
	logger, err := Open(directory, testControllerLogConfig(), &stderr)
	if err != nil {
		t.Fatal(err)
	}
	bootID := logger.BootID()
	logger.Emit(logging.Event{
		Level: logging.LevelWarn, Component: "test", Name: "configuration_loaded", Message: "loaded",
		Fields: map[string]string{"token": "do-not-persist", "safe": "value"},
	})
	if strings.Contains(stderr.String(), "do-not-persist") || !strings.Contains(stderr.String(), "[REDACTED]") {
		t.Fatalf("stderr event = %q; expected redaction", stderr.String())
	}

	stream := &captureControllerLogStream{ctx: context.Background()}
	lines := uint64(1)
	if err := NewService(logger).ObserveControllerLogs(&codev1.ObserveControllerLogsRequest{
		Start: &codev1.ObserveControllerLogsRequest_TailLines{TailLines: lines},
	}, stream); err != nil {
		t.Fatalf("ObserveControllerLogs() error = %v", err)
	}
	responses := stream.snapshot()
	if len(responses) != 4 {
		t.Fatalf("responses = %d, want header, entry, checkpoint, end", len(responses))
	}
	entry := responses[1].GetEntry()
	if entry == nil || entry.GetBootId() != bootID || entry.GetOffset() != 0 || entry.GetNextOffset() != 1 || entry.GetEvent() != "configuration_loaded" || entry.GetFields()["token"] != "[REDACTED]" || entry.GetFields()["safe"] != "value" {
		t.Fatalf("entry = %+v, want persisted event and redacted fields", entry)
	}
	if got := responses[2].GetCheckpoint().GetNextOffset(); got != 1 {
		t.Fatalf("checkpoint next offset = %d, want 1", got)
	}
	if got := responses[3].GetEnd().GetReason(); got != codev1.ControllerLogEndReason_CONTROLLER_LOG_END_REASON_SNAPSHOT_COMPLETE {
		t.Fatalf("end reason = %s, want snapshot complete", got)
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(directory, testControllerLogConfig(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if reopened.BootID() == bootID {
		t.Fatal("reopened logger reused boot id")
	}
	reopened.Emit(logging.Event{Level: logging.LevelInfo, Component: "test", Name: "after_restart"})
	stream = &captureControllerLogStream{ctx: context.Background()}
	if err := NewService(reopened).ObserveControllerLogs(&codev1.ObserveControllerLogsRequest{}, stream); err != nil {
		t.Fatalf("ObserveControllerLogs(reopened) error = %v", err)
	}
	responses = stream.snapshot()
	if entry := responses[1].GetEntry(); entry == nil || entry.GetBootId() != bootID {
		t.Fatalf("first historical entry = %+v, want original boot id", entry)
	}
	if entry := responses[2].GetEntry(); entry == nil || entry.GetBootId() != reopened.BootID() {
		t.Fatalf("reopened entry = %+v, want new boot id", entry)
	}
}

func TestObserveControllerLogsFollowEndsOnFinalize(t *testing.T) {
	logger, err := Open(t.TempDir(), testControllerLogConfig(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()
	logger.Emit(logging.Event{Component: "test", Name: "before_follow"})
	stream := &captureControllerLogStream{ctx: context.Background()}
	done := make(chan error, 1)
	go func() {
		done <- NewService(logger).ObserveControllerLogs(&codev1.ObserveControllerLogsRequest{Follow: true}, stream)
	}()
	deadline := time.After(2 * time.Second)
	for stream.responseCount() < 3 {
		select {
		case <-deadline:
			t.Fatalf("follow did not send initial replay: %d responses", stream.responseCount())
		default:
			time.Sleep(time.Millisecond)
		}
	}
	if err := logger.Finalize(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("follow returned error = %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("follow did not finish after logger finalization")
	}
	responses := stream.snapshot()
	end := responses[len(responses)-1].GetEnd()
	if end == nil || end.GetReason() != codev1.ControllerLogEndReason_CONTROLLER_LOG_END_REASON_CONTROLLER_SHUTDOWN {
		t.Fatalf("follow end = %+v, want controller shutdown", end)
	}
}

func TestOpenRejectsConcurrentControllerLogger(t *testing.T) {
	directory := t.TempDir()
	first, err := Open(directory, testControllerLogConfig(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Close()
	if second, err := Open(directory, testControllerLogConfig(), io.Discard); err == nil {
		_ = second.Close()
		t.Fatal("opened two controller loggers for one runtime directory")
	}
}

func TestLoggerRetentionAfterRestartRemovesSealedSegments(t *testing.T) {
	directory := t.TempDir()
	config := testControllerLogConfig()
	config.RetentionAfterRestart = time.Nanosecond
	logger, err := Open(directory, config, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	logger.Emit(logging.Event{Component: "test", Name: "before_restart"})
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}

	reopened, err := Open(directory, config, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got := reopened.Store().DiskBytes(); got != 0 {
		t.Fatalf("reopened disk bytes = %d, want old sealed segments expired", got)
	}
	stream := &captureControllerLogStream{ctx: context.Background()}
	if err := NewService(reopened).ObserveControllerLogs(&codev1.ObserveControllerLogsRequest{
		Start: &codev1.ObserveControllerLogsRequest_TailLines{TailLines: 1},
	}, stream); err != nil {
		t.Fatal(err)
	}
	responses := stream.snapshot()
	if len(responses) != 3 || responses[1].GetEntry() != nil || !responses[0].GetHeader().GetTailTruncated() {
		t.Fatalf("retained responses = %+v, want truncated empty tail", responses)
	}
}

func TestLoggerRedactsInlineCredentialsAndSecuresExistingLogFiles(t *testing.T) {
	directory := t.TempDir()
	var stderr bytes.Buffer
	logger, err := Open(directory, testControllerLogConfig(), &stderr)
	if err != nil {
		t.Fatal(err)
	}
	logger.Emit(logging.Event{
		Component: "test", Name: "credentials", Message: "token=inline-token access_token=compound-token env=environment-secret password=\"quoted secret with spaces\" Authorization: Bearer bearer-token {\"password\":\"json-secret\",\"access_token\":\"json-access-token\",\"client_secret\":\"escaped\\\"suffix\"}",
		Fields: map[string]string{
			"api_key": "api-secret", "safe": "password=field-secret", "ordinary": "value",
		},
	})
	if strings.Contains(stderr.String(), "inline-token") || strings.Contains(stderr.String(), "compound-token") || strings.Contains(stderr.String(), "environment-secret") || strings.Contains(stderr.String(), "quoted secret with spaces") || strings.Contains(stderr.String(), "bearer-token") || strings.Contains(stderr.String(), "json-secret") || strings.Contains(stderr.String(), "json-access-token") || strings.Contains(stderr.String(), "escaped\\\"suffix") || strings.Contains(stderr.String(), "api-secret") || strings.Contains(stderr.String(), "field-secret") {
		t.Fatalf("credentials leaked to stderr: %q", stderr.String())
	}
	if !strings.Contains(stderr.String(), "[REDACTED]") {
		t.Fatalf("redacted marker missing from stderr: %q", stderr.String())
	}
	if err := logger.Close(); err != nil {
		t.Fatal(err)
	}
	logDirectory := filepath.Join(directory, controllerLogDirectoryName)
	if err := os.Chmod(logDirectory, 0o755); err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(logDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			if err := os.Chmod(filepath.Join(logDirectory, entry.Name()), 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
	reopened, err := Open(directory, testControllerLogConfig(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	directoryInfo, err := os.Stat(logDirectory)
	if err != nil {
		t.Fatal(err)
	}
	if directoryInfo.Mode().Perm() != 0o700 {
		t.Fatalf("controller log directory mode = %o, want 700", directoryInfo.Mode().Perm())
	}
	entries, err = os.ReadDir(logDirectory)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if entry.Type().IsRegular() {
			info, statErr := entry.Info()
			if statErr != nil {
				t.Fatal(statErr)
			}
			if info.Mode().Perm() != 0o600 {
				t.Fatalf("controller log file %s mode = %o, want 600", entry.Name(), info.Mode().Perm())
			}
		}
	}
}

func TestOpenRejectsControllerLogDirectorySymlink(t *testing.T) {
	directory := t.TempDir()
	target := t.TempDir()
	if err := os.Symlink(target, filepath.Join(directory, controllerLogDirectoryName)); err != nil {
		t.Skipf("symlink unavailable: %v", err)
	}
	if _, err := Open(directory, testControllerLogConfig(), io.Discard); err == nil {
		t.Fatal("opened controller logs through a symbolic link")
	}
}

func TestObserveControllerLogsReportsOffsetBounds(t *testing.T) {
	logger, err := Open(t.TempDir(), testControllerLogConfig(), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()
	logger.Emit(logging.Event{Component: "test", Name: "one"})
	badOffset := uint64(99)
	err = NewService(logger).ObserveControllerLogs(&codev1.ObserveControllerLogsRequest{
		Start: &codev1.ObserveControllerLogsRequest_Offset{Offset: badOffset},
	}, &captureControllerLogStream{ctx: context.Background()})
	if status.Code(err) != codes.OutOfRange {
		t.Fatalf("ObserveControllerLogs(offset) code = %s, error = %v", status.Code(err), err)
	}
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatalf("offset error details = %#v", details)
	}
	detail, ok := details[0].(*errdetails.ErrorInfo)
	if !ok || detail.GetMetadata()["earliest_offset"] != "0" || detail.GetMetadata()["next_offset"] != "1" {
		t.Fatalf("offset error detail = %#v", details[0])
	}
}
