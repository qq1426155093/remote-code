package process

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestProcessLogReopensAfterOldSegmentsAreRemoved(t *testing.T) {
	config, err := normalizeLogConfig(LogConfig{
		MaxBytesPerProcess: minProcessLogSegmentBytes,
		MaxTotalBytes:      minProcessLogSegmentBytes,
		SegmentBytes:       minProcessLogSegmentBytes,
		RetentionAfterExit: time.Hour,
		MaxObservers:       1,
	})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), processLogDirectoryName)
	logs, err := newProcessLog(directory, config)
	if err != nil {
		t.Fatal(err)
	}
	payload := bytes.Repeat([]byte("x"), int(minProcessLogSegmentBytes*3))
	if _, err := logs.write(codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, payload); err != nil {
		t.Fatal(err)
	}
	if err := logs.finalize(); err != nil {
		t.Fatal(err)
	}
	prepared, err := logs.prepareOffset(logs.earliest)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.earliest == 0 || prepared.end <= prepared.earliest {
		t.Fatalf("retained range = [%d,%d), want trimmed non-empty range", prepared.earliest, prepared.end)
	}

	reopened, err := openProcessLog(directory, config, true)
	if err != nil {
		t.Fatalf("openProcessLog() error = %v", err)
	}
	second, err := reopened.prepareOffset(prepared.earliest)
	if err != nil {
		t.Fatal(err)
	}
	if second.earliest != prepared.earliest || second.end != prepared.end {
		t.Fatalf("reopened range = [%d,%d), want [%d,%d)", second.earliest, second.end, prepared.earliest, prepared.end)
	}
}

func TestProcessLogRecoveryTruncatesPartialRecordAndRebuildsIndices(t *testing.T) {
	config, err := normalizeLogConfig(LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), processLogDirectoryName)
	logs, err := newProcessLog(directory, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := logs.write(codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, []byte("complete\n")); err != nil {
		t.Fatal(err)
	}
	if err := logs.finalize(); err != nil {
		t.Fatal(err)
	}
	segment := logs.segments[0]
	for _, name := range []string{segment.oidx, segment.stdoutLines, segment.stderrLines} {
		if err := os.Remove(name); err != nil {
			t.Fatal(err)
		}
	}
	file, err := os.OpenFile(segment.path, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.Write([]byte{0x52, 0x43, 0x4c}); err != nil {
		t.Fatal(err)
	}
	_ = file.Close()

	reopened, err := openProcessLog(directory, config, true)
	if err != nil {
		t.Fatalf("openProcessLog() error = %v", err)
	}
	if reopened.complete {
		t.Fatal("recovered partial log is marked complete")
	}
	for _, name := range []string{segment.oidx, segment.stdoutLines, segment.stderrLines} {
		if _, err := os.Stat(name); err != nil {
			t.Fatalf("rebuilt index %s: %v", filepath.Base(name), err)
		}
	}
	prepared, err := reopened.prepareOffset(0)
	if err != nil {
		t.Fatal(err)
	}
	var output []byte
	err = reopened.readRange(0, prepared.end, prepared.earliest, map[codev1.ProcessLogStream]bool{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: true,
	}, nil, func(record storedProcessLogRecord, _ bool) error {
		output = append(output, record.payload...)
		return nil
	})
	if err != nil || string(output) != "complete\n" {
		t.Fatalf("recovered output=%q error=%v", output, err)
	}
}

func TestRecordStoreMigratesLegacyLogs(t *testing.T) {
	store, err := openRecordStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.close() })
	id := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	directory := store.processDirectory(id)
	if err := os.Mkdir(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	baseTime := time.Unix(100, 0)
	for _, value := range []struct {
		name    string
		payload string
		at      time.Time
	}{
		{name: stdoutFileName, payload: "old-out\n", at: baseTime},
		{name: stderrFileName, payload: "old-err\n", at: baseTime.Add(time.Second)},
	} {
		file, err := os.OpenFile(filepath.Join(directory, value.name), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal(err)
		}
		writer := newFrameWriter(file)
		writer.now = func() time.Time { return value.at }
		if _, err := writer.Write([]byte(value.payload)); err != nil {
			t.Fatal(err)
		}
		_ = file.Close()
	}
	logs, err := store.openLog(&codev1.ProcessInfo{Id: id, State: codev1.ProcessState_PROCESS_STATE_EXITED})
	if err != nil {
		t.Fatalf("openLog() error = %v", err)
	}
	prepared, err := logs.prepareOffset(0)
	if err != nil {
		t.Fatal(err)
	}
	var streams []codev1.ProcessLogStream
	var output []byte
	err = logs.readRange(0, prepared.end, prepared.earliest, map[codev1.ProcessLogStream]bool{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: true,
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR: true,
	}, nil, func(record storedProcessLogRecord, _ bool) error {
		streams = append(streams, record.stream)
		output = append(output, record.payload...)
		return nil
	})
	if err != nil || string(output) != "old-out\nold-err\n" || len(streams) != 2 || streams[0] != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT || streams[1] != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
		t.Fatalf("migrated streams=%v output=%q error=%v", streams, output, err)
	}
	for _, name := range []string{stdoutFileName, stderrFileName} {
		if _, err := os.Stat(filepath.Join(directory, name)); !os.IsNotExist(err) {
			t.Fatalf("legacy file %s was not removed", name)
		}
	}
}

func TestProcessLogRecoveryRejectsSymlinkedIndex(t *testing.T) {
	config, err := normalizeLogConfig(LogConfig{})
	if err != nil {
		t.Fatal(err)
	}
	directory := filepath.Join(t.TempDir(), processLogDirectoryName)
	logs, err := newProcessLog(directory, config)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := logs.write(codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, []byte("safe\n")); err != nil {
		t.Fatal(err)
	}
	if err := logs.finalize(); err != nil {
		t.Fatal(err)
	}
	indexName := logs.segments[0].oidx
	if err := os.Remove(indexName); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "outside")
	if err := os.WriteFile(target, []byte("must-not-change"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, indexName); err != nil {
		t.Fatal(err)
	}
	if _, err := openProcessLog(directory, config, true); err == nil {
		t.Fatal("openProcessLog() accepted symlinked index")
	}
	contents, err := os.ReadFile(target)
	if err != nil || string(contents) != "must-not-change" {
		t.Fatalf("outside target=%q error=%v", contents, err)
	}
}
