package process

import (
	"context"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestSnapshotLogsFromOffsetReturnsResumableNextOffset(t *testing.T) {
	service, record := newObservedProcess(t, false)
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "one\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR, "two\n")
	mustWriteProcessLog(t, record.logs, codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, "three\n")
	if err := record.logs.finalize(); err != nil {
		t.Fatal(err)
	}

	first, err := service.SnapshotLogsFromOffset(context.Background(), observedProcessID, nil, 0, 4)
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Chunks) != 1 || string(first.Chunks[0].Data) != "one\n" || !first.BytesTruncated || first.NextOffset != 1 {
		t.Fatalf("first snapshot = %+v", first)
	}
	second, err := service.SnapshotLogsFromOffset(context.Background(), observedProcessID, nil, first.NextOffset, 16)
	if err != nil {
		t.Fatal(err)
	}
	if len(second.Chunks) != 2 || string(second.Chunks[0].Data) != "two\n" || string(second.Chunks[1].Data) != "three\n" ||
		second.BytesTruncated || second.NextOffset != second.SnapshotEnd {
		t.Fatalf("second snapshot = %+v", second)
	}
}

func TestGetProcessInfoReturnsClone(t *testing.T) {
	service, record := newObservedProcess(t, false)
	got, err := service.GetProcessInfo(context.Background(), &codev1.ProcessReference{
		Value: &codev1.ProcessReference_Id{Id: observedProcessID},
	})
	if err != nil {
		t.Fatal(err)
	}
	got.Name = "changed"
	if record.info.GetName() == "changed" {
		t.Fatal("GetProcessInfo returned mutable registry state")
	}
}
