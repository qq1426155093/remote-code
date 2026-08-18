package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

// MaxRuntimeLogTailLines is the common upper bound used by the controller
// runtime-log observer. It intentionally matches the process-log contract.
const MaxRuntimeLogTailLines = maxProcessLogTailLines

// RuntimeLog is a small generic adapter around the durable segment store used
// by process output logs. Runtime logs use the stdout record kind internally;
// the adapter keeps that implementation detail out of controller/server code.
// Every appended controller event is one newline-terminated logical record.
type RuntimeLog struct {
	log *processLog
}

// RuntimeLogRead is an atomic read boundary captured under the log mutex.
// Callers must use ReadRange with the value before consulting Current again.
type RuntimeLogRead struct {
	Earliest         uint64
	End              uint64
	Start            uint64
	Finalized        bool
	Complete         bool
	HistoryTruncated bool
	TailTruncated    bool
	SelectedLines    map[uint64]struct{}
	notify           <-chan struct{}
}

// RuntimeLogRecord is the public representation of one durable record.
type RuntimeLogRecord struct {
	Offset        uint64
	NextOffset    uint64
	LineOffset    uint64
	Timestamp     time.Time
	Data          []byte
	LineStart     bool
	LineEnd       bool
	LineTruncated bool
}

// RuntimeLogCurrent describes the live boundary and notification channel.
type RuntimeLogCurrent struct {
	End       uint64
	Earliest  uint64
	Finalized bool
	Complete  bool
	Notify    <-chan struct{}
}

// NewRuntimeLog creates a new durable runtime log directory.
func NewRuntimeLog(directory string, config LogConfig) (*RuntimeLog, error) {
	normalized, err := normalizeLogConfig(config)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(directory), 0o700); err != nil {
		return nil, fmt.Errorf("create runtime log parent: %w", err)
	}
	log, err := newProcessLog(directory, normalized)
	if err != nil {
		return nil, err
	}
	return &RuntimeLog{log: log}, nil
}

// OpenRuntimeLog reopens a durable runtime log after a controller restart.
// The log is deliberately reopened as non-finalized so a new boot can append
// after the previous boot's final record.
func OpenRuntimeLog(directory string, config LogConfig) (*RuntimeLog, error) {
	normalized, err := normalizeLogConfig(config)
	if err != nil {
		return nil, err
	}
	if _, err := os.Lstat(directory); errors.Is(err, os.ErrNotExist) {
		return NewRuntimeLog(directory, normalized)
	} else if err != nil {
		return nil, fmt.Errorf("stat runtime log directory: %w", err)
	}
	log, err := openProcessLog(directory, normalized, false)
	if err != nil {
		return nil, err
	}
	return &RuntimeLog{log: log}, nil
}

// Append appends one or more newline-terminated controller event records.
func (l *RuntimeLog) Append(data []byte) error {
	if l == nil || l.log == nil {
		return errors.New("runtime log is unavailable")
	}
	if len(data) == 0 {
		return nil
	}
	written, err := l.log.write(codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT, data)
	if err != nil {
		return err
	}
	if written != len(data) {
		return io.ErrShortWrite
	}
	return nil
}

// PrepareOffset captures a read beginning at an exact logical offset.
func (l *RuntimeLog) PrepareOffset(offset uint64) (RuntimeLogRead, error) {
	if l == nil || l.log == nil {
		return RuntimeLogRead{}, errors.New("runtime log is unavailable")
	}
	prepared, err := l.log.prepareOffset(offset)
	if err != nil {
		return RuntimeLogRead{}, err
	}
	return runtimeLogReadFromPrepared(prepared), nil
}

// PrepareTail captures the last logical event lines. A zero count starts at
// the current end without replaying history.
func (l *RuntimeLog) PrepareTail(lines uint64) (RuntimeLogRead, error) {
	if l == nil || l.log == nil {
		return RuntimeLogRead{}, errors.New("runtime log is unavailable")
	}
	if lines > MaxRuntimeLogTailLines {
		return RuntimeLogRead{}, fmt.Errorf("tail lines exceeds %d", MaxRuntimeLogTailLines)
	}
	prepared, err := l.log.prepareTail(lines, map[codev1.ProcessLogStream]bool{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: true,
	})
	if err != nil {
		return RuntimeLogRead{}, err
	}
	return runtimeLogReadFromPrepared(prepared), nil
}

func runtimeLogReadFromPrepared(prepared preparedProcessLogRead) RuntimeLogRead {
	return RuntimeLogRead{
		Earliest:         prepared.earliest,
		End:              prepared.end,
		Start:            prepared.start,
		Finalized:        prepared.finalized,
		Complete:         prepared.complete,
		HistoryTruncated: prepared.historyTruncated,
		TailTruncated:    prepared.tailTruncated,
		SelectedLines:    prepared.selectedLines,
		notify:           prepared.notify,
	}
}

// ReadRange reads the records within a previously captured boundary. It never
// holds the writer mutex while invoking send.
func (l *RuntimeLog) ReadRange(ctx context.Context, prepared RuntimeLogRead, send func(RuntimeLogRecord) error) error {
	if l == nil || l.log == nil {
		return errors.New("runtime log is unavailable")
	}
	if send == nil {
		return errors.New("runtime log record callback is required")
	}
	return l.log.readRange(prepared.Start, prepared.End, prepared.Earliest, map[codev1.ProcessLogStream]bool{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: true,
	}, prepared.SelectedLines, func(record storedProcessLogRecord, truncated bool) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		return send(RuntimeLogRecord{
			Offset: record.offset, NextOffset: record.offset + 1, LineOffset: record.lineOffset,
			Timestamp: time.Unix(0, record.timestamp), Data: append([]byte(nil), record.payload...),
			LineStart: record.lineStart(), LineEnd: record.lineEnd(), LineTruncated: truncated,
		})
	})
}

// Current returns a live boundary and notification channel for follow loops.
func (l *RuntimeLog) Current() RuntimeLogCurrent {
	if l == nil || l.log == nil {
		return RuntimeLogCurrent{}
	}
	end, earliest, finalized, complete, notify := l.log.current(0)
	return RuntimeLogCurrent{End: end, Earliest: earliest, Finalized: finalized, Complete: complete, Notify: notify}
}

// AcquireObserver reserves one observer slot and prevents retention from
// deleting segments needed by the caller.
func (l *RuntimeLog) AcquireObserver() error {
	if l == nil || l.log == nil {
		return errors.New("runtime log is unavailable")
	}
	return l.log.acquireObserver()
}

// ReleaseObserver releases a previously acquired observer slot.
func (l *RuntimeLog) ReleaseObserver() {
	if l != nil && l.log != nil {
		l.log.releaseObserver()
	}
}

// Finalize closes the active segment and wakes all follow observers. A later
// OpenRuntimeLog call reopens the store for the next controller boot.
func (l *RuntimeLog) Finalize() error {
	if l == nil || l.log == nil {
		return nil
	}
	return l.log.finalize()
}

// DiskBytes reports the encoded segment and index size.
func (l *RuntimeLog) DiskBytes() int64 {
	if l == nil || l.log == nil {
		return 0
	}
	return l.log.diskBytes()
}

// TrimOldestSegment removes one sealed segment when no observer is using it.
func (l *RuntimeLog) TrimOldestSegment() bool {
	if l == nil || l.log == nil {
		return false
	}
	return l.log.trimOldestSegment()
}

// Expire removes all retained segments after the log has been finalized.
func (l *RuntimeLog) Expire() bool {
	if l == nil || l.log == nil {
		return false
	}
	return l.log.expire()
}

// Notify exposes the notification channel captured by a read boundary.
func (r RuntimeLogRead) Notify() <-chan struct{} { return r.notify }
