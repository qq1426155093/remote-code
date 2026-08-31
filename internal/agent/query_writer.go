package agent

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// queryObserverBuffer bounds one observer's queued frames. The writer must
// never block on a consumer, so an observer this far behind is kicked with
// ErrQueryObserverLags and re-observes from its last received sequence —
// replay makes slow consumers recoverable instead of fatal.
const queryObserverBuffer = 1024

// QueryWriter appends one running turn's frames to its record directory and
// fans them out to attached observers. All mutation happens under mu in the
// turn pump's goroutine; Attach may be called from any goroutine.
type QueryWriter struct {
	store     *QueryStore
	id        string
	directory string

	mu           sync.Mutex
	state        *queryStateFile
	segments     []querySegmentInfo
	totalBytes   int64
	file         *os.File
	buffered     *bufio.Writer
	subscribers  map[uint64]*QuerySubscription
	subscriberID uint64
	finished     bool
}

// QuerySubscription delivers a live query's frames in order. Frames closes
// after the final frame; Done then yields exactly one value: nil when the
// query settled, or the reason the subscription ended early.
type QuerySubscription struct {
	frames chan *codev1.QueryResponse
	done   chan error
}

// Frames returns the ordered frame channel; it is closed after the last frame.
func (s *QuerySubscription) Frames() <-chan *codev1.QueryResponse { return s.frames }

// Done reports the subscription's terminal condition once Frames is closed.
func (s *QuerySubscription) Done() <-chan error { return s.done }

// end delivers the terminal condition and closes the frame channel. Called
// with the writer's mutex held, after every frame has been enqueued.
func (s *QuerySubscription) end(err error) {
	s.done <- err
	close(s.frames)
}

// newQueryWriter opens the record's first segment.
func newQueryWriter(store *QueryStore, id, directory string, state *queryStateFile) (*QueryWriter, error) {
	writer := &QueryWriter{
		store:       store,
		id:          id,
		directory:   directory,
		state:       state,
		subscribers: make(map[uint64]*QuerySubscription),
	}
	if err := writer.openSegmentLocked(0); err != nil {
		return nil, err
	}
	return writer, nil
}

// openSegmentLocked creates the segment that starts at base.
func (w *QueryWriter) openSegmentLocked(base uint64) error {
	name := querySegmentName(base)
	file, err := os.OpenFile(filepath.Join(w.directory, name), os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("create query segment: %w", err)
	}
	w.file = file
	w.buffered = bufio.NewWriter(file)
	w.segments = append(w.segments, querySegmentInfo{base: base, name: name})
	return nil
}

// Append persists one frame and forwards it to every observer. The frame must
// carry the query id and the record's next sequence, which the turn pump
// guarantees. An I/O failure returns without advancing the record: replay
// then serves the frames before the failure, and the turn keeps streaming.
func (w *QueryWriter) Append(frame *codev1.QueryResponse) error {
	payload, err := proto.Marshal(frame)
	if err != nil {
		return fmt.Errorf("encode query frame: %w", err)
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.finished {
		return errors.New("append to finished query record")
	}
	if frame.GetSequence() != w.state.NextSequence {
		return fmt.Errorf("query frame sequence %d, want %d", frame.GetSequence(), w.state.NextSequence)
	}
	current := &w.segments[len(w.segments)-1]
	if current.bytes > 0 && current.bytes >= w.store.config.SegmentBytes {
		if err := w.rotateLocked(); err != nil {
			return err
		}
		current = &w.segments[len(w.segments)-1]
	}
	written, err := writeQueryRecord(w.buffered, payload)
	if err != nil {
		return err
	}
	// Flush every frame so replay readers and crash recovery see complete
	// records on disk; durability (fsync) waits for rotation and settle.
	if err := w.buffered.Flush(); err != nil {
		return fmt.Errorf("flush query frame: %w", err)
	}
	current.bytes += written
	w.totalBytes += written
	w.state.NextSequence++
	w.enforceCapLocked()
	w.publishLocked(frame)
	return nil
}

// rotateLocked flushes and closes the current segment and opens the next one
// at the record's next sequence.
func (w *QueryWriter) rotateLocked() error {
	if err := w.buffered.Flush(); err != nil {
		return fmt.Errorf("flush query segment: %w", err)
	}
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sync query segment: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("close query segment: %w", err)
	}
	return w.openSegmentLocked(w.state.NextSequence)
}

// enforceCapLocked drops whole head segments while the record exceeds its
// byte cap. The segment holding the newest record is never dropped, so a
// single huge frame can exceed the cap but cannot erase the present.
func (w *QueryWriter) enforceCapLocked() {
	dropped := false
	for w.totalBytes > w.store.config.MaxBytesPerQuery && len(w.segments) > 1 {
		oldest := w.segments[0]
		if err := os.Remove(filepath.Join(w.directory, oldest.name)); err != nil && !os.IsNotExist(err) {
			return
		}
		w.segments = w.segments[1:]
		w.totalBytes -= oldest.bytes
		w.state.EarliestSequence = w.segments[0].base
		dropped = true
	}
	if dropped {
		if err := writeQueryState(w.directory, w.state); err != nil {
			w.store.logger.Warn("query state update after retention failed", "query_id", w.id, "err", err.Error())
		}
	}
}

// publishLocked forwards one frame to the attached observers, kicking any that
// cannot keep up.
func (w *QueryWriter) publishLocked(frame *codev1.QueryResponse) {
	for key, subscriber := range w.subscribers {
		select {
		case subscriber.frames <- frame:
		default:
			delete(w.subscribers, key)
			subscriber.end(ErrQueryObserverLags)
		}
	}
}

// Snapshot reports the record's current window and state.
func (w *QueryWriter) Snapshot() QuerySnapshot {
	w.mu.Lock()
	defer w.mu.Unlock()
	return snapshotOf(w.state)
}

// Attach validates a replay start against the live record and registers an
// observer. The returned snapshot pins next: frames before it are read from
// disk (immutable once pinned), later frames arrive on the subscription. A
// nil subscription means the query finished in between — serve it from disk.
func (w *QueryWriter) Attach(from uint64) (QuerySnapshot, *QuerySubscription, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if from < w.state.EarliestSequence {
		return QuerySnapshot{}, nil, fmt.Errorf("query %s from %d: %w", w.id, from, ErrQueryPruned)
	}
	if from > w.state.NextSequence {
		return QuerySnapshot{}, nil, fmt.Errorf("query %s from %d: %w", w.id, from, ErrQuerySequence)
	}
	snapshot := snapshotOf(w.state)
	if w.finished || snapshot.State != QueryStateRunning {
		return snapshot, nil, nil
	}
	if len(w.subscribers) >= w.store.config.MaxObservers {
		return QuerySnapshot{}, nil, fmt.Errorf("query %s: %w", w.id, ErrQueryObservers)
	}
	w.subscriberID++
	subscription := &QuerySubscription{
		frames: make(chan *codev1.QueryResponse, queryObserverBuffer),
		done:   make(chan error, 1),
	}
	w.subscribers[w.subscriberID] = subscription
	return snapshot, subscription, nil
}

// Settle records a clean terminal state; stop_reason carries the ACP value
// (end_turn, cancelled, ...).
func (w *QueryWriter) Settle(stopReason string) error {
	return w.finish(QueryStateSettled, stopReason, nil)
}

// Fail records a terminal error for a turn that ended without settling
// cleanly (for example an agent-rejected prompt).
func (w *QueryWriter) Fail(err error) error {
	return w.finish(QueryStateSettled, "", queryErrorOf(err))
}

// Lost records a turn that was in flight when its agent process or the
// controller went away.
func (w *QueryWriter) Lost(err error) error {
	return w.finish(QueryStateLost, "", queryErrorOf(err))
}

// finish is idempotent: the first caller's terminal state wins.
func (w *QueryWriter) finish(state QueryState, stopReason string, terminal *QueryError) error {
	w.mu.Lock()
	if w.finished {
		w.mu.Unlock()
		return nil
	}
	w.finished = true
	_ = w.buffered.Flush()
	_ = w.file.Sync()
	_ = w.file.Close()
	w.state.State = string(state)
	w.state.StopReason = stopReason
	w.state.Error = terminal
	now := w.store.now()
	w.state.SettledAt = &now
	if err := writeQueryState(w.directory, w.state); err != nil {
		w.mu.Unlock()
		return err
	}
	subscribers := make([]*QuerySubscription, 0, len(w.subscribers))
	for _, subscriber := range w.subscribers {
		subscribers = append(subscribers, subscriber)
	}
	w.subscribers = nil
	totalBytes := w.totalBytes
	stateFile := w.state
	w.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber.end(nil)
	}
	w.store.settleRecord(w, stateFile, totalBytes)
	return nil
}

// queryErrorOf flattens a status error into its persisted form.
func queryErrorOf(err error) *QueryError {
	if err == nil {
		return nil
	}
	return &QueryError{
		Code:    uint32(status.Code(err)),
		Reason:  string(rpcerror.ReasonOf(err)),
		Message: err.Error(),
	}
}

// queryErrorStatus rebuilds the stream-ending error from its persisted form.
func (e *QueryError) status() error {
	if e == nil {
		return nil
	}
	code := codes.Code(e.Code)
	if e.Reason != "" {
		return rpcerror.Errorf(code, rpcerror.Reason(e.Reason), "%s", e.Message)
	}
	return status.Error(code, e.Message)
}
