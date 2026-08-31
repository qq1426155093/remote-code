package agent

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// newTestStore opens a query store over a fresh temporary directory with the
// given configuration, applying test defaults for zero fields.
func newTestStore(t *testing.T, config EventLogConfig) *QueryStore {
	t.Helper()
	config.applyTestDefaults()
	store, err := OpenQueryStore(t.TempDir(), config, testLogger(t))
	if err != nil {
		t.Fatalf("open query store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

// applyTestDefaults keeps individual tests focused on the knob they exercise.
func (c *EventLogConfig) applyTestDefaults() {
	if c.MaxBytesPerQuery == 0 {
		c.MaxBytesPerQuery = 1 << 20
	}
	if c.MaxTotalBytes == 0 {
		c.MaxTotalBytes = 64 << 20
	}
	if c.SegmentBytes == 0 {
		c.SegmentBytes = 1 << 20
	}
	if c.RetentionAfterSettle == 0 {
		c.RetentionAfterSettle = 24 * 3600e9 // 24h in ns
	}
	if c.MaxObservers == 0 {
		c.MaxObservers = 4
	}
}

// queryFrame builds one message frame carrying its sequence, the way the turn
// pump does before handing it to the store.
func queryFrame(queryID string, sequence uint64, text string) *codev1.QueryResponse {
	return &codev1.QueryResponse{
		QueryId:  queryID,
		Sequence: sequence,
		Event: &codev1.QueryResponse_Message{
			Message: &codev1.AgentMessage{Text: text},
		},
	}
}

// appendFrames writes texts as sequenced frames from zero.
func appendFrames(t *testing.T, writer *QueryWriter, queryID string, texts ...string) {
	t.Helper()
	for index, text := range texts {
		if err := writer.Append(queryFrame(queryID, uint64(index), text)); err != nil {
			t.Fatalf("append frame %d: %v", index, err)
		}
	}
}

// replayTexts drains [from, to) from disk and returns the frame texts.
func replayTexts(t *testing.T, store *QueryStore, queryID string, from, to uint64) []string {
	t.Helper()
	var texts []string
	err := store.ReadFrames(queryID, from, to, func(frame *codev1.QueryResponse) error {
		if frame.GetQueryId() != queryID {
			t.Fatalf("frame carries query id %q, want %q", frame.GetQueryId(), queryID)
		}
		if want := from + uint64(len(texts)); frame.GetSequence() != want {
			t.Fatalf("frame sequence %d, want %d", frame.GetSequence(), want)
		}
		texts = append(texts, frame.GetMessage().GetText())
		return nil
	})
	if err != nil {
		t.Fatalf("read frames: %v", err)
	}
	return texts
}

func TestQueryStore_AppendSettleReplayAfterReopen(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1", WorkingDirectory: "sub"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	if queryID == "" {
		t.Fatal("begin returned an empty query id")
	}
	appendFrames(t, writer, queryID, "alpha", "beta", "gamma")
	if err := writer.Settle("end_turn"); err != nil {
		t.Fatalf("settle: %v", err)
	}

	// A fresh store over the same root must serve the settled query from disk.
	reopened, err := OpenQueryStore(store.root, store.config, testLogger(t))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	snapshot, ok := reopened.Stat(queryID)
	if !ok {
		t.Fatal("settled query not found after reopen")
	}
	if snapshot.State != QueryStateSettled {
		t.Fatalf("state %q, want settled", snapshot.State)
	}
	if snapshot.StopReason != "end_turn" {
		t.Fatalf("stop reason %q, want end_turn", snapshot.StopReason)
	}
	if snapshot.SessionID != "session-1" {
		t.Fatalf("session id %q, want session-1", snapshot.SessionID)
	}
	if snapshot.Next != 3 || snapshot.Earliest != 0 {
		t.Fatalf("window [%d,%d), want [0,3)", snapshot.Earliest, snapshot.Next)
	}
	if got := replayTexts(t, reopened, queryID, 0, snapshot.Next); len(got) != 3 || got[2] != "gamma" {
		t.Fatalf("replayed texts %v, want three frames ending gamma", got)
	}
}

func TestQueryStore_ReplayFromMidSequence(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	appendFrames(t, writer, queryID, "one", "two", "three", "four")
	if err := writer.Settle("end_turn"); err != nil {
		t.Fatalf("settle: %v", err)
	}
	if got := replayTexts(t, store, queryID, 2, 4); len(got) != 2 || got[0] != "three" || got[1] != "four" {
		t.Fatalf("texts %v, want [three four]", got)
	}
	if got := replayTexts(t, store, queryID, 4, 4); len(got) != 0 {
		t.Fatalf("empty window returned %v", got)
	}
}

func TestQueryStore_StatUnknownQuery(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	if _, ok := store.Stat("missing"); ok {
		t.Fatal("unknown query reported as found")
	}
	err := store.ReadFrames("missing", 0, 1, func(*codev1.QueryResponse) error { return nil })
	if !errors.Is(err, ErrQueryNotFound) {
		t.Fatalf("read error %v, want ErrQueryNotFound", err)
	}
}

func TestQueryStore_RunningQueryMarkedLostOnReopen(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	appendFrames(t, writer, queryID, "partial-one", "partial-two")
	// No settle: the process "crashed" with the turn in flight.

	reopened, err := OpenQueryStore(store.root, store.config, testLogger(t))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()

	snapshot, ok := reopened.Stat(queryID)
	if !ok {
		t.Fatal("running query not recovered from disk")
	}
	if snapshot.State != QueryStateLost {
		t.Fatalf("state %q, want lost", snapshot.State)
	}
	if snapshot.Err == nil || rpcerror.ReasonOf(snapshot.Err.status()) != rpcerror.AgentProcessLost {
		t.Fatalf("recovered error %v, want AGENT_PROCESS_LOST", snapshot.Err)
	}
	if snapshot.Next != 2 {
		t.Fatalf("next %d, want 2 (events before the crash replay)", snapshot.Next)
	}
	if got := replayTexts(t, reopened, queryID, 0, 2); len(got) != 2 || got[1] != "partial-two" {
		t.Fatalf("texts %v, want both pre-crash frames", got)
	}
}

// testLogger returns a quiet logger; store failures surface through returns.
func testLogger(t *testing.T) *slog.Logger {
	t.Helper()
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestQueryStore_HeadTruncationAdvancesEarliest(t *testing.T) {
	store := newTestStore(t, EventLogConfig{SegmentBytes: 64 << 10, MaxBytesPerQuery: 64 << 10})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	big := strings.Repeat("x", 8<<10)
	for sequence := 0; sequence < 24; sequence++ {
		if err := writer.Append(queryFrame(queryID, uint64(sequence), big)); err != nil {
			t.Fatalf("append frame %d: %v", sequence, err)
		}
	}
	snapshot, ok := store.Stat(queryID)
	if !ok {
		t.Fatal("live query not found")
	}
	if snapshot.Earliest == 0 {
		t.Fatal("per-query cap did not drop any head segment")
	}
	if snapshot.Next != 24 {
		t.Fatalf("next %d, want 24 (truncation never drops the newest records)", snapshot.Next)
	}
	err = store.ReadFrames(queryID, 0, 1, func(*codev1.QueryResponse) error { return nil })
	if !errors.Is(err, ErrQueryPruned) {
		t.Fatalf("replay from zero error %v, want ErrQueryPruned", err)
	}
	// The retained window still replays to the newest frame.
	var last uint64
	err = store.ReadFrames(queryID, snapshot.Earliest, snapshot.Next, func(frame *codev1.QueryResponse) error {
		last = frame.GetSequence()
		return nil
	})
	if err != nil {
		t.Fatalf("replay retained window: %v", err)
	}
	if last != 23 {
		t.Fatalf("last replayed sequence %d, want 23", last)
	}
}

func TestQueryStore_TornTailTruncatedOnReopen(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	appendFrames(t, writer, queryID, "one", "two")
	// Simulate a crash mid-record: raw partial bytes after the valid frames.
	segment := filepath.Join(store.root, queryID, querySegmentName(0))
	file, err := os.OpenFile(segment, os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		t.Fatalf("open segment: %v", err)
	}
	if _, err := file.Write([]byte{0, 0}); err != nil {
		t.Fatalf("write torn tail: %v", err)
	}
	_ = file.Close()

	reopened, err := OpenQueryStore(store.root, store.config, testLogger(t))
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	defer func() { _ = reopened.Close() }()
	snapshot, ok := reopened.Stat(queryID)
	if !ok || snapshot.State != QueryStateLost {
		t.Fatalf("snapshot %+v, want lost", snapshot)
	}
	if snapshot.Next != 2 {
		t.Fatalf("next %d, want 2 (torn tail dropped)", snapshot.Next)
	}
}

func TestQueryStore_RetentionDeletesExpiredSettledQueries(t *testing.T) {
	store := newTestStore(t, EventLogConfig{RetentionAfterSettle: time.Hour})
	current := time.Now()
	store.now = func() time.Time { return current }

	oldID, oldWriter, err := store.Begin(QueryMetadata{SessionID: "old"})
	if err != nil {
		t.Fatalf("begin old: %v", err)
	}
	appendFrames(t, oldWriter, oldID, "old-event")
	if err := oldWriter.Settle("end_turn"); err != nil {
		t.Fatalf("settle old: %v", err)
	}

	// A running query must never be collected.
	liveID, liveWriter, err := store.Begin(QueryMetadata{SessionID: "live"})
	if err != nil {
		t.Fatalf("begin live: %v", err)
	}
	appendFrames(t, liveWriter, liveID, "live-event")

	current = current.Add(2 * time.Hour)
	store.RunGC()
	if _, ok := store.Stat(oldID); ok {
		t.Fatal("expired settled query survived retention")
	}
	if _, ok := store.Stat(liveID); !ok {
		t.Fatal("retention collected a running query")
	}
}

func TestQueryStore_TotalCapDeletesOldestSettledFirst(t *testing.T) {
	store := newTestStore(t, EventLogConfig{SegmentBytes: 64 << 10, MaxBytesPerQuery: 64 << 10, MaxTotalBytes: 96 << 10})
	current := time.Now()
	store.now = func() time.Time { return current }

	settle := func(session string, frames int) string {
		queryID, writer, err := store.Begin(QueryMetadata{SessionID: session})
		if err != nil {
			t.Fatalf("begin %s: %v", session, err)
		}
		big := strings.Repeat("y", 8<<10)
		for sequence := 0; sequence < frames; sequence++ {
			if err := writer.Append(queryFrame(queryID, uint64(sequence), big)); err != nil {
				t.Fatalf("append: %v", err)
			}
		}
		if err := writer.Settle("end_turn"); err != nil {
			t.Fatalf("settle %s: %v", session, err)
		}
		return queryID
	}
	oldID := settle("old", 8)
	current = current.Add(time.Minute)
	newID := settle("new", 8)

	store.RunGC()
	if _, ok := store.Stat(oldID); ok {
		t.Fatal("oldest settled query survived the total-bytes cap")
	}
	if _, ok := store.Stat(newID); !ok {
		t.Fatal("newest settled query was collected")
	}
}

func TestQueryWriter_AttachFollowsLiveAppends(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	appendFrames(t, writer, queryID, "one", "two")

	snapshot, subscription, err := writer.Attach(1)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	if subscription == nil {
		t.Fatal("attach to a running query returned no subscription")
	}
	if snapshot.Next != 2 {
		t.Fatalf("snapshot next %d, want 2 (pinned at attach)", snapshot.Next)
	}
	// The disk part [1, 2) replays before the live channel takes over.
	disk := replayTexts(t, store, queryID, 1, snapshot.Next)
	if len(disk) != 1 || disk[0] != "two" {
		t.Fatalf("disk part %v, want [two]", disk)
	}

	if err := writer.Append(queryFrame(queryID, 2, "three")); err != nil {
		t.Fatalf("append live: %v", err)
	}
	if err := writer.Append(queryFrame(queryID, 3, "four")); err != nil {
		t.Fatalf("append live: %v", err)
	}
	if err := writer.Settle("end_turn"); err != nil {
		t.Fatalf("settle: %v", err)
	}
	seen := 0
	for frame := range subscription.Frames() {
		if want := uint64(2 + seen); frame.GetSequence() != want {
			t.Fatalf("live frame sequence %d, want %d", frame.GetSequence(), want)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("received %d live frames, want 2", seen)
	}
	if err := <-subscription.Done(); err != nil {
		t.Fatalf("subscription done: %v", err)
	}
}

func TestQueryWriter_AttachValidatesWindow(t *testing.T) {
	store := newTestStore(t, EventLogConfig{SegmentBytes: 64 << 10, MaxBytesPerQuery: 64 << 10})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	appendFrames(t, writer, queryID, "one")
	if _, _, err := writer.Attach(2); !errors.Is(err, ErrQuerySequence) {
		t.Fatalf("attach beyond next error %v, want ErrQuerySequence", err)
	}
	big := strings.Repeat("z", 8<<10)
	for sequence := 1; sequence < 24; sequence++ {
		if err := writer.Append(queryFrame(queryID, uint64(sequence), big)); err != nil {
			t.Fatalf("append: %v", err)
		}
	}
	if _, _, err := writer.Attach(0); !errors.Is(err, ErrQueryPruned) {
		t.Fatalf("attach below earliest error %v, want ErrQueryPruned", err)
	}
}

func TestQueryWriter_SlowObserverIsKicked(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	queryID, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	_, subscription, err := writer.Attach(0)
	if err != nil {
		t.Fatalf("attach: %v", err)
	}
	// Never read from the subscription while the writer pushes past the
	// observer buffer: the writer must kick instead of blocking.
	for sequence := uint64(0); sequence < queryObserverBuffer+8; sequence++ {
		if err := writer.Append(queryFrame(queryID, sequence, "x")); err != nil {
			t.Fatalf("append %d: %v", sequence, err)
		}
	}
	select {
	case err := <-subscription.Done():
		if !errors.Is(err, ErrQueryObserverLags) {
			t.Fatalf("done error %v, want ErrQueryObserverLags", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("slow observer was never kicked")
	}
}

func TestQueryWriter_ObserverLimit(t *testing.T) {
	store := newTestStore(t, EventLogConfig{MaxObservers: 1})
	_, writer, err := store.Begin(QueryMetadata{SessionID: "session-1"})
	if err != nil {
		t.Fatalf("begin query: %v", err)
	}
	if _, first, err := writer.Attach(0); err != nil || first == nil {
		t.Fatalf("first attach: %v", err)
	}
	if _, _, err := writer.Attach(0); !errors.Is(err, ErrQueryObservers) {
		t.Fatalf("second attach error %v, want ErrQueryObservers", err)
	}
}

func TestQueryWriter_FailAndLostPersistTerminalError(t *testing.T) {
	store := newTestStore(t, EventLogConfig{})
	terminal := rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process exited during the turn")

	failID, failWriter, err := store.Begin(QueryMetadata{SessionID: "fail"})
	if err != nil {
		t.Fatalf("begin fail: %v", err)
	}
	appendFrames(t, failWriter, failID, "before-failure")
	if err := failWriter.Fail(terminal); err != nil {
		t.Fatalf("fail: %v", err)
	}

	lostID, lostWriter, err := store.Begin(QueryMetadata{SessionID: "lost"})
	if err != nil {
		t.Fatalf("begin lost: %v", err)
	}
	appendFrames(t, lostWriter, lostID, "before-crash")
	if err := lostWriter.Lost(terminal); err != nil {
		t.Fatalf("lost: %v", err)
	}

	failSnapshot, _ := store.Stat(failID)
	if failSnapshot.State != QueryStateSettled || failSnapshot.Err == nil {
		t.Fatalf("fail snapshot %+v, want settled with error", failSnapshot)
	}
	lostSnapshot, _ := store.Stat(lostID)
	if lostSnapshot.State != QueryStateLost || lostSnapshot.Err == nil {
		t.Fatalf("lost snapshot %+v, want lost with error", lostSnapshot)
	}
	rebuilt := lostSnapshot.Err.status()
	if rpcerror.ReasonOf(rebuilt) != rpcerror.AgentProcessLost || status.Code(rebuilt) != codes.Unavailable {
		t.Fatalf("rebuilt error %v, want Unavailable AGENT_PROCESS_LOST", rebuilt)
	}

	// Settling twice keeps the first terminal state.
	if err := failWriter.Settle("end_turn"); err != nil {
		t.Fatalf("second settle: %v", err)
	}
	again, _ := store.Stat(failID)
	if again.StopReason != "" || again.Err == nil {
		t.Fatalf("second settle overrode the terminal state: %+v", again)
	}
}
