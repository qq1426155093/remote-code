package agent

import (
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
)

// turnBufferCapacity bounds the events queued for one turn. The acpClient
// sink must never block, so an overflowing buffer drops the event instead of
// stalling the shared connection; a consumer this far behind has already lost
// the stream. 1024 chunks covers a long turn with room to spare.
const turnBufferCapacity = 1024

// session is one ACP session on the shared agent process. It owns the
// single-active-turn slot and the fan-in point for that session's updates.
type session struct {
	id               acp.SessionId
	workingDirectory string
	generation       uint64
	createdAt        time.Time

	mu             sync.Mutex
	turn           *turn
	lastActivityAt time.Time
}

// turn is the in-flight prompt of a session. incoming carries events pushed by
// the acpClient sink; the turn pump drains it for as long as the prompt is
// unsettled, including after the caller disconnects, because the protocol
// requires post-cancel updates to be consumed and the SDK's notification
// barrier makes the prompt response wait on that consumption.
//
// incoming is never closed: racing dispatchers must be able to send at any
// time without panicking, so a settled turn simply abandons its buffer and
// lets the garbage collector reclaim it.
type turn struct {
	incoming chan Event
	queryID  string
}

type sessionSnapshot struct {
	ID               string
	WorkingDirectory string
	Generation       uint64
	Running          bool
	ActiveQueryID    string
	CreatedAt        time.Time
	LastActivityAt   time.Time
}

func newSession(id acp.SessionId, workingDirectory string, generation uint64, now time.Time) *session {
	return &session{
		id: id, workingDirectory: workingDirectory, generation: generation,
		createdAt: now, lastActivityAt: now,
	}
}

// beginTurn claims the session's single turn slot. The bool reports whether
// the slot was free; the caller attaches the gRPC reason at the transport
// boundary.
func (s *session) beginTurn(now time.Time) (*turn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.turn != nil {
		return nil, false
	}
	s.turn = &turn{incoming: make(chan Event, turnBufferCapacity)}
	s.lastActivityAt = now
	return s.turn, true
}

// bindQuery makes the stable query id visible to session listing once the
// event record has been allocated.
func (s *session) bindQuery(t *turn, queryID string) {
	s.mu.Lock()
	if s.turn == t {
		t.queryID = queryID
	}
	s.mu.Unlock()
}

// endTurn releases the turn slot. Events dispatched after this point land in
// the abandoned buffer and are dropped with it.
func (s *session) endTurn(t *turn, now time.Time) {
	s.mu.Lock()
	if s.turn == t {
		s.turn = nil
		s.lastActivityAt = now
	}
	s.mu.Unlock()
}

// dispatch enqueues an event for the active turn. It returns false when the
// session has no active turn or its buffer is full; the sink contract forbids
// blocking, so both cases drop.
func (s *session) dispatch(event Event) bool {
	s.mu.Lock()
	t := s.turn
	s.mu.Unlock()
	if t == nil {
		return false
	}
	select {
	case t.incoming <- event:
		return true
	default:
		return false
	}
}

// activeTurn returns the current turn, if any, without claiming it. Shutdown
// uses it to find turns that still need cancellation.
func (s *session) activeTurn() *turn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.turn
}

// snapshot returns one race-free view for ListSessions.
func (s *session) snapshot() sessionSnapshot {
	s.mu.Lock()
	defer s.mu.Unlock()
	snapshot := sessionSnapshot{
		ID:               string(s.id),
		WorkingDirectory: s.workingDirectory,
		Generation:       s.generation,
		CreatedAt:        s.createdAt,
		LastActivityAt:   s.lastActivityAt,
	}
	if s.turn != nil {
		snapshot.Running = true
		snapshot.ActiveQueryID = s.turn.queryID
	}
	return snapshot
}
