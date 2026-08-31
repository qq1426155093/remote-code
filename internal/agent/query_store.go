package agent

import (
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"sync"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
)

// EventLogConfig bounds the on-disk store of query event records, mirroring
// the process log knobs: a per-query cap keeps one huge turn from crowding out
// history (head segments are dropped and earliest advances), a total cap and a
// retention window bound the disk footprint across queries.
type EventLogConfig struct {
	MaxBytesPerQuery     int64
	MaxTotalBytes        int64
	SegmentBytes         int64
	RetentionAfterSettle time.Duration
	MaxObservers         int
}

// DefaultEventLogConfig returns the operator-facing defaults.
func DefaultEventLogConfig() EventLogConfig {
	return EventLogConfig{
		MaxBytesPerQuery:     16 << 20,
		MaxTotalBytes:        1 << 30,
		SegmentBytes:         1 << 20,
		RetentionAfterSettle: 7 * 24 * time.Hour,
		MaxObservers:         8,
	}
}

// ValidateEventLogConfig checks knob magnitudes without touching the
// filesystem, mirroring processservice.ValidateLogConfig.
func ValidateEventLogConfig(config EventLogConfig) error {
	if config.MaxBytesPerQuery < 64<<10 {
		return errors.New("agent event max_bytes_per_query must be at least 64KiB")
	}
	if config.MaxTotalBytes < config.MaxBytesPerQuery {
		return errors.New("agent event max_total_bytes must be at least max_bytes_per_query")
	}
	if config.SegmentBytes < 16<<10 || config.SegmentBytes > config.MaxBytesPerQuery {
		return errors.New("agent event segment_bytes must be between 16KiB and max_bytes_per_query")
	}
	if config.RetentionAfterSettle < time.Minute {
		return errors.New("agent event retention_after_settle must be at least one minute")
	}
	if config.MaxObservers < 1 || config.MaxObservers > 64 {
		return errors.New("agent event max_observers must be between 1 and 64")
	}
	return nil
}

// QueryState is the lifecycle state of a query record.
type QueryState string

const (
	QueryStateRunning QueryState = "running"
	QueryStateSettled QueryState = "settled"
	QueryStateLost    QueryState = "lost"
)

// QueryError is the terminal status error of a query that did not settle
// cleanly, persisted so replay can end the stream the way the original query
// stream ended.
type QueryError struct {
	Code    uint32 `json:"code"`
	Reason  string `json:"reason,omitempty"`
	Message string `json:"message"`
}

// Store addressing failures. Callers wrap them with context; the gRPC layer
// maps each to its rpcerror reason.
var (
	ErrQueryNotFound     = errors.New("agent query not found")
	ErrQueryPruned       = errors.New("agent query events pruned by retention")
	ErrQuerySequence     = errors.New("agent query sequence not yet written")
	ErrQueryObservers    = errors.New("agent query observer limit reached")
	ErrQueryObserverLags = errors.New("agent query observer fell behind; re-observe from the last received sequence")
)

// queryStateFile is the persisted record state. next_sequence only matters
// once settled: a running record's window is recovered by scanning segments,
// and a running record found at startup is rewritten as lost.
type queryStateFile struct {
	FormatVersion    int         `json:"format_version"`
	QueryID          string      `json:"query_id"`
	SessionID        string      `json:"session_id"`
	WorkingDirectory string      `json:"working_directory,omitempty"`
	State            string      `json:"state"`
	StopReason       string      `json:"stop_reason,omitempty"`
	Error            *QueryError `json:"error,omitempty"`
	EarliestSequence uint64      `json:"earliest_sequence"`
	NextSequence     uint64      `json:"next_sequence"`
	CreatedAt        time.Time   `json:"created_at"`
	SettledAt        *time.Time  `json:"settled_at,omitempty"`
}

// QuerySnapshot is one instant's view of a query record.
type QuerySnapshot struct {
	ID         string
	SessionID  string
	State      QueryState
	Earliest   uint64
	Next       uint64
	StopReason string
	Err        *QueryError
	SettledAt  time.Time
	CreatedAt  time.Time
}

// QueryMetadata identifies the turn a record belongs to. The prompt itself is
// deliberately not persisted: the replaying client already knows what it sent.
type QueryMetadata struct {
	SessionID        string
	WorkingDirectory string
}

// queryDirectoryName matches the UUID query ids handed to clients.
var queryDirectoryPattern = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

// gcInterval spaces background retention sweeps; RunGC is also invoked on
// every settle, which is where deletions naturally concentrate.
const gcInterval = 10 * time.Minute

// QueryStore owns <runtime-dir>/agent-events/: it recovers records at open
// time, hands out live writers, serves snapshots and disk replay for settled
// or lost queries, and enforces retention.
type QueryStore struct {
	root   string
	config EventLogConfig
	logger *slog.Logger
	now    func() time.Time

	mu       sync.Mutex
	live     map[string]*QueryWriter
	settled  map[string]querySettledEntry
	totalSet int64
	stop     chan struct{}
	stopOnce sync.Once
}

// querySettledEntry tracks a non-live record for retention ordering.
type querySettledEntry struct {
	directory string
	settledAt time.Time
	bytes     int64
}

// OpenQueryStore opens (and creates) the store root and recovers every record
// found there. A record left in the running state belonged to a turn that was
// in flight when this process ended, so it is rewritten as lost — the same
// recovery posture as the process registry, which never re-adopts live
// processes either.
func OpenQueryStore(root string, config EventLogConfig, logger *slog.Logger) (*QueryStore, error) {
	if err := ValidateEventLogConfig(config); err != nil {
		return nil, fmt.Errorf("agent event store: %w", err)
	}
	if logger == nil {
		logger = slog.Default()
	}
	if err := os.MkdirAll(root, 0o700); err != nil {
		return nil, fmt.Errorf("create agent event store: %w", err)
	}
	store := &QueryStore{
		root:    root,
		config:  config,
		logger:  logger,
		now:     time.Now,
		live:    make(map[string]*QueryWriter),
		settled: make(map[string]querySettledEntry),
		stop:    make(chan struct{}),
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		return nil, fmt.Errorf("scan agent event store: %w", err)
	}
	for _, entry := range entries {
		if !entry.IsDir() || !queryDirectoryPattern.MatchString(entry.Name()) {
			continue
		}
		store.recoverDirectory(filepath.Join(root, entry.Name()))
	}
	go store.gcLoop()
	store.RunGC()
	return store, nil
}

// Close stops the retention sweep. Live writers stay owned by their turns,
// which settle them during service shutdown.
func (s *QueryStore) Close() error {
	s.stopOnce.Do(func() { close(s.stop) })
	return nil
}

// recoverDirectory brings one record directory to a servable state.
func (s *QueryStore) recoverDirectory(directory string) {
	id := filepath.Base(directory)
	state, err := readQueryState(directory)
	if err != nil || QueryState(state.State) != QueryStateRunning {
		if err != nil {
			// Corrupt state is salvageable from the segments alone: the
			// window is the truth on disk, only the metadata is lost.
			s.logger.Warn("agent query state unreadable; salvaging from segments", "query_id", id, "err", err.Error())
			state = &queryStateFile{QueryID: id}
		}
		if QueryState(state.State) != QueryStateRunning {
			// Settled records may still carry a crash-truncated tail if the
			// process died mid-settle; trim it so replay stops cleanly.
			_ = truncateQueryTornTails(directory)
			s.rememberSettled(directory, state)
			return
		}
	}
	// Running: the turn belonged to a previous controller life. Mark lost,
	// trimming any torn tail first, and let the scanned window answer replay.
	if err := truncateQueryTornTails(directory); err != nil {
		s.logger.Warn("agent query tail trim failed", "query_id", id, "err", err.Error())
	}
	earliest, next, _ := scanOrZero(directory)
	now := s.now()
	state.State = string(QueryStateLost)
	state.StopReason = ""
	if state.Error == nil {
		state.Error = &QueryError{
			Code:    uint32(codes.Unavailable),
			Reason:  string(rpcerror.AgentProcessLost),
			Message: "the controller restarted while the query was running; its outcome is unknown",
		}
	}
	state.EarliestSequence = earliest
	state.NextSequence = next
	state.SettledAt = &now
	if state.CreatedAt.IsZero() {
		state.CreatedAt = now
	}
	if err := writeQueryState(directory, state); err != nil {
		s.logger.Warn("agent query state rewrite failed", "query_id", id, "err", err.Error())
	}
	s.rememberSettled(directory, state)
}

// scanOrZero scans a record directory, falling back to an empty window.
func scanOrZero(directory string) (earliest, next uint64, bytes int64) {
	earliest, next, bytes, err := scanQueryRecordBounds(directory)
	if err != nil {
		return 0, 0, 0
	}
	return earliest, next, bytes
}

// rememberSettled registers a non-live record for retention accounting.
func (s *QueryStore) rememberSettled(directory string, state *queryStateFile) {
	_, _, bytes := scanOrZero(directory)
	settledAt := s.now()
	if state.SettledAt != nil {
		settledAt = *state.SettledAt
	}
	s.mu.Lock()
	s.settled[state.QueryID] = querySettledEntry{directory: directory, settledAt: settledAt, bytes: bytes}
	s.totalSet += bytes
	s.mu.Unlock()
}

// Begin creates a new running query record and returns its writer.
func (s *QueryStore) Begin(metadata QueryMetadata) (string, *QueryWriter, error) {
	s.mu.Lock()
	closed := s.isClosedLocked()
	s.mu.Unlock()
	if closed {
		return "", nil, errors.New("agent event store is closed")
	}
	id, err := newQueryUUID()
	if err != nil {
		return "", nil, fmt.Errorf("allocate query id: %w", err)
	}
	directory := filepath.Join(s.root, id)
	if err := os.Mkdir(directory, 0o700); err != nil {
		return "", nil, fmt.Errorf("create query record: %w", err)
	}
	state := &queryStateFile{
		FormatVersion:    queryEventFormat,
		QueryID:          id,
		SessionID:        metadata.SessionID,
		WorkingDirectory: metadata.WorkingDirectory,
		State:            string(QueryStateRunning),
		CreatedAt:        s.now(),
	}
	if err := writeQueryState(directory, state); err != nil {
		_ = os.RemoveAll(directory)
		return "", nil, err
	}
	writer, err := newQueryWriter(s, id, directory, state)
	if err != nil {
		_ = os.RemoveAll(directory)
		return "", nil, err
	}
	s.mu.Lock()
	s.live[id] = writer
	s.mu.Unlock()
	return id, writer, nil
}

// Stat reports a query's snapshot: live writers first, then disk.
func (s *QueryStore) Stat(id string) (QuerySnapshot, bool) {
	if !queryDirectoryPattern.MatchString(id) {
		return QuerySnapshot{}, false
	}
	s.mu.Lock()
	writer := s.live[id]
	s.mu.Unlock()
	if writer != nil {
		return writer.Snapshot(), true
	}
	state, err := readQueryState(filepath.Join(s.root, id))
	if err != nil {
		return QuerySnapshot{}, false
	}
	return snapshotOf(state), true
}

// Attach pins a query's window for observation. A non-nil subscription means
// the query is live: frames before the pinned next come from ReadFrames, later
// ones from the subscription. A nil subscription means the record is settled
// (or unknown), and the snapshot path serves it from disk alone. The two
// outcomes race only at settle time, where the writer's mutex keeps the pinned
// snapshot consistent either way.
func (s *QueryStore) Attach(id string, from uint64) (QuerySnapshot, *QuerySubscription, error) {
	if !queryDirectoryPattern.MatchString(id) {
		return QuerySnapshot{}, nil, fmt.Errorf("query %q: %w", id, ErrQueryNotFound)
	}
	s.mu.Lock()
	writer := s.live[id]
	s.mu.Unlock()
	if writer == nil {
		// Not live: serve whatever the disk state says, if anything.
		state, err := readQueryState(filepath.Join(s.root, id))
		if err != nil {
			return QuerySnapshot{}, nil, fmt.Errorf("query %q: %w", id, ErrQueryNotFound)
		}
		if state.State == string(QueryStateRunning) {
			// A running record with no writer belongs to an earlier controller
			// life that recovery already rewrote as lost; re-read cannot race
			// Begin, which only creates fresh directories.
			return QuerySnapshot{}, nil, fmt.Errorf("query %q: %w", id, ErrQueryNotFound)
		}
		return snapshotOf(state), nil, nil
	}
	return writer.Attach(from)
}

// ReadFrames streams the retained frames [from, to) of a query from disk. The
// window bounds come from the caller (Stat or a live Attach), so this never
// blocks on a running turn's writer.
func (s *QueryStore) ReadFrames(id string, from, to uint64, send func(frame *codev1.QueryResponse) error) error {
	if !queryDirectoryPattern.MatchString(id) {
		return fmt.Errorf("query %q: %w", id, ErrQueryNotFound)
	}
	if _, err := os.Stat(filepath.Join(s.root, id)); err != nil {
		return fmt.Errorf("query %q: %w", id, ErrQueryNotFound)
	}
	return replayQuerySegments(filepath.Join(s.root, id), from, to, send)
}

// gcLoop drives periodic retention sweeps until Close.
func (s *QueryStore) gcLoop() {
	ticker := time.NewTicker(gcInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.RunGC()
		case <-s.stop:
			return
		}
	}
}

// RunGC deletes settled records past their retention window and the oldest
// settled records while the total exceeds the cap. Live queries are exempt:
// their only bound is the per-query cap enforced by the writer.
func (s *QueryStore) RunGC() {
	now := s.now()
	type candidate struct {
		id        string
		directory string
		settledAt time.Time
		bytes     int64
	}
	s.mu.Lock()
	var candidates []candidate
	total := s.totalSet
	for id, entry := range s.settled {
		candidates = append(candidates, candidate{id: id, directory: entry.directory, settledAt: entry.settledAt, bytes: entry.bytes})
	}
	s.mu.Unlock()
	sort.Slice(candidates, func(i, j int) bool { return candidates[i].settledAt.Before(candidates[j].settledAt) })
	removed := make([]string, 0, len(candidates))
	for _, entry := range candidates {
		if s.config.RetentionAfterSettle > 0 && now.Sub(entry.settledAt) >= s.config.RetentionAfterSettle {
			removed = append(removed, entry.id)
			total -= entry.bytes
		}
	}
	for _, entry := range candidates {
		if total <= s.config.MaxTotalBytes {
			break
		}
		if containsString(removed, entry.id) {
			continue
		}
		removed = append(removed, entry.id)
		total -= entry.bytes
	}
	for _, id := range removed {
		s.mu.Lock()
		entry := s.settled[id]
		delete(s.settled, id)
		s.mu.Unlock()
		if err := os.RemoveAll(entry.directory); err != nil {
			s.logger.Warn("agent query record removal failed", "query_id", id, "err", err.Error())
		}
	}
}

// settled migrates a finished writer's record out of the live table.
func (s *QueryStore) settleRecord(writer *QueryWriter, state *queryStateFile, bytes int64) {
	s.mu.Lock()
	if s.live[writer.id] == writer {
		delete(s.live, writer.id)
	}
	entry := querySettledEntry{directory: writer.directory, settledAt: s.now(), bytes: bytes}
	if state.SettledAt != nil {
		entry.settledAt = *state.SettledAt
	}
	s.settled[writer.id] = entry
	s.totalSet += bytes
	s.mu.Unlock()
}

func (s *QueryStore) isClosedLocked() bool {
	select {
	case <-s.stop:
		return true
	default:
		return false
	}
}

func containsString(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

// newQueryUUID allocates a v4 UUID, formatted like the process ids clients
// already handle.
func newQueryUUID() (string, error) {
	value := make([]byte, 16)
	if _, err := rand.Read(value); err != nil {
		return "", err
	}
	value[6] = value[6]&0x0f | 0x40
	value[8] = value[8]&0x3f | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

// snapshotOf projects the persisted state into the public snapshot.
func snapshotOf(state *queryStateFile) QuerySnapshot {
	snapshot := QuerySnapshot{
		ID:         state.QueryID,
		SessionID:  state.SessionID,
		State:      QueryState(state.State),
		Earliest:   state.EarliestSequence,
		Next:       state.NextSequence,
		StopReason: state.StopReason,
		Err:        state.Error,
		CreatedAt:  state.CreatedAt,
	}
	if state.SettledAt != nil {
		snapshot.SettledAt = *state.SettledAt
	}
	return snapshot
}

// readQueryState loads and sanity-checks one record's state file.
func readQueryState(directory string) (*queryStateFile, error) {
	path := filepath.Join(directory, queryStateFileName)
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("query state is not a regular file")
	}
	if info.Size() <= 0 || info.Size() > 1<<20 {
		return nil, errors.New("query state size is invalid")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	state := &queryStateFile{}
	if err := json.Unmarshal(data, state); err != nil {
		return nil, err
	}
	if state.QueryID == "" {
		return nil, errors.New("query state lacks a query id")
	}
	return state, nil
}

// writeQueryState persists the record state atomically: same-directory temp
// file, fsync, rename — the upload rename pattern, sized down.
func writeQueryState(directory string, state *queryStateFile) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return fmt.Errorf("encode query state: %w", err)
	}
	data = append(data, '\n')
	path := filepath.Join(directory, queryStateFileName)
	temporary := path + ".tmp"
	file, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("create query state: %w", err)
	}
	if _, err := file.Write(data); err != nil {
		_ = file.Close()
		return fmt.Errorf("write query state: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		return fmt.Errorf("sync query state: %w", err)
	}
	if err := file.Close(); err != nil {
		return fmt.Errorf("close query state: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		return fmt.Errorf("replace query state: %w", err)
	}
	return nil
}
