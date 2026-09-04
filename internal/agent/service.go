// Package agent bridges the gRPC AgentService to an ACP agent child process
// (claude-agent-acp). One shared child process carries every session; the
// bridge speaks the ACP client role through coder/acp-go-sdk over the
// registry's raw-pipe process entry, so the child keeps normal process
// observability while its protocol stdout never touches disk.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"

	"github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// agentProcessName is the registry name of the shared agent child.
const agentProcessName = "agent"

// streamBufferCapacity bounds how many frames one TurnStream may queue for its
// consumer; beyond it, forwarding to that consumer fails and the turn detaches.
const streamBufferCapacity = 16

// Config wires the agent bridge. Command/Arguments/Environment form the agent
// child command line; Environment is the operator baseline that every query's
// overrides merge over.
type Config struct {
	Enabled     bool
	Command     string
	Arguments   []string
	Environment map[string]string
	// WorkspaceRoot is the absolute workspace root every session cwd is
	// confined to.
	WorkspaceRoot string
	// RuntimeDirectory hosts the query event store at <it>/agent-events. When
	// empty, turns stream as before but nothing is retained for replay.
	RuntimeDirectory string
	// Events bounds the query event store; a zero value adopts the defaults.
	Events EventLogConfig
	// Processes is the process registry the agent child is started through.
	// Required in production; unit tests inject Dial instead.
	Processes *process.Service
	// Dial overrides process spawning. Production leaves it nil and gets
	// dialProcess.
	Dial dialFunc
	// Logger receives bridge diagnostics. Prompt and message content is never
	// logged.
	Logger *slog.Logger
}

// replayEnabled reports whether query events are persisted for replay.
func (c Config) replayEnabled() bool {
	return c.Enabled && c.RuntimeDirectory != ""
}

// queryStoreDirectoryName names the store inside the runtime directory,
// following the file-transfers precedent of named non-process stores.
const queryStoreDirectoryName = "agent-events"

// Service is the agent bridge core: it owns the shared agent process, the
// session table, and turn execution. The gRPC surface is a thin adapter over
// StartTurn/ObserveQuery/CancelQuery/CloseSession/Shutdown.
type Service struct {
	config    Config
	logger    *slog.Logger
	processes *process.Service

	process *agentProcess
	// queries persists turn events for replay; nil when replay is disabled.
	queries *QueryStore
	now     func() time.Time

	mu          sync.Mutex
	sessions    map[string]*session
	liveQueries map[string]*agentQuery
	// agentNames caches the personas the generation numbered below offered in
	// its sessions' config options; nil until that generation created one.
	agentNames           []string
	agentNamesGeneration uint64
	// pendingEnvs holds the launch environments of queries that passed
	// validation but have not registered their session yet; an arrival here is
	// invisible to the session table, so environment switches must respect it.
	pendingEnvs  []map[string]string
	shuttingDown bool
	// stop is closed once at shutdown; every turn pump selects on it so a
	// shutdown interrupts even a blocked event emit.
	stop     chan struct{}
	stopOnce sync.Once
	turns    sync.WaitGroup
}

// agentQuery tracks one in-flight turn for CancelQuery and live observation.
// It is registered from StartTurn until the pump exits.
type agentQuery struct {
	id     string
	writer *QueryWriter
	// cancel triggers the turn's cancellation path; closed at most once.
	cancel     chan struct{}
	cancelOnce sync.Once
}

// cancel closes the cancel channel exactly once.
func (q *agentQuery) trigger() { q.cancelOnce.Do(func() { close(q.cancel) }) }

// New assembles the bridge. It starts nothing: the agent process appears
// lazily on the first query. Opening the event store (and recovering previous
// query records) is the one step that can fail construction.
func New(config Config) (*Service, error) {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	service := &Service{
		config:      config,
		logger:      logger,
		processes:   config.Processes,
		sessions:    make(map[string]*session),
		liveQueries: make(map[string]*agentQuery),
		now:         time.Now,
		stop:        make(chan struct{}),
	}
	if config.replayEnabled() {
		events := config.Events
		if events == (EventLogConfig{}) {
			events = DefaultEventLogConfig()
		}
		store, err := OpenQueryStore(filepath.Join(config.RuntimeDirectory, queryStoreDirectoryName), events, logger)
		if err != nil {
			return nil, fmt.Errorf("open agent event store: %w", err)
		}
		service.queries = store
	}
	dial := config.Dial
	if dial == nil {
		dial = service.dialProcess
	}
	service.process = &agentProcess{
		logger:    logger,
		dial:      dial,
		sink:      service,
		onCrash:   service.handleCrash,
		maySwitch: service.maySwitchEnvironment,
	}
	return service, nil
}

// TurnRequest is one query. A prompt without SessionID starts a fresh session
// (at the workspace root, or the request's working directory) that
// auto-closes when the turn settles; a prompt with SessionID resumes that
// agent-side conversation first. Environment carries caller-supplied
// overrides merged over the operator baseline for whichever child runs the
// turn; a value that requires a different child than the running one is
// refused while that child still has work in flight. Agent names the
// main-thread persona the turn runs as, applied through the child's agent
// session config option; empty keeps the default.
type TurnRequest struct {
	Prompt           string
	SessionID        string
	WorkingDirectory string
	Environment      map[string]string
	Agent            string
}

// launchEnvironment validates the caller's overrides and merges them over the
// operator's [agent].environment — caller wins. The merged map is the child's
// launch environment (and generation identity); the sorted override keys are
// all that records ever retain.
func (s *Service) launchEnvironment(overrides map[string]string) (map[string]string, []string, error) {
	keys, err := process.ValidateEnvironment(overrides)
	if err != nil {
		return nil, nil, rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentEnvironment, "%s", status.Convert(err).Message())
	}
	merged := make(map[string]string, len(s.config.Environment)+len(overrides))
	for key, value := range s.config.Environment {
		merged[key] = value
	}
	for key, value := range overrides {
		merged[key] = value
	}
	if _, err := process.ValidateEnvironment(merged); err != nil {
		return nil, nil, rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentEnvironment, "operator baseline plus overrides: %s", status.Convert(err).Message())
	}
	return merged, keys, nil
}

// maySwitchEnvironment reports whether the shared child may be replaced by a
// generation launched for environment: the bridge must be idle — no sessions
// in the table, no live queries — and every still-arriving query must want
// that same environment. It runs with the process mutex held, so it must
// never call back into the process.
func (s *Service) maySwitchEnvironment(environment map[string]string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.sessions) > 0 || len(s.liveQueries) > 0 {
		return false
	}
	for _, pending := range s.pendingEnvs {
		if !environmentsEqual(pending, environment) {
			return false
		}
	}
	return true
}

// TurnStream is the event stream of one running turn. Events yields turn
// frames (query id and sequence included) and closes when the turn settles;
// Wait then returns the terminal error, nil for a completed turn. The stream
// is an observation window: a caller that stops receiving detaches, and the
// turn keeps running to completion with its frames persisted for replay.
type TurnStream struct {
	events      chan *codev1.QueryResponse
	stop        <-chan struct{}
	abandoned   chan struct{}
	abandonOnce sync.Once
	done        chan struct{}
	err         error
}

// Events returns the turn's frame channel. It is closed when the turn settles.
func (t *TurnStream) Events() <-chan *codev1.QueryResponse { return t.events }

// Wait blocks until the turn settled and returns its terminal error.
func (t *TurnStream) Wait() error {
	<-t.done
	return t.err
}

// emit forwards one frame to the caller. It gives up when the caller's
// context ended, the service is shutting down, or the stream was abandoned —
// never blocking the pump on a consumer that stopped receiving.
func (t *TurnStream) emit(ctx context.Context, frame *codev1.QueryResponse) bool {
	select {
	case t.events <- frame:
		return true
	case <-ctx.Done():
	case <-t.stop:
	case <-t.abandoned:
	}
	return false
}

// abandon stops further forwarding while the pump keeps consuming.
func (t *TurnStream) abandon() { t.abandonOnce.Do(func() { close(t.abandoned) }) }

// setErr records the terminal error before finish closes the stream.
func (t *TurnStream) setErr(err error) { t.err = err }

// finish closes the caller-facing channels. It runs on the pump goroutine,
// which is the only sender, so no emit can race the close.
func (t *TurnStream) finish() {
	close(t.events)
	close(t.done)
}

// StartTurn validates the request, resolves the child for its environment,
// creates or resumes the session, claims its single turn slot, and starts the
// prompt pump. The returned stream ends with a Completed event (or a Wait
// error when the turn failed); the session auto-closes as the stream ends.
func (s *Service) StartTurn(ctx context.Context, request TurnRequest) (*TurnStream, error) {
	if strings.TrimSpace(request.Prompt) == "" {
		return nil, status.Error(codes.InvalidArgument, "prompt must not be empty")
	}
	environment, environmentKeys, err := s.launchEnvironment(request.Environment)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "agent service is shutting down")
	}
	// Fast-fail a resume whose session already runs a turn; the authoritative
	// check happens again at table registration, which closes the race.
	if request.SessionID != "" {
		if _, busy := s.sessions[request.SessionID]; busy {
			s.mu.Unlock()
			return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q already has an active turn", request.SessionID)
		}
	}
	// Claim the arrival before touching the child: a query between here and
	// its session registration is invisible to the session table, and an
	// environment switch must not restart the child under it.
	s.pendingEnvs = append(s.pendingEnvs, environment)
	s.mu.Unlock()
	dropArrival := func() {
		s.mu.Lock()
		s.dropArrivalLocked(environment)
		s.mu.Unlock()
	}

	connection, err := s.process.ensure(ctx, environment)
	if err != nil {
		dropArrival()
		return nil, err
	}

	cwd, err := s.resolveWorkingDirectory(request.WorkingDirectory)
	if err != nil {
		dropArrival()
		return nil, err
	}
	var sess *session
	created := false
	if request.SessionID == "" {
		newSessionCtx, cancel := context.WithTimeout(ctx, initializeTimeout)
		response, err := connection.conn.NewSession(newSessionCtx, acp.NewSessionRequest{
			Cwd:        cwd,
			McpServers: []acp.McpServer{},
		})
		cancel()
		if err != nil {
			dropArrival()
			return nil, mapAgentRequestError("create session", err)
		}
		if err := s.applyAgentSelection(ctx, connection, response.SessionId, request.Agent, response.ConfigOptions); err != nil {
			dropArrival()
			// The session exists agent-side; give it back rather than leak it.
			s.closeAgentSession(string(response.SessionId), connection)
			return nil, err
		}
		sess = newSession(response.SessionId, s.displaySessionWorkingDirectory(cwd), connection.generation, s.now())
		created = true
	} else {
		if connection.caps.SessionCapabilities.Resume == nil {
			dropArrival()
			return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentSessionNotResumable, "agent %q does not advertise session/resume", processLabel(connection.transport))
		}
		resumeCtx, cancel := context.WithTimeout(ctx, initializeTimeout)
		response, err := connection.conn.ResumeSession(resumeCtx, acp.ResumeSessionRequest{
			Cwd:        cwd,
			SessionId:  acp.SessionId(request.SessionID),
			McpServers: []acp.McpServer{},
		})
		cancel()
		if err != nil {
			dropArrival()
			return nil, mapAgentRequestError("resume session", err)
		}
		// A refused selection leaves the resumed conversation untouched — it
		// predates this query, so closing it would destroy the caller's work.
		if err := s.applyAgentSelection(ctx, connection, acp.SessionId(request.SessionID), request.Agent, response.ConfigOptions); err != nil {
			dropArrival()
			return nil, err
		}
		sess = newSession(acp.SessionId(request.SessionID), s.displaySessionWorkingDirectory(cwd), connection.generation, s.now())
	}

	active, ok := sess.beginTurn(s.now())
	if !ok {
		dropArrival()
		return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q already has an active turn", string(sess.id))
	}

	queryID, writer, err := s.beginQuery(sess, environmentKeys, request.Agent)
	if err != nil {
		sess.endTurn(active, s.now())
		dropArrival()
		return nil, err
	}

	stream := &TurnStream{
		events:    make(chan *codev1.QueryResponse, streamBufferCapacity),
		stop:      s.stop,
		abandoned: make(chan struct{}),
		done:      make(chan struct{}),
	}
	query := &agentQuery{id: queryID, writer: writer, cancel: make(chan struct{})}
	sess.bindQuery(active, queryID)
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		sess.endTurn(active, s.now())
		dropArrival()
		return nil, status.Error(codes.Unavailable, "agent service is shutting down")
	}
	if _, clash := s.sessions[string(sess.id)]; clash {
		// Two arrivals raced to the same session id; the loser unwinds without
		// ever reaching the agent's prompt.
		s.mu.Unlock()
		sess.endTurn(active, s.now())
		dropArrival()
		return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q already has an active turn", string(sess.id))
	}
	s.sessions[string(sess.id)] = sess
	s.dropArrivalLocked(environment)
	s.liveQueries[queryID] = query
	s.mu.Unlock()
	s.turns.Add(1)
	go s.runTurn(ctx, sess, connection, active, stream, request.Prompt, created, query)
	return stream, nil
}

// dropArrivalLocked removes one pending-arrival claim for environment. Callers
// on error paths hold no lock and wrap it themselves; the registration path
// already holds mu.
func (s *Service) dropArrivalLocked(environment map[string]string) {
	for index, pending := range s.pendingEnvs {
		if environmentsEqual(pending, environment) {
			s.pendingEnvs = append(s.pendingEnvs[:index], s.pendingEnvs[index+1:]...)
			return
		}
	}
}

// beginQuery allocates the turn's query id and opens its event record,
// retaining the environment override keys (never values) and the requested
// agent persona. A nil writer means replay is disabled: the turn still
// streams and carries its id, but nothing is retained.
func (s *Service) beginQuery(sess *session, environmentKeys []string, agent string) (string, *QueryWriter, error) {
	if s.queries == nil {
		id, err := newQueryUUID()
		if err != nil {
			return "", nil, status.Errorf(codes.Internal, "allocate query id: %v", err)
		}
		return id, nil, nil
	}
	id, writer, err := s.queries.Begin(QueryMetadata{
		SessionID:        string(sess.id),
		WorkingDirectory: strings.TrimPrefix(sess.workingDirectory, "/"),
		EnvironmentKeys:  environmentKeys,
		Agent:            agent,
	})
	if err != nil {
		return "", nil, status.Errorf(codes.Unavailable, "open query record: %v", err)
	}
	return id, writer, nil
}

// forgetQuery drops the live-query registration at pump exit.
func (s *Service) forgetQuery(query *agentQuery) {
	s.mu.Lock()
	if s.liveQueries[query.id] == query {
		delete(s.liveQueries, query.id)
	}
	s.mu.Unlock()
}

// CancelQuery asks a running turn to stop. It is the only way besides letting
// it finish: a caller that merely stops reading a Query stream detaches. The
// call triggers the cancellation and returns immediately — frames (ending with
// a cancelled stop reason) arrive on the streams observing the query.
func (s *Service) CancelQuery(queryID string) error {
	if queryID == "" {
		return status.Error(codes.InvalidArgument, "query id must not be empty")
	}
	s.mu.Lock()
	query := s.liveQueries[queryID]
	s.mu.Unlock()
	if query != nil {
		query.trigger()
		return nil
	}
	if s.queries != nil {
		if _, ok := s.queries.Stat(queryID); ok {
			// Settled or lost: there is nothing left to stop.
			return nil
		}
	}
	return rpcerror.Errorf(codes.NotFound, rpcerror.AgentQueryNotFound, "query %q was not found", queryID)
}

// QueryObserver receives one ObserveQuery stream: a header describing the
// window, then the frames from the requested sequence, then an end marker. A
// turn that failed ends the stream with an error instead of an end marker —
// the same terminal shape the original Query stream had.
type QueryObserver interface {
	QueryHeader(header *codev1.AgentQueryHeader) error
	QueryEvent(frame *codev1.QueryResponse) error
	QueryEnd(end *codev1.AgentQueryEnd) error
}

// ObserveQuery streams a query's frames starting at `from`. Without follow it
// ends at the current window boundary (snapshot complete); with follow on a
// running query it stays attached until the turn settles. The next sequence to
// request on a resumed connection is always the end marker's next_sequence, or
// one past the last received frame when the stream broke early.
func (s *Service) ObserveQuery(ctx context.Context, queryID string, from uint64, follow bool, observer QueryObserver) error {
	if s.queries == nil {
		return rpcerror.Errorf(codes.NotFound, rpcerror.AgentQueryNotFound, "query %q: replay is disabled on this controller", queryID)
	}
	snapshot, subscription, err := s.queries.Attach(queryID, from)
	if err != nil {
		return mapQueryStoreError(queryID, from, err)
	}
	// Attach pins the live writer's next sequence, but a settled record only
	// carries it on the snapshot — validate uniformly so a from beyond the end
	// is refused instead of silently replaying an empty window.
	if from > snapshot.Next {
		return mapQueryStoreError(queryID, from, ErrQuerySequence)
	}

	header := &codev1.AgentQueryHeader{
		QueryId:               queryID,
		SessionId:             snapshot.SessionID,
		State:                 agentQueryStateOf(snapshot.State),
		EarliestSequence:      snapshot.Earliest,
		SnapshotEndSequence:   snapshot.Next,
		ResolvedStartSequence: from,
		HistoryTruncated:      snapshot.Earliest > 0,
		Follow:                follow && subscription != nil,
	}
	if snapshot.State == QueryStateSettled && snapshot.Err == nil {
		stopReason := snapshot.StopReason
		header.StopReason = &stopReason
	}
	if snapshot.Agent != "" {
		agent := snapshot.Agent
		header.Agent = &agent
	}
	if err := observer.QueryHeader(header); err != nil {
		return err
	}
	// The pinned disk part [from, snapshot.Next) is immutable once attached.
	if snapshot.Next > from {
		if err := s.queries.ReadFrames(queryID, from, snapshot.Next, observer.QueryEvent); err != nil {
			return mapQueryStoreError(queryID, from, err)
		}
	}
	if subscription == nil {
		if snapshot.Err != nil {
			return snapshot.Err.status()
		}
		reason := codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SETTLED
		if snapshot.State == QueryStateRunning {
			reason = codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SNAPSHOT_COMPLETE
		}
		return observer.QueryEnd(&codev1.AgentQueryEnd{NextSequence: snapshot.Next, Reason: reason})
	}

	delivered := snapshot.Next
	for {
		select {
		case frame, ok := <-subscription.Frames():
			if !ok {
				err := <-subscription.Done()
				if err != nil {
					return mapQueryStoreError(queryID, delivered, err)
				}
				// The writer settled before ending its subscribers, so the
				// state on disk is already terminal and answers the end marker.
				final, found := s.queries.Stat(queryID)
				if !found {
					return rpcerror.Errorf(codes.NotFound, rpcerror.AgentQueryNotFound, "query %q vanished while settling", queryID)
				}
				if final.Err != nil {
					return final.Err.status()
				}
				return observer.QueryEnd(&codev1.AgentQueryEnd{
					NextSequence: final.Next,
					Reason:       codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SETTLED,
				})
			}
			if err := observer.QueryEvent(frame); err != nil {
				return err
			}
			delivered = frame.GetSequence() + 1
		case <-ctx.Done():
			return status.FromContextError(ctx.Err()).Err()
		case <-s.stop:
			return observer.QueryEnd(&codev1.AgentQueryEnd{
				NextSequence: delivered,
				Reason:       codev1.AgentQueryEndReason_AGENT_QUERY_END_REASON_SHUTDOWN,
			})
		}
	}
}

// mapQueryStoreError lifts store addressing failures onto the wire with their
// rpcerror reasons; anything else is an internal replay failure.
func mapQueryStoreError(queryID string, from uint64, err error) error {
	switch {
	case errors.Is(err, ErrQueryNotFound):
		return rpcerror.Errorf(codes.NotFound, rpcerror.AgentQueryNotFound, "query %q was not found", queryID)
	case errors.Is(err, ErrQueryPruned):
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentQueryEventsPruned, "query %q no longer retains sequence %d; re-observe from its earliest retained sequence", queryID, from)
	case errors.Is(err, ErrQuerySequence):
		return rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentQuerySequenceInvalid, "query %q has not written sequence %d yet", queryID, from)
	case errors.Is(err, ErrQueryObservers):
		return rpcerror.Errorf(codes.ResourceExhausted, rpcerror.AgentQueryObserverLimit, "query %q reached its observer limit; retry once one disconnects", queryID)
	case errors.Is(err, ErrQueryObserverLags):
		return rpcerror.Errorf(codes.ResourceExhausted, rpcerror.AgentQueryObserverLag, "observer of query %q fell behind; re-observe from the last received sequence", queryID)
	}
	return status.Errorf(codes.Internal, "replay query %q: %v", queryID, err)
}

// agentQueryStateOf maps the stored state onto its wire enum.
func agentQueryStateOf(state QueryState) codev1.AgentQueryState {
	switch state {
	case QueryStateRunning:
		return codev1.AgentQueryState_AGENT_QUERY_STATE_RUNNING
	case QueryStateSettled:
		return codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED
	case QueryStateLost:
		return codev1.AgentQueryState_AGENT_QUERY_STATE_LOST
	}
	return codev1.AgentQueryState_AGENT_QUERY_STATE_UNSPECIFIED
}

// runTurn drives one prompt: it persists and forwards session events while
// waiting for the prompt response on a context detached from the caller's. A
// caller that stops reading detaches — the turn runs to completion (or an
// explicit CancelQuery) either way, because the protocol requires the prompt
// to settle and every frame must reach the replay record.
func (s *Service) runTurn(ctx context.Context, sess *session, connection *agentConnection, active *turn, stream *TurnStream, prompt string, created bool, query *agentQuery) {
	defer s.turns.Done()
	recordSettled := false
	defer func() {
		sess.endTurn(active, s.now())
		s.forgetQuery(query)
		s.retireSession(sess)
		if !recordSettled && query.writer != nil {
			_ = query.writer.Lost(status.Error(codes.Internal, "turn pump exited without settling"))
		}
		stream.finish()
	}()

	sequence := uint64(0)
	forward := func(event Event) {
		frame := queryResponseOf(event)
		frame.QueryId = query.id
		frame.Sequence = sequence
		if query.writer != nil {
			if err := query.writer.Append(frame); err != nil {
				s.logger.Warn("persist query frame failed", "query_id", query.id, "err", err.Error())
			}
		}
		sequence++
		if !stream.emit(ctx, frame) {
			// The consumer is gone. Detach: keep persisting and consuming
			// until the turn settles, so replay serves the full answer.
			stream.abandon()
		}
	}
	drainIncoming := func() {
		for {
			select {
			case event := <-active.incoming:
				forward(event)
			default:
				return
			}
		}
	}

	if created {
		forward(Event{Kind: EventKindSessionStarted, SessionStarted: SessionStarted{SessionID: string(sess.id)}})
	}

	promptCtx, cancelPrompt := context.WithCancel(context.Background())
	defer cancelPrompt()

	type promptResult struct {
		response acp.PromptResponse
		err      error
	}
	results := make(chan promptResult, 1)
	go func() {
		response, err := connection.conn.Prompt(promptCtx, acp.PromptRequest{
			SessionId: sess.id,
			Prompt:    []acp.ContentBlock{acp.TextBlock(prompt)},
		})
		results <- promptResult{response: response, err: err}
	}()

	var terminalErr error
	var stopReason string
	cancelling := false
	var orphanDeadline <-chan time.Time
	beginCancel := func() {
		if cancelling {
			return
		}
		cancelling = true
		s.requestCancel(connection, sess)
		orphanDeadline = time.After(orphanTurnGrace)
	}
	settleRecord := func(err error) {
		recordSettled = true
		if query.writer == nil {
			return
		}
		// A lost process (or an agent that never answered the cancel) leaves
		// the turn's fate unknown; every other failure is a clean refusal the
		// caller can read back from the record.
		if rpcerror.ReasonOf(err) == rpcerror.AgentProcessLost || status.Code(err) == codes.DeadlineExceeded {
			if settleErr := query.writer.Lost(err); settleErr != nil {
				s.logger.Warn("mark query record lost failed", "query_id", query.id, "err", settleErr.Error())
			}
			return
		}
		if settleErr := query.writer.Fail(err); settleErr != nil {
			s.logger.Warn("settle query record failed", "query_id", query.id, "err", settleErr.Error())
		}
	}
	for {
		// Drain buffered updates before considering anything else. The SDK's
		// notification barrier delivers every update that preceded the prompt
		// response before that response, but select picks randomly among ready
		// cases — without this drain a turn could settle and abandon updates
		// the agent already sent.
		drainIncoming()
		select {
		case event := <-active.incoming:
			forward(event)
		case result := <-results:
			// The initial drain and this select are not atomic: if the final
			// update and prompt response become ready together, select may pick
			// the response. Prompt's notification barrier guarantees that every
			// preceding update has been dispatched by now, so drain once more
			// before releasing the session turn.
			drainIncoming()
			if result.err != nil {
				if !connection.alive() {
					terminalErr = rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process %q exited during the turn", connection.transport.processID)
					goto settled
				}
				if cancelling {
					// The turn was already being cancelled, so an explicit
					// CancelQuery (or shutdown) is in flight. A live agent may
					// answer with a cancelled stop reason, the SDK's -32800,
					// or an error raised while tearing down on its
					// already-cancelled handler context (the SDK turns those
					// into jsonrpc -32603); every live-agent answer settles
					// the turn as cancelled rather than failed.
					stopReason = string(acp.StopReasonCancelled)
				} else {
					terminalErr = s.translatePromptError(connection, result.err)
				}
			} else {
				stopReason = string(result.response.StopReason)
			}
			goto settled
		case <-query.cancel:
			beginCancel()
		case <-s.stop:
			beginCancel()
		case <-connection.exited:
			terminalErr = rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process %q exited during the turn", connection.transport.processID)
			goto settled
		case <-orphanDeadline:
			terminalErr = status.Error(codes.DeadlineExceeded, "agent did not settle the cancelled turn in time; the turn was orphaned")
			goto settled
		}
	}

settled:
	// Release the session's turn slot and retire the turn-scoped session as
	// soon as the outcome is known: the caller acts on the completed frame (or
	// the stream error) immediately — closing the session, resuming it, or
	// switching the environment — while the record below may still be flushing
	// to disk. Both calls are idempotent, so the deferred release stays as the
	// abnormal-exit safety net.
	sess.endTurn(active, s.now())
	s.forgetQuery(query)
	s.retireSession(sess)
	if terminalErr == nil {
		forward(Event{Kind: EventKindCompleted, Completed: Completed{StopReason: stopReason}})
		recordSettled = true
		if query.writer != nil {
			if err := query.writer.Settle(stopReason); err != nil {
				s.logger.Warn("settle query record failed", "query_id", query.id, "err", err.Error())
			}
		}
	} else {
		settleRecord(terminalErr)
	}
	s.closeAgentSession(string(sess.id), connection)
	stream.setErr(terminalErr)
}

// retireSession drops a turn-scoped session from the table once its turn's
// outcome is known — before the caller sees the completed frame — so
// follow-up calls (CloseSession, an environment switch) never race the
// stream's tail.
func (s *Service) retireSession(sess *session) {
	s.mu.Lock()
	if registered, ok := s.sessions[string(sess.id)]; ok && registered == sess {
		delete(s.sessions, string(sess.id))
	}
	s.mu.Unlock()
}

// closeAgentSession forwards session/close for a turn-scoped session on a
// live connection that advertised the capability, bounded in time. Failures
// only log — by the time it runs the turn is over and its record is already
// written, or the query was refused before it ever prompted.
func (s *Service) closeAgentSession(sessionID string, connection *agentConnection) {
	s.mu.Lock()
	shuttingDown := s.shuttingDown
	s.mu.Unlock()
	if shuttingDown || !connection.alive() || connection.caps.SessionCapabilities.Close == nil {
		return
	}
	closeCtx, cancel := context.WithTimeout(context.Background(), sessionCloseWait)
	defer cancel()
	if _, err := connection.conn.CloseSession(closeCtx, acp.CloseSessionRequest{SessionId: acp.SessionId(sessionID)}); err != nil {
		s.logger.Warn("auto-close session failed", "session_id", sessionID, "err", err.Error())
	}
}

// requestCancel sends the session/cancel notification. Fire and forget: the
// turn keeps waiting for the prompt to settle on its own.
func (s *Service) requestCancel(connection *agentConnection, sess *session) {
	notifyCtx, cancel := context.WithTimeout(context.Background(), cancelNotifyWait)
	go func() {
		defer cancel()
		if err := connection.conn.Cancel(notifyCtx, acp.CancelNotification{SessionId: sess.id}); err != nil {
			s.logger.Warn("session/cancel notification failed", "session_id", string(sess.id), "err", err.Error())
		}
	}()
}

// translatePromptError maps a failed prompt to a transport error, preferring
// the process-lost reason when the child died mid-turn.
func (s *Service) translatePromptError(connection *agentConnection, err error) error {
	if !connection.alive() {
		return rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process %q exited during the turn", connection.transport.processID)
	}
	var requestError *acp.RequestError
	if errors.As(err, &requestError) {
		return rpcerror.ErrorfWithMetadata(codes.Unknown, rpcerror.AgentRequestError, map[string]string{
			"jsonrpc_code": fmt.Sprintf("%d", requestError.Code),
		}, "agent rejected the turn (jsonrpc %d): %s", requestError.Code, requestError.Message)
	}
	return status.Errorf(codes.Unknown, "agent turn failed: %v", err)
}

// mapAgentRequestError wraps non-prompt SDK request failures (session
// creation, closing) with the agent's diagnosis where available.
func mapAgentRequestError(operation string, err error) error {
	var requestError *acp.RequestError
	if errors.As(err, &requestError) {
		return status.Errorf(codes.Unknown, "%s failed (jsonrpc %d): %s", operation, requestError.Code, requestError.Message)
	}
	return status.Errorf(codes.Unknown, "%s failed: %v", operation, err)
}

// CloseSession ends a session. Sessions are turn-scoped and already
// auto-close when their turn settles, so this only refuses while the turn is
// still running (cancel its query instead) and is an idempotent success for
// every other id — there is nothing left to close.
func (s *Service) CloseSession(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session id must not be empty")
	}
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "agent service is shutting down")
	}
	_, running := s.sessions[sessionID]
	s.mu.Unlock()
	if running {
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q has an active turn; cancel its query instead", sessionID)
	}
	return nil
}

// Shutdown cancels live turns, waits for them to settle within a bounded
// grace, and stops the agent process (stdin EOF first, then TERM/KILL).
func (s *Service) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return nil
	}
	s.shuttingDown = true
	s.stopOnce.Do(func() { close(s.stop) })
	s.mu.Unlock()

	settled := make(chan struct{})
	go func() {
		s.turns.Wait()
		close(settled)
	}()
	select {
	case <-settled:
	case <-time.After(turnSettleDeadline):
		s.logger.Warn("agent turns did not settle within the shutdown grace")
	case <-ctx.Done():
	}

	stopErr := s.process.stop(ctx)
	if s.queries != nil {
		if err := s.queries.Close(); err != nil {
			s.logger.Warn("close agent event store failed", "err", err.Error())
		}
	}
	return stopErr
}

// handleCrash empties the session table after the agent process died.
// In-flight turn pumps settle on their own through the transport done
// channel; their sessions are disk-backed on the agent side, so a later query
// with the same session id resumes them on the next generation instead of
// failing.
func (s *Service) handleCrash() {
	s.mu.Lock()
	clear(s.sessions)
	s.mu.Unlock()
}

// dispatchUpdate implements updateSink: the acpClient hands every session
// event here keyed by the agent-side session id.
func (s *Service) dispatchUpdate(sessionID acp.SessionId, event Event) {
	s.mu.Lock()
	sess := s.sessions[string(sessionID)]
	s.mu.Unlock()
	if sess == nil {
		s.logger.Debug("dropped agent update for unknown session", "session_id", string(sessionID), "kind", string(event.Kind))
		return
	}
	if !sess.dispatch(event) {
		s.logger.Warn("dropped agent update", "session_id", string(sessionID), "kind", string(event.Kind))
	}
}

// resolveWorkingDirectory confines a requested session cwd to the workspace
// root: relative only, no parent escapes, and symlink targets must stay inside
// the root.
func (s *Service) resolveWorkingDirectory(requested string) (string, error) {
	root, err := filepath.EvalSymlinks(s.config.WorkspaceRoot)
	if err != nil {
		return "", status.Errorf(codes.FailedPrecondition, "workspace root is not accessible: %v", err)
	}
	if requested == "" {
		return root, nil
	}
	if filepath.IsAbs(requested) {
		return "", rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentWorkingDirectory, "working directory %q must be relative to the workspace", requested)
	}
	cleaned := filepath.Clean(requested)
	if cleaned == ".." || strings.HasPrefix(cleaned, ".."+string(filepath.Separator)) {
		return "", rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentWorkingDirectory, "working directory %q escapes the workspace", requested)
	}
	resolved, err := filepath.EvalSymlinks(filepath.Join(root, cleaned))
	if err != nil {
		return "", rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentWorkingDirectory, "working directory %q is not accessible: %v", requested, err)
	}
	if !withinRoot(resolved, root) {
		return "", rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentWorkingDirectory, "working directory %q escapes the workspace", requested)
	}
	info, err := os.Stat(resolved)
	if err != nil || !info.IsDir() {
		return "", rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentWorkingDirectory, "working directory %q is not a directory", requested)
	}
	return resolved, nil
}

// displaySessionWorkingDirectory projects a verified native cwd onto the
// workspace-absolute path syntax used by the remote CLI.
func (s *Service) displaySessionWorkingDirectory(cwd string) string {
	root, err := filepath.EvalSymlinks(s.config.WorkspaceRoot)
	if err != nil {
		return "/"
	}
	relative, err := filepath.Rel(root, cwd)
	if err != nil || relative == "." {
		return "/"
	}
	return "/" + filepath.ToSlash(relative)
}

func withinRoot(path, root string) bool {
	return path == root || strings.HasPrefix(path, root+string(filepath.Separator))
}

// Status snapshots the bridge for capability reporting.
type Status struct {
	Started        bool
	ProcessID      string
	Generation     uint64
	LoadSession    bool
	CloseSupported bool
	Sessions       int
	// Agents lists the personas the current generation offered in its
	// sessions' config options; nil until it created one.
	Agents []string
	// Replay reports the event-store bounds when query replay is enabled.
	Replay *AgentReplayStatus
}

// AgentReplayStatus mirrors the store bounds for AgentInfo.
type AgentReplayStatus struct {
	MaxObservers     int
	MaxBytesPerQuery int64
	MaxTotalBytes    int64
}

// Snapshot reports the current bridge state for GetInfo.
func (s *Service) Snapshot() Status {
	s.mu.Lock()
	sessions := len(s.sessions)
	s.mu.Unlock()
	connection := s.process.currentConnection()
	snapshot := Status{Sessions: sessions}
	if s.queries != nil {
		snapshot.Replay = &AgentReplayStatus{
			MaxObservers:     s.queries.config.MaxObservers,
			MaxBytesPerQuery: s.queries.config.MaxBytesPerQuery,
			MaxTotalBytes:    s.queries.config.MaxTotalBytes,
		}
	}
	if connection == nil {
		return snapshot
	}
	snapshot.Started = true
	snapshot.ProcessID = connection.transport.processID
	snapshot.Generation = connection.generation
	snapshot.LoadSession = connection.caps.LoadSession
	snapshot.CloseSupported = connection.caps.SessionCapabilities.Close != nil
	snapshot.Agents = s.agentNamesFor(connection.generation)
	return snapshot
}
