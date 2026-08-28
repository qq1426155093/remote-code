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

	"github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// agentProcessName is the registry name of the shared agent child.
const agentProcessName = "agent"

// lostSessionHistory bounds how many crashed-generation session ids are
// remembered so a reuse attempt can be answered with AGENT_SESSION_LOST
// instead of the less specific AGENT_SESSION_NOT_FOUND.
const lostSessionHistory = 1024

// Config wires the agent bridge. Command/Arguments/Environment form the agent
// child command line; the environment must come from operator configuration
// or controller inheritance, never from callers.
type Config struct {
	Enabled     bool
	Command     string
	Arguments   []string
	Environment map[string]string
	// WorkspaceRoot is the absolute workspace root every session cwd is
	// confined to.
	WorkspaceRoot string
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

// Service is the agent bridge core: it owns the shared agent process, the
// session table, and turn execution. The gRPC surface is a thin adapter over
// StartTurn/CloseSession/Shutdown.
type Service struct {
	config    Config
	logger    *slog.Logger
	processes *process.Service

	process *agentProcess

	mu           sync.Mutex
	sessions     map[string]*session
	lostSessions map[string]struct{}
	lostOrder    []string
	shuttingDown bool
	// stop is closed once at shutdown; every turn pump selects on it so a
	// shutdown interrupts even a blocked event emit.
	stop     chan struct{}
	stopOnce sync.Once
	turns    sync.WaitGroup
}

// New assembles the bridge. It starts nothing: the agent process appears
// lazily on the first query.
func New(config Config) *Service {
	logger := config.Logger
	if logger == nil {
		logger = slog.Default()
	}
	service := &Service{
		config:       config,
		logger:       logger,
		processes:    config.Processes,
		sessions:     make(map[string]*session),
		lostSessions: make(map[string]struct{}),
		stop:         make(chan struct{}),
	}
	dial := config.Dial
	if dial == nil {
		dial = service.dialProcess
	}
	service.process = &agentProcess{
		logger:  logger,
		dial:    dial,
		sink:    service,
		onCrash: service.handleCrash,
	}
	return service
}

// TurnRequest is one query: a prompt for an existing session, or for a new
// session created at the workspace root (or the request's working directory).
type TurnRequest struct {
	Prompt           string
	SessionID        string
	WorkingDirectory string
}

// TurnStream is the event stream of one running turn. Events yields turn
// events and closes when the turn settles; Wait then returns the terminal
// error, nil for a completed turn. Callers should stop consuming Events as
// soon as their own context ends; the turn keeps draining agent updates until
// the prompt settles (the protocol requires it), so a caller going away
// cancels the turn rather than abandoning it.
type TurnStream struct {
	events    chan Event
	stop      <-chan struct{}
	abandoned chan struct{}
	done      chan struct{}
	err       error
}

// Events returns the turn's event channel. It is closed when the turn settles.
func (t *TurnStream) Events() <-chan Event { return t.events }

// Wait blocks until the turn settled and returns its terminal error.
func (t *TurnStream) Wait() error {
	<-t.done
	return t.err
}

// emit forwards one event to the caller. It gives up when the caller's
// context ended, the service is shutting down, or the stream was abandoned —
// never blocking the pump on a consumer that stopped receiving.
func (t *TurnStream) emit(ctx context.Context, event Event) bool {
	select {
	case t.events <- event:
		return true
	case <-ctx.Done():
	case <-t.stop:
	case <-t.abandoned:
	}
	return false
}

// abandon stops further forwarding while the pump keeps consuming.
func (t *TurnStream) abandon() { close(t.abandoned) }

// setErr records the terminal error before finish closes the stream.
func (t *TurnStream) setErr(err error) { t.err = err }

// finish closes the caller-facing channels. It runs on the pump goroutine,
// which is the only sender, so no emit can race the close.
func (t *TurnStream) finish() {
	close(t.events)
	close(t.done)
}

// StartTurn validates the request, resolves (or creates) the session, claims
// its single turn slot, and starts the prompt pump. The returned stream ends
// with a Completed event (or a Wait error when the turn failed).
func (s *Service) StartTurn(ctx context.Context, request TurnRequest) (*TurnStream, error) {
	if strings.TrimSpace(request.Prompt) == "" {
		return nil, status.Error(codes.InvalidArgument, "prompt must not be empty")
	}

	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "agent service is shutting down")
	}
	var existing *session
	if request.SessionID != "" {
		existing = s.sessions[request.SessionID]
		if existing == nil {
			_, lost := s.lostSessions[request.SessionID]
			s.mu.Unlock()
			if lost {
				return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentSessionLost, "session %q was lost when the agent process restarted", request.SessionID)
			}
			return nil, rpcerror.Errorf(codes.NotFound, rpcerror.AgentSessionNotFound, "session %q was not found", request.SessionID)
		}
	}
	s.mu.Unlock()

	connection, err := s.process.ensure(ctx)
	if err != nil {
		return nil, err
	}

	var sess *session
	created := false
	if existing != nil {
		sess = existing
	} else {
		cwd, err := s.resolveWorkingDirectory(request.WorkingDirectory)
		if err != nil {
			return nil, err
		}
		newSessionCtx, cancel := context.WithTimeout(ctx, initializeTimeout)
		response, err := connection.conn.NewSession(newSessionCtx, acp.NewSessionRequest{
			Cwd:        cwd,
			McpServers: []acp.McpServer{},
		})
		cancel()
		if err != nil {
			return nil, mapAgentRequestError("create session", err)
		}
		sess = newSession(response.SessionId, cwd)
		created = true
		s.mu.Lock()
		if s.shuttingDown {
			s.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "agent service is shutting down")
		}
		s.sessions[string(sess.id)] = sess
		s.mu.Unlock()
	}

	active, ok := sess.beginTurn()
	if !ok {
		return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q already has an active turn", string(sess.id))
	}

	stream := &TurnStream{
		events:    make(chan Event, 16),
		stop:      s.stop,
		abandoned: make(chan struct{}),
		done:      make(chan struct{}),
	}
	s.turns.Add(1)
	go s.runTurn(ctx, sess, connection, active, stream, request.Prompt, created)
	return stream, nil
}

// runTurn drives one prompt: it forwards session events to the caller while
// waiting for the prompt response on a context detached from the caller's, so
// a disconnect cancels the turn explicitly instead of aborting the wait.
func (s *Service) runTurn(ctx context.Context, sess *session, connection *agentConnection, active *turn, stream *TurnStream, prompt string, created bool) {
	defer s.turns.Done()
	defer func() {
		sess.endTurn(active)
		stream.finish()
	}()

	if created {
		stream.emit(ctx, Event{Kind: EventKindSessionStarted, SessionStarted: SessionStarted{SessionID: string(sess.id)}})
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
		stream.abandon()
		s.requestCancel(connection, sess)
		orphanDeadline = time.After(orphanTurnGrace)
	}
	for {
		// Drain buffered updates before considering anything else. The SDK's
		// notification barrier delivers every update that preceded the prompt
		// response before that response, but select picks randomly among ready
		// cases — without this drain a turn could settle and abandon updates
		// the agent already sent.
		select {
		case event := <-active.incoming:
			if !cancelling && !stream.emit(ctx, event) {
				beginCancel()
			}
			continue
		default:
		}
		select {
		case event := <-active.incoming:
			if !cancelling && !stream.emit(ctx, event) {
				beginCancel()
			}
		case result := <-results:
			if result.err != nil {
				if !connection.alive() {
					terminalErr = rpcerror.Errorf(codes.Unavailable, rpcerror.AgentProcessLost, "agent process %q exited during the turn", connection.transport.processID)
					goto settled
				}
				if cancelling {
					// The turn was already being cancelled, so the caller is
					// gone and the stream abandoned. A live agent may answer
					// with a cancelled stop reason, the SDK's -32800, or an
					// error raised while tearing down on its already-cancelled
					// handler context (the SDK turns those into jsonrpc
					// -32603); every live-agent answer settles the turn as
					// cancelled rather than failed.
					stopReason = string(acp.StopReasonCancelled)
				} else {
					terminalErr = s.translatePromptError(connection, result.err)
				}
			} else {
				stopReason = string(result.response.StopReason)
			}
			goto settled
		case <-ctx.Done():
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
	if terminalErr == nil {
		stream.emit(ctx, Event{Kind: EventKindCompleted, Completed: Completed{StopReason: stopReason}})
	}
	stream.setErr(terminalErr)
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

// CloseSession ends a session. It refuses while a turn is active, forwards
// session/close when the agent advertised the capability, and always removes
// the local session entry.
func (s *Service) CloseSession(ctx context.Context, sessionID string) error {
	if sessionID == "" {
		return status.Error(codes.InvalidArgument, "session id must not be empty")
	}
	s.mu.Lock()
	if s.shuttingDown {
		s.mu.Unlock()
		return status.Error(codes.Unavailable, "agent service is shutting down")
	}
	sess := s.sessions[sessionID]
	if sess == nil {
		_, lost := s.lostSessions[sessionID]
		s.mu.Unlock()
		if lost {
			return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentSessionLost, "session %q was lost when the agent process restarted", sessionID)
		}
		return rpcerror.Errorf(codes.NotFound, rpcerror.AgentSessionNotFound, "session %q was not found", sessionID)
	}
	if sess.activeTurn() != nil {
		s.mu.Unlock()
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentTurnActive, "session %q has an active turn", sessionID)
	}
	delete(s.sessions, sessionID)
	s.mu.Unlock()

	connection, err := s.process.ensure(ctx)
	if err != nil {
		return err
	}
	if connection.caps.SessionCapabilities.Close != nil {
		if _, err := connection.conn.CloseSession(ctx, acp.CloseSessionRequest{SessionId: sess.id}); err != nil {
			return mapAgentRequestError("close session", err)
		}
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

	return s.process.stop(ctx)
}

// handleCrash empties the session table after the agent process died. Every
// session of the crashed generation is remembered as lost so reuse attempts
// get AGENT_SESSION_LOST; in-flight turn pumps terminate on their own through
// the transport done channel.
func (s *Service) handleCrash() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id := range s.sessions {
		s.rememberLostLocked(id)
		delete(s.sessions, id)
	}
}

func (s *Service) rememberLostLocked(id string) {
	if _, ok := s.lostSessions[id]; ok {
		return
	}
	if len(s.lostOrder) >= lostSessionHistory {
		oldest := s.lostOrder[0]
		s.lostOrder = s.lostOrder[1:]
		delete(s.lostSessions, oldest)
	}
	s.lostSessions[id] = struct{}{}
	s.lostOrder = append(s.lostOrder, id)
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
}

// Snapshot reports the current bridge state for GetInfo.
func (s *Service) Snapshot() Status {
	s.mu.Lock()
	sessions := len(s.sessions)
	s.mu.Unlock()
	connection := s.process.currentConnection()
	snapshot := Status{Sessions: sessions}
	if connection == nil {
		return snapshot
	}
	snapshot.Started = true
	snapshot.ProcessID = connection.transport.processID
	snapshot.Generation = connection.generation
	snapshot.LoadSession = connection.caps.LoadSession
	snapshot.CloseSupported = connection.caps.SessionCapabilities.Close != nil
	return snapshot
}
