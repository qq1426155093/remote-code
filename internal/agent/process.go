package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/coder/acp-go-sdk"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/process"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Lifecycle budgets. The initialize timeout bounds the handshake with the
// agent child; the shutdown ladder mirrors the order claude-agent-acp itself
// uses on the other side (stdin EOF is a clean exit, then signals).
const (
	initializeTimeout = 30 * time.Second
	stdinCloseGrace   = 3 * time.Second
	terminateGrace    = 5 * time.Second
	killGrace         = 5 * time.Second
	cancelNotifyWait  = 5 * time.Second
	// orphanTurnGrace bounds how long a cancelled turn may keep its pump alive
	// waiting for the agent to acknowledge; claude-agent-acp force-kills its
	// own turns after 30s, so this matches the agent's own worst case.
	orphanTurnGrace = 30 * time.Second
	// sessionCloseWait bounds the session/close forwarded when a turn-scoped
	// session auto-closes; a stuck close must not hold the stream open forever.
	sessionCloseWait   = 5 * time.Second
	turnSettleDeadline = 5 * time.Second
)

// transport is the protocol channel to one agent process plus its lifecycle
// handles. Production transports wrap a registry-managed raw process; tests
// substitute in-memory pipes.
type transport struct {
	stdin     io.WriteCloser
	stdout    io.ReadCloser
	done      <-chan struct{}
	processID string // registry id when the transport owns a registry process
	// terminate escalates TERM then KILL on the process group. It is nil for
	// in-memory transports, where closing stdin ends the peer.
	terminate func(ctx context.Context) error
}

// dialFunc produces one live transport. The environment is the merged launch
// environment for the generation being started; implementations must not
// return a transport whose peer handshake has already been attempted.
type dialFunc func(ctx context.Context, environment map[string]string) (transport, error)

// agentConnection is one generation of the shared agent process together with
// its negotiated capabilities and launch environment.
type agentConnection struct {
	conn       *acp.ClientSideConnection
	transport  transport
	caps       acp.AgentCapabilities
	generation uint64
	// environment is the merged map this generation was started with; it is
	// generation identity for environment switching and lives in memory only.
	environment map[string]string
	// exited closes after the process died AND the crash bookkeeping
	// (clearing the current connection, marking sessions lost) finished, so a
	// turn that settles on it observes a consistent session table.
	exited chan struct{}
}

// alive reports whether the agent process behind the connection is still
// running. A false answer is advisory: the watch goroutine is the authority
// for clearing the current connection.
func (c *agentConnection) alive() bool {
	select {
	case <-c.transport.done:
		return false
	default:
		return true
	}
}

// agentProcess owns the single shared agent child process across all sessions.
// It starts lazily, restarts on demand after a crash, and never retries on its
// own: callers drive retries by issuing their next query. The launch
// environment is generation identity: a query whose environment differs from
// the running generation's may switch to a fresh child, but only when the
// bridge reports the current one idle (no in-flight sessions, no running
// queries, no other arrival racing us).
type agentProcess struct {
	logger *slog.Logger
	dial   dialFunc
	sink   updateSink
	// onCrash runs once per crashed generation, after the connection has been
	// cleared, so the session table can be emptied while no new generation
	// exists yet.
	onCrash func()
	// maySwitch reports whether the bridge allows replacing the current
	// generation with one launched for environment. It runs with mu held, so
	// it must never call back into the process (lock order: mu, then the
	// service's own mutex — service code takes them in exactly that order and
	// never the reverse).
	maySwitch func(environment map[string]string) bool

	mu         sync.Mutex
	current    *agentConnection
	starting   chan struct{}
	generation uint64
	shutdown   bool
}

// currentConnection returns the live connection, or nil when the agent
// process is not running.
func (p *agentProcess) currentConnection() *agentConnection {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.current
}

// environmentsEqual compares two launch environments; nil and empty are the
// same generation.
func environmentsEqual(left, right map[string]string) bool {
	if len(left) != len(right) {
		return false
	}
	for key, value := range left {
		if other, ok := right[key]; !ok || other != value {
			return false
		}
	}
	return true
}

// ensure returns a live connection launched for environment, starting (or
// switching to) the agent process as needed. Concurrent callers single-flight:
// the first spawns and initializes, the rest wait and reuse (or retry the
// spawn when the first attempt failed). A request for a different environment
// than the running generation's is refused with AGENT_ENV_CONFLICT while the
// bridge still has work on the current one; once idle, the old child is
// stopped and the new one takes over.
func (p *agentProcess) ensure(ctx context.Context, environment map[string]string) (*agentConnection, error) {
	for {
		p.mu.Lock()
		if p.shutdown {
			p.mu.Unlock()
			return nil, status.Error(codes.Unavailable, "agent service is shutting down")
		}
		var retired *agentConnection
		if p.current != nil {
			if environmentsEqual(p.current.environment, environment) {
				connection := p.current
				p.mu.Unlock()
				return connection, nil
			}
			if p.maySwitch != nil && !p.maySwitch(environment) {
				p.mu.Unlock()
				return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentEnvConflict,
					"agent child runs a different environment and still has work in flight; retry once it is idle")
			}
			// Detach the old generation first: every other caller now sees a
			// switch in progress instead of a mismatched environment.
			retired = p.current
			p.current = nil
		}
		if p.starting != nil {
			wait := p.starting
			p.mu.Unlock()
			select {
			case <-wait:
			case <-ctx.Done():
				return nil, status.FromContextError(ctx.Err()).Err()
			}
			continue
		}
		started := make(chan struct{})
		p.starting = started
		p.mu.Unlock()

		// The ladder can block for seconds; keep it out of the lock so
		// currentConnection and other ensure calls stay responsive.
		p.stopConnection(ctx, retired)
		connection, err := p.start(ctx, environment)

		p.mu.Lock()
		p.starting = nil
		if err != nil {
			p.mu.Unlock()
			close(started)
			return nil, err
		}
		p.current = connection
		p.mu.Unlock()
		close(started)
		return connection, nil
	}
}

// start dials one agent process, runs the ACP initialize handshake with no
// client capabilities advertised, snapshots the agent capabilities, and starts
// the crash watch.
func (p *agentProcess) start(ctx context.Context, environment map[string]string) (*agentConnection, error) {
	tr, err := p.dial(ctx, environment)
	if err != nil {
		// Registry refusals (limit reached, invalid request, shutdown) keep
		// their own code and reason; anything else is a spawn failure.
		if status.Code(err) != codes.Unknown || rpcerror.ReasonOf(err) != "" {
			return nil, err
		}
		return nil, rpcerror.Errorf(codes.Unavailable, rpcerror.AgentStartFailed, "start agent process: %v", err)
	}
	client := &acpClient{sink: p.sink, logger: p.logger}
	connection := acp.NewClientSideConnection(client, tr.stdin, tr.stdout)
	connection.SetLogger(p.logger)

	initializeCtx, cancel := context.WithTimeout(ctx, initializeTimeout)
	defer cancel()
	response, err := connection.Initialize(initializeCtx, acp.InitializeRequest{
		ProtocolVersion: acp.ProtocolVersionNumber,
		ClientInfo:      &acp.Implementation{Name: "remote-code", Version: "0"},
		// No ClientCapabilities: remote-code advertises neither fs nor
		// terminal nor elicitation, so the agent must not call those methods.
		ClientCapabilities: acp.ClientCapabilities{},
	})
	if err != nil {
		p.discard(tr)
		return nil, rpcerror.Errorf(codes.Unavailable, rpcerror.AgentStartFailed, "agent initialize failed (process %q): %v", processLabel(tr), acpErrorDetail(err))
	}

	p.mu.Lock()
	p.generation++
	generation := p.generation
	p.mu.Unlock()

	agentConn := &agentConnection{
		conn:        connection,
		transport:   tr,
		caps:        response.AgentCapabilities,
		generation:  generation,
		environment: environment,
		exited:      make(chan struct{}),
	}
	go p.watch(agentConn)
	p.logger.Info("agent process started",
		"process_id", tr.processID, "generation", generation)
	return agentConn, nil
}

// watch clears the current connection when its process exits, reports the
// crash, and only then closes exited. Deliberate stops (already cleared by
// stop) skip the crash bookkeeping but still close exited so turn pumps can
// settle.
func (p *agentProcess) watch(connection *agentConnection) {
	<-connection.transport.done
	p.mu.Lock()
	crashed := p.current == connection
	if crashed {
		p.current = nil
	}
	p.mu.Unlock()
	if crashed {
		p.logger.Warn("agent process exited", "process_id", connection.transport.processID)
		if p.onCrash != nil {
			p.onCrash()
		}
	}
	close(connection.exited)
}

// stop shuts the process owner down and closes the current connection, if
// any. Idempotent.
func (p *agentProcess) stop(ctx context.Context) error {
	p.mu.Lock()
	p.shutdown = true
	connection := p.current
	p.current = nil
	p.mu.Unlock()
	return p.stopConnection(ctx, connection)
}

// stopConnection follows the shutdown ladder for one connection: stdin EOF (a
// clean exit for ACP agents), a grace period, then the terminate escalation.
// It does not touch p.current; callers decide lifetime under the mutex, so
// this serves both shutdown and an environment switch.
func (p *agentProcess) stopConnection(ctx context.Context, connection *agentConnection) error {
	if connection == nil {
		return nil
	}
	tr := connection.transport
	if err := tr.stdin.Close(); err != nil {
		p.logger.Warn("closing agent stdin failed", "process_id", tr.processID, "err", err.Error())
	}
	select {
	case <-tr.done:
		return nil
	case <-time.After(stdinCloseGrace):
	case <-ctx.Done():
		return ctx.Err()
	}
	if tr.terminate == nil {
		return nil
	}
	return tr.terminate(ctx)
}

// discard tears down a transport whose handshake failed.
func (p *agentProcess) discard(tr transport) {
	_ = tr.stdin.Close()
	if tr.terminate != nil {
		discardCtx, cancel := context.WithTimeout(context.Background(), terminateGrace+killGrace)
		defer cancel()
		_ = tr.terminate(discardCtx)
	}
}

// dialProcess is the production dialFunc: it starts the agent through the
// process registry's raw-pipe entry so the child keeps registry semantics
// (records, stderr logs, signals, LOST marking) while its protocol stdout
// stays off disk.
func (s *Service) dialProcess(ctx context.Context, environment map[string]string) (transport, error) {
	raw, err := s.processes.StartRawProcess(ctx, process.RawProcessSpec{
		Name:        agentProcessName,
		Command:     s.config.Command,
		Arguments:   s.config.Arguments,
		Environment: environment,
	})
	if err != nil {
		return transport{}, err
	}
	id := raw.Process.GetId()
	return transport{
		stdin:     raw.Stdin,
		stdout:    raw.Stdout,
		done:      raw.Done,
		processID: id,
		terminate: func(ctx context.Context) error {
			return s.terminateAgentProcess(ctx, id)
		},
	}, nil
}

// terminateAgentProcess escalates TERM then KILL through the registry so the
// process group (not just the child) is covered. KILL is only reached when the
// group survived TERM for the whole grace period.
func (s *Service) terminateAgentProcess(ctx context.Context, processID string) error {
	if err := s.signalAgentProcess(ctx, processID, codev1.ProcessSignal_PROCESS_SIGNAL_TERM, terminateGrace); err == nil {
		return nil
	}
	return s.signalAgentProcess(ctx, processID, codev1.ProcessSignal_PROCESS_SIGNAL_KILL, killGrace)
}

func (s *Service) signalAgentProcess(ctx context.Context, processID string, signal codev1.ProcessSignal, grace time.Duration) error {
	signalCtx, cancel := context.WithTimeout(ctx, grace)
	defer cancel()
	_, err := s.processes.SignalProcess(signalCtx, &codev1.SignalProcessRequest{
		Process: &codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: processID}},
		Signal:  signal,
		Wait:    true,
	})
	if err == nil || rpcerror.ReasonOf(err) == rpcerror.ProcessNotRunning {
		return nil
	}
	return err
}

// processLabel identifies a transport in errors without ever embedding
// environment values or argv payloads.
func processLabel(tr transport) string {
	if tr.processID != "" {
		return tr.processID
	}
	return "in-memory"
}

// acpErrorDetail renders an SDK error for an error message. JSON-RPC errors
// carry the agent's diagnosis (for example an auth-required refusal), which is
// exactly what an operator needs to see; other errors stringify directly.
func acpErrorDetail(err error) string {
	var requestError *acp.RequestError
	if errors.As(err, &requestError) {
		return fmt.Sprintf("jsonrpc %d: %s", requestError.Code, requestError.Message)
	}
	return err.Error()
}
