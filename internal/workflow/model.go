package workflow

import (
	"encoding/json"
	"errors"
	"time"
)

var (
	ErrNotFound       = errors.New("workflow object not found")
	ErrConflict       = errors.New("workflow state conflict")
	ErrLeaseExpired   = errors.New("workflow activity lease expired")
	ErrCapacity       = errors.New("workflow capacity exhausted")
	ErrShuttingDown   = errors.New("workflow service is shutting down")
	ErrNonDeterminism = errors.New("workflow replay is non-deterministic")
)

// RunState is the durable state of a workflow run.
type RunState string

const (
	RunPending   RunState = "PENDING"
	RunRunning   RunState = "RUNNING"
	RunPaused    RunState = "PAUSED"
	RunSucceeded RunState = "SUCCEEDED"
	RunFailed    RunState = "FAILED"
	RunCancelled RunState = "CANCELLED"
)

// NodeState is the durable state of a node invocation.
type NodeState string

const (
	NodePending           NodeState = "PENDING"
	NodeReady             NodeState = "READY"
	NodeEvaluating        NodeState = "EVALUATING"
	NodeWaitingActivity   NodeState = "WAITING_ACTIVITY"
	NodeWaitingRetry      NodeState = "WAITING_RETRY"
	NodeNeedsIntervention NodeState = "NEEDS_INTERVENTION"
	NodeSucceeded         NodeState = "SUCCEEDED"
	NodeFailed            NodeState = "FAILED"
	NodeCancelled         NodeState = "CANCELLED"
	NodeSkipped           NodeState = "SKIPPED"
)

// ActivityState is the durable state of a host operation.
type ActivityState string

const (
	ActivityScheduled         ActivityState = "SCHEDULED"
	ActivityClaimed           ActivityState = "CLAIMED"
	ActivityRunning           ActivityState = "RUNNING"
	ActivityWaitingRetry      ActivityState = "WAITING_RETRY"
	ActivityNeedsIntervention ActivityState = "NEEDS_INTERVENTION"
	ActivitySucceeded         ActivityState = "SUCCEEDED"
	ActivityFailed            ActivityState = "FAILED"
	ActivityCancelled         ActivityState = "CANCELLED"
)

// AttemptState is the state of one executor lease.
type AttemptState string

const (
	AttemptClaimed   AttemptState = "CLAIMED"
	AttemptRunning   AttemptState = "RUNNING"
	AttemptSucceeded AttemptState = "SUCCEEDED"
	AttemptFailed    AttemptState = "FAILED"
	AttemptLost      AttemptState = "LOST"
	AttemptCancelled AttemptState = "CANCELLED"
)

type JoinPolicy string

const (
	JoinAll JoinPolicy = "all"
	JoinAny JoinPolicy = "any"
)

type TerminalOutcome string

const (
	TerminalNone      TerminalOutcome = ""
	TerminalSucceeded TerminalOutcome = "succeeded"
	TerminalFailed    TerminalOutcome = "failed"
)

type ResourceMode string

const (
	ResourceShared    ResourceMode = "shared"
	ResourceExclusive ResourceMode = "exclusive"
)

// ActivityResult is data returned to Expr. Business-level failure belongs in
// Status or Output and is not a workflow system error.
type ActivityResult struct {
	Status      string `json:"status"`
	Code        string `json:"code,omitempty"`
	Message     string `json:"message,omitempty"`
	Output      any    `json:"output,omitempty"`
	ExternalRef string `json:"external_ref,omitempty"`
}

type Attempt struct {
	Number        int          `json:"number"`
	State         AttemptState `json:"state"`
	ExecutorID    string       `json:"executor_id"`
	LeaseDeadline time.Time    `json:"lease_deadline,omitempty"`
	StartedAt     time.Time    `json:"started_at,omitempty"`
	FinishedAt    time.Time    `json:"finished_at,omitempty"`
	Error         string       `json:"error,omitempty"`
}

type Activity struct {
	ID                 string          `json:"id"`
	OperationID        string          `json:"operation_id"`
	ExecutorKind       string          `json:"executor_kind"`
	Input              any             `json:"input,omitempty"`
	InputHash          string          `json:"input_hash"`
	State              ActivityState   `json:"state"`
	Result             *ActivityResult `json:"result,omitempty"`
	Attempts           []Attempt       `json:"attempts,omitempty"`
	AvailableAt        time.Time       `json:"available_at,omitempty"`
	InterventionReason string          `json:"intervention_reason,omitempty"`
	CreatedAt          time.Time       `json:"created_at"`
	UpdatedAt          time.Time       `json:"updated_at"`
}

type NodeRun struct {
	ID             string               `json:"id"`
	State          NodeState            `json:"state"`
	SelectedRoute  string               `json:"selected_route,omitempty"`
	Activities     map[string]*Activity `json:"activities,omitempty"`
	ResolvedInputs map[string]bool      `json:"resolved_inputs,omitempty"`
	Acquired       bool                 `json:"acquired_resources,omitempty"`
	Error          string               `json:"error,omitempty"`
	StartedAt      time.Time            `json:"started_at,omitempty"`
	FinishedAt     time.Time            `json:"finished_at,omitempty"`
}

type Run struct {
	ID               string              `json:"id"`
	WorkflowName     string              `json:"workflow_name"`
	Revision         int                 `json:"revision"`
	DefinitionDigest string              `json:"definition_digest"`
	IdempotencyKey   string              `json:"idempotency_key,omitempty"`
	Parameters       map[string]any      `json:"parameters"`
	Context          map[string]string   `json:"context"`
	ContextVersion   uint64              `json:"context_version"`
	State            RunState            `json:"state"`
	Nodes            map[string]*NodeRun `json:"nodes"`
	Error            string              `json:"error,omitempty"`
	EventSequence    uint64              `json:"event_sequence"`
	CreatedAt        time.Time           `json:"created_at"`
	UpdatedAt        time.Time           `json:"updated_at"`
	FinishedAt       time.Time           `json:"finished_at,omitempty"`
}

type Event struct {
	RunID      string          `json:"run_id"`
	Sequence   uint64          `json:"sequence"`
	Time       time.Time       `json:"time"`
	Type       string          `json:"type"`
	NodeID     string          `json:"node_id,omitempty"`
	ActivityID string          `json:"activity_id,omitempty"`
	Data       json.RawMessage `json:"data,omitempty"`
}

// ActivityClaim is returned only at claim time. LeaseToken is deliberately
// not part of snapshots or events.
type ActivityClaim struct {
	RunID         string
	NodeID        string
	ActivityID    string
	OperationID   string
	ExecutorKind  string
	Input         any
	Attempt       int
	LeaseToken    string
	LeaseDeadline time.Time
}

type InterventionAction string

const (
	InterventionContinue InterventionAction = "continue"
	InterventionRetry    InterventionAction = "retry"
	InterventionResolve  InterventionAction = "resolve"
	InterventionFail     InterventionAction = "fail"
	InterventionCancel   InterventionAction = "cancel"
)

type InterventionResolution struct {
	Action    InterventionAction
	Principal string
	Result    *ActivityResult
	Message   string
}

func terminalRunState(state RunState) bool {
	return state == RunSucceeded || state == RunFailed || state == RunCancelled
}

func terminalNodeState(state NodeState) bool {
	return state == NodeSucceeded || state == NodeFailed || state == NodeCancelled || state == NodeSkipped
}
