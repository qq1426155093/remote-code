package workflow

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// Service owns durable workflow state. Its methods are internal controller
// APIs; no public RPC surface is registered by this package.
type Service struct {
	mu          sync.Mutex
	config      Config
	registry    *Registry
	store       *store
	runs        map[string]*runRecord
	definitions map[string]*compiledDefinition
	signals     map[string]chan struct{}
	closing     bool
	stop        chan struct{}
	done        chan struct{}
	closeOnce   sync.Once
}

func New(config Config, runtimeDirectory string, registry *Registry) (*Service, error) {
	config.ApplyDefaults()
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	if !config.Enabled {
		return nil, errors.New("cannot create a disabled workflow service")
	}
	if registry == nil {
		return nil, errors.New("compiled workflow registry is required")
	}
	persistence, err := openStore(runtimeDirectory)
	if err != nil {
		return nil, err
	}
	service := &Service{
		config: config, registry: registry, store: persistence, runs: make(map[string]*runRecord),
		definitions: make(map[string]*compiledDefinition), signals: make(map[string]chan struct{}),
		stop: make(chan struct{}), done: make(chan struct{}),
	}
	records, err := persistence.list()
	if err != nil {
		_ = persistence.close()
		return nil, err
	}
	for _, record := range records {
		if record.Version != runRecordVersion {
			_ = persistence.close()
			return nil, fmt.Errorf("workflow run %q has unsupported record version %d", record.Run.ID, record.Version)
		}
		if definitionDigest(record.Definition) != record.Digest {
			_ = persistence.close()
			return nil, fmt.Errorf("workflow run %q definition snapshot digest does not match", record.Run.ID)
		}
		compiled, compileErr := compileDefinition(record.Definition)
		if compileErr != nil {
			_ = persistence.close()
			return nil, fmt.Errorf("compile workflow run %q definition snapshot: %w", record.Run.ID, compileErr)
		}
		initializeRecordMaps(record)
		if validateErr := validateRecoveredRecord(record, compiled, persistence); validateErr != nil {
			_ = persistence.close()
			return nil, fmt.Errorf("validate workflow run %q: %w", record.Run.ID, validateErr)
		}
		service.runs[record.Run.ID] = record
		service.definitions[record.Run.ID] = compiled
		service.signals[record.Run.ID] = make(chan struct{})
	}
	if err := service.Reconcile(time.Now()); err != nil {
		_ = persistence.close()
		return nil, fmt.Errorf("recover workflow runs: %w", err)
	}
	go service.reconcileLoop()
	return service, nil
}

func initializeRecordMaps(record *runRecord) {
	if record.Leases == nil {
		record.Leases = make(map[string]leaseRecord)
	}
	if record.Commands == nil {
		record.Commands = make(map[string]commandRecord)
	}
	if record.Run.Nodes == nil {
		record.Run.Nodes = make(map[string]*NodeRun)
	}
	if record.Run.Context == nil {
		record.Run.Context = make(map[string]string)
	}
	for _, node := range record.Run.Nodes {
		if node.Activities == nil {
			node.Activities = make(map[string]*Activity)
		}
		if node.ResolvedInputs == nil {
			node.ResolvedInputs = make(map[string]bool)
		}
	}
}

func (s *Service) reconcileLoop() {
	defer close(s.done)
	ticker := time.NewTicker(s.config.ReconcileInterval)
	defer ticker.Stop()
	for {
		select {
		case now := <-ticker.C:
			_ = s.Reconcile(now)
		case <-s.stop:
			return
		}
	}
}

func (s *Service) StartRun(ctx context.Context, workflowName, idempotencyKey string, parameters map[string]any) (*Run, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrShuttingDown
	}
	definition, exists := s.registry.definition(workflowName)
	if !exists {
		return nil, fmt.Errorf("%w: workflow %q", ErrNotFound, workflowName)
	}
	if idempotencyKey != "" {
		if !identifierPattern.MatchString(idempotencyKey) {
			return nil, fmt.Errorf("idempotency key must match %s", identifierPattern)
		}
		existingID, err := s.store.idempotentRun(workflowName, idempotencyKey)
		if err != nil {
			return nil, err
		}
		if existingID != "" {
			record := s.runs[existingID]
			if record == nil {
				return nil, fmt.Errorf("idempotency index references missing run %q", existingID)
			}
			return cloneRun(record.Run)
		}
	}
	if s.activeRunCount() >= s.config.MaxActiveRuns {
		return nil, ErrCapacity
	}
	clonedValue, err := cloneJSONValue(parameters)
	if err != nil {
		return nil, fmt.Errorf("normalize workflow parameters: %w", err)
	}
	clonedParameters, ok := clonedValue.(map[string]any)
	if !ok {
		if clonedValue == nil {
			clonedParameters = map[string]any{}
		} else {
			return nil, errors.New("workflow parameters must be an object")
		}
	}
	if err := definition.validator.Validate(clonedParameters); err != nil {
		return nil, fmt.Errorf("workflow parameters do not match schema: %w", err)
	}
	runID, err := randomIdentifier("run")
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	record := &runRecord{
		Version: runRecordVersion, Definition: definition.definition, Digest: definitionDigest(definition.definition),
		Leases: make(map[string]leaseRecord), Commands: make(map[string]commandRecord),
		Run: Run{
			ID: runID, WorkflowName: workflowName, Revision: definition.definition.Revision,
			DefinitionDigest: definitionDigest(definition.definition),
			IdempotencyKey:   idempotencyKey, Parameters: clonedParameters, State: RunRunning,
			Context: make(map[string]string), Nodes: make(map[string]*NodeRun, len(definition.nodes)),
			CreatedAt: now, UpdatedAt: now,
		},
	}
	for id := range definition.nodes {
		record.Run.Nodes[id] = &NodeRun{
			ID: id, State: NodePending, Activities: make(map[string]*Activity), ResolvedInputs: make(map[string]bool),
		}
	}
	record.Run.Nodes[definition.definition.Entry].State = NodeReady
	var events []Event
	s.appendEvent(record, &events, now, "run_started", "", "", nil)
	if err := s.advance(ctx, record, definition, &events, now); err != nil {
		return nil, err
	}
	if err := s.store.create(record, events); err != nil {
		if errors.Is(err, ErrConflict) && idempotencyKey != "" {
			existingID, lookupErr := s.store.idempotentRun(workflowName, idempotencyKey)
			if lookupErr == nil && existingID != "" && s.runs[existingID] != nil {
				return cloneRun(s.runs[existingID].Run)
			}
		}
		return nil, err
	}
	s.runs[runID] = record
	s.definitions[runID] = definition
	s.signals[runID] = make(chan struct{})
	s.notify(runID)
	return cloneRun(record.Run)
}

func (s *Service) GetRun(runID string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record := s.runs[runID]
	if record == nil {
		return nil, ErrNotFound
	}
	return cloneRun(record.Run)
}

func (s *Service) ListRuns() ([]Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runs := make([]Run, 0, len(s.runs))
	for _, record := range s.runs {
		cloned, err := cloneRun(record.Run)
		if err != nil {
			return nil, err
		}
		runs = append(runs, *cloned)
	}
	sort.Slice(runs, func(i, j int) bool {
		if runs[i].CreatedAt.Equal(runs[j].CreatedAt) {
			return runs[i].ID < runs[j].ID
		}
		return runs[i].CreatedAt.Before(runs[j].CreatedAt)
	})
	return runs, nil
}

func (s *Service) RunCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.runs)
}

func (s *Service) CancelRun(runID, commandID, message string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, "", "cancel-run", message)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	now := time.Now().UTC()
	var events []Event
	s.finishRun(record, RunCancelled, boundedMessage(message), now, &events)
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) PauseRun(runID, commandID, message string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, "", "pause-run", message)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	if record.Run.State != RunRunning {
		return nil, fmt.Errorf("%w: run state is %s", ErrConflict, record.Run.State)
	}
	now := time.Now().UTC()
	record.Run.State = RunPaused
	var events []Event
	s.appendEvent(record, &events, now, "run_paused", "", "", map[string]any{"reason": boundedMessage(message)})
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) ResumeRun(runID, commandID string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, "", "resume-run", nil)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	if record.Run.State != RunPaused {
		return nil, fmt.Errorf("%w: run state is %s", ErrConflict, record.Run.State)
	}
	now := time.Now().UTC()
	record.Run.State = RunRunning
	var events []Event
	s.appendEvent(record, &events, now, "run_resumed", "", "", nil)
	if err := s.advance(context.Background(), record, s.definitions[runID], &events, now); err != nil {
		return nil, err
	}
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) DeleteRun(runID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return ErrShuttingDown
	}
	record := s.runs[runID]
	if record == nil {
		return ErrNotFound
	}
	if !terminalRunState(record.Run.State) {
		return fmt.Errorf("%w: only a terminal run can be deleted", ErrConflict)
	}
	if err := s.store.delete(record); err != nil {
		return err
	}
	if signal := s.signals[runID]; signal != nil {
		close(signal)
	}
	delete(s.signals, runID)
	delete(s.definitions, runID)
	delete(s.runs, runID)
	return nil
}

func (s *Service) ClaimActivity(executorID string, kinds []string) (*ActivityClaim, error) {
	if !identifierPattern.MatchString(executorID) {
		return nil, fmt.Errorf("executor id must match %s", identifierPattern)
	}
	kindSet := make(map[string]struct{}, len(kinds))
	for _, kind := range kinds {
		if !identifierPattern.MatchString(kind) {
			return nil, fmt.Errorf("executor kind must match %s", identifierPattern)
		}
		kindSet[kind] = struct{}{}
	}
	if len(kindSet) == 0 {
		return nil, errors.New("at least one executor kind is required")
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return nil, ErrShuttingDown
	}
	if s.activeAttemptCount() >= s.config.MaxActiveAttempts {
		return nil, ErrCapacity
	}
	now := time.Now().UTC()
	runIDs := s.sortedRunIDs()
	for _, runID := range runIDs {
		original := s.runs[runID]
		if original.Run.State != RunRunning {
			continue
		}
		definition := s.definitions[runID]
		for _, nodeID := range definition.order {
			node := original.Run.Nodes[nodeID]
			operations := make([]string, 0, len(node.Activities))
			for operation := range node.Activities {
				operations = append(operations, operation)
			}
			sort.Strings(operations)
			for _, operation := range operations {
				activity := node.Activities[operation]
				if _, accepted := kindSet[activity.ExecutorKind]; !accepted {
					continue
				}
				if activity.State != ActivityScheduled && activity.State != ActivityWaitingRetry {
					continue
				}
				if !activity.AvailableAt.IsZero() && activity.AvailableAt.After(now) {
					continue
				}
				record, err := cloneRunRecord(original)
				if err != nil {
					return nil, err
				}
				activity = record.Run.Nodes[nodeID].Activities[operation]
				token, tokenHash, err := randomToken()
				if err != nil {
					return nil, err
				}
				attemptNumber := len(activity.Attempts) + 1
				deadline := now.Add(s.config.LeaseDuration)
				activity.State = ActivityClaimed
				activity.UpdatedAt = now
				activity.Attempts = append(activity.Attempts, Attempt{
					Number: attemptNumber, State: AttemptClaimed, ExecutorID: executorID, LeaseDeadline: deadline,
				})
				record.Leases[activity.ID] = leaseRecord{Attempt: attemptNumber, TokenHash: tokenHash, Deadline: deadline}
				record.Run.Nodes[nodeID].State = NodeWaitingActivity
				var events []Event
				s.appendEvent(record, &events, now, "activity_claimed", nodeID, activity.ID, nil)
				if err := s.commit(record, events); err != nil {
					return nil, err
				}
				input, _ := cloneJSONValue(activity.Input)
				return &ActivityClaim{
					RunID: runID, NodeID: nodeID, ActivityID: activity.ID, OperationID: activity.OperationID,
					ExecutorKind: activity.ExecutorKind, Input: input, Attempt: attemptNumber,
					LeaseToken: token, LeaseDeadline: deadline,
				}, nil
			}
		}
	}
	return nil, ErrNotFound
}

func (s *Service) StartActivity(runID, activityID string, attempt int, token, commandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "start-activity", attempt)
	if err != nil || duplicate {
		return err
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := verifyLease(record, activity, attempt, token, now); err != nil {
		return err
	}
	if activity.State != ActivityClaimed {
		return fmt.Errorf("%w: activity state is %s", ErrConflict, activity.State)
	}
	activity.State = ActivityRunning
	activity.UpdatedAt = now
	activity.Attempts[len(activity.Attempts)-1].State = AttemptRunning
	activity.Attempts[len(activity.Attempts)-1].StartedAt = now
	var events []Event
	s.appendEvent(record, &events, now, "activity_started", node.ID, activity.ID, nil)
	return s.commit(record, events)
}

func (s *Service) HeartbeatActivity(runID, activityID string, attempt int, token, commandID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "heartbeat-activity", attempt)
	if err != nil || duplicate {
		return err
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	if err := verifyLease(record, activity, attempt, token, now); err != nil {
		return err
	}
	if activity.State != ActivityClaimed && activity.State != ActivityRunning {
		return fmt.Errorf("%w: activity state is %s", ErrConflict, activity.State)
	}
	deadline := now.Add(s.config.LeaseDuration)
	lease := record.Leases[activity.ID]
	lease.Deadline = deadline
	record.Leases[activity.ID] = lease
	activity.Attempts[len(activity.Attempts)-1].LeaseDeadline = deadline
	activity.UpdatedAt = now
	var events []Event
	s.appendEvent(record, &events, now, "activity_heartbeat", node.ID, activity.ID, nil)
	return s.commit(record, events)
}

func (s *Service) CompleteActivity(runID, activityID string, attempt int, token, commandID string, result ActivityResult) (*Run, error) {
	result, err := normalizeActivityResult(result)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "complete-activity", result)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := verifyLease(record, activity, attempt, token, now); err != nil {
		return nil, err
	}
	if activity.State != ActivityClaimed && activity.State != ActivityRunning {
		return nil, fmt.Errorf("%w: activity state is %s", ErrConflict, activity.State)
	}
	activity.State = ActivitySucceeded
	activity.Result = &result
	activity.UpdatedAt = now
	current := &activity.Attempts[len(activity.Attempts)-1]
	current.State = AttemptSucceeded
	current.FinishedAt = now
	delete(record.Leases, activity.ID)
	node.State = NodeEvaluating
	var events []Event
	s.appendEvent(record, &events, now, "activity_completed", node.ID, activity.ID, nil)
	if err := s.advance(context.Background(), record, s.definitions[runID], &events, now); err != nil {
		return nil, err
	}
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) FailAttempt(runID, activityID string, attempt int, token, commandID, message string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "fail-attempt", message)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := verifyLease(record, activity, attempt, token, now); err != nil {
		return nil, err
	}
	var events []Event
	s.failCurrentAttempt(record, node, activity, AttemptFailed, boundedMessage(message), now, &events)
	if err := s.advance(context.Background(), record, s.definitions[runID], &events, now); err != nil {
		return nil, err
	}
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) RequestIntervention(runID, activityID string, attempt int, token, commandID, reason string) (*Run, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "request-intervention", reason)
	if err != nil {
		return nil, err
	}
	if duplicate {
		return cloneRun(s.runs[runID].Run)
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if err := verifyLease(record, activity, attempt, token, now); err != nil {
		return nil, err
	}
	if activity.State != ActivityClaimed && activity.State != ActivityRunning {
		return nil, fmt.Errorf("%w: activity state is %s", ErrConflict, activity.State)
	}
	activity.State = ActivityNeedsIntervention
	activity.InterventionReason = boundedMessage(reason)
	activity.UpdatedAt = now
	node.State = NodeNeedsIntervention
	lease := record.Leases[activity.ID]
	lease.Deadline = time.Time{}
	record.Leases[activity.ID] = lease
	var events []Event
	s.appendEvent(record, &events, now, "activity_needs_intervention", node.ID, activity.ID, nil)
	if err := s.commit(record, events); err != nil {
		return nil, err
	}
	return cloneRun(record.Run)
}

func (s *Service) ResolveIntervention(runID, activityID, commandID string, resolution InterventionResolution) (*ActivityClaim, *Run, error) {
	if !identifierPattern.MatchString(resolution.Principal) {
		return nil, nil, fmt.Errorf("intervention principal must match %s", identifierPattern)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	record, err := s.mutableRecord(runID)
	if err != nil {
		return nil, nil, err
	}
	duplicate, err := registerCommand(record, commandID, activityID, "resolve-intervention", resolution)
	if err != nil {
		return nil, nil, err
	}
	if duplicate {
		run, cloneErr := cloneRun(s.runs[runID].Run)
		return nil, run, cloneErr
	}
	node, activity, err := findActivity(record, activityID)
	if err != nil {
		return nil, nil, err
	}
	if activity.State != ActivityNeedsIntervention {
		return nil, nil, fmt.Errorf("%w: activity state is %s", ErrConflict, activity.State)
	}
	now := time.Now().UTC()
	var events []Event
	var claim *ActivityClaim
	switch resolution.Action {
	case InterventionContinue:
		token, tokenHash, tokenErr := randomToken()
		if tokenErr != nil {
			return nil, nil, tokenErr
		}
		deadline := now.Add(s.config.LeaseDuration)
		current := &activity.Attempts[len(activity.Attempts)-1]
		current.State = AttemptRunning
		current.LeaseDeadline = deadline
		if current.StartedAt.IsZero() {
			current.StartedAt = now
		}
		activity.State = ActivityRunning
		activity.InterventionReason = ""
		record.Leases[activity.ID] = leaseRecord{Attempt: current.Number, TokenHash: tokenHash, Deadline: deadline}
		claim = &ActivityClaim{
			RunID: runID, NodeID: node.ID, ActivityID: activity.ID, OperationID: activity.OperationID,
			ExecutorKind: activity.ExecutorKind, Input: activity.Input, Attempt: current.Number,
			LeaseToken: token, LeaseDeadline: deadline,
		}
	case InterventionRetry:
		current := &activity.Attempts[len(activity.Attempts)-1]
		current.State = AttemptFailed
		current.Error = boundedMessage(resolution.Message)
		current.FinishedAt = now
		delete(record.Leases, activity.ID)
		activity.State = ActivityScheduled
		activity.AvailableAt = now
		activity.InterventionReason = ""
		node.State = NodeWaitingActivity
	case InterventionResolve:
		if resolution.Result == nil {
			return nil, nil, errors.New("resolve intervention requires an activity result")
		}
		normalized, cloneErr := normalizeActivityResult(*resolution.Result)
		if cloneErr != nil {
			return nil, nil, cloneErr
		}
		activity.Result = &normalized
		activity.State = ActivitySucceeded
		activity.InterventionReason = ""
		current := &activity.Attempts[len(activity.Attempts)-1]
		current.State = AttemptSucceeded
		current.FinishedAt = now
		delete(record.Leases, activity.ID)
		node.State = NodeEvaluating
	case InterventionFail:
		activity.State = ActivityFailed
		activity.InterventionReason = ""
		current := &activity.Attempts[len(activity.Attempts)-1]
		current.State = AttemptFailed
		current.Error = boundedMessage(resolution.Message)
		current.FinishedAt = now
		delete(record.Leases, activity.ID)
		s.failNode(record, node, boundedMessage(resolution.Message), now, &events)
	case InterventionCancel:
		s.finishRun(record, RunCancelled, boundedMessage(resolution.Message), now, &events)
	default:
		return nil, nil, fmt.Errorf("unknown intervention action %q", resolution.Action)
	}
	activity.UpdatedAt = now
	s.appendEvent(record, &events, now, "intervention_resolved", node.ID, activity.ID, map[string]any{"action": resolution.Action, "principal": resolution.Principal})
	if record.Run.State == RunRunning {
		if err := s.advance(context.Background(), record, s.definitions[runID], &events, now); err != nil {
			return nil, nil, err
		}
	}
	if err := s.commit(record, events); err != nil {
		return nil, nil, err
	}
	run, err := cloneRun(record.Run)
	return claim, run, err
}

func (s *Service) Reconcile(now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return ErrShuttingDown
	}
	now = now.UTC()
	for _, runID := range s.sortedRunIDs() {
		original := s.runs[runID]
		if terminalRunState(original.Run.State) {
			continue
		}
		record, err := cloneRunRecord(original)
		if err != nil {
			return err
		}
		changed := false
		var events []Event
		for _, node := range record.Run.Nodes {
			nodeDefinition := s.definitions[runID].nodes[node.ID].definition
			if node.Acquired && !node.StartedAt.IsZero() && !node.StartedAt.Add(nodeDefinition.Timeout).After(now) {
				s.failNode(record, node, "workflow node timeout exceeded", now, &events)
				changed = true
				break
			}
			for _, activity := range node.Activities {
				switch activity.State {
				case ActivityWaitingRetry:
					if !activity.AvailableAt.After(now) {
						activity.State = ActivityScheduled
						activity.AvailableAt = time.Time{}
						node.State = NodeWaitingActivity
						activity.UpdatedAt = now
						s.appendEvent(record, &events, now, "activity_retry_ready", node.ID, activity.ID, nil)
						changed = true
					}
				case ActivityClaimed, ActivityRunning:
					lease, exists := record.Leases[activity.ID]
					if !exists || lease.Deadline.IsZero() || !lease.Deadline.After(now) {
						s.failCurrentAttempt(record, node, activity, AttemptLost, "activity lease expired", now, &events)
						changed = true
					}
				}
			}
		}
		beforeAdvance := record.Run.EventSequence
		if record.Run.State == RunRunning {
			if err := s.advance(context.Background(), record, s.definitions[runID], &events, now); err != nil {
				return err
			}
		}
		changed = changed || record.Run.EventSequence != beforeAdvance
		if !changed {
			continue
		}
		if err := s.commit(record, events); err != nil {
			return err
		}
	}
	return nil
}

func (s *Service) ListEvents(runID string, after uint64, limit int) ([]Event, error) {
	return s.store.events(runID, after, limit)
}

// ObserveEvents replays events after the cursor and then follows committed
// events until ctx is cancelled.
func (s *Service) ObserveEvents(ctx context.Context, runID string, after uint64) (<-chan Event, error) {
	s.mu.Lock()
	if s.runs[runID] == nil {
		s.mu.Unlock()
		return nil, ErrNotFound
	}
	s.mu.Unlock()
	output := make(chan Event, 32)
	go func() {
		defer close(output)
		cursor := after
		for {
			s.mu.Lock()
			record := s.runs[runID]
			if record == nil {
				s.mu.Unlock()
				return
			}
			latest := record.Run.EventSequence
			signal := s.signals[runID]
			s.mu.Unlock()
			if cursor < latest {
				events, err := s.store.events(runID, cursor, 4096)
				if err != nil {
					return
				}
				for _, event := range events {
					select {
					case output <- event:
						cursor = event.Sequence
					case <-ctx.Done():
						return
					}
				}
				continue
			}
			select {
			case <-signal:
			case <-ctx.Done():
				return
			}
		}
	}()
	return output, nil
}

func (s *Service) BeginShutdown() {
	s.mu.Lock()
	s.closing = true
	s.mu.Unlock()
}

func (s *Service) Close() error {
	var closeErr error
	s.closeOnce.Do(func() {
		s.BeginShutdown()
		close(s.stop)
		<-s.done
		s.mu.Lock()
		defer s.mu.Unlock()
		for runID, signal := range s.signals {
			close(signal)
			delete(s.signals, runID)
		}
		closeErr = s.store.close()
	})
	return closeErr
}

func (s *Service) advance(ctx context.Context, record *runRecord, definition *compiledDefinition, events *[]Event, now time.Time) error {
	for {
		if record.Run.State != RunRunning {
			return nil
		}
		changed := false
		for _, nodeID := range definition.order {
			node := record.Run.Nodes[nodeID]
			if node.State != NodePending {
				continue
			}
			incoming := definition.incoming[nodeID]
			selected := 0
			for _, sourceID := range incoming {
				if _, resolved := node.ResolvedInputs[sourceID]; resolved {
					if node.ResolvedInputs[sourceID] {
						selected++
					}
					continue
				}
				source := record.Run.Nodes[sourceID]
				if !terminalNodeState(source.State) {
					continue
				}
				value := source.State == NodeSucceeded && routeTargets(definition.nodes[sourceID].definition.Routes[source.SelectedRoute], nodeID)
				node.ResolvedInputs[sourceID] = value
				if value {
					selected++
				}
				changed = true
			}
			resolved := len(node.ResolvedInputs)
			join := definition.nodes[nodeID].definition.Join
			switch {
			case join == JoinAny && selected > 0:
				node.State = NodeReady
				s.appendEvent(record, events, now, "node_ready", nodeID, "", nil)
				changed = true
			case resolved == len(incoming) && join == JoinAll && selected == len(incoming):
				node.State = NodeReady
				s.appendEvent(record, events, now, "node_ready", nodeID, "", nil)
				changed = true
			case resolved == len(incoming) && ((join == JoinAll && selected != len(incoming)) || (join == JoinAny && selected == 0)):
				node.State = NodeSkipped
				node.FinishedAt = now
				s.appendEvent(record, events, now, "node_skipped", nodeID, "", nil)
				changed = true
			}
		}
		for _, nodeID := range definition.order {
			node := record.Run.Nodes[nodeID]
			if node.State != NodeReady && node.State != NodeEvaluating {
				continue
			}
			compiledNode := definition.nodes[nodeID]
			if compiledNode.definition.Terminal != TerminalNone {
				node.State = NodeSucceeded
				node.StartedAt = now
				node.FinishedAt = now
				s.appendEvent(record, events, now, "terminal_reached", nodeID, "", map[string]any{"outcome": compiledNode.definition.Terminal})
				if compiledNode.definition.Terminal == TerminalSucceeded {
					s.finishRun(record, RunSucceeded, "", now, events)
				} else {
					s.finishRun(record, RunFailed, "workflow reached failed terminal", now, events)
				}
				return nil
			}
			if node.State == NodeReady {
				if s.acquiredNodeCount(record) >= definition.definition.MaxParallelism || !s.resourcesAvailable(record, definition, compiledNode.definition.Resources) {
					continue
				}
				node.State = NodeEvaluating
				node.Acquired = true
				if node.StartedAt.IsZero() {
					node.StartedAt = now
				}
				s.appendEvent(record, events, now, "node_started", nodeID, "", nil)
				changed = true
			}
			result, err := runScript(
				ctx, node, compiledNode, record.Run.Parameters, record.Run.Context, nodeSummaries(record.Run.Nodes),
			)
			if err != nil {
				var suspension *suspendActivity
				if errors.As(err, &suspension) {
					if !suspension.Existing {
						activityID, idErr := randomIdentifier("act")
						if idErr != nil {
							return idErr
						}
						node.Activities[suspension.OperationID] = &Activity{
							ID: activityID, OperationID: suspension.OperationID, ExecutorKind: suspension.ExecutorKind,
							Input: suspension.Input, InputHash: suspension.InputHash, State: ActivityScheduled,
							CreatedAt: now, UpdatedAt: now,
						}
						s.appendEvent(record, events, now, "activity_scheduled", nodeID, activityID, nil)
					}
					activity := node.Activities[suspension.OperationID]
					switch activity.State {
					case ActivityWaitingRetry:
						node.State = NodeWaitingRetry
					case ActivityNeedsIntervention:
						node.State = NodeNeedsIntervention
					default:
						node.State = NodeWaitingActivity
					}
					changed = true
					continue
				}
				s.failNode(record, node, boundedMessage(err.Error()), now, events)
				return nil
			}
			s.applyContextChanges(record, nodeID, result.ContextWrites, result.ContextDeletes, now, events)
			node.State = NodeSucceeded
			node.SelectedRoute = result.Route
			node.Acquired = false
			node.FinishedAt = now
			s.appendEvent(record, events, now, "node_succeeded", nodeID, "", map[string]any{"route": result.Route})
			changed = true
		}
		if !changed {
			allTerminal := true
			for _, node := range record.Run.Nodes {
				if !terminalNodeState(node.State) {
					allTerminal = false
					break
				}
			}
			if allTerminal {
				s.finishRun(record, RunFailed, "workflow completed without reaching a terminal node", now, events)
			}
			return nil
		}
	}
}

func (s *Service) applyContextChanges(
	record *runRecord,
	nodeID string,
	writes map[string]string,
	deletes map[string]struct{},
	now time.Time,
	events *[]Event,
) {
	setKeys := make([]string, 0, len(writes))
	deletedKeys := make([]string, 0, len(deletes))
	for key := range deletes {
		if _, exists := record.Run.Context[key]; exists {
			delete(record.Run.Context, key)
			deletedKeys = append(deletedKeys, key)
		}
	}
	for key, value := range writes {
		if current, exists := record.Run.Context[key]; !exists || current != value {
			record.Run.Context[key] = value
			setKeys = append(setKeys, key)
		}
	}
	if len(setKeys) == 0 && len(deletedKeys) == 0 {
		return
	}
	sort.Strings(setKeys)
	sort.Strings(deletedKeys)
	record.Run.ContextVersion++
	s.appendEvent(record, events, now, "context_updated", nodeID, "", map[string]any{
		"version": record.Run.ContextVersion, "set_keys": setKeys, "deleted_keys": deletedKeys,
	})
}

func (s *Service) failCurrentAttempt(record *runRecord, node *NodeRun, activity *Activity, state AttemptState, message string, now time.Time, events *[]Event) {
	current := &activity.Attempts[len(activity.Attempts)-1]
	current.State = state
	current.Error = message
	current.FinishedAt = now
	delete(record.Leases, activity.ID)
	activity.UpdatedAt = now
	maxAttempts := s.definitions[record.Run.ID].nodes[node.ID].definition.Retry.MaxAttempts
	if len(activity.Attempts) < maxAttempts {
		activity.State = ActivityWaitingRetry
		activity.AvailableAt = now.Add(s.retryBackoff(len(activity.Attempts)))
		node.State = NodeWaitingRetry
		s.appendEvent(record, events, now, "activity_retry_scheduled", node.ID, activity.ID, nil)
		return
	}
	activity.State = ActivityFailed
	s.appendEvent(record, events, now, "activity_failed", node.ID, activity.ID, nil)
	s.failNode(record, node, message, now, events)
}

func (s *Service) failNode(record *runRecord, node *NodeRun, message string, now time.Time, events *[]Event) {
	node.State = NodeFailed
	node.Acquired = false
	node.Error = message
	node.FinishedAt = now
	s.appendEvent(record, events, now, "node_failed", node.ID, "", nil)
	s.finishRun(record, RunFailed, message, now, events)
}

func (s *Service) finishRun(record *runRecord, state RunState, message string, now time.Time, events *[]Event) {
	if terminalRunState(record.Run.State) {
		return
	}
	for _, node := range record.Run.Nodes {
		if terminalNodeState(node.State) {
			continue
		}
		node.State = NodeCancelled
		node.Acquired = false
		node.FinishedAt = now
		for _, activity := range node.Activities {
			if activity.State == ActivitySucceeded || activity.State == ActivityFailed || activity.State == ActivityCancelled {
				continue
			}
			activity.State = ActivityCancelled
			activity.UpdatedAt = now
			if len(activity.Attempts) > 0 {
				current := &activity.Attempts[len(activity.Attempts)-1]
				if current.State == AttemptClaimed || current.State == AttemptRunning {
					current.State = AttemptCancelled
					current.FinishedAt = now
				}
			}
			delete(record.Leases, activity.ID)
		}
	}
	record.Run.State = state
	record.Run.Error = message
	record.Run.FinishedAt = now
	s.appendEvent(record, events, now, "run_"+strings.ToLower(string(state)), "", "", nil)
}

func (s *Service) resourcesAvailable(candidate *runRecord, candidateDefinition *compiledDefinition, claims []ResourceClaim) bool {
	if len(claims) == 0 {
		return true
	}
	candidateSeen := false
	for runID, record := range s.runs {
		if runID == candidate.Run.ID {
			record = candidate
			candidateSeen = true
		}
		if record.Run.State != RunRunning {
			continue
		}
		definition := s.definitions[runID]
		if runID == candidate.Run.ID {
			definition = candidateDefinition
		}
		if resourceConflict(record, definition, claims) {
			return false
		}
	}
	if !candidateSeen && resourceConflict(candidate, candidateDefinition, claims) {
		return false
	}
	return true
}

func resourceConflict(record *runRecord, definition *compiledDefinition, claims []ResourceClaim) bool {
	for nodeID, node := range record.Run.Nodes {
		if !node.Acquired {
			continue
		}
		for _, held := range definition.nodes[nodeID].definition.Resources {
			for _, requested := range claims {
				if held.Key == requested.Key && (held.Mode == ResourceExclusive || requested.Mode == ResourceExclusive) {
					return true
				}
			}
		}
	}
	return false
}

func (s *Service) mutableRecord(runID string) (*runRecord, error) {
	if s.closing {
		return nil, ErrShuttingDown
	}
	record := s.runs[runID]
	if record == nil {
		return nil, ErrNotFound
	}
	return cloneRunRecord(record)
}

func (s *Service) commit(record *runRecord, events []Event) error {
	record.Run.UpdatedAt = time.Now().UTC()
	if err := s.store.update(record, events); err != nil {
		return err
	}
	s.runs[record.Run.ID] = record
	s.notify(record.Run.ID)
	return nil
}

func (s *Service) appendEvent(record *runRecord, events *[]Event, now time.Time, eventType, nodeID, activityID string, data any) {
	record.Run.EventSequence++
	event := Event{RunID: record.Run.ID, Sequence: record.Run.EventSequence, Time: now, Type: eventType, NodeID: nodeID, ActivityID: activityID}
	if data != nil {
		event.Data, _ = json.Marshal(data)
	}
	*events = append(*events, event)
}

func (s *Service) notify(runID string) {
	signal := s.signals[runID]
	if signal != nil {
		close(signal)
	}
	s.signals[runID] = make(chan struct{})
}

func (s *Service) activeRunCount() int {
	count := 0
	for _, record := range s.runs {
		if !terminalRunState(record.Run.State) {
			count++
		}
	}
	return count
}

func (s *Service) activeAttemptCount() int {
	count := 0
	for _, record := range s.runs {
		for _, node := range record.Run.Nodes {
			for _, activity := range node.Activities {
				if activity.State == ActivityClaimed || activity.State == ActivityRunning {
					count++
				}
			}
		}
	}
	return count
}

func (s *Service) acquiredNodeCount(record *runRecord) int {
	count := 0
	for _, node := range record.Run.Nodes {
		if node.Acquired {
			count++
		}
	}
	return count
}

func (s *Service) sortedRunIDs() []string {
	ids := make([]string, 0, len(s.runs))
	for id := range s.runs {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	return ids
}

func (s *Service) retryBackoff(attempts int) time.Duration {
	delay := s.config.RetryInitialBackoff
	for index := 1; index < attempts && delay < s.config.RetryMaxBackoff; index++ {
		if delay > s.config.RetryMaxBackoff/2 {
			return s.config.RetryMaxBackoff
		}
		delay *= 2
	}
	if delay > s.config.RetryMaxBackoff {
		return s.config.RetryMaxBackoff
	}
	return delay
}

func findActivity(record *runRecord, activityID string) (*NodeRun, *Activity, error) {
	for _, node := range record.Run.Nodes {
		for _, activity := range node.Activities {
			if activity.ID == activityID {
				return node, activity, nil
			}
		}
	}
	return nil, nil, ErrNotFound
}

func verifyLease(record *runRecord, activity *Activity, attempt int, token string, now time.Time) error {
	lease, exists := record.Leases[activity.ID]
	if !exists || lease.Attempt != attempt || len(activity.Attempts) == 0 || activity.Attempts[len(activity.Attempts)-1].Number != attempt {
		return ErrLeaseExpired
	}
	if lease.Deadline.IsZero() || !lease.Deadline.After(now) {
		return ErrLeaseExpired
	}
	hash := sha256.Sum256([]byte(token))
	actual := hex.EncodeToString(hash[:])
	if subtle.ConstantTimeCompare([]byte(actual), []byte(lease.TokenHash)) != 1 {
		return ErrLeaseExpired
	}
	return nil
}

func registerCommand(record *runRecord, commandID, activityID, action string, payload any) (bool, error) {
	if !identifierPattern.MatchString(commandID) {
		return false, fmt.Errorf("command id must match %s", identifierPattern)
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		return false, err
	}
	digest := sha256.Sum256(encoded)
	entry := commandRecord{ActivityID: activityID, Action: action, PayloadHash: hex.EncodeToString(digest[:])}
	if existing, duplicate := record.Commands[commandID]; duplicate {
		if existing != entry {
			return false, fmt.Errorf("%w: command id %q was already used for another operation", ErrConflict, commandID)
		}
		return true, nil
	}
	record.Commands[commandID] = entry
	return false, nil
}

func nodeSummaries(nodes map[string]*NodeRun) map[string]any {
	result := make(map[string]any, len(nodes))
	for id, node := range nodes {
		if terminalNodeState(node.State) {
			result[id] = map[string]any{"state": node.State, "route": node.SelectedRoute}
		}
	}
	return result
}

func routeTargets(targets []string, expected string) bool {
	for _, target := range targets {
		if target == expected {
			return true
		}
	}
	return false
}

func randomIdentifier(prefix string) (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("generate workflow id: %w", err)
	}
	return prefix + "-" + hex.EncodeToString(value[:]), nil
}

func randomToken() (string, string, error) {
	var value [32]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", "", fmt.Errorf("generate workflow lease token: %w", err)
	}
	token := hex.EncodeToString(value[:])
	digest := sha256.Sum256([]byte(token))
	return token, hex.EncodeToString(digest[:]), nil
}

func boundedMessage(message string) string {
	const max = 4096
	message = strings.TrimSpace(message)
	if len(message) <= max {
		return message
	}
	return message[:max]
}

func validateRecoveredRecord(record *runRecord, definition *compiledDefinition, persistence *store) error {
	if record.Run.ID == "" || record.Run.WorkflowName != definition.definition.Name ||
		record.Run.Revision != definition.definition.Revision || record.Run.DefinitionDigest != record.Digest {
		return errors.New("run identity does not match its definition snapshot")
	}
	if !validRunState(record.Run.State) {
		return fmt.Errorf("invalid run state %q", record.Run.State)
	}
	if err := definition.validator.Validate(record.Run.Parameters); err != nil {
		return fmt.Errorf("persisted parameters do not match schema: %w", err)
	}
	if err := validateWorkflowContext(record.Run.Context); err != nil {
		return fmt.Errorf("persisted workflow context is invalid: %w", err)
	}
	if len(record.Run.Nodes) != len(definition.nodes) {
		return errors.New("persisted node set does not match definition")
	}
	for nodeID, nodeDefinition := range definition.nodes {
		node := record.Run.Nodes[nodeID]
		if node == nil || node.ID != nodeID || !validNodeState(node.State) {
			return fmt.Errorf("node %q is missing or has invalid identity/state", nodeID)
		}
		for operationID, activity := range node.Activities {
			executorKind, declared := nodeDefinition.operations[operationID]
			if !declared || activity == nil || activity.OperationID != operationID || activity.ExecutorKind != executorKind ||
				!validActivityState(activity.State) {
				return fmt.Errorf("node %q activity %q does not match the compiled journal", nodeID, operationID)
			}
			if activity.State == ActivitySucceeded && activity.Result == nil {
				return fmt.Errorf("node %q activity %q is succeeded without a result", nodeID, operationID)
			}
			encoded, err := json.Marshal(activity.Input)
			if err != nil || len(encoded) > maxInputOutputBytes {
				return fmt.Errorf("node %q activity %q has invalid input", nodeID, operationID)
			}
			digest := sha256.Sum256(encoded)
			if hex.EncodeToString(digest[:]) != activity.InputHash {
				return fmt.Errorf("node %q activity %q input digest does not match", nodeID, operationID)
			}
			for index, attempt := range activity.Attempts {
				if attempt.Number != index+1 || !validAttemptState(attempt.State) {
					return fmt.Errorf("activity %q attempt history is invalid", activity.ID)
				}
			}
			lease, hasLease := record.Leases[activity.ID]
			needsLease := activity.State == ActivityClaimed || activity.State == ActivityRunning || activity.State == ActivityNeedsIntervention
			if needsLease != hasLease {
				return fmt.Errorf("activity %q lease presence does not match state", activity.ID)
			}
			if hasLease && (len(activity.Attempts) == 0 || lease.Attempt != activity.Attempts[len(activity.Attempts)-1].Number || len(lease.TokenHash) != 64) {
				return fmt.Errorf("activity %q lease does not match current attempt", activity.ID)
			}
		}
	}
	lastSequence, err := persistence.lastEventSequence(record.Run.ID)
	if err != nil {
		return err
	}
	if lastSequence != record.Run.EventSequence {
		return fmt.Errorf("event cursor is %d but run records %d", lastSequence, record.Run.EventSequence)
	}
	return nil
}

func validRunState(state RunState) bool {
	switch state {
	case RunPending, RunRunning, RunPaused, RunSucceeded, RunFailed, RunCancelled:
		return true
	default:
		return false
	}
}

func validNodeState(state NodeState) bool {
	switch state {
	case NodePending, NodeReady, NodeEvaluating, NodeWaitingActivity, NodeWaitingRetry, NodeNeedsIntervention,
		NodeSucceeded, NodeFailed, NodeCancelled, NodeSkipped:
		return true
	default:
		return false
	}
}

func validActivityState(state ActivityState) bool {
	switch state {
	case ActivityScheduled, ActivityClaimed, ActivityRunning, ActivityWaitingRetry, ActivityNeedsIntervention,
		ActivitySucceeded, ActivityFailed, ActivityCancelled:
		return true
	default:
		return false
	}
}

func validAttemptState(state AttemptState) bool {
	switch state {
	case AttemptClaimed, AttemptRunning, AttemptSucceeded, AttemptFailed, AttemptLost, AttemptCancelled:
		return true
	default:
		return false
	}
}

func normalizeActivityResult(result ActivityResult) (ActivityResult, error) {
	fields := []struct {
		name     string
		value    string
		max      int
		required bool
	}{
		{name: "status", value: result.Status, max: 128, required: true},
		{name: "code", value: result.Code, max: 256},
		{name: "message", value: result.Message, max: 4096},
		{name: "external_ref", value: result.ExternalRef, max: 2048},
	}
	for _, field := range fields {
		if field.required && strings.TrimSpace(field.value) == "" {
			return ActivityResult{}, fmt.Errorf("activity result %s is required", field.name)
		}
		if len(field.value) > field.max || strings.ContainsRune(field.value, '\x00') {
			return ActivityResult{}, fmt.Errorf("activity result %s must not contain NUL and must be at most %d bytes", field.name, field.max)
		}
	}
	output, err := cloneJSONValue(result.Output)
	if err != nil {
		return ActivityResult{}, fmt.Errorf("normalize activity result output: %w", err)
	}
	result.Output = output
	return result, nil
}
