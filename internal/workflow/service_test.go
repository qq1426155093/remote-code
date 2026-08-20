package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"
)

func TestServiceActivityReplayAndBusinessFallback(t *testing.T) {
	service := newTestService(t)

	run, err := service.StartRun(t.Context(), "review", "first-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatalf("start run: %v", err)
	}
	if run.State != RunRunning || run.Nodes["work"].State != NodeWaitingActivity {
		t.Fatalf("run after start = %#v", run)
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatalf("claim activity: %v", err)
	}
	if err := service.StartActivity(claim.RunID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "start-one"); err != nil {
		t.Fatalf("start activity: %v", err)
	}
	run, err = service.CompleteActivity(claim.RunID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "complete-one", ActivityResult{Status: "not-ok", Output: map[string]any{"reason": "review rejected"}})
	if err != nil {
		t.Fatalf("complete activity: %v", err)
	}
	if run.State != RunFailed || run.Nodes["work"].SelectedRoute != "failed" {
		t.Fatalf("run after completion = %#v", run)
	}
	if len(run.Nodes["work"].Activities) != 1 {
		t.Fatalf("activity count = %d, want 1", len(run.Nodes["work"].Activities))
	}
	events, err := service.ListEvents(run.ID, 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) == 0 || events[len(events)-1].Type != "run_failed" {
		t.Fatalf("events = %#v", events)
	}
}

func TestServiceStartRunAndCommandsAreIdempotent(t *testing.T) {
	service := newTestService(t)
	run1, err := service.StartRun(t.Context(), "review", "same-key", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	run2, err := service.StartRun(t.Context(), "review", "same-key", map[string]any{"value": "different"})
	if err != nil {
		t.Fatal(err)
	}
	if run1.ID != run2.ID {
		t.Fatalf("idempotent runs have ids %q and %q", run1.ID, run2.ID)
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.StartActivity(claim.RunID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "start-same"); err != nil {
		t.Fatal(err)
	}
	if err := service.StartActivity(claim.RunID, claim.ActivityID, claim.Attempt, "expired-or-hidden", "start-same"); err != nil {
		t.Fatalf("duplicate command should return first success: %v", err)
	}
	if err := service.HeartbeatActivity(claim.RunID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "start-same"); !errors.Is(err, ErrConflict) {
		t.Fatalf("reused command id error = %v", err)
	}
}

func TestServiceLeaseExpiryRetriesAndRejectsLateCompletion(t *testing.T) {
	service := newTestService(t)
	run, err := service.StartRun(t.Context(), "review", "lease-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(claim.LeaseDeadline.Add(time.Millisecond)); err != nil {
		t.Fatal(err)
	}
	_, err = service.CompleteActivity(run.ID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "late-complete", ActivityResult{Status: "ok"})
	if !errors.Is(err, ErrLeaseExpired) {
		t.Fatalf("late completion error = %v", err)
	}
	if err := service.Reconcile(claim.LeaseDeadline.Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	retry, err := service.ClaimActivity("executor-two", []string{"manual"})
	if err != nil {
		current, _ := service.GetRun(run.ID)
		t.Fatalf("claim retry: %v; node=%#v activity=%#v", err, current.Nodes["work"], current.Nodes["work"].Activities["agent"])
	}
	if retry.Attempt != 2 {
		t.Fatalf("retry attempt = %d, want 2", retry.Attempt)
	}
}

func TestServiceManualResolveAndRecovery(t *testing.T) {
	runtimeDirectory := t.TempDir()
	registry := testRegistry(t)
	config := testConfig()
	service, err := New(config, runtimeDirectory, registry)
	if err != nil {
		t.Fatal(err)
	}
	run, err := service.StartRun(t.Context(), "review", "manual-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := service.RequestIntervention(run.ID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "need-human", "tool needs login"); err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	service, err = New(config, runtimeDirectory, registry)
	if err != nil {
		t.Fatalf("reopen service: %v", err)
	}
	t.Cleanup(func() { _ = service.Close() })
	_, run, err = service.ResolveIntervention(run.ID, claim.ActivityID, "human-resolve", InterventionResolution{
		Action: InterventionResolve, Principal: "operator-one", Result: &ActivityResult{Status: "ok"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if run.State != RunSucceeded {
		t.Fatalf("resolved run state = %s", run.State)
	}
}

func TestServiceRecoveryPreservesLargeIntegerReplayInput(t *testing.T) {
	runtimeDirectory := t.TempDir()
	registry := integerTestRegistry(t)
	config := testConfig()
	service, err := New(config, runtimeDirectory, registry)
	if err != nil {
		t.Fatal(err)
	}
	run, err := service.StartRun(t.Context(), "review-integer", "integer-run", map[string]any{"value": int64(9_007_199_254_740_993)})
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	service, err = New(config, runtimeDirectory, registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := service.CompleteActivity(run.ID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "complete-integer", ActivityResult{Status: "ok"})
	if err != nil {
		t.Fatalf("complete after recovery: %v", err)
	}
	if completed.State != RunSucceeded {
		t.Fatalf("completed state = %s", completed.State)
	}
}

func TestServiceRecoveryRejectsCorruptActivityJournal(t *testing.T) {
	runtimeDirectory := t.TempDir()
	registry := testRegistry(t)
	service, err := New(testConfig(), runtimeDirectory, registry)
	if err != nil {
		t.Fatal(err)
	}
	run, err := service.StartRun(t.Context(), "review", "corrupt-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	service.mu.Lock()
	corrupt, err := cloneRunRecord(service.runs[run.ID])
	if err == nil {
		corrupt.Run.Nodes["work"].Activities["agent"].InputHash = "invalid"
		err = service.store.update(corrupt, nil)
	}
	service.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := New(testConfig(), runtimeDirectory, registry); err == nil || !strings.Contains(err.Error(), "input digest does not match") {
		t.Fatalf("reopen corrupt journal error = %v", err)
	}
}

func TestObserveEventsReplaysAndFollows(t *testing.T) {
	service := newTestService(t)
	run, err := service.StartRun(t.Context(), "review", "observe-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	events, err := service.ObserveEvents(ctx, run.ID, 0)
	if err != nil {
		t.Fatal(err)
	}
	first := <-events
	if first.Sequence != 1 || first.Type != "run_started" {
		t.Fatalf("first event = %#v", first)
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	deadline := time.After(2 * time.Second)
	for {
		select {
		case event := <-events:
			if event.Type == "activity_claimed" && event.ActivityID == claim.ActivityID {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for followed event")
		}
	}
}

func TestServicePauseResumeTimeoutAndDelete(t *testing.T) {
	service := newTestService(t)
	run, err := service.StartRun(t.Context(), "review", "lifecycle-run", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	paused, err := service.PauseRun(run.ID, "pause-one", "operator requested pause")
	if err != nil || paused.State != RunPaused {
		t.Fatalf("pause run = %#v, %v", paused, err)
	}
	if _, err := service.ClaimActivity("executor-one", []string{"manual"}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("claim while paused error = %v", err)
	}
	resumed, err := service.ResumeRun(run.ID, "resume-one")
	if err != nil || resumed.State != RunRunning {
		t.Fatalf("resume run = %#v, %v", resumed, err)
	}
	if err := service.DeleteRun(run.ID); !errors.Is(err, ErrConflict) {
		t.Fatalf("delete active run error = %v", err)
	}
	started := resumed.Nodes["work"].StartedAt
	if err := service.Reconcile(started.Add(25 * time.Hour)); err != nil {
		t.Fatal(err)
	}
	timedOut, err := service.GetRun(run.ID)
	if err != nil {
		t.Fatal(err)
	}
	if timedOut.State != RunFailed || timedOut.Nodes["work"].State != NodeFailed {
		t.Fatalf("timed out run = %#v", timedOut)
	}
	if err := service.DeleteRun(run.ID); err != nil {
		t.Fatalf("delete terminal run: %v", err)
	}
	if _, err := service.GetRun(run.ID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("get deleted run error = %v", err)
	}
	recreated, err := service.StartRun(t.Context(), "review", "lifecycle-run", map[string]any{"value": "two"})
	if err != nil || recreated.ID == run.ID {
		t.Fatalf("recreate after delete = %#v, %v", recreated, err)
	}
}

func TestServiceExclusiveResourceSerializesRuns(t *testing.T) {
	service := newTestService(t)
	first, err := service.StartRun(t.Context(), "review", "resource-first", map[string]any{"value": "one"})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.StartRun(t.Context(), "review", "resource-second", map[string]any{"value": "two"})
	if err != nil {
		t.Fatal(err)
	}
	if second.Nodes["work"].State != NodeReady || len(second.Nodes["work"].Activities) != 0 {
		t.Fatalf("blocked second run node = %#v", second.Nodes["work"])
	}
	claim, err := service.ClaimActivity("executor-one", []string{"manual"})
	if err != nil || claim.RunID != first.ID {
		t.Fatalf("first claim = %#v, %v", claim, err)
	}
	if _, err := service.CompleteActivity(claim.RunID, claim.ActivityID, claim.Attempt, claim.LeaseToken, "finish-first", ActivityResult{Status: "ok"}); err != nil {
		t.Fatal(err)
	}
	if err := service.Reconcile(time.Now()); err != nil {
		t.Fatal(err)
	}
	claim, err = service.ClaimActivity("executor-two", []string{"manual"})
	if err != nil || claim.RunID != second.ID {
		t.Fatalf("second claim = %#v, %v", claim, err)
	}
}

func TestServiceStaticFanOutAndAllJoin(t *testing.T) {
	definition := fanOutDefinition(t)
	registry := &Registry{byName: map[string]*compiledDefinition{"fanout": definition}, ordered: []*compiledDefinition{definition}}
	service, err := New(testConfig(), t.TempDir(), registry)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	run, err := service.StartRun(t.Context(), "fanout", "fanout-run", map[string]any{})
	if err != nil {
		t.Fatal(err)
	}
	if len(run.Nodes["left"].Activities) != 1 || len(run.Nodes["right"].Activities) != 1 || run.Nodes["join"].State != NodePending {
		t.Fatalf("fan-out run = %#v", run)
	}
	first, err := service.ClaimActivity("executor-left", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	run, err = service.CompleteActivity(first.RunID, first.ActivityID, first.Attempt, first.LeaseToken, "complete-left", ActivityResult{Status: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if run.Nodes["join"].State != NodePending {
		t.Fatalf("join state after one branch = %s", run.Nodes["join"].State)
	}
	second, err := service.ClaimActivity("executor-right", []string{"manual"})
	if err != nil {
		t.Fatal(err)
	}
	run, err = service.CompleteActivity(second.RunID, second.ActivityID, second.Attempt, second.LeaseToken, "complete-right", ActivityResult{Status: "ok"})
	if err != nil {
		t.Fatal(err)
	}
	if run.State != RunSucceeded || run.Nodes["join"].State != NodeSucceeded {
		t.Fatalf("joined run = %#v", run)
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	service, err := New(testConfig(), t.TempDir(), testRegistry(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close() })
	return service
}

func testConfig() Config {
	return Config{
		Enabled: true, DefinitionFiles: []string{"test.workflow.yaml"}, MaxActiveRuns: 16,
		MaxActiveAttempts: 4, LeaseDuration: time.Second, RetryInitialBackoff: 10 * time.Millisecond,
		RetryMaxBackoff: time.Second, ReconcileInterval: time.Minute,
	}
}

func testRegistry(t *testing.T) *Registry {
	t.Helper()
	definition := Definition{
		Name: "review", Description: "review one task", Revision: 1, Entry: "work", MaxParallelism: 1,
		ParametersSchema: json.RawMessage(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "properties":{"value":{"type":"string"}},
  "required":["value"],
  "additionalProperties":false
}`),
		Nodes: []NodeDefinition{
			{
				ID:        "work",
				Script:    `let result = activity("agent", "manual", {value: parameters.value}); if result.status == "ok" { "done" } else { "failed" }`,
				Routes:    map[string][]string{"done": {"success"}, "failed": {"failure"}},
				Retry:     RetryPolicy{MaxAttempts: 3},
				Resources: []ResourceClaim{{Key: "workspace", Mode: ResourceExclusive}},
			},
			{ID: "success", Terminal: TerminalSucceeded},
			{ID: "failure", Terminal: TerminalFailed},
		},
	}
	compiled, err := compileDefinition(definition)
	if err != nil {
		t.Fatalf("compile test definition: %v", err)
	}
	return &Registry{byName: map[string]*compiledDefinition{"review": compiled}, ordered: []*compiledDefinition{compiled}}
}

func fanOutDefinition(t *testing.T) *compiledDefinition {
	t.Helper()
	definition := Definition{
		Name: "fanout", Description: "fan out and join", Revision: 1, Entry: "dispatch", MaxParallelism: 2,
		ParametersSchema: json.RawMessage(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "properties":{},
  "additionalProperties":false
}`),
		Nodes: []NodeDefinition{
			{ID: "dispatch", Script: `"both"`, Routes: map[string][]string{"both": {"left", "right"}}},
			{ID: "left", Script: `let result = activity("left-op", "manual", {}); "done"`, Routes: map[string][]string{"done": {"join"}}},
			{ID: "right", Script: `let result = activity("right-op", "manual", {}); "done"`, Routes: map[string][]string{"done": {"join"}}},
			{ID: "join", Join: JoinAll, Script: `"done"`, Routes: map[string][]string{"done": {"success"}}},
			{ID: "success", Terminal: TerminalSucceeded},
		},
	}
	compiled, err := compileDefinition(definition)
	if err != nil {
		t.Fatalf("compile fanout definition: %v", err)
	}
	return compiled
}

func integerTestRegistry(t *testing.T) *Registry {
	t.Helper()
	definition, ok := testRegistry(t).Get("review")
	if !ok {
		t.Fatal("missing test definition")
	}
	definition.Name = "review-integer"
	definition.ParametersSchema = json.RawMessage(`{
  "$schema":"https://json-schema.org/draft/2020-12/schema",
  "type":"object",
  "properties":{"value":{"type":"integer"}},
  "required":["value"],
  "additionalProperties":false
}`)
	compiled, err := compileDefinition(definition)
	if err != nil {
		t.Fatalf("compile integer definition: %v", err)
	}
	return &Registry{byName: map[string]*compiledDefinition{definition.Name: compiled}, ordered: []*compiledDefinition{compiled}}
}
