package process

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestPrepareTemplatesCompilesAndRendersPureExpr(t *testing.T) {
	workspace := t.TempDir()
	definition := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("agent-command"))
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatalf("PrepareTemplates() error = %v", err)
	}
	if registry.Count() != 1 {
		t.Fatalf("Count() = %d, want 1", registry.Count())
	}
	summaries := registry.summaries()
	if len(summaries) != 1 || summaries[0].GetName() != "agent" || len(summaries[0].GetRevision()) != 64 ||
		summaries[0].GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY ||
		summaries[0].GetInputMode() != codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED {
		t.Fatalf("summaries() = %+v", summaries)
	}
	template, ok := registry.lookup("agent")
	if !ok {
		t.Fatal("lookup(agent) failed")
	}
	parameters, err := structpb.NewStruct(map[string]any{
		"model": "fast", "prompt": "fix tests safely", "working_directory": "work", "debug": true,
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := template.render(context.Background(), parameters)
	if err != nil {
		t.Fatalf("render() error = %v", err)
	}
	if request.GetCommand() != "agent-command" || request.GetWorkingDirectory() != "work" ||
		request.GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY ||
		request.GetInputMode() != codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED ||
		!reflect.DeepEqual(request.GetArguments(), []string{"--model", "fast", "--prompt", "fix tests safely", "--debug"}) ||
		!reflect.DeepEqual(request.GetEnvironment(), map[string]string{"AGENT_MODEL": "fast"}) {
		t.Fatalf("render() = %+v", request)
	}
	public := template.publicDefinition()
	if public.GetSummary().GetName() != "agent" || public.GetParametersSchema().GetFields()["properties"] == nil {
		t.Fatalf("publicDefinition() = %+v", public)
	}
}

func TestPrepareTemplatesRendersAndFreezesExtraParameters(t *testing.T) {
	definitionSource := strings.Replace(validProcessTemplateDefinition("agent-command"), `      {
        "arguments": concat(["--model", parameters.model], parameters.prompt == "" ? [] : ["--prompt", parameters.prompt], parameters.debug ? ["--debug"] : []),
        "working_directory": parameters.working_directory,
        "environment": {"AGENT_MODEL": parameters.model}
      }`, `      {
        "arguments": concat(extra_parameters.common_arguments, ["--model", extra_parameters["default_model"]], parameters.prompt == "" ? [] : ["--prompt", parameters.prompt], parameters.debug ? ["--debug"] : []),
        "working_directory": parameters.working_directory,
        "environment": extra_parameters.environment
      }`, 1)
	definition := writeProcessTemplateDefinition(t, definitionSource)
	commonArguments := []any{"--profile", "shared"}
	environment := map[string]any{"AGENT_MODEL": "shared-model", "AGENT_MODE": "safe"}
	extraParameters := map[string]any{
		"default_model":    "shared-model",
		"common_arguments": commonArguments,
		"environment":      environment,
	}
	registry, err := PrepareTemplates(TemplateConfig{
		DefinitionFiles: []string{definition},
		ExtraParameters: extraParameters,
	}, t.TempDir())
	if err != nil {
		t.Fatalf("PrepareTemplates() error = %v", err)
	}

	commonArguments[1] = "mutated"
	environment["AGENT_MODEL"] = "mutated"
	extraParameters["default_model"] = "mutated"

	template, _ := registry.lookup("agent")
	parameters, _ := structpb.NewStruct(map[string]any{
		"model": "request-model", "prompt": "fix tests", "working_directory": "work", "debug": true,
	})
	request, err := template.render(context.Background(), parameters)
	if err != nil {
		t.Fatalf("render() error = %v", err)
	}
	if !reflect.DeepEqual(request.GetArguments(), []string{"--profile", "shared", "--model", "shared-model", "--prompt", "fix tests", "--debug"}) ||
		!reflect.DeepEqual(request.GetEnvironment(), map[string]string{"AGENT_MODEL": "shared-model", "AGENT_MODE": "safe"}) {
		t.Fatalf("render() = %+v", request)
	}
}

func TestPrepareTemplatesValidatesExtraParameterReferences(t *testing.T) {
	tests := []struct {
		name       string
		expression string
		want       string
	}{
		{name: "missing", expression: "extra_parameters.missing", want: `undefined extra parameter "missing"`},
		{name: "dynamic", expression: "extra_parameters[parameters.model]", want: "literal top-level key"},
		{name: "aggregate environment", expression: "$env.extra_parameters.known", want: "must not be accessed through Expr $env"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := strings.Replace(validProcessTemplateDefinition("agent-command"), "parameters.model", test.expression, 1)
			_, err := PrepareTemplates(TemplateConfig{
				DefinitionFiles: []string{writeProcessTemplateDefinition(t, definition)},
				ExtraParameters: map[string]any{"known": "value", "fast": "value"},
			}, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PrepareTemplates() error = %v, want containing %q", err, test.want)
			}
		})
	}

	compatibleDefinition := strings.Replace(validProcessTemplateDefinition("agent-command"), "parameters.model", "$env.parameters.model", 1)
	if _, err := PrepareTemplates(TemplateConfig{
		DefinitionFiles: []string{writeProcessTemplateDefinition(t, compatibleDefinition)},
	}, t.TempDir()); err != nil {
		t.Fatalf("PrepareTemplates(existing $env expression) error = %v", err)
	}
	if _, err := PrepareTemplates(TemplateConfig{
		DefinitionFiles: []string{writeProcessTemplateDefinition(t, compatibleDefinition)},
		ExtraParameters: map[string]any{"known": "value"},
	}, t.TempDir()); err != nil {
		t.Fatalf("PrepareTemplates($env.parameters with extra_parameters) error = %v", err)
	}
}

func TestTemplateRevisionIncludesCanonicalExtraParameters(t *testing.T) {
	definition := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("agent-command"))
	workspace := t.TempDir()
	revision := func(extraParameters map[string]any) string {
		t.Helper()
		registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}, ExtraParameters: extraParameters}, workspace)
		if err != nil {
			t.Fatal(err)
		}
		return registry.summaries()[0].GetRevision()
	}

	base := revision(nil)
	if empty := revision(map[string]any{}); empty != base {
		t.Fatalf("empty extra_parameters revision = %q, want existing revision %q", empty, base)
	}
	first := revision(map[string]any{"alpha": "one", "beta": []any{"two", int64(3)}})
	second := revision(map[string]any{"beta": []any{"two", int64(3)}, "alpha": "one"})
	if first == base || second != first {
		t.Fatalf("revisions base=%q first=%q second=%q", base, first, second)
	}
	if changed := revision(map[string]any{"alpha": "changed", "beta": []any{"two", int64(3)}}); changed == first {
		t.Fatalf("changed extra_parameters kept revision %q", changed)
	}
	if integer, floating := revision(map[string]any{"number": int64(2)}), revision(map[string]any{"number": float64(2)}); integer == floating {
		t.Fatalf("integer and float extra_parameters shared revision %q", integer)
	}
}

func TestPrepareTemplatesRejectsUnsupportedExtraParameters(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "date", value: time.Now(), want: "unsupported value type"},
		{name: "non-finite", value: math.Inf(1), want: "non-finite number"},
		{name: "oversized", value: strings.Repeat("x", maxTemplateValueBytes+1), want: "exceeds"},
		{name: "nul", value: "private-value\x00", want: "without NUL"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PrepareTemplates(TemplateConfig{ExtraParameters: map[string]any{"value": test.value}}, t.TempDir())
			if err == nil || !strings.Contains(err.Error(), test.want) || strings.Contains(err.Error(), "private-value") {
				t.Fatalf("PrepareTemplates() error = %v, want containing %q without values", err, test.want)
			}
		})
	}
}

func TestExampleProcessTemplateCompiles(t *testing.T) {
	definition := filepath.Join("..", "..", "configs", "process-templates", "code-agents.process-template.yaml")
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, t.TempDir())
	if err != nil {
		t.Fatalf("PrepareTemplates(example) error = %v", err)
	}
	if registry.Count() != 1 || registry.summaries()[0].GetName() != "code-agent" {
		t.Fatalf("example registry = %+v", registry.summaries())
	}
}

func TestTemplateRenderRejectsParametersAndResultTypeWithoutLeakingValues(t *testing.T) {
	workspace := t.TempDir()
	definition := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("agent-command"))
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatal(err)
	}
	template, _ := registry.lookup("agent")
	parameters, _ := structpb.NewStruct(map[string]any{
		"model": "secret-model-value", "prompt": "secret-prompt-value", "working_directory": ".", "debug": false,
		"unexpected": "secret-extra-value",
	})
	_, err = template.render(context.Background(), parameters)
	if status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "secret") || !strings.Contains(err.Error(), "/") {
		t.Fatalf("render(invalid parameters) error = %v", err)
	}

	badResult := strings.Replace(validProcessTemplateDefinition("agent-command"),
		`"arguments": concat(["--model", parameters.model],`, `"arguments": concat([42],`, 1)
	badDefinition := writeProcessTemplateDefinition(t, badResult)
	badRegistry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{badDefinition}}, workspace)
	if err != nil {
		t.Fatalf("PrepareTemplates(bad runtime result) error = %v", err)
	}
	badTemplate, _ := badRegistry.lookup("agent")
	valid, _ := structpb.NewStruct(map[string]any{
		"model": "secret-model-value", "prompt": "", "working_directory": ".", "debug": false,
	})
	_, err = badTemplate.render(context.Background(), valid)
	if status.Code(err) != codes.FailedPrecondition || strings.Contains(err.Error(), "secret") {
		t.Fatalf("render(invalid result) error = %v", err)
	}
}

func TestTemplateRenderIsConcurrent(t *testing.T) {
	definition := strings.ReplaceAll(validProcessTemplateDefinition("agent-command"), "parameters.model", "extra_parameters.model")
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{
		writeProcessTemplateDefinition(t, definition),
	}, ExtraParameters: map[string]any{"model": "fast"}}, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	template, _ := registry.lookup("agent")
	const calls = 32
	errorsChannel := make(chan error, calls)
	var wait sync.WaitGroup
	for index := range calls {
		wait.Add(1)
		go func() {
			defer wait.Done()
			prompt := fmt.Sprintf("prompt-%d", index)
			parameters, _ := structpb.NewStruct(map[string]any{
				"model": "fast", "prompt": prompt, "working_directory": ".", "debug": false,
			})
			request, err := template.render(context.Background(), parameters)
			if err != nil {
				errorsChannel <- err
				return
			}
			if len(request.GetArguments()) != 4 || request.GetArguments()[3] != prompt {
				errorsChannel <- fmt.Errorf("rendered arguments = %q", request.GetArguments())
			}
		}()
	}
	wait.Wait()
	close(errorsChannel)
	for err := range errorsChannel {
		t.Error(err)
	}
}

func TestPrepareTemplatesRejectsUnsafeDefinitions(t *testing.T) {
	workspace := t.TempDir()
	inside := filepath.Join(workspace, "inside.process-template.yaml")
	if err := os.WriteFile(inside, []byte(validProcessTemplateDefinition("agent-command")), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{inside}}, workspace); err == nil || !strings.Contains(err.Error(), "outside the workspace") {
		t.Fatalf("PrepareTemplates(workspace definition) error = %v", err)
	}

	target := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("agent-command"))
	symlink := filepath.Join(t.TempDir(), "link.process-template.yaml")
	if err := os.Symlink(target, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{symlink}}, workspace); err == nil {
		t.Fatal("PrepareTemplates(symlink) succeeded")
	}

	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "unknown field", content: strings.Replace(validProcessTemplateDefinition("cmd"), "    io_mode: pty", "    unknown: value\n    io_mode: pty", 1), want: "unknown field"},
		{name: "duplicate key", content: strings.Replace(validProcessTemplateDefinition("cmd"), "version: 1", "version: 1\nversion: 1", 1), want: "duplicate mapping key"},
		{name: "duplicate template", content: strings.Replace(validProcessTemplateDefinition("cmd"), "templates:\n", "templates:\n", 1) + strings.TrimPrefix(strings.SplitN(validProcessTemplateDefinition("cmd"), "templates:\n", 2)[1], ""), want: "duplicate process template name"},
		{name: "external ref", content: strings.Replace(validProcessTemplateDefinition("cmd"), "      properties:", "      properties:\n        external:\n          description: External value.\n          $ref: https://example.com/schema.json", 1), want: "may only contain a local fragment"},
		{name: "now", content: strings.Replace(validProcessTemplateDefinition("cmd"), `"environment": {"AGENT_MODEL": parameters.model}`, `"environment": {"TIME": string(now())}`, 1), want: "not allowed"},
		{name: "range", content: strings.Replace(validProcessTemplateDefinition("cmd"), `"environment": {"AGENT_MODEL": parameters.model}`, `"environment": {"RANGE": string(1..2)}`, 1), want: "range operator"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			definition := writeProcessTemplateDefinition(t, test.content)
			_, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("PrepareTemplates() error = %v, want containing %q", err, test.want)
			}
		})
	}
}

func TestServiceStartsFromTemplateAndPersistsOnlyRedactedOrigin(t *testing.T) {
	t.Setenv(helperEnvironment, "1")
	workspace := t.TempDir()
	runtimeDirectory := t.TempDir()
	definition := writeProcessTemplateDefinition(t, helperProcessTemplateDefinition(t))
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatalf("PrepareTemplates() error = %v", err)
	}
	service, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxProcesses: 1, Templates: registry})
	if err != nil {
		t.Fatal(err)
	}
	parameters, _ := structpb.NewStruct(map[string]any{"exit_code": "0", "secret_prompt": "do not persist this prompt"})
	template, _ := registry.lookup("helper")
	response, err := service.StartProcessFromTemplate(context.Background(), &codev1.StartProcessFromTemplateRequest{
		TemplateName: "helper", ProcessName: "templated", Parameters: parameters,
		ExpectedTemplateRevision: template.summary.GetRevision(),
	})
	if err != nil {
		t.Fatalf("StartProcessFromTemplate() error = %v", err)
	}
	info := response.GetProcess()
	if !info.GetArgumentsRedacted() || len(info.GetArguments()) != 0 || info.GetTemplateName() != "helper" ||
		info.GetTemplateRevision() != template.summary.GetRevision() || info.GetCommand() != os.Args[0] {
		t.Fatalf("StartProcessFromTemplate().Process = %+v", info)
	}
	exited := waitForProcessExit(t, service, info.GetId())
	if exited.GetExitCode() != 0 || len(exited.GetArguments()) != 0 || !exited.GetArgumentsRedacted() {
		t.Fatalf("templated exit = %+v", exited)
	}
	metadata, err := os.ReadFile(filepath.Join(runtimeDirectory, info.GetId(), metadataFileName))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(metadata), "do not persist") || strings.Contains(string(metadata), "-test.run") {
		t.Fatalf("metadata leaked rendered arguments: %s", metadata)
	}
	if err := service.Close(); err != nil {
		t.Fatal(err)
	}

	restored, err := New(Config{Workspace: workspace, RuntimeDirectory: runtimeDirectory, MaxProcesses: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	listed, err := restored.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if err != nil || len(listed.GetProcesses()) != 1 {
		t.Fatalf("ListProcesses(restored) = %+v, %v", listed, err)
	}
	restoredInfo := listed.GetProcesses()[0]
	if restoredInfo.GetTemplateName() != "helper" || !restoredInfo.GetArgumentsRedacted() || len(restoredInfo.GetArguments()) != 0 {
		t.Fatalf("restored template process = %+v", restoredInfo)
	}
}

func TestTemplateServiceDiscoveryAndRevisionErrors(t *testing.T) {
	workspace := t.TempDir()
	definition := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("agent-command"))
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatal(err)
	}
	service := newTestProcessServiceWithTemplates(t, workspace, registry)
	listed, err := service.ListProcessTemplates(context.Background(), &codev1.ListProcessTemplatesRequest{})
	if err != nil || len(listed.GetTemplates()) != 1 || listed.GetTemplates()[0].GetName() != "agent" {
		t.Fatalf("ListProcessTemplates() = %+v, %v", listed, err)
	}
	got, err := service.GetProcessTemplate(context.Background(), &codev1.GetProcessTemplateRequest{Name: "agent"})
	if err != nil || got.GetTemplate().GetParametersSchema() == nil {
		t.Fatalf("GetProcessTemplate() = %+v, %v", got, err)
	}
	if _, err := service.GetProcessTemplate(context.Background(), &codev1.GetProcessTemplateRequest{Name: "missing"}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetProcessTemplate(missing) code = %s", status.Code(err))
	}
	parameters, _ := structpb.NewStruct(map[string]any{
		"model": "fast", "prompt": "", "working_directory": ".", "debug": false,
	})
	_, err = service.StartProcessFromTemplate(context.Background(), &codev1.StartProcessFromTemplateRequest{
		TemplateName: "agent", Parameters: parameters, ExpectedTemplateRevision: strings.Repeat("0", 64),
	})
	if status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("StartProcessFromTemplate(revision mismatch) code = %s, error = %v", status.Code(err), err)
	}
}

func TestTemplateStartFailureKeepsOnlyRedactedRecord(t *testing.T) {
	workspace := t.TempDir()
	definition := writeProcessTemplateDefinition(t, validProcessTemplateDefinition("remote-code-missing-template-executable"))
	registry, err := PrepareTemplates(TemplateConfig{DefinitionFiles: []string{definition}}, workspace)
	if err != nil {
		t.Fatal(err)
	}
	service := newTestProcessServiceWithTemplates(t, workspace, registry)
	parameters, _ := structpb.NewStruct(map[string]any{
		"model": "private-model", "prompt": "private prompt", "working_directory": ".", "debug": false,
	})
	_, err = service.StartProcessFromTemplate(context.Background(), &codev1.StartProcessFromTemplateRequest{
		TemplateName: "agent", ProcessName: "failed-template", Parameters: parameters,
	})
	if status.Code(err) != codes.Internal {
		t.Fatalf("StartProcessFromTemplate(missing executable) code = %s, error = %v", status.Code(err), err)
	}
	listed, err := service.ListProcesses(context.Background(), &codev1.ListProcessesRequest{All: true})
	if err != nil || len(listed.GetProcesses()) != 1 {
		t.Fatalf("ListProcesses() = %+v, %v", listed, err)
	}
	failed := listed.GetProcesses()[0]
	if failed.GetState() != codev1.ProcessState_PROCESS_STATE_FAILED || !failed.GetArgumentsRedacted() ||
		len(failed.GetArguments()) != 0 || failed.GetTemplateName() != "agent" {
		t.Fatalf("failed template record = %+v", failed)
	}
}

func validProcessTemplateDefinition(command string) string {
	encodedCommand, _ := json.Marshal(command)
	return `version: 1
language: expr
templates:
  - name: agent
    description: Start a configured code agent.
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      required: [model, prompt, working_directory, debug]
      additionalProperties: false
      properties:
        model:
          type: string
          description: Model name.
        prompt:
          type: string
          description: Initial prompt.
        working_directory:
          type: string
          description: Workspace-relative directory.
        debug:
          type: boolean
          description: Enable debug output.
    command: ` + string(encodedCommand) + `
    io_mode: pty
    input_mode: managed
    render: |-
      {
        "arguments": concat(["--model", parameters.model], parameters.prompt == "" ? [] : ["--prompt", parameters.prompt], parameters.debug ? ["--debug"] : []),
        "working_directory": parameters.working_directory,
        "environment": {"AGENT_MODEL": parameters.model}
      }
`
}

func helperProcessTemplateDefinition(t *testing.T) string {
	t.Helper()
	encodedCommand, _ := json.Marshal(os.Args[0])
	return `version: 1
language: expr
templates:
  - name: helper
    description: Run the deterministic process test helper.
    parameters_schema:
      $schema: https://json-schema.org/draft/2020-12/schema
      type: object
      required: [exit_code, secret_prompt]
      additionalProperties: false
      properties:
        exit_code:
          type: string
          description: Exit code for the helper.
        secret_prompt:
          type: string
          description: Sensitive value that must not be persisted.
    command: ` + string(encodedCommand) + `
    io_mode: pipe
    input_mode: disabled
    render: |-
      {
        "arguments": ["-test.run=^TestProcessHelper$", "--", "exit", parameters.exit_code, parameters.secret_prompt][0:4],
        "working_directory": ".",
        "environment": {}
      }
`
}

func writeProcessTemplateDefinition(t *testing.T, contents string) string {
	t.Helper()
	name := filepath.Join(t.TempDir(), "test.process-template.yaml")
	if err := os.WriteFile(name, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	return name
}

func newTestProcessServiceWithTemplates(t *testing.T, workspace string, templates *TemplateRegistry) *Service {
	t.Helper()
	service, err := New(Config{Workspace: workspace, RuntimeDirectory: t.TempDir(), MaxProcesses: 1, Templates: templates})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = service.Close()
	})
	return service
}
