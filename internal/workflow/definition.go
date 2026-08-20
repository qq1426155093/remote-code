package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/expr-lang/expr/vm"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	maxDefinitionFiles     = 64
	maxDefinitions         = 256
	maxWorkflowsPerFile    = 128
	maxNodesPerWorkflow    = 256
	maxRoutesPerNode       = 64
	maxTargetsPerRoute     = 64
	maxEdgesPerWorkflow    = 4096
	maxResourcesPerNode    = 32
	maxDefinitionDescBytes = 16 << 10
	maxInputOutputBytes    = 1 << 20
)

var identifierPattern = regexp.MustCompile(`^[a-z][a-z0-9._-]{0,62}$`)

// Config configures the controller-owned workflow service.
type Config struct {
	Enabled             bool
	DefinitionFiles     []string
	MaxActiveRuns       int
	MaxActiveAttempts   int
	LeaseDuration       time.Duration
	RetryInitialBackoff time.Duration
	RetryMaxBackoff     time.Duration
	ReconcileInterval   time.Duration
}

func (c *Config) ApplyDefaults() {
	if c.MaxActiveRuns == 0 {
		c.MaxActiveRuns = 64
	}
	if c.MaxActiveAttempts == 0 {
		c.MaxActiveAttempts = 16
	}
	if c.LeaseDuration == 0 {
		c.LeaseDuration = 30 * time.Second
	}
	if c.RetryInitialBackoff == 0 {
		c.RetryInitialBackoff = time.Second
	}
	if c.RetryMaxBackoff == 0 {
		c.RetryMaxBackoff = time.Minute
	}
	if c.ReconcileInterval == 0 {
		c.ReconcileInterval = time.Second
	}
}

func ValidateConfig(config Config) error {
	config.ApplyDefaults()
	if !config.Enabled {
		if len(config.DefinitionFiles) != 0 {
			return errors.New("workflow definition files require workflows to be enabled")
		}
	} else if len(config.DefinitionFiles) == 0 {
		return errors.New("enabled workflows require at least one definition file")
	}
	if len(config.DefinitionFiles) > maxDefinitionFiles {
		return fmt.Errorf("workflows accept at most %d definition files", maxDefinitionFiles)
	}
	if config.MaxActiveRuns < 1 || config.MaxActiveRuns > 4096 {
		return errors.New("workflow max active runs must be between 1 and 4096")
	}
	if config.MaxActiveAttempts < 1 || config.MaxActiveAttempts > 4096 {
		return errors.New("workflow max active attempts must be between 1 and 4096")
	}
	if config.LeaseDuration < time.Second || config.LeaseDuration > 24*time.Hour {
		return errors.New("workflow lease duration must be between 1s and 24h")
	}
	if config.RetryInitialBackoff <= 0 || config.RetryInitialBackoff > config.RetryMaxBackoff {
		return errors.New("workflow retry initial backoff must be positive and not exceed max backoff")
	}
	if config.RetryMaxBackoff > 24*time.Hour {
		return errors.New("workflow retry max backoff must not exceed 24h")
	}
	if config.ReconcileInterval < 100*time.Millisecond || config.ReconcileInterval > time.Minute {
		return errors.New("workflow reconcile interval must be between 100ms and 1m")
	}
	return nil
}

type Definition struct {
	Name             string           `json:"name"`
	Description      string           `json:"description,omitempty"`
	Revision         int              `json:"revision"`
	Entry            string           `json:"entry"`
	MaxParallelism   int              `json:"max_parallelism"`
	ParametersSchema json.RawMessage  `json:"parameters_schema"`
	Nodes            []NodeDefinition `json:"nodes"`
}

type NodeDefinition struct {
	ID        string              `json:"id"`
	Script    string              `json:"script,omitempty"`
	Routes    map[string][]string `json:"routes,omitempty"`
	Join      JoinPolicy          `json:"join,omitempty"`
	Terminal  TerminalOutcome     `json:"terminal,omitempty"`
	Timeout   time.Duration       `json:"timeout"`
	Retry     RetryPolicy         `json:"retry"`
	Resources []ResourceClaim     `json:"resources,omitempty"`
}

type RetryPolicy struct {
	MaxAttempts int `json:"max_attempts"`
}

type ResourceClaim struct {
	Key  string       `json:"key"`
	Mode ResourceMode `json:"mode"`
}

type Registry struct {
	byName  map[string]*compiledDefinition
	ordered []*compiledDefinition
}

type DefinitionSummary struct {
	Name        string
	Description string
	Revision    int
	Digest      string
	NodeCount   int
}

type compiledDefinition struct {
	definition Definition
	validator  *jsonschema.Schema
	nodes      map[string]*compiledNode
	order      []string
	incoming   map[string][]string
}

type compiledNode struct {
	definition NodeDefinition
	program    *vm.Program
	operations map[string]string
}

type workflowDocument struct {
	Version   int           `json:"version"`
	Language  string        `json:"language"`
	Workflows []workflowRaw `json:"workflows"`
}

type workflowRaw struct {
	Name             string          `json:"name"`
	Description      string          `json:"description"`
	Revision         int             `json:"revision"`
	Entry            string          `json:"entry"`
	MaxParallelism   int             `json:"max_parallelism"`
	ParametersSchema json.RawMessage `json:"parameters_schema"`
	Nodes            []nodeRaw       `json:"nodes"`
}

type nodeRaw struct {
	ID        string                     `json:"id"`
	Script    string                     `json:"script"`
	Routes    map[string]json.RawMessage `json:"routes"`
	Join      JoinPolicy                 `json:"join"`
	Terminal  TerminalOutcome            `json:"terminal"`
	Timeout   string                     `json:"timeout"`
	Retry     RetryPolicy                `json:"retry"`
	Resources []ResourceClaim            `json:"resources"`
}

// Prepare loads and compiles immutable workflow definitions without opening a
// database or starting background work.
func Prepare(config Config, workspace string) (*Registry, error) {
	config.ApplyDefaults()
	if err := ValidateConfig(config); err != nil {
		return nil, err
	}
	registry := &Registry{byName: make(map[string]*compiledDefinition)}
	if !config.Enabled {
		return registry, nil
	}
	documents, err := loadDefinitionFiles(config.DefinitionFiles, workspace)
	if err != nil {
		return nil, err
	}
	for documentIndex, document := range documents {
		path := config.DefinitionFiles[documentIndex]
		if document.Version != 1 {
			return nil, fmt.Errorf("workflow definition %q version must be 1", path)
		}
		if document.Language != "expr" {
			return nil, fmt.Errorf("workflow definition %q language must be expr", path)
		}
		if len(document.Workflows) == 0 || len(document.Workflows) > maxWorkflowsPerFile {
			return nil, fmt.Errorf("workflow definition %q must contain between 1 and %d workflows", path, maxWorkflowsPerFile)
		}
		for index, raw := range document.Workflows {
			definition, normalizeErr := normalizeDefinition(raw)
			if normalizeErr != nil {
				return nil, fmt.Errorf("workflow definition %q workflow %d: %w", path, index, normalizeErr)
			}
			compiled, compileErr := compileDefinition(definition)
			if compileErr != nil {
				return nil, fmt.Errorf("workflow definition %q workflow %q: %w", path, definition.Name, compileErr)
			}
			if _, duplicate := registry.byName[definition.Name]; duplicate {
				return nil, fmt.Errorf("duplicate workflow name %q", definition.Name)
			}
			registry.byName[definition.Name] = compiled
			registry.ordered = append(registry.ordered, compiled)
			if len(registry.ordered) > maxDefinitions {
				return nil, fmt.Errorf("workflow registry contains more than %d workflows", maxDefinitions)
			}
		}
	}
	sort.Slice(registry.ordered, func(i, j int) bool {
		return registry.ordered[i].definition.Name < registry.ordered[j].definition.Name
	})
	return registry, nil
}

func normalizeDefinition(raw workflowRaw) (Definition, error) {
	definition := Definition{
		Name: raw.Name, Description: raw.Description, Revision: raw.Revision, Entry: raw.Entry,
		MaxParallelism: raw.MaxParallelism, ParametersSchema: raw.ParametersSchema,
	}
	for index, node := range raw.Nodes {
		timeout := 24 * time.Hour
		if node.Timeout != "" {
			parsed, err := time.ParseDuration(node.Timeout)
			if err != nil {
				return Definition{}, fmt.Errorf("node %d timeout: %w", index, err)
			}
			timeout = parsed
		}
		normalized := NodeDefinition{
			ID: node.ID, Script: node.Script, Join: node.Join, Terminal: node.Terminal,
			Timeout: timeout, Retry: node.Retry, Resources: append([]ResourceClaim(nil), node.Resources...),
		}
		if len(node.Routes) > 0 {
			normalized.Routes = make(map[string][]string, len(node.Routes))
		}
		for route, value := range node.Routes {
			var single string
			if err := json.Unmarshal(value, &single); err == nil {
				normalized.Routes[route] = []string{single}
				continue
			}
			var targets []string
			if err := json.Unmarshal(value, &targets); err != nil {
				return Definition{}, fmt.Errorf("node %d route %q must be a string or string array", index, route)
			}
			normalized.Routes[route] = targets
		}
		definition.Nodes = append(definition.Nodes, normalized)
	}
	return definition, nil
}

func compileDefinition(definition Definition) (*compiledDefinition, error) {
	if !identifierPattern.MatchString(definition.Name) {
		return nil, fmt.Errorf("name must match %s", identifierPattern)
	}
	if definition.Revision < 1 {
		return nil, errors.New("revision must be positive")
	}
	if len(definition.Description) > maxDefinitionDescBytes || !utf8.ValidString(definition.Description) || strings.ContainsRune(definition.Description, '\x00') {
		return nil, fmt.Errorf("description must be valid UTF-8 without NUL and at most %d bytes", maxDefinitionDescBytes)
	}
	if definition.MaxParallelism == 0 {
		definition.MaxParallelism = 1
	}
	if definition.MaxParallelism < 1 || definition.MaxParallelism > 64 {
		return nil, errors.New("max_parallelism must be between 1 and 64")
	}
	if len(definition.Nodes) == 0 || len(definition.Nodes) > maxNodesPerWorkflow {
		return nil, fmt.Errorf("nodes must contain between 1 and %d entries", maxNodesPerWorkflow)
	}
	validator, schema, err := compileParameterSchema(definition.Name, definition.ParametersSchema)
	if err != nil {
		return nil, fmt.Errorf("parameters_schema: %w", err)
	}
	definition.ParametersSchema = schema
	compiled := &compiledDefinition{
		definition: definition, validator: validator, nodes: make(map[string]*compiledNode),
		incoming: make(map[string][]string),
	}
	terminalCount := 0
	for index := range definition.Nodes {
		node := &compiled.definition.Nodes[index]
		if !identifierPattern.MatchString(node.ID) {
			return nil, fmt.Errorf("node %d id must match %s", index, identifierPattern)
		}
		if _, duplicate := compiled.nodes[node.ID]; duplicate {
			return nil, fmt.Errorf("duplicate node id %q", node.ID)
		}
		if node.Join == "" {
			node.Join = JoinAll
		}
		if node.Join != JoinAll && node.Join != JoinAny {
			return nil, fmt.Errorf("node %q join must be all or any", node.ID)
		}
		if node.Retry.MaxAttempts == 0 {
			node.Retry.MaxAttempts = 3
		}
		if node.Timeout == 0 {
			node.Timeout = 24 * time.Hour
		}
		if node.Timeout < time.Second || node.Timeout > 30*24*time.Hour {
			return nil, fmt.Errorf("node %q timeout must be between 1s and 720h", node.ID)
		}
		if node.Retry.MaxAttempts < 1 || node.Retry.MaxAttempts > 100 {
			return nil, fmt.Errorf("node %q retry.max_attempts must be between 1 and 100", node.ID)
		}
		if err := normalizeResources(node); err != nil {
			return nil, fmt.Errorf("node %q: %w", node.ID, err)
		}
		if node.Terminal != TerminalNone {
			if node.Terminal != TerminalSucceeded && node.Terminal != TerminalFailed {
				return nil, fmt.Errorf("node %q terminal must be succeeded or failed", node.ID)
			}
			if strings.TrimSpace(node.Script) != "" || len(node.Routes) != 0 {
				return nil, fmt.Errorf("terminal node %q cannot have script or routes", node.ID)
			}
			compiled.nodes[node.ID] = &compiledNode{definition: *node}
			terminalCount++
			continue
		}
		if strings.TrimSpace(node.Script) == "" || len(node.Routes) == 0 {
			return nil, fmt.Errorf("executable node %q requires script and routes", node.ID)
		}
		if len(node.Routes) > maxRoutesPerNode {
			return nil, fmt.Errorf("node %q has more than %d routes", node.ID, maxRoutesPerNode)
		}
		program, operations, err := compileScript(node.Script, node.Routes)
		if err != nil {
			return nil, fmt.Errorf("node %q: %w", node.ID, err)
		}
		compiled.nodes[node.ID] = &compiledNode{definition: *node, program: program, operations: operations}
	}
	if terminalCount == 0 {
		return nil, errors.New("workflow requires at least one terminal node")
	}
	if _, exists := compiled.nodes[definition.Entry]; !exists {
		return nil, fmt.Errorf("entry node %q does not exist", definition.Entry)
	}
	if err := compileGraph(compiled); err != nil {
		return nil, err
	}
	return compiled, nil
}

func normalizeResources(node *NodeDefinition) error {
	if len(node.Resources) > maxResourcesPerNode {
		return fmt.Errorf("resources contains more than %d entries", maxResourcesPerNode)
	}
	seen := make(map[string]struct{}, len(node.Resources))
	for _, resource := range node.Resources {
		if !identifierPattern.MatchString(resource.Key) {
			return fmt.Errorf("resource key %q must match %s", resource.Key, identifierPattern)
		}
		if resource.Mode != ResourceShared && resource.Mode != ResourceExclusive {
			return fmt.Errorf("resource %q mode must be shared or exclusive", resource.Key)
		}
		if _, duplicate := seen[resource.Key]; duplicate {
			return fmt.Errorf("duplicate resource key %q", resource.Key)
		}
		seen[resource.Key] = struct{}{}
	}
	sort.Slice(node.Resources, func(i, j int) bool { return node.Resources[i].Key < node.Resources[j].Key })
	return nil
}

func compileGraph(compiled *compiledDefinition) error {
	indegree := make(map[string]int, len(compiled.nodes))
	adjacency := make(map[string][]string, len(compiled.nodes))
	edges := 0
	for id := range compiled.nodes {
		indegree[id] = 0
	}
	for source, node := range compiled.nodes {
		seenTarget := make(map[string]string)
		for route, targets := range node.definition.Routes {
			if !identifierPattern.MatchString(route) {
				return fmt.Errorf("node %q route %q must match %s", source, route, identifierPattern)
			}
			if len(targets) == 0 || len(targets) > maxTargetsPerRoute {
				return fmt.Errorf("node %q route %q must have between 1 and %d targets", source, route, maxTargetsPerRoute)
			}
			seenWithinRoute := make(map[string]struct{}, len(targets))
			for _, target := range targets {
				if _, exists := compiled.nodes[target]; !exists {
					return fmt.Errorf("node %q route %q targets missing node %q", source, route, target)
				}
				if _, duplicate := seenWithinRoute[target]; duplicate {
					return fmt.Errorf("node %q route %q repeats target %q", source, route, target)
				}
				seenWithinRoute[target] = struct{}{}
				if earlier, duplicate := seenTarget[target]; duplicate {
					return fmt.Errorf("node %q routes %q and %q both target %q", source, earlier, route, target)
				}
				seenTarget[target] = route
				adjacency[source] = append(adjacency[source], target)
				edges++
				if edges > maxEdgesPerWorkflow {
					return fmt.Errorf("workflow graph contains more than %d edges", maxEdgesPerWorkflow)
				}
				compiled.incoming[target] = append(compiled.incoming[target], source)
				indegree[target]++
			}
		}
	}
	if indegree[compiled.definition.Entry] != 0 {
		return fmt.Errorf("entry node %q must have zero incoming edges", compiled.definition.Entry)
	}
	for id, degree := range indegree {
		if id != compiled.definition.Entry && degree == 0 {
			return fmt.Errorf("node %q has no incoming edge", id)
		}
	}
	queue := []string{compiled.definition.Entry}
	visited := make(map[string]bool, len(compiled.nodes))
	for len(queue) > 0 {
		node := queue[0]
		queue = queue[1:]
		if visited[node] {
			continue
		}
		visited[node] = true
		compiled.order = append(compiled.order, node)
		for _, target := range adjacency[node] {
			indegree[target]--
			if indegree[target] == 0 {
				queue = append(queue, target)
			}
		}
	}
	if len(visited) != len(compiled.nodes) {
		return errors.New("workflow graph contains a cycle or unreachable node")
	}
	for id := range compiled.incoming {
		sort.Strings(compiled.incoming[id])
	}
	return nil
}

func (r *Registry) definition(name string) (*compiledDefinition, bool) {
	if r == nil {
		return nil, false
	}
	definition, ok := r.byName[name]
	return definition, ok
}

func (r *Registry) Count() int {
	if r == nil {
		return 0
	}
	return len(r.ordered)
}

func (r *Registry) List() []DefinitionSummary {
	if r == nil {
		return nil
	}
	result := make([]DefinitionSummary, 0, len(r.ordered))
	for _, compiled := range r.ordered {
		result = append(result, DefinitionSummary{
			Name: compiled.definition.Name, Description: compiled.definition.Description,
			Revision: compiled.definition.Revision, Digest: definitionDigest(compiled.definition),
			NodeCount: len(compiled.definition.Nodes),
		})
	}
	return result
}

func (r *Registry) Get(name string) (Definition, bool) {
	compiled, exists := r.definition(name)
	if !exists {
		return Definition{}, false
	}
	encoded, err := json.Marshal(compiled.definition)
	if err != nil {
		return Definition{}, false
	}
	var cloned Definition
	if err := json.Unmarshal(encoded, &cloned); err != nil {
		return Definition{}, false
	}
	return cloned, true
}

func definitionDigest(definition Definition) string {
	encoded, _ := json.Marshal(definition)
	digest := sha256.Sum256(encoded)
	return hex.EncodeToString(digest[:])
}

func cloneJSONValue(value any) (any, error) {
	encoded, err := json.Marshal(value)
	if err != nil || len(encoded) > maxInputOutputBytes {
		if err != nil {
			return nil, err
		}
		return nil, fmt.Errorf("JSON value exceeds %d bytes", maxInputOutputBytes)
	}
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	var result any
	if err := decoder.Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
