package agent

import (
	"context"
	"time"

	"github.com/coder/acp-go-sdk"

	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
)

// agentConfigID names the session config option the ACP v1 config-options
// extension uses to select the agent child's main-thread persona;
// claude-agent-acp lists "default" plus every configured custom agent under
// it. Config ids are agent-defined surface: the option is matched by id, so a
// child without one simply reports no selectable agents.
const agentConfigID = acp.SessionConfigId("agent")

// defaultAgentValue is the sentinel value listed alongside every custom
// persona; selecting it explicitly asks for the standard agent.
const defaultAgentValue = acp.SessionConfigValueId("default")

// agentSelectWait bounds one session/set_config_option call. The picker is a
// local lookup on the agent side, so this only guards a hung child.
const agentSelectWait = 10 * time.Second

// offeredAgents returns the agent picker from a session's config options, or
// nil when the child offered none.
func offeredAgents(options []acp.SessionConfigOption) *acp.SessionConfigOptionSelect {
	for index := range options {
		if option := options[index]; option.Select != nil && option.Select.Id == agentConfigID {
			return option.Select
		}
	}
	return nil
}

// selectOptionValues flattens the picker's values, which may be grouped under
// headers or listed flat.
func selectOptionValues(options acp.SessionConfigSelectOptions) []acp.SessionConfigSelectOption {
	if options.Ungrouped != nil {
		return *options.Ungrouped
	}
	if options.Grouped != nil {
		var flattened []acp.SessionConfigSelectOption
		for _, group := range *options.Grouped {
			flattened = append(flattened, group.Options...)
		}
		return flattened
	}
	return nil
}

// customAgentNames lists the picker's persona names, "default" excluded: the
// wire surface names only real personas because an empty QueryRequest.agent
// already means the default one.
func customAgentNames(picker *acp.SessionConfigOptionSelect) []string {
	if picker == nil {
		return nil
	}
	names := make([]string, 0, len(selectOptionValues(picker.Options)))
	for _, option := range selectOptionValues(picker.Options) {
		if option.Value != defaultAgentValue {
			names = append(names, string(option.Value))
		}
	}
	return names
}

// applyAgentSelection selects the turn's persona through the standard ACP
// session/set_config_option call, validating the requested name against the
// options this session's own creation response offered. The advertised names
// refresh the per-generation discovery cache either way, so GetInfo can list
// them even for queries that kept the default.
func (s *Service) applyAgentSelection(ctx context.Context, connection *agentConnection, sessionID acp.SessionId, agent string, options []acp.SessionConfigOption) error {
	picker := offeredAgents(options)
	s.rememberAgentNames(connection.generation, options, picker)
	if agent == "" {
		return nil
	}
	if picker == nil {
		return rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentSelectionUnsupported,
			"agent %q offers no agent picker; configure custom agents on the child first", processLabel(connection.transport))
	}
	if !pickerOffers(picker, agent) {
		return rpcerror.Errorf(codes.InvalidArgument, rpcerror.AgentNameInvalid,
			"agent %q is not offered by this agent child; AgentInfo.agents lists the current choices", agent)
	}
	selectCtx, cancel := context.WithTimeout(ctx, agentSelectWait)
	defer cancel()
	_, err := connection.conn.SetSessionConfigOption(selectCtx, acp.SetSessionConfigOptionRequest{
		ValueId: &acp.SetSessionConfigOptionValueId{
			ConfigId:  agentConfigID,
			SessionId: sessionID,
			Value:     acp.SessionConfigValueId(agent),
		},
	})
	if err != nil {
		return mapAgentRequestError("select agent", err)
	}
	return nil
}

// pickerOffers reports whether name is one of the picker's listed values.
func pickerOffers(picker *acp.SessionConfigOptionSelect, name string) bool {
	for _, option := range selectOptionValues(picker.Options) {
		if option.Value == acp.SessionConfigValueId(name) {
			return true
		}
	}
	return false
}

// rememberAgentNames caches the personas one generation's sessions offered;
// GetInfo reports them until a newer generation replaces the child. A response
// carrying no config options at all leaves the cache alone — a resume from a
// child that reports options only on session/new must not erase what
// session/new learned.
func (s *Service) rememberAgentNames(generation uint64, options []acp.SessionConfigOption, picker *acp.SessionConfigOptionSelect) {
	if len(options) == 0 {
		return
	}
	names := customAgentNames(picker)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agentNames == nil || s.agentNamesGeneration <= generation {
		s.agentNames, s.agentNamesGeneration = names, generation
	}
}

// agentNamesFor reports the cached persona names for a generation; nil when
// that generation never reported a session's options.
func (s *Service) agentNamesFor(generation uint64) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.agentNamesGeneration != generation || s.agentNames == nil {
		return nil
	}
	return s.agentNames
}
