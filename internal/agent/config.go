package agent

import (
	"errors"
	"fmt"
	"strings"
)

// defaultAgentCommand and defaultAgentArguments launch the reference Claude
// Code ACP adapter through npx. Offline deployments replace them with an
// absolute path to a pre-installed adapter.
var (
	defaultAgentCommand   = "npx"
	defaultAgentArguments = []string{"@agentclientprotocol/claude-agent-acp"}
)

// ApplyDefaults fills the launch defaults for an enabled agent with no
// explicit command, and the event-store bounds when none were configured. It
// never turns the service on: enabled defaults to true at the controller
// option layer, where a v9 configuration can still opt out.
func (c *Config) ApplyDefaults() {
	if c.Command == "" {
		c.Command = defaultAgentCommand
		if c.Arguments == nil {
			c.Arguments = append([]string(nil), defaultAgentArguments...)
		}
	}
	if c.replayEnabled() && c.Events == (EventLogConfig{}) {
		c.Events = DefaultEventLogConfig()
	}
}

// ValidateConfig checks the agent table without touching the filesystem: the
// command is resolved lazily on the first query, so a missing binary must be a
// runtime AGENT_START_FAILED rather than a configuration error.
func ValidateConfig(config Config) error {
	if !config.Enabled {
		return nil
	}
	if strings.TrimSpace(config.Command) == "" {
		return errors.New("agent command must not be empty")
	}
	for key := range config.Environment {
		if key == "" || strings.ContainsAny(key, "=\x00") || strings.ContainsRune(key, ' ') {
			return fmt.Errorf("agent environment key %q is not a valid variable name", key)
		}
	}
	if config.replayEnabled() && config.Events != (EventLogConfig{}) {
		if err := ValidateEventLogConfig(config.Events); err != nil {
			return err
		}
	}
	return nil
}
