package cli

import (
	"fmt"
	"strings"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func (r *REPL) controllerInfo(arguments []string) error {
	if len(arguments) != 0 {
		return usageError()
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	info, err := r.client.GetInfo(ctx)
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "Controller version: %s\nAPI version: %s\nWorkspace: %s\nMax upload bytes: %d\nResumable upload: %t\nResumable download: %t\nPreferred transfer chunk bytes: %d\nMax processes: %d\nProcess template count: %d\n",
		info.GetControllerVersion(), info.GetApiVersion(), info.GetWorkspaceName(), info.GetMaxUploadBytes(),
		info.GetFileTransfers().GetResumableUpload(), info.GetFileTransfers().GetResumableDownload(), info.GetFileTransfers().GetPreferredChunkBytes(),
		info.GetMaxProcesses(), info.GetProcessTemplateCount())
	fmt.Fprintf(r.stdout, "Agent: %s\n", agentInfoSummary(info.GetAgent()))
	return nil
}

// agentInfoSummary renders the agent bridge status for `info`: absent when the
// service is disabled, and noting that the child starts lazily on the first
// query.
func agentInfoSummary(agent *codev1.AgentInfo) string {
	if agent == nil {
		return "disabled"
	}
	if !agent.GetStarted() {
		return "enabled (not started)"
	}
	summary := fmt.Sprintf("enabled (process %s, generation %d, sessions %d",
		agent.GetProcessId(), agent.GetGeneration(), agent.GetSessions())
	if names := agent.GetAgents(); len(names) > 0 {
		summary += fmt.Sprintf(", agents %s", strings.Join(names, ", "))
	}
	return summary + ")"
}
