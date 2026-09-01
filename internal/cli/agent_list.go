package cli

import (
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type agentQueryListOptions struct {
	sessionID string
	states    []codev1.AgentQueryState
	pageSize  uint32
	pageToken string
}

type agentSessionListOptions struct {
	states    []codev1.AgentSessionState
	pageSize  uint32
	pageToken string
}

func parseAgentQueryListOptions(arguments []string) (agentQueryListOptions, error) {
	var options agentQueryListOptions
	sessionSet := false
	pageSizeSet := false
	pageTokenSet := false
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "--session":
			if index+1 >= len(arguments) || sessionSet {
				return agentQueryListOptions{}, usageError()
			}
			index++
			if arguments[index] == "" {
				return agentQueryListOptions{}, usageError()
			}
			sessionSet = true
			options.sessionID = arguments[index]
		case "--state":
			if index+1 >= len(arguments) {
				return agentQueryListOptions{}, usageError()
			}
			index++
			state, err := parseAgentQueryState(arguments[index])
			if err != nil {
				return agentQueryListOptions{}, err
			}
			options.states = append(options.states, state)
		case "--page-size":
			if index+1 >= len(arguments) || pageSizeSet {
				return agentQueryListOptions{}, usageError()
			}
			index++
			pageSize, err := parseAgentPageSize(arguments[index])
			if err != nil {
				return agentQueryListOptions{}, err
			}
			pageSizeSet = true
			options.pageSize = pageSize
		case "--page-token":
			if index+1 >= len(arguments) || pageTokenSet {
				return agentQueryListOptions{}, usageError()
			}
			index++
			if arguments[index] == "" {
				return agentQueryListOptions{}, usageError()
			}
			pageTokenSet = true
			options.pageToken = arguments[index]
		default:
			return agentQueryListOptions{}, usageErrorf("unknown agent-queries option %q", arguments[index])
		}
	}
	return options, nil
}

func parseAgentSessionListOptions(arguments []string) (agentSessionListOptions, error) {
	var options agentSessionListOptions
	pageSizeSet := false
	pageTokenSet := false
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "--state":
			if index+1 >= len(arguments) {
				return agentSessionListOptions{}, usageError()
			}
			index++
			state, err := parseAgentSessionState(arguments[index])
			if err != nil {
				return agentSessionListOptions{}, err
			}
			options.states = append(options.states, state)
		case "--page-size":
			if index+1 >= len(arguments) || pageSizeSet {
				return agentSessionListOptions{}, usageError()
			}
			index++
			pageSize, err := parseAgentPageSize(arguments[index])
			if err != nil {
				return agentSessionListOptions{}, err
			}
			pageSizeSet = true
			options.pageSize = pageSize
		case "--page-token":
			if index+1 >= len(arguments) || pageTokenSet {
				return agentSessionListOptions{}, usageError()
			}
			index++
			if arguments[index] == "" {
				return agentSessionListOptions{}, usageError()
			}
			pageTokenSet = true
			options.pageToken = arguments[index]
		default:
			return agentSessionListOptions{}, usageErrorf("unknown agent-sessions option %q", arguments[index])
		}
	}
	return options, nil
}

func parseAgentQueryState(value string) (codev1.AgentQueryState, error) {
	switch strings.ToLower(value) {
	case "running":
		return codev1.AgentQueryState_AGENT_QUERY_STATE_RUNNING, nil
	case "settled":
		return codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED, nil
	case "lost":
		return codev1.AgentQueryState_AGENT_QUERY_STATE_LOST, nil
	default:
		return codev1.AgentQueryState_AGENT_QUERY_STATE_UNSPECIFIED, usageErrorf("invalid agent query state %q", value)
	}
}

func parseAgentSessionState(value string) (codev1.AgentSessionState, error) {
	switch strings.ToLower(value) {
	case "idle":
		return codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE, nil
	case "running":
		return codev1.AgentSessionState_AGENT_SESSION_STATE_RUNNING, nil
	default:
		return codev1.AgentSessionState_AGENT_SESSION_STATE_UNSPECIFIED, usageErrorf("invalid agent session state %q", value)
	}
}

func parseAgentPageSize(value string) (uint32, error) {
	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil || parsed == 0 {
		return 0, usageErrorf("invalid agent list page size %q", value)
	}
	return uint32(parsed), nil
}

func (r *REPL) agentQueries(arguments []string) error {
	options, err := parseAgentQueryListOptions(arguments)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	response, err := r.client.ListAgentQueries(ctx, remoteclient.AgentQueryListOptions{
		SessionID: options.sessionID, States: options.states,
		PageSize: options.pageSize, PageToken: options.pageToken,
	})
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "QUERY ID\tSESSION ID\tSTATE\tSEQUENCES\tCREATED\tSETTLED\tSTOP\tERROR")
	for _, query := range response.GetQueries() {
		fmt.Fprintf(writer, "%s\t%s\t%s\t%d..%d\t%s\t%s\t%s\t%s\n",
			query.GetQueryId(), query.GetSessionId(), agentQueryStateText(query.GetState()),
			query.GetEarliestSequence(), query.GetNextSequence(), agentListTime(query.GetCreatedAt()),
			agentListTime(query.GetSettledAt()), query.GetStopReason(), agentTerminalStatus(query))
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if response.GetNextPageToken() != "" {
		fmt.Fprintf(r.stdout, "next page token: %s\n", response.GetNextPageToken())
	}
	return nil
}

func (r *REPL) agentSessions(arguments []string) error {
	options, err := parseAgentSessionListOptions(arguments)
	if err != nil {
		return err
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	response, err := r.client.ListAgentSessions(ctx, remoteclient.AgentSessionListOptions{
		States: options.states, PageSize: options.pageSize, PageToken: options.pageToken,
	})
	if err != nil {
		return err
	}
	writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(writer, "SESSION ID\tSTATE\tACTIVE QUERY\tGENERATION\tLAST ACTIVITY\tCWD")
	for _, session := range response.GetSessions() {
		fmt.Fprintf(writer, "%s\t%s\t%s\t%d\t%s\t%s\n",
			session.GetSessionId(), agentSessionStateText(session.GetState()), session.GetActiveQueryId(),
			session.GetGeneration(), agentListTime(session.GetLastActivityAt()), session.GetWorkingDirectory())
	}
	if err := writer.Flush(); err != nil {
		return err
	}
	if response.GetNextPageToken() != "" {
		fmt.Fprintf(r.stdout, "next page token: %s\n", response.GetNextPageToken())
	}
	return nil
}

func agentSessionStateText(state codev1.AgentSessionState) string {
	return strings.ToLower(strings.TrimPrefix(state.String(), "AGENT_SESSION_STATE_"))
}

func agentListTime(value *timestamppb.Timestamp) string {
	if value == nil || !value.IsValid() {
		return "-"
	}
	return value.AsTime().Local().Format(time.RFC3339)
}

func agentTerminalStatus(query *codev1.AgentQueryInfo) string {
	terminal := query.GetTerminalStatus()
	if terminal == nil {
		return "-"
	}
	return codes.Code(terminal.GetCode()).String()
}
