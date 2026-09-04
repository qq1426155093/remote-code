package agent

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"strings"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	agentListDefaultPageSize uint32 = 100
	agentListMaxPageSize     uint32 = 500
	maxAgentPageTokenBytes          = 4096
)

const (
	queryPageKind   = "agent-queries-v1"
	sessionPageKind = "agent-sessions-v1"
)

// agentPageCursor is deliberately opaque on the wire. The filter fingerprint
// prevents accidentally reusing a cursor with a different list request.
type agentPageCursor struct {
	Kind         string `json:"kind"`
	Filter       string `json:"filter"`
	TimeUnixNano int64  `json:"time_unix_nano"`
	ID           string `json:"id"`
}

// ListQueries returns retained query metadata without reading event payloads.
// Results are weakly consistent across pages: GC and turn settlement may occur
// between calls, while the keyset cursor prevents newly-created records from
// displacing the continuation window.
func (s *Service) ListQueries(ctx context.Context, request *codev1.ListQueriesRequest) (*codev1.ListQueriesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if s.queries == nil {
		return nil, rpcerror.Errorf(codes.FailedPrecondition, rpcerror.AgentQueryReplayDisabled, "query listing requires the agent replay store")
	}
	if request == nil {
		request = &codev1.ListQueriesRequest{}
	}
	if request.SessionId != nil && request.GetSessionId() == "" {
		return nil, status.Error(codes.InvalidArgument, "session id filter must not be empty")
	}
	states, stateKey, err := queryStateFilter(request.GetStates())
	if err != nil {
		return nil, err
	}
	pageSize, err := agentPageSize(request.GetPageSize())
	if err != nil {
		return nil, err
	}
	filterKey := request.GetSessionId() + "|" + stateKey
	cursor, err := decodeAgentPageCursor(request.GetPageToken(), queryPageKind, filterKey)
	if err != nil {
		return nil, err
	}

	snapshots := s.queries.List()
	filtered := make([]QuerySnapshot, 0, len(snapshots))
	for _, snapshot := range snapshots {
		if request.SessionId != nil && snapshot.SessionID != request.GetSessionId() {
			continue
		}
		if len(states) > 0 {
			if _, ok := states[snapshot.State]; !ok {
				continue
			}
		}
		if cursor != nil && !afterAgentCursor(snapshot.CreatedAt, snapshot.ID, cursor) {
			continue
		}
		filtered = append(filtered, snapshot)
	}
	sort.Slice(filtered, func(i, j int) bool {
		return newerAgentListItem(filtered[i].CreatedAt, filtered[i].ID, filtered[j].CreatedAt, filtered[j].ID)
	})

	response := &codev1.ListQueriesResponse{}
	limit := min(len(filtered), pageSize+1)
	for _, snapshot := range filtered[:min(limit, pageSize)] {
		response.Queries = append(response.Queries, queryInfoOf(snapshot))
	}
	if limit > pageSize {
		last := filtered[pageSize-1]
		response.NextPageToken, err = encodeAgentPageCursor(agentPageCursor{
			Kind: queryPageKind, Filter: filterKey, TimeUnixNano: last.CreatedAt.UnixNano(), ID: last.ID,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode query page token: %v", err)
		}
	}
	return response, nil
}

// ListSessions returns the sessions with a turn currently in flight.
// Sessions are turn-scoped: the completed frame already retired the settled
// ones, whose agent-side transcripts resume by session id through Query.
func (s *Service) ListSessions(ctx context.Context, request *codev1.ListSessionsRequest) (*codev1.ListSessionsResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil {
		request = &codev1.ListSessionsRequest{}
	}
	states, stateKey, err := sessionStateFilter(request.GetStates())
	if err != nil {
		return nil, err
	}
	pageSize, err := agentPageSize(request.GetPageSize())
	if err != nil {
		return nil, err
	}
	cursor, err := decodeAgentPageCursor(request.GetPageToken(), sessionPageKind, stateKey)
	if err != nil {
		return nil, err
	}

	s.mu.Lock()
	sessions := make([]*session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	snapshots := make([]sessionSnapshot, 0, len(sessions))
	for _, sess := range sessions {
		snapshot := sess.snapshot()
		state := sessionStateOf(snapshot)
		if len(states) > 0 {
			if _, ok := states[state]; !ok {
				continue
			}
		}
		if cursor != nil && !afterAgentCursor(snapshot.LastActivityAt, snapshot.ID, cursor) {
			continue
		}
		snapshots = append(snapshots, snapshot)
	}
	sort.Slice(snapshots, func(i, j int) bool {
		return newerAgentListItem(snapshots[i].LastActivityAt, snapshots[i].ID, snapshots[j].LastActivityAt, snapshots[j].ID)
	})

	response := &codev1.ListSessionsResponse{}
	limit := min(len(snapshots), pageSize+1)
	for _, snapshot := range snapshots[:min(limit, pageSize)] {
		response.Sessions = append(response.Sessions, sessionInfoOf(snapshot))
	}
	if limit > pageSize {
		last := snapshots[pageSize-1]
		response.NextPageToken, err = encodeAgentPageCursor(agentPageCursor{
			Kind: sessionPageKind, Filter: stateKey, TimeUnixNano: last.LastActivityAt.UnixNano(), ID: last.ID,
		})
		if err != nil {
			return nil, status.Errorf(codes.Internal, "encode session page token: %v", err)
		}
	}
	return response, nil
}

func queryInfoOf(snapshot QuerySnapshot) *codev1.AgentQueryInfo {
	info := &codev1.AgentQueryInfo{
		QueryId: snapshot.ID, SessionId: snapshot.SessionID,
		State:            agentQueryStateOf(snapshot.State),
		EarliestSequence: snapshot.Earliest, NextSequence: snapshot.Next,
		HistoryTruncated: snapshot.Earliest > 0,
		CreatedAt:        agentTimestamp(snapshot.CreatedAt),
		SettledAt:        agentTimestamp(snapshot.SettledAt),
	}
	if snapshot.StopReason != "" {
		stopReason := snapshot.StopReason
		info.StopReason = &stopReason
	}
	if snapshot.Agent != "" {
		agent := snapshot.Agent
		info.Agent = &agent
	}
	if snapshot.Err != nil {
		info.TerminalStatus = status.Convert(snapshot.Err.status()).Proto()
	}
	return info
}

func sessionInfoOf(snapshot sessionSnapshot) *codev1.AgentSessionInfo {
	info := &codev1.AgentSessionInfo{
		SessionId: snapshot.ID, WorkingDirectory: snapshot.WorkingDirectory,
		State: sessionStateOf(snapshot), Generation: snapshot.Generation,
		CreatedAt: agentTimestamp(snapshot.CreatedAt), LastActivityAt: agentTimestamp(snapshot.LastActivityAt),
	}
	if snapshot.ActiveQueryID != "" {
		queryID := snapshot.ActiveQueryID
		info.ActiveQueryId = &queryID
	}
	return info
}

func sessionStateOf(snapshot sessionSnapshot) codev1.AgentSessionState {
	if snapshot.Running {
		return codev1.AgentSessionState_AGENT_SESSION_STATE_RUNNING
	}
	return codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE
}

func agentTimestamp(value time.Time) *timestamppb.Timestamp {
	if value.IsZero() {
		return nil
	}
	timestamp := timestamppb.New(value)
	if timestamp.CheckValid() != nil {
		return nil
	}
	return timestamp
}

func queryStateFilter(values []codev1.AgentQueryState) (map[QueryState]struct{}, string, error) {
	states := make(map[QueryState]struct{}, len(values))
	keys := make([]int, 0, len(values))
	for _, value := range values {
		var state QueryState
		switch value {
		case codev1.AgentQueryState_AGENT_QUERY_STATE_RUNNING:
			state = QueryStateRunning
		case codev1.AgentQueryState_AGENT_QUERY_STATE_SETTLED:
			state = QueryStateSettled
		case codev1.AgentQueryState_AGENT_QUERY_STATE_LOST:
			state = QueryStateLost
		default:
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid agent query state filter %s", value)
		}
		if _, duplicate := states[state]; duplicate {
			continue
		}
		states[state] = struct{}{}
		keys = append(keys, int(value))
	}
	sort.Ints(keys)
	return states, integerFilterKey(keys), nil
}

func sessionStateFilter(values []codev1.AgentSessionState) (map[codev1.AgentSessionState]struct{}, string, error) {
	states := make(map[codev1.AgentSessionState]struct{}, len(values))
	keys := make([]int, 0, len(values))
	for _, value := range values {
		switch value {
		case codev1.AgentSessionState_AGENT_SESSION_STATE_IDLE,
			codev1.AgentSessionState_AGENT_SESSION_STATE_RUNNING:
		default:
			return nil, "", status.Errorf(codes.InvalidArgument, "invalid agent session state filter %s", value)
		}
		if _, duplicate := states[value]; duplicate {
			continue
		}
		states[value] = struct{}{}
		keys = append(keys, int(value))
	}
	sort.Ints(keys)
	return states, integerFilterKey(keys), nil
}

func integerFilterKey(values []int) string {
	parts := make([]string, len(values))
	for index, value := range values {
		parts[index] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}

func agentPageSize(requested uint32) (int, error) {
	if requested > agentListMaxPageSize {
		return 0, status.Errorf(codes.InvalidArgument, "page size %d exceeds maximum %d", requested, agentListMaxPageSize)
	}
	if requested == 0 {
		requested = agentListDefaultPageSize
	}
	return int(requested), nil
}

func decodeAgentPageCursor(token, kind, filter string) (*agentPageCursor, error) {
	if token == "" {
		return nil, nil
	}
	if len(token) > maxAgentPageTokenBytes {
		return nil, status.Error(codes.InvalidArgument, "page token is too large")
	}
	payload, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "page token is invalid")
	}
	cursor := &agentPageCursor{}
	if err := json.Unmarshal(payload, cursor); err != nil || cursor.Kind != kind || cursor.Filter != filter || cursor.ID == "" {
		return nil, status.Error(codes.InvalidArgument, "page token does not match this request")
	}
	return cursor, nil
}

func encodeAgentPageCursor(cursor agentPageCursor) (string, error) {
	payload, err := json.Marshal(cursor)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(payload), nil
}

func afterAgentCursor(value time.Time, id string, cursor *agentPageCursor) bool {
	unixNano := value.UnixNano()
	return unixNano < cursor.TimeUnixNano || unixNano == cursor.TimeUnixNano && id < cursor.ID
}

func newerAgentListItem(leftTime time.Time, leftID string, rightTime time.Time, rightID string) bool {
	if leftTime.Equal(rightTime) {
		return leftID > rightID
	}
	return leftTime.After(rightTime)
}

func agentListCapabilities(queryListing bool) *codev1.AgentListCapabilities {
	return &codev1.AgentListCapabilities{
		Queries: queryListing, Sessions: true,
		DefaultPageSize: agentListDefaultPageSize, MaxPageSize: agentListMaxPageSize,
	}
}
