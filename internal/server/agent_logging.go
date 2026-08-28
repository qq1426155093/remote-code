package server

import (
	"context"
	"log/slog"
	"time"

	"github.com/qq1426155093/remote-code/internal/logging"
)

// agentLogFieldValueLimit bounds one structured value forwarded from the agent
// bridge to the persistent controller log.
const agentLogFieldValueLimit = 512

// agentLogHandler forwards agent bridge diagnostics — including the acp-go-sdk
// connection logger — into the bounded controller event log.
//
// The handler is also a content boundary: the SDK logs the raw protocol frame
// of a message it failed to parse, and that frame may carry prompt or message
// text, so "raw" attributes are dropped entirely rather than truncated, and
// every other value is length-capped.
type agentLogHandler struct {
	sink logging.Logger
}

func newAgentLogHandler(sink logging.Logger) *slog.Logger {
	return slog.New(agentLogHandler{sink: sink})
}

func (h agentLogHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h agentLogHandler) Handle(_ context.Context, record slog.Record) error {
	level := logging.LevelInfo
	switch {
	case record.Level < slog.LevelInfo:
		level = logging.LevelDebug
	case record.Level >= slog.LevelError:
		level = logging.LevelError
	case record.Level >= slog.LevelWarn:
		level = logging.LevelWarn
	}
	var fields map[string]string
	record.Attrs(func(attr slog.Attr) bool {
		if attr.Key == "raw" {
			return true
		}
		if fields == nil {
			fields = make(map[string]string, record.NumAttrs())
		}
		fields[attr.Key] = truncateLogValue(attr.Value.String())
		return true
	})
	logging.Emit(h.sink, logging.Event{
		Timestamp: time.Now(),
		Level:     level,
		Component: "agent",
		Name:      "sdk_log",
		Message:   truncateLogValue(record.Message),
		Fields:    fields,
	})
	return nil
}

func (h agentLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h agentLogHandler) WithGroup(string) slog.Handler            { return h }

func truncateLogValue(value string) string {
	if len(value) <= agentLogFieldValueLimit {
		return value
	}
	return value[:agentLogFieldValueLimit] + "…"
}
