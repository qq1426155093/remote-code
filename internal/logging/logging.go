// Package logging defines the small, dependency-free event contract shared by
// controller services.  The controllerlog package provides the persistent
// implementation; services only depend on this interface so logging failures
// cannot become part of their lifecycle contract.
package logging

import "time"

// Level is the severity of one controller diagnostic event.
type Level string

const (
	LevelDebug Level = "DEBUG"
	LevelInfo  Level = "INFO"
	LevelWarn  Level = "WARN"
	LevelError Level = "ERROR"
)

// Event is intentionally made of bounded, string-valued fields.  Callers must
// not place secrets, request bodies, process output, or environment values in
// it; the persistent logger applies an additional size and key redaction
// boundary before encoding the event.
type Event struct {
	Timestamp time.Time
	Level     Level
	Component string
	Name      string
	Message   string
	Fields    map[string]string
}

// Logger is the service-side logging seam. Implementations must be safe for
// concurrent callers and must not panic when their backing sink is unavailable.
type Logger interface {
	Emit(Event)
}

// Nop is useful for package-level tests and for embedding a service without a
// controller runtime log.
type Nop struct{}

func (Nop) Emit(Event) {}

// Emit is a convenience helper that keeps nil logger handling at call sites
// small and consistent.
func Emit(logger Logger, event Event) {
	if logger != nil {
		logger.Emit(event)
	}
}
