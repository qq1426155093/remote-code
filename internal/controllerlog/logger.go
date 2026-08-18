package controllerlog

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"github.com/qq1426155093/remote-code/internal/logging"
	processservice "github.com/qq1426155093/remote-code/internal/process"
)

const (
	controllerLogDirectoryName = "controller-logs"
	// The file-transfer store already owns runtimeDirectory/file-transfers/controller.lock.
	// Keep the controller-log lock independent so enabling diagnostics does not
	// prevent resumable file transfers from opening.
	controllerLockFileName    = "controller-log.lock"
	controllerMetadataName    = "controller.json"
	controllerMetadataVersion = 1
	maxControllerEventBytes   = 60 << 10
	maxControllerFieldCount   = 32
	maxControllerFieldBytes   = 2048
	maxControllerMessageBytes = 16 << 10
)

const sensitiveNamePattern = `token|secret|password|authorization|credential|api[_-]?key|access[_-]?(?:key|token)|private[_-]?key|refresh[_-]?token|auth[_-]?(?:token|secret)|client[_-]?secret|session[_-]?token|cookie|bearer|jwt|passphrase|environment|env(?:[_-]?(?:value|var))?`

// The delimiter alternative catches compound names such as access_token and
// client-secret when they appear in free-form messages. Quoted values are
// consumed as a whole so a secret containing spaces cannot leak its suffix.
// Structured field keys are checked separately by isSensitiveKey below.
var sensitiveInlineValue = regexp.MustCompile(`(?i)(?:\b|[_-])(` + sensitiveNamePattern + `)\b(\s*[:=]\s*|\s+)(?:"(?:\\.|[^"\\])*"|'(?:\\.|[^'\\])*'|(?:Bearer\s+)?[^\s,;]+)`)

// JSON-shaped fragments are still treated as text (messages are not parsed as
// arbitrary JSON), so match any quoted key containing a sensitive marker. The
// escaped-character branch handles values such as "abc\\\"def" as one value.
var sensitiveJSONValue = regexp.MustCompile(`(?i)([\"'][^\"']*(?:` + sensitiveNamePattern + `)[^\"']*[\"']\s*:\s*[\"'])(?:\\.|[^\"\\']*)([\"'])`)

// FormatVersion identifies the durable record representation exposed by the
// controller log API. It follows the shared process segment format version.
const FormatVersion = 2

type persistedEvent struct {
	Timestamp time.Time         `json:"timestamp"`
	BootID    string            `json:"boot_id"`
	Level     logging.Level     `json:"level"`
	Component string            `json:"component"`
	Name      string            `json:"event"`
	Message   string            `json:"message,omitempty"`
	Fields    map[string]string `json:"fields,omitempty"`
}

type bootMetadata struct {
	FormatVersion int        `json:"format_version"`
	BootID        string     `json:"boot_id"`
	StartedAt     time.Time  `json:"started_at"`
	ShutdownAt    *time.Time `json:"shutdown_at,omitempty"`
}

// Logger is the persistent controller runtime logger and the implementation
// of logging.Logger used by internal services.
type Logger struct {
	mu sync.Mutex

	store      *processservice.RuntimeLog
	config     Config
	stderr     io.Writer
	lock       *runtimeLock
	metadata   string
	bootID     string
	startedAt  time.Time
	finalized  bool
	closed     bool
	storeError bool
}

// Open creates or reopens the controller runtime log under runtimeDirectory.
// The returned logger owns an exclusive runtime lock until Close is called.
func Open(runtimeDirectory string, config Config, stderr io.Writer) (*Logger, error) {
	normalized, err := Normalize(config)
	if err != nil {
		return nil, err
	}
	if runtimeDirectory == "" {
		return nil, errors.New("runtime directory is required for controller logs")
	}
	if err := os.MkdirAll(runtimeDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("create controller runtime directory: %w", err)
	}
	rootInfo, err := os.Lstat(runtimeDirectory)
	if err != nil {
		return nil, fmt.Errorf("stat controller runtime directory: %w", err)
	}
	if !rootInfo.IsDir() || rootInfo.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("controller runtime directory must be a directory, not a symbolic link")
	}
	if err := os.Chmod(runtimeDirectory, 0o700); err != nil {
		return nil, fmt.Errorf("secure controller runtime directory: %w", err)
	}

	lock, err := acquireRuntimeLock(filepath.Join(runtimeDirectory, controllerLockFileName))
	if err != nil {
		return nil, err
	}
	releaseOnError := true
	defer func() {
		if releaseOnError {
			_ = lock.close()
		}
	}()

	metadataPath := filepath.Join(runtimeDirectory, controllerMetadataName)
	previous, err := readMetadata(metadataPath)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, fmt.Errorf("read controller log metadata: %w", err)
	}
	logDirectory := filepath.Join(runtimeDirectory, controllerLogDirectoryName)
	store, err := processservice.OpenRuntimeLog(logDirectory, processLogConfig(normalized))
	if err != nil {
		return nil, fmt.Errorf("open controller runtime log: %w", err)
	}
	if err := secureLogDirectory(logDirectory); err != nil {
		_ = store.Finalize()
		return nil, fmt.Errorf("secure controller runtime log: %w", err)
	}
	storeNeedsCleanup := true
	defer func() {
		if storeNeedsCleanup {
			_ = store.Finalize()
		}
	}()
	if previous.ShutdownAt != nil && normalized.RetentionAfterRestart > 0 && time.Since(previous.ShutdownAt.UTC()) >= normalized.RetentionAfterRestart {
		// Keep the logical boundary in state while removing every sealed old
		// segment. The next append continues the offset sequence even when the
		// physical history is empty.
		for store.TrimOldestSegment() {
		}
	}
	bootID, err := newBootID()
	if err != nil {
		return nil, err
	}
	startedAt := time.Now().UTC()
	if err := writeMetadata(metadataPath, bootMetadata{FormatVersion: controllerMetadataVersion, BootID: bootID, StartedAt: startedAt}); err != nil {
		return nil, fmt.Errorf("persist controller log metadata: %w", err)
	}
	logger := &Logger{
		store: store, config: normalized, stderr: normalizeWriter(stderr), lock: lock,
		metadata: metadataPath, bootID: bootID, startedAt: startedAt,
	}
	storeNeedsCleanup = false
	releaseOnError = false
	return logger, nil
}

// NewFallback returns a logger that preserves stderr diagnostics when the
// persistent runtime log cannot be opened. It is safe to pass to all services.
func NewFallback(stderr io.Writer) *Logger {
	bootID, _ := newBootID()
	return &Logger{stderr: normalizeWriter(stderr), config: DefaultConfig(), bootID: bootID, startedAt: time.Now().UTC()}
}

func normalizeWriter(writer io.Writer) io.Writer {
	if writer == nil {
		return io.Discard
	}
	return writer
}

// Emit implements logging.Logger. It writes one JSON event line to stderr and
// then appends the same bounded event to the durable store when available.
func (l *Logger) Emit(event logging.Event) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	persisted := normalizeEvent(event, l.bootID)
	data, err := json.Marshal(persisted)
	if err != nil {
		return
	}
	data = append(data, '\n')
	stderr := l.stderr
	if stderr == nil {
		stderr = io.Discard
	}
	_, _ = stderr.Write(data)
	if l.store == nil || l.closed || l.finalized {
		return
	}
	if err := l.store.Append(data); err != nil {
		if !l.storeError {
			l.storeError = true
			_, _ = fmt.Fprintf(stderr, "controller runtime log store unavailable: %v\n", err)
		}
		// A write failure must wake and terminate follow observers; otherwise
		// they can wait forever on a store that can no longer make progress.
		l.finalized = true
		_ = l.store.Finalize()
	}
}

// BootID identifies the current controller run. It is empty only if a caller
// constructed a zero Logger directly.
func (l *Logger) BootID() string {
	if l == nil {
		return ""
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.bootID
}

// Available reports whether a durable store is attached.
func (l *Logger) Available() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.store != nil && !l.storeError && !l.closed
}

// Store returns the durable store for the gRPC observer. The pointer remains
// owned by Logger and must not be closed by callers.
func (l *Logger) Store() *processservice.RuntimeLog {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.store == nil || l.storeError || l.closed {
		return nil
	}
	return l.store
}

// Capabilities describes the public controller-log limits. It remains useful
// after Finalize so clients can still inspect retained history during drain.
func (l *Logger) Capabilities() *codev1.ControllerLogCapabilities {
	if l == nil {
		return &codev1.ControllerLogCapabilities{}
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return &codev1.ControllerLogCapabilities{
		Available:     l.store != nil && !l.storeError && !l.closed,
		FormatVersion: FormatVersion,
		MaxTailLines:  processservice.MaxRuntimeLogTailLines,
		MaxObservers:  uint32(l.config.MaxObservers),
	}
}

// Finalize closes the active segment and wakes follow observers, but keeps the
// logger's lock and metadata file until Close.
func (l *Logger) Finalize() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finalized {
		return nil
	}
	l.finalized = true
	if l.store == nil {
		return nil
	}
	err := l.store.Finalize()
	if err != nil {
		l.storeError = true
	}
	return err
}

// Close finalizes the store if necessary, records the shutdown time, and
// releases the runtime lock.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return nil
	}
	l.closed = true
	var closeErr error
	if !l.finalized && l.store != nil {
		l.finalized = true
		closeErr = l.store.Finalize()
		if closeErr != nil {
			l.storeError = true
		}
	}
	if l.metadata != "" {
		now := time.Now().UTC()
		if err := writeMetadata(l.metadata, bootMetadata{FormatVersion: controllerMetadataVersion, BootID: l.bootID, StartedAt: l.startedAt, ShutdownAt: &now}); closeErr == nil {
			closeErr = err
		}
	}
	if err := l.lock.close(); closeErr == nil {
		closeErr = err
	}
	l.mu.Unlock()
	return closeErr
}

func normalizeEvent(event logging.Event, bootID string) persistedEvent {
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now().UTC()
	} else {
		event.Timestamp = event.Timestamp.UTC()
	}
	if event.Level == "" {
		event.Level = logging.LevelInfo
	}
	switch event.Level {
	case logging.LevelDebug, logging.LevelInfo, logging.LevelWarn, logging.LevelError:
	default:
		event.Level = logging.LevelInfo
	}
	event.Component = boundedText(redactInlineText(event.Component), 128)
	if event.Component == "" {
		event.Component = "controller"
	}
	event.Name = boundedText(redactInlineText(event.Name), 128)
	if event.Name == "" {
		event.Name = "runtime"
	}
	event.Message = boundedText(redactInlineText(event.Message), maxControllerMessageBytes)
	fields := make(map[string]string, len(event.Fields))
	keys := make([]string, 0, len(event.Fields))
	for key := range event.Fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		value := event.Fields[key]
		if len(fields) >= maxControllerFieldCount {
			break
		}
		sensitive := isSensitiveKey(key)
		key = boundedText(key, 128)
		if key == "" {
			continue
		}
		if sensitive {
			fields[key] = "[REDACTED]"
			continue
		}
		fields[key] = boundedText(redactInlineText(value), maxControllerFieldBytes)
	}
	result := persistedEvent{
		Timestamp: event.Timestamp, BootID: boundedText(bootID, 64), Level: event.Level,
		Component: event.Component, Name: event.Name, Message: event.Message,
		Fields: fields,
	}
	// Keep the JSON payload below the shared 64 KiB record limit even when a
	// caller supplies many large values. Deterministically reduce field values.
	for {
		encoded, err := json.Marshal(result)
		if err == nil && len(encoded)+1 <= maxControllerEventBytes {
			return result
		}
		if len(result.Fields) == 0 {
			result.Message = boundedText(result.Message, maxControllerMessageBytes/2)
			return result
		}
		for key := range result.Fields {
			value := result.Fields[key]
			if len(value) > 256 {
				result.Fields[key] = boundedText(value, len(value)/2)
			} else {
				delete(result.Fields, key)
			}
			break
		}
	}
}

func boundedText(value string, maximum int) string {
	if maximum <= 0 || len(value) <= maximum {
		return value
	}
	value = value[:maximum]
	for !utf8.ValidString(value) {
		value = value[:len(value)-1]
	}
	return value
}

func isSensitiveKey(key string) bool {
	lower := strings.ToLower(key)
	for _, marker := range []string{
		"token", "secret", "password", "authorization", "credential", "api_key", "api-key", "apikey",
		"access_key", "access-key", "accesskey", "private_key", "private-key", "privatekey", "cookie", "bearer", "jwt",
		"passphrase", "env_value", "environment", "env",
	} {
		if strings.Contains(lower, marker) {
			return true
		}
	}
	return false
}

func redactInlineText(value string) string {
	value = sensitiveJSONValue.ReplaceAllString(value, "${1}[REDACTED]${2}")
	return sensitiveInlineValue.ReplaceAllString(value, "$1$2[REDACTED]")
}

func secureLogDirectory(directory string) error {
	info, err := os.Lstat(directory)
	if err != nil {
		return err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return errors.New("controller log path is not a directory")
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return err
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		entryInfo, err := entry.Info()
		if err != nil {
			return err
		}
		if entryInfo.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("controller log entry %q is a symbolic link", entry.Name())
		}
		if entryInfo.Mode().IsRegular() {
			if err := os.Chmod(filepath.Join(directory, entry.Name()), 0o600); err != nil {
				return err
			}
		}
	}
	return nil
}

func newBootID() (string, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate controller boot id: %w", err)
	}
	bytes[6] = (bytes[6] & 0x0f) | 0x40
	bytes[8] = (bytes[8] & 0x3f) | 0x80
	return fmt.Sprintf("%s-%s-%s-%s-%s", hex.EncodeToString(bytes[0:4]), hex.EncodeToString(bytes[4:6]), hex.EncodeToString(bytes[6:8]), hex.EncodeToString(bytes[8:10]), hex.EncodeToString(bytes[10:16])), nil
}

func readMetadata(name string) (bootMetadata, error) {
	info, err := os.Lstat(name)
	if err != nil {
		return bootMetadata{}, err
	}
	if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 || info.Size() <= 0 || info.Size() > 64<<10 {
		return bootMetadata{}, errors.New("controller log metadata is not a safe regular file")
	}
	data, err := os.ReadFile(name)
	if err != nil {
		return bootMetadata{}, err
	}
	var metadata bootMetadata
	if err := json.Unmarshal(data, &metadata); err != nil {
		return bootMetadata{}, err
	}
	if metadata.FormatVersion != controllerMetadataVersion || metadata.BootID == "" {
		return bootMetadata{}, errors.New("unsupported controller log metadata")
	}
	return metadata, nil
}

func writeMetadata(name string, metadata bootMetadata) error {
	data, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	directory := filepath.Dir(name)
	temporary, err := os.CreateTemp(directory, ".controller-log-metadata-")
	if err != nil {
		return err
	}
	temporaryName := temporary.Name()
	defer os.Remove(temporaryName)
	if err := temporary.Chmod(0o600); err != nil {
		_ = temporary.Close()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return err
	}
	if err := temporary.Close(); err != nil {
		return err
	}
	if err := os.Rename(temporaryName, name); err != nil {
		return err
	}
	directoryFile, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer directoryFile.Close()
	return directoryFile.Sync()
}
