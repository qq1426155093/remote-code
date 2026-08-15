package process

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

const (
	processLogFormatVersion   = 2
	processLogDirectoryName   = "logs"
	processLogStateFileName   = "state.json"
	processLogSegmentHeader   = 32
	processLogRecordHeader    = 44
	processLogRecordFooter    = 4
	processLogOffsetIndexSize = 16
	processLogLineIndexSize   = 24
	processLogIndexStride     = 256
	maxProcessLogTailLines    = 100_000

	defaultProcessLogMaxBytes      = int64(64 << 20)
	defaultProcessLogMaxTotalBytes = int64(4 << 30)
	defaultProcessLogSegmentBytes  = int64(4 << 20)
	defaultProcessLogRetention     = 7 * 24 * time.Hour
	defaultProcessLogMaxObservers  = 8
	minProcessLogSegmentBytes      = int64(256 << 10)
	maxProcessLogConfiguredBytes   = int64(1 << 40)
)

var (
	processLogSegmentMagic = [8]byte{'R', 'C', 'L', 'O', 'G', 'V', '2', '\n'}
	processLogCRCTable     = crc32.MakeTable(crc32.Castagnoli)
)

const (
	processLogRecordMagic uint32 = 0x52434c32

	processLogFlagLineStart byte = 1 << 0
	processLogFlagLineEnd   byte = 1 << 1
)

// LogConfig bounds persistent process output and concurrent observers.
type LogConfig struct {
	MaxBytesPerProcess int64
	MaxTotalBytes      int64
	SegmentBytes       int64
	RetentionAfterExit time.Duration
	MaxObservers       int
}

// ValidateLogConfig checks configured process log retention bounds.
func ValidateLogConfig(config LogConfig) error {
	_, err := normalizeLogConfig(config)
	return err
}

func normalizeLogConfig(config LogConfig) (LogConfig, error) {
	if config.MaxBytesPerProcess == 0 {
		config.MaxBytesPerProcess = defaultProcessLogMaxBytes
	}
	if config.MaxTotalBytes == 0 {
		config.MaxTotalBytes = defaultProcessLogMaxTotalBytes
	}
	if config.SegmentBytes == 0 {
		config.SegmentBytes = defaultProcessLogSegmentBytes
	}
	if config.RetentionAfterExit == 0 {
		config.RetentionAfterExit = defaultProcessLogRetention
	}
	if config.MaxObservers == 0 {
		config.MaxObservers = defaultProcessLogMaxObservers
	}
	if config.MaxBytesPerProcess < minProcessLogSegmentBytes || config.MaxBytesPerProcess > maxProcessLogConfiguredBytes {
		return LogConfig{}, fmt.Errorf("process log max bytes must be between %d and %d", minProcessLogSegmentBytes, maxProcessLogConfiguredBytes)
	}
	if config.MaxTotalBytes < config.MaxBytesPerProcess || config.MaxTotalBytes > maxProcessLogConfiguredBytes {
		return LogConfig{}, errors.New("process log total max bytes must be at least the per-process maximum")
	}
	if config.SegmentBytes < minProcessLogSegmentBytes || config.SegmentBytes > config.MaxBytesPerProcess {
		return LogConfig{}, fmt.Errorf("process log segment bytes must be between %d and the per-process maximum", minProcessLogSegmentBytes)
	}
	if config.RetentionAfterExit < 0 {
		return LogConfig{}, errors.New("process log retention must not be negative")
	}
	if config.MaxObservers < 1 || config.MaxObservers > 1024 {
		return LogConfig{}, errors.New("process log max observers must be between 1 and 1024")
	}
	return config, nil
}

type processLogState struct {
	FormatVersion  int       `json:"format_version"`
	EarliestOffset uint64    `json:"earliest_offset"`
	NextOffset     uint64    `json:"next_offset"`
	Finalized      bool      `json:"finalized"`
	Complete       bool      `json:"complete"`
	Expired        bool      `json:"expired,omitempty"`
	UpdatedAt      time.Time `json:"updated_at"`
}

type processLogSegment struct {
	first       uint64
	next        uint64
	path        string
	oidx        string
	stdoutLines string
	stderrLines string
	logSize     int64
	logFile     *os.File
	offsetIndex *os.File
	stdoutIndex *os.File
	stderrIndex *os.File
}

type processLogLineState struct {
	open      bool
	start     uint64
	last      uint64
	timestamp int64
}

type processLog struct {
	mu sync.Mutex

	directory string
	config    LogConfig
	segments  []*processLogSegment
	active    *processLogSegment
	earliest  uint64
	next      uint64
	finalized bool
	complete  bool
	expired   bool
	writeErr  error
	notify    chan struct{}
	observers int
	now       func() time.Time
	stdout    processLogLineState
	stderr    processLogLineState
}

type processLogWriter struct {
	log    *processLog
	stream codev1.ProcessLogStream
}

func (w *processLogWriter) Write(payload []byte) (int, error) {
	return w.log.write(w.stream, payload)
}

type storedProcessLogRecord struct {
	offset     uint64
	lineOffset uint64
	timestamp  int64
	stream     codev1.ProcessLogStream
	flags      byte
	payload    []byte
}

func (r storedProcessLogRecord) lineStart() bool { return r.flags&processLogFlagLineStart != 0 }
func (r storedProcessLogRecord) lineEnd() bool   { return r.flags&processLogFlagLineEnd != 0 }

type processLogLineEntry struct {
	start     uint64
	last      uint64
	timestamp int64
}

type preparedProcessLogRead struct {
	earliest         uint64
	end              uint64
	start            uint64
	finalized        bool
	complete         bool
	notify           <-chan struct{}
	historyTruncated bool
	tailTruncated    bool
	selectedLines    map[uint64]struct{}
}

func newProcessLog(directory string, config LogConfig) (*processLog, error) {
	if err := os.Mkdir(directory, 0o700); err != nil {
		return nil, fmt.Errorf("create process log directory: %w", err)
	}
	if err := os.Chmod(directory, 0o700); err != nil {
		return nil, fmt.Errorf("secure process log directory: %w", err)
	}
	log := &processLog{
		directory: directory,
		config:    config,
		complete:  true,
		notify:    make(chan struct{}),
		now:       time.Now,
	}
	if err := log.writeStateLocked(); err != nil {
		return nil, err
	}
	return log, nil
}

func openProcessLog(directory string, config LogConfig, finalized bool) (*processLog, error) {
	info, err := os.Lstat(directory)
	if err != nil {
		return nil, fmt.Errorf("stat process log directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("process log path is not a secure directory")
	}
	log := &processLog{
		directory: directory,
		config:    config,
		complete:  true,
		finalized: finalized,
		notify:    make(chan struct{}),
		now:       time.Now,
	}
	var persisted processLogState
	if readErr := readJSON(filepath.Join(directory, processLogStateFileName), &persisted); readErr == nil {
		if persisted.FormatVersion != processLogFormatVersion {
			return nil, fmt.Errorf("unsupported process log format %d", persisted.FormatVersion)
		}
		log.complete = persisted.Complete
		log.expired = persisted.Expired
		log.next = persisted.NextOffset
		log.earliest = persisted.EarliestOffset
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return nil, fmt.Errorf("read process log state: %w", readErr)
	}
	if err := log.rebuildLocked(); err != nil {
		return nil, err
	}
	log.finalized = finalized
	if finalized {
		close(log.notify)
	}
	if err := log.writeStateLocked(); err != nil {
		return nil, err
	}
	return log, nil
}

func (l *processLog) stdoutWriter() io.Writer {
	return &processLogWriter{log: l, stream: codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT}
}

func (l *processLog) stderrWriter() io.Writer {
	return &processLogWriter{log: l, stream: codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR}
}

func (l *processLog) write(stream codev1.ProcessLogStream, payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finalized {
		return 0, errors.New("process log is finalized")
	}
	if l.writeErr != nil {
		return 0, l.writeErr
	}
	state := l.lineState(stream)
	written := 0
	timestamp := l.now().UnixNano()
	for len(payload) > 0 {
		if !state.open {
			state.open = true
			state.start = l.next
		}
		length := len(payload)
		if length > maxLogFrameBytes {
			length = maxLogFrameBytes
		}
		lineEnd := false
		if index := bytes.IndexByte(payload[:length], '\n'); index >= 0 {
			length = index + 1
			lineEnd = true
		}
		flags := byte(0)
		if state.last < state.start || l.next == state.start {
			flags |= processLogFlagLineStart
		}
		if lineEnd {
			flags |= processLogFlagLineEnd
		}
		record := storedProcessLogRecord{
			offset: l.next, lineOffset: state.start, timestamp: timestamp,
			stream: stream, flags: flags, payload: payload[:length],
		}
		if err := l.appendRecordLocked(record); err != nil {
			l.complete = false
			l.writeErr = fmt.Errorf("append process log: %w", err)
			l.broadcastLocked()
			return written, l.writeErr
		}
		state.last = record.offset
		state.timestamp = timestamp
		written += length
		payload = payload[length:]
		if lineEnd {
			*state = processLogLineState{}
		}
	}
	l.broadcastLocked()
	return written, nil
}

func (l *processLog) lineState(stream codev1.ProcessLogStream) *processLogLineState {
	if stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
		return &l.stderr
	}
	return &l.stdout
}

func (l *processLog) appendRecordLocked(record storedProcessLogRecord) error {
	encoded := encodeProcessLogRecord(record)
	if err := l.ensureActiveLocked(int64(len(encoded))); err != nil {
		return err
	}
	segment := l.active
	oldLogSize := segment.logSize
	oldOffsetSize, _ := segment.offsetIndex.Seek(0, io.SeekEnd)
	oldStdoutSize, _ := segment.stdoutIndex.Seek(0, io.SeekEnd)
	oldStderrSize, _ := segment.stderrIndex.Seek(0, io.SeekEnd)
	rollback := func() {
		_ = segment.logFile.Truncate(oldLogSize)
		_, _ = segment.logFile.Seek(0, io.SeekEnd)
		_ = segment.offsetIndex.Truncate(oldOffsetSize)
		_, _ = segment.offsetIndex.Seek(0, io.SeekEnd)
		_ = segment.stdoutIndex.Truncate(oldStdoutSize)
		_, _ = segment.stdoutIndex.Seek(0, io.SeekEnd)
		_ = segment.stderrIndex.Truncate(oldStderrSize)
		_, _ = segment.stderrIndex.Seek(0, io.SeekEnd)
	}
	if err := writeFull(segment.logFile, encoded); err != nil {
		rollback()
		return err
	}
	if record.offset%processLogIndexStride == 0 {
		if err := writeOffsetIndexEntry(segment.offsetIndex, record.offset, uint64(oldLogSize)); err != nil {
			rollback()
			return err
		}
	}
	if record.lineEnd() {
		entry := processLogLineEntry{start: record.lineOffset, last: record.offset, timestamp: record.timestamp}
		index := segment.stdoutIndex
		if record.stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
			index = segment.stderrIndex
		}
		if err := writeLineIndexEntry(index, entry); err != nil {
			rollback()
			return err
		}
	}
	segment.logSize += int64(len(encoded))
	segment.next = record.offset + 1
	l.next = record.offset + 1
	if len(l.segments) == 1 && l.earliest == 0 {
		l.earliest = segment.first
	}
	return nil
}

func (l *processLog) ensureActiveLocked(recordBytes int64) error {
	if l.active != nil && l.active.next > l.active.first && l.active.logSize+recordBytes > l.config.SegmentBytes {
		if err := l.sealActiveLocked(); err != nil {
			return err
		}
		_ = l.trimLocked()
	}
	if l.active != nil {
		return nil
	}
	segment, err := createProcessLogSegment(l.directory, l.next, l.now())
	if err != nil {
		return err
	}
	l.active = segment
	l.segments = append(l.segments, segment)
	if len(l.segments) == 1 {
		l.earliest = l.next
	}
	return nil
}

func createProcessLogSegment(directory string, first uint64, created time.Time) (*processLogSegment, error) {
	base := filepath.Join(directory, fmt.Sprintf("%020d", first))
	segment := &processLogSegment{
		first: first, next: first, path: base + ".log", oidx: base + ".oidx",
		stdoutLines: base + ".stdout.lidx", stderrLines: base + ".stderr.lidx",
	}
	files := []*os.File{}
	closeFiles := func() {
		for _, file := range files {
			_ = file.Close()
		}
	}
	var err error
	if segment.logFile, err = openExclusiveLogFile(segment.path); err != nil {
		return nil, err
	}
	files = append(files, segment.logFile)
	if segment.offsetIndex, err = openExclusiveLogFile(segment.oidx); err != nil {
		closeFiles()
		return nil, err
	}
	files = append(files, segment.offsetIndex)
	if segment.stdoutIndex, err = openExclusiveLogFile(segment.stdoutLines); err != nil {
		closeFiles()
		return nil, err
	}
	files = append(files, segment.stdoutIndex)
	if segment.stderrIndex, err = openExclusiveLogFile(segment.stderrLines); err != nil {
		closeFiles()
		return nil, err
	}
	header := make([]byte, processLogSegmentHeader)
	copy(header[:8], processLogSegmentMagic[:])
	binary.BigEndian.PutUint32(header[8:12], processLogFormatVersion)
	binary.BigEndian.PutUint32(header[12:16], processLogSegmentHeader)
	binary.BigEndian.PutUint64(header[16:24], first)
	binary.BigEndian.PutUint64(header[24:32], uint64(created.UnixNano()))
	if err := writeFull(segment.logFile, header); err != nil {
		closeFiles()
		return nil, fmt.Errorf("write process log segment header: %w", err)
	}
	segment.logSize = processLogSegmentHeader
	return segment, nil
}

func openExclusiveLogFile(name string) (*os.File, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create process log file: %w", err)
	}
	return file, nil
}

func encodeProcessLogRecord(record storedProcessLogRecord) []byte {
	total := processLogRecordHeader + len(record.payload) + processLogRecordFooter
	encoded := make([]byte, total)
	binary.BigEndian.PutUint32(encoded[0:4], processLogRecordMagic)
	binary.BigEndian.PutUint32(encoded[4:8], uint32(total))
	binary.BigEndian.PutUint64(encoded[8:16], record.offset)
	binary.BigEndian.PutUint64(encoded[16:24], record.lineOffset)
	binary.BigEndian.PutUint64(encoded[24:32], uint64(record.timestamp))
	binary.BigEndian.PutUint32(encoded[32:36], uint32(len(record.payload)))
	encoded[36] = byte(record.stream)
	encoded[37] = record.flags
	copy(encoded[processLogRecordHeader:], record.payload)
	hash := crc32.New(processLogCRCTable)
	_, _ = hash.Write(encoded[8:40])
	_, _ = hash.Write(record.payload)
	binary.BigEndian.PutUint32(encoded[40:44], hash.Sum32())
	binary.BigEndian.PutUint32(encoded[total-4:], uint32(total))
	return encoded
}

func readProcessLogRecordAt(file *os.File, position int64) (storedProcessLogRecord, int64, error) {
	header := make([]byte, processLogRecordHeader)
	if _, err := file.ReadAt(header, position); err != nil {
		return storedProcessLogRecord{}, position, err
	}
	if binary.BigEndian.Uint32(header[0:4]) != processLogRecordMagic {
		return storedProcessLogRecord{}, position, errors.New("invalid process log record magic")
	}
	total := int(binary.BigEndian.Uint32(header[4:8]))
	payloadLength := int(binary.BigEndian.Uint32(header[32:36]))
	if payloadLength < 0 || payloadLength > maxLogFrameBytes || total != processLogRecordHeader+payloadLength+processLogRecordFooter {
		return storedProcessLogRecord{}, position, errors.New("invalid process log record length")
	}
	tail := make([]byte, payloadLength+processLogRecordFooter)
	if _, err := file.ReadAt(tail, position+processLogRecordHeader); err != nil {
		return storedProcessLogRecord{}, position, err
	}
	if binary.BigEndian.Uint32(tail[payloadLength:]) != uint32(total) {
		return storedProcessLogRecord{}, position, errors.New("process log record footer mismatch")
	}
	hash := crc32.New(processLogCRCTable)
	_, _ = hash.Write(header[8:40])
	_, _ = hash.Write(tail[:payloadLength])
	if binary.BigEndian.Uint32(header[40:44]) != hash.Sum32() {
		return storedProcessLogRecord{}, position, errors.New("process log record checksum mismatch")
	}
	stream := codev1.ProcessLogStream(header[36])
	if stream != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT && stream != codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
		return storedProcessLogRecord{}, position, errors.New("invalid process log stream")
	}
	return storedProcessLogRecord{
		offset:     binary.BigEndian.Uint64(header[8:16]),
		lineOffset: binary.BigEndian.Uint64(header[16:24]),
		timestamp:  int64(binary.BigEndian.Uint64(header[24:32])),
		stream:     stream, flags: header[37], payload: tail[:payloadLength],
	}, position + int64(total), nil
}

func writeOffsetIndexEntry(file *os.File, offset, position uint64) error {
	entry := make([]byte, processLogOffsetIndexSize)
	binary.BigEndian.PutUint64(entry[0:8], offset)
	binary.BigEndian.PutUint64(entry[8:16], position)
	return writeFull(file, entry)
}

func writeLineIndexEntry(file *os.File, entry processLogLineEntry) error {
	encoded := make([]byte, processLogLineIndexSize)
	binary.BigEndian.PutUint64(encoded[0:8], entry.start)
	binary.BigEndian.PutUint64(encoded[8:16], entry.last)
	binary.BigEndian.PutUint64(encoded[16:24], uint64(entry.timestamp))
	return writeFull(file, encoded)
}

func (l *processLog) sealActiveLocked() error {
	if l.active == nil {
		return nil
	}
	segment := l.active
	for _, file := range []*os.File{segment.logFile, segment.offsetIndex, segment.stdoutIndex, segment.stderrIndex} {
		if file == nil {
			continue
		}
		if err := file.Sync(); err != nil {
			return fmt.Errorf("sync process log segment: %w", err)
		}
		if err := file.Close(); err != nil {
			return fmt.Errorf("close process log segment: %w", err)
		}
	}
	segment.logFile = nil
	segment.offsetIndex = nil
	segment.stdoutIndex = nil
	segment.stderrIndex = nil
	l.active = nil
	return l.writeStateLocked()
}

func (l *processLog) finalize() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.finalized {
		return l.writeErr
	}
	for stream, state := range map[codev1.ProcessLogStream]*processLogLineState{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT: &l.stdout,
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR: &l.stderr,
	} {
		if !state.open || l.active == nil {
			continue
		}
		index := l.active.stdoutIndex
		if stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
			index = l.active.stderrIndex
		}
		if err := writeLineIndexEntry(index, processLogLineEntry{start: state.start, last: state.last, timestamp: state.timestamp}); err != nil {
			l.complete = false
			l.writeErr = fmt.Errorf("finalize process log line index: %w", err)
		}
	}
	l.stdout = processLogLineState{}
	l.stderr = processLogLineState{}
	if err := l.sealActiveLocked(); err != nil && l.writeErr == nil {
		l.complete = false
		l.writeErr = err
	}
	l.finalized = true
	_ = l.trimLocked()
	_ = l.writeStateLocked()
	l.broadcastLocked()
	return l.writeErr
}

func (l *processLog) broadcastLocked() {
	select {
	case <-l.notify:
	default:
		close(l.notify)
	}
	l.notify = make(chan struct{})
	if l.finalized {
		close(l.notify)
	}
}

func (l *processLog) writeStateLocked() error {
	state := processLogState{
		FormatVersion:  processLogFormatVersion,
		EarliestOffset: l.earliest,
		NextOffset:     l.next,
		Finalized:      l.finalized,
		Complete:       l.complete,
		Expired:        l.expired,
		UpdatedAt:      l.now(),
	}
	if err := atomicWriteJSON(l.directory, processLogStateFileName, state); err != nil {
		return fmt.Errorf("persist process log state: %w", err)
	}
	return nil
}

func (l *processLog) acquireObserver() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.observers >= l.config.MaxObservers {
		return errors.New("process log observer limit reached")
	}
	l.observers++
	return nil
}

func (l *processLog) releaseObserver() {
	l.mu.Lock()
	if l.observers > 0 {
		l.observers--
	}
	if l.trimLocked() {
		_ = l.writeStateLocked()
	}
	l.mu.Unlock()
}

func (l *processLog) prepareOffset(offset uint64) (preparedProcessLogRead, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if offset < l.earliest {
		return preparedProcessLogRead{}, fmt.Errorf("offset %d expired; earliest=%d next=%d", offset, l.earliest, l.next)
	}
	if offset > l.next {
		return preparedProcessLogRead{}, fmt.Errorf("offset %d exceeds next offset %d", offset, l.next)
	}
	return l.preparedLocked(offset, nil, false), nil
}

func (l *processLog) prepareTail(lines uint64, streams map[codev1.ProcessLogStream]bool) (preparedProcessLogRead, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lines == 0 {
		return l.preparedLocked(l.next, map[uint64]struct{}{}, false), nil
	}
	limit := int(lines)
	candidates := make([]processLogLineEntry, 0, limit*2+2)
	for _, stream := range []codev1.ProcessLogStream{
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT,
		codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR,
	} {
		if !streams[stream] {
			continue
		}
		entries, err := l.lastLineEntriesLocked(stream, limit)
		if err != nil {
			return preparedProcessLogRead{}, err
		}
		candidates = append(candidates, entries...)
		state := l.lineState(stream)
		if state.open {
			candidates = append(candidates, processLogLineEntry{start: state.start, last: state.last, timestamp: state.timestamp})
		}
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].last == candidates[j].last {
			return candidates[i].start > candidates[j].start
		}
		return candidates[i].last > candidates[j].last
	})
	seen := make(map[uint64]struct{}, limit)
	selected := make(map[uint64]struct{}, limit)
	start := l.next
	for _, candidate := range candidates {
		if _, ok := seen[candidate.start]; ok {
			continue
		}
		seen[candidate.start] = struct{}{}
		selected[candidate.start] = struct{}{}
		candidateStart := candidate.start
		if candidateStart < l.earliest {
			candidateStart = l.earliest
		}
		if candidateStart < start {
			start = candidateStart
		}
		if len(selected) == limit {
			break
		}
	}
	truncated := len(selected) < limit && l.earliest > 0
	return l.preparedLocked(start, selected, truncated), nil
}

func (l *processLog) preparedLocked(start uint64, selected map[uint64]struct{}, tailTruncated bool) preparedProcessLogRead {
	return preparedProcessLogRead{
		earliest:         l.earliest,
		end:              l.next,
		start:            start,
		finalized:        l.finalized,
		complete:         l.complete,
		notify:           l.notify,
		historyTruncated: l.earliest > 0 || l.expired,
		tailTruncated:    tailTruncated,
		selectedLines:    selected,
	}
}

func (l *processLog) current(cursor uint64) (end uint64, earliest uint64, finalized bool, complete bool, notify <-chan struct{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.next, l.earliest, l.finalized, l.complete, l.notify
}

func (l *processLog) lastLineEntriesLocked(stream codev1.ProcessLogStream, limit int) ([]processLogLineEntry, error) {
	entries := make([]processLogLineEntry, 0, limit)
	for index := len(l.segments) - 1; index >= 0 && len(entries) < limit; index-- {
		name := l.segments[index].stdoutLines
		if stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
			name = l.segments[index].stderrLines
		}
		file, err := openExistingProcessLogFile(name, os.O_RDONLY)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, fmt.Errorf("open process line index: %w", err)
		}
		info, err := file.Stat()
		if err != nil {
			_ = file.Close()
			return nil, err
		}
		count := int(info.Size() / processLogLineIndexSize)
		need := limit - len(entries)
		if count > need {
			count = need
		}
		for item := 0; item < count; item++ {
			position := info.Size() - int64(item+1)*processLogLineIndexSize
			encoded := make([]byte, processLogLineIndexSize)
			if _, err := file.ReadAt(encoded, position); err != nil {
				_ = file.Close()
				return nil, fmt.Errorf("read process line index: %w", err)
			}
			entries = append(entries, processLogLineEntry{
				start:     binary.BigEndian.Uint64(encoded[0:8]),
				last:      binary.BigEndian.Uint64(encoded[8:16]),
				timestamp: int64(binary.BigEndian.Uint64(encoded[16:24])),
			})
		}
		_ = file.Close()
	}
	return entries, nil
}

func (l *processLog) readRange(start, end, earliest uint64, streams map[codev1.ProcessLogStream]bool, selectedLines map[uint64]struct{}, send func(storedProcessLogRecord, bool) error) error {
	if start >= end {
		return nil
	}
	l.mu.Lock()
	segments := make([]processLogSegment, len(l.segments))
	for index, segment := range l.segments {
		segments[index] = *segment
	}
	l.mu.Unlock()
	for _, segment := range segments {
		if segment.next <= start || segment.first >= end {
			continue
		}
		file, err := openExistingProcessLogFile(segment.path, os.O_RDONLY)
		if err != nil {
			return fmt.Errorf("open process log segment: %w", err)
		}
		position, err := seekProcessLogOffset(segment, start)
		if err != nil {
			_ = file.Close()
			return err
		}
		for {
			record, nextPosition, err := readProcessLogRecordAt(file, position)
			if err != nil {
				if errors.Is(err, io.EOF) && record.offset == 0 {
					break
				}
				_ = file.Close()
				return fmt.Errorf("read process log record at %d: %w", position, err)
			}
			position = nextPosition
			if record.offset < start {
				continue
			}
			if record.offset >= end {
				break
			}
			if !streams[record.stream] {
				continue
			}
			if selectedLines != nil {
				if _, ok := selectedLines[record.lineOffset]; !ok {
					continue
				}
			}
			if err := send(record, record.lineOffset < earliest); err != nil {
				_ = file.Close()
				return err
			}
		}
		_ = file.Close()
	}
	return nil
}

func seekProcessLogOffset(segment processLogSegment, offset uint64) (int64, error) {
	position := int64(processLogSegmentHeader)
	file, err := openExistingProcessLogFile(segment.oidx, os.O_RDONLY)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return position, nil
		}
		return 0, fmt.Errorf("open process offset index: %w", err)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return 0, err
	}
	count := info.Size() / processLogOffsetIndexSize
	low, high := int64(0), count
	entry := make([]byte, processLogOffsetIndexSize)
	for low < high {
		middle := (low + high) / 2
		if _, err := file.ReadAt(entry, middle*processLogOffsetIndexSize); err != nil {
			return 0, err
		}
		if binary.BigEndian.Uint64(entry[0:8]) <= offset {
			low = middle + 1
		} else {
			high = middle
		}
	}
	if low > 0 {
		if _, err := file.ReadAt(entry, (low-1)*processLogOffsetIndexSize); err != nil {
			return 0, err
		}
		position = int64(binary.BigEndian.Uint64(entry[8:16]))
	}
	return position, nil
}

func (l *processLog) trimLocked() bool {
	if l.observers > 0 || l.config.MaxBytesPerProcess <= 0 {
		return false
	}
	originalCount := len(l.segments)
	originalEarliest := l.earliest
	for len(l.segments) > 0 && l.diskBytesLocked() > l.config.MaxBytesPerProcess {
		segment := l.segments[0]
		if segment == l.active || (!l.finalized && len(l.segments) == 1) {
			break
		}
		l.removeSegmentLocked(0)
	}
	if len(l.segments) > 0 {
		l.earliest = l.segments[0].first
	} else {
		l.earliest = l.next
	}
	return len(l.segments) != originalCount || l.earliest != originalEarliest
}

func (l *processLog) diskBytes() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.diskBytesLocked()
}

func (l *processLog) diskBytesLocked() int64 {
	var total int64
	for _, segment := range l.segments {
		for _, name := range []string{segment.path, segment.oidx, segment.stdoutLines, segment.stderrLines} {
			if info, err := os.Stat(name); err == nil {
				total += info.Size()
			}
		}
	}
	return total
}

func (l *processLog) trimOldestSegment() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.observers > 0 || len(l.segments) == 0 {
		return false
	}
	segment := l.segments[0]
	if segment == l.active || (!l.finalized && len(l.segments) == 1) {
		return false
	}
	l.removeSegmentLocked(0)
	if len(l.segments) > 0 {
		l.earliest = l.segments[0].first
	} else {
		l.earliest = l.next
	}
	_ = l.writeStateLocked()
	return true
}

func (l *processLog) expire() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.observers > 0 || !l.finalized {
		return false
	}
	for len(l.segments) > 0 {
		l.removeSegmentLocked(0)
	}
	l.earliest = l.next
	l.expired = true
	_ = l.writeStateLocked()
	return true
}

func (l *processLog) removeSegmentLocked(index int) {
	segment := l.segments[index]
	for _, file := range []*os.File{segment.logFile, segment.offsetIndex, segment.stdoutIndex, segment.stderrIndex} {
		if file != nil {
			_ = file.Close()
		}
	}
	for _, name := range []string{segment.path, segment.oidx, segment.stdoutLines, segment.stderrLines} {
		_ = os.Remove(name)
	}
	l.segments = append(l.segments[:index], l.segments[index+1:]...)
}

func (l *processLog) rebuildLocked() error {
	entries, err := os.ReadDir(l.directory)
	if err != nil {
		return fmt.Errorf("read process log directory: %w", err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		base := strings.TrimSuffix(entry.Name(), ".log")
		if len(base) != 20 {
			continue
		}
		if _, err := strconv.ParseUint(base, 10, 64); err == nil {
			info, infoErr := entry.Info()
			if infoErr != nil {
				return fmt.Errorf("stat process log segment: %w", infoErr)
			}
			if !info.Mode().IsRegular() || info.Mode()&os.ModeSymlink != 0 {
				return errors.New("process log segment is not a regular file")
			}
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	l.segments = nil
	persistedNext := l.next
	persistedEarliest := l.earliest
	if len(names) == 0 {
		// Retention can remove every segment. Keep the logical offsets from the
		// state file so a stale resume token is still rejected after restart.
		l.next = persistedNext
		l.earliest = persistedEarliest
		return nil
	}
	firstName := strings.TrimSuffix(names[0], ".log")
	firstOffset, err := strconv.ParseUint(firstName, 10, 64)
	if err != nil {
		return fmt.Errorf("parse process log segment offset: %w", err)
	}
	l.next = firstOffset
	openLines := map[codev1.ProcessLogStream]processLogLineState{}
	for _, name := range names {
		segment, records, truncated, err := rebuildProcessLogSegment(l.directory, name, l.next)
		if err != nil {
			return err
		}
		if truncated {
			l.complete = false
		}
		l.segments = append(l.segments, segment)
		for _, record := range records {
			state := openLines[record.stream]
			if record.lineStart() || !state.open {
				state = processLogLineState{open: true, start: record.lineOffset}
			}
			state.last = record.offset
			state.timestamp = record.timestamp
			if record.lineEnd() {
				state = processLogLineState{}
			}
			openLines[record.stream] = state
		}
		l.next = segment.next
	}
	if len(l.segments) > 0 {
		l.earliest = l.segments[0].first
	} else if !l.expired {
		l.earliest = l.next
	}
	if l.finalized && len(l.segments) > 0 {
		last := l.segments[len(l.segments)-1]
		for stream, state := range openLines {
			if !state.open {
				continue
			}
			name := last.stdoutLines
			if stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
				name = last.stderrLines
			}
			file, err := openExistingProcessLogFile(name, os.O_WRONLY|os.O_APPEND)
			if err != nil {
				return err
			}
			err = writeLineIndexEntry(file, processLogLineEntry{start: state.start, last: state.last, timestamp: state.timestamp})
			_ = file.Close()
			if err != nil {
				return err
			}
		}
	} else {
		l.stdout = openLines[codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT]
		l.stderr = openLines[codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR]
	}
	return nil
}

func rebuildProcessLogSegment(directory, name string, expected uint64) (*processLogSegment, []storedProcessLogRecord, bool, error) {
	path := filepath.Join(directory, name)
	file, err := openExistingProcessLogFile(path, os.O_RDWR)
	if err != nil {
		return nil, nil, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, nil, false, err
	}
	header := make([]byte, processLogSegmentHeader)
	if _, err := io.ReadFull(file, header); err != nil {
		return nil, nil, false, fmt.Errorf("read process log segment header: %w", err)
	}
	if !bytes.Equal(header[:8], processLogSegmentMagic[:]) || binary.BigEndian.Uint32(header[8:12]) != processLogFormatVersion || binary.BigEndian.Uint32(header[12:16]) != processLogSegmentHeader {
		return nil, nil, false, errors.New("invalid process log segment header")
	}
	first := binary.BigEndian.Uint64(header[16:24])
	if first != expected {
		return nil, nil, false, fmt.Errorf("process log offset gap: got %d want %d", first, expected)
	}
	base := strings.TrimSuffix(path, ".log")
	segment := &processLogSegment{
		first: first, next: first, path: path, oidx: base + ".oidx",
		stdoutLines: base + ".stdout.lidx", stderrLines: base + ".stderr.lidx",
	}
	for _, indexName := range []string{segment.oidx, segment.stdoutLines, segment.stderrLines} {
		index, err := resetProcessLogIndex(indexName)
		if err != nil {
			return nil, nil, false, err
		}
		_ = index.Close()
	}
	offsetIndex, err := openExistingProcessLogFile(segment.oidx, os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return nil, nil, false, err
	}
	stdoutIndex, err := openExistingProcessLogFile(segment.stdoutLines, os.O_WRONLY|os.O_APPEND)
	if err != nil {
		_ = offsetIndex.Close()
		return nil, nil, false, err
	}
	stderrIndex, err := openExistingProcessLogFile(segment.stderrLines, os.O_WRONLY|os.O_APPEND)
	if err != nil {
		_ = offsetIndex.Close()
		_ = stdoutIndex.Close()
		return nil, nil, false, err
	}
	defer offsetIndex.Close()
	defer stdoutIndex.Close()
	defer stderrIndex.Close()
	position := int64(processLogSegmentHeader)
	records := make([]storedProcessLogRecord, 0)
	truncated := false
	for {
		record, nextPosition, err := readProcessLogRecordAt(file, position)
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				truncated = position < info.Size()
				if truncateErr := file.Truncate(position); truncateErr != nil {
					return nil, nil, false, truncateErr
				}
				break
			}
			return nil, nil, false, fmt.Errorf("recover process log segment: %w", err)
		}
		if record.offset != segment.next {
			return nil, nil, false, fmt.Errorf("process log record offset %d, want %d", record.offset, segment.next)
		}
		if record.offset%processLogIndexStride == 0 {
			if err := writeOffsetIndexEntry(offsetIndex, record.offset, uint64(position)); err != nil {
				return nil, nil, false, err
			}
		}
		if record.lineEnd() {
			lineIndex := stdoutIndex
			if record.stream == codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR {
				lineIndex = stderrIndex
			}
			if err := writeLineIndexEntry(lineIndex, processLogLineEntry{start: record.lineOffset, last: record.offset, timestamp: record.timestamp}); err != nil {
				return nil, nil, false, err
			}
		}
		records = append(records, record)
		segment.next++
		position = nextPosition
	}
	segment.logSize = position
	return segment, records, truncated, nil
}

func openExistingProcessLogFile(name string, flags int) (*os.File, error) {
	before, err := os.Lstat(name)
	if err != nil {
		return nil, err
	}
	if !before.Mode().IsRegular() || before.Mode()&os.ModeSymlink != 0 {
		return nil, errors.New("process log path is not a regular file")
	}
	file, err := os.OpenFile(name, flags, 0)
	if err != nil {
		return nil, err
	}
	after, err := file.Stat()
	if err != nil || !after.Mode().IsRegular() || !os.SameFile(before, after) {
		_ = file.Close()
		return nil, errors.New("process log file changed while opening")
	}
	return file, nil
}

func resetProcessLogIndex(name string) (*os.File, error) {
	file, err := openExistingProcessLogFile(name, os.O_WRONLY)
	if errors.Is(err, os.ErrNotExist) {
		return openExclusiveLogFile(name)
	}
	if err != nil {
		return nil, err
	}
	if err := file.Truncate(0); err != nil {
		_ = file.Close()
		return nil, err
	}
	return file, nil
}
