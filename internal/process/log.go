package process

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	logFrameHeaderBytes = 12
	maxLogFrameBytes    = 64 << 10
)

// LogFrame is one decoded persistent output block.
type LogFrame struct {
	Timestamp time.Time
	Payload   []byte
}

type frameWriter struct {
	mu    sync.Mutex
	write io.Writer
	now   func() time.Time
}

func newFrameWriter(writer io.Writer) *frameWriter {
	return &frameWriter{write: writer, now: time.Now}
}

func (w *frameWriter) Write(payload []byte) (int, error) {
	if len(payload) == 0 {
		return 0, nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	written := 0
	for len(payload) > 0 {
		length := len(payload)
		if length > maxLogFrameBytes {
			length = maxLogFrameBytes
		}
		header := make([]byte, logFrameHeaderBytes)
		binary.BigEndian.PutUint64(header[0:8], uint64(w.now().UnixNano()))
		binary.BigEndian.PutUint32(header[8:12], uint32(length))
		if err := writeFull(w.write, header); err != nil {
			return written, err
		}
		if err := writeFull(w.write, payload[:length]); err != nil {
			return written, err
		}
		written += length
		payload = payload[length:]
	}
	return written, nil
}

func writeFull(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		n, err := writer.Write(value)
		if n > 0 {
			value = value[n:]
		}
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
	}
	return nil
}

// ReadLogFrames decodes complete frames from a persistent process log. An
// incomplete trailing frame is ignored because a running process may be in the
// middle of appending it.
func ReadLogFrames(reader io.Reader) ([]LogFrame, error) {
	frames := make([]LogFrame, 0)
	header := make([]byte, logFrameHeaderBytes)
	for {
		if _, err := io.ReadFull(reader, header); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return frames, nil
			}
			return nil, fmt.Errorf("read process log header: %w", err)
		}
		length := binary.BigEndian.Uint32(header[8:12])
		if length > maxLogFrameBytes {
			return nil, fmt.Errorf("process log frame length %d exceeds limit", length)
		}
		payload := make([]byte, int(length))
		if _, err := io.ReadFull(reader, payload); err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
				return frames, nil
			}
			return nil, fmt.Errorf("read process log payload: %w", err)
		}
		nanoseconds := int64(binary.BigEndian.Uint64(header[0:8]))
		frames = append(frames, LogFrame{Timestamp: time.Unix(0, nanoseconds), Payload: payload})
	}
}
