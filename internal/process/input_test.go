package process

import (
	"bytes"
	"context"
	"io"
	"sync"
	"testing"
	"time"
)

type bufferWriteCloser struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	closed bool
}

func (w *bufferWriteCloser) Write(data []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.closed {
		return 0, io.ErrClosedPipe
	}
	return w.buffer.Write(data)
}

func (w *bufferWriteCloser) Close() error {
	w.mu.Lock()
	w.closed = true
	w.mu.Unlock()
	return nil
}

func TestProcessInputWritesInOrderAndCloses(t *testing.T) {
	writer := &bufferWriteCloser{}
	input := newProcessInput(writer)
	if count, err := input.write(context.Background(), []byte("first")); err != nil || count != 5 {
		t.Fatalf("write(first) = %d, %v", count, err)
	}
	if count, err := input.write(context.Background(), []byte("-second")); err != nil || count != 7 {
		t.Fatalf("write(second) = %d, %v", count, err)
	}
	if err := input.closeInput(context.Background()); err != nil {
		t.Fatalf("closeInput() error = %v", err)
	}
	if !input.isClosed() {
		t.Fatal("input remains open")
	}
	writer.mu.Lock()
	got := writer.buffer.String()
	writer.mu.Unlock()
	if got != "first-second" {
		t.Fatalf("written data = %q", got)
	}
	if _, err := input.write(context.Background(), []byte("late")); err != errProcessInputClosed {
		t.Fatalf("write(after close) error = %v", err)
	}
}

func TestProcessInputCancellationDoesNotLeakBlockedCaller(t *testing.T) {
	reader, writer := io.Pipe()
	input := newProcessInput(writer)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if _, err := input.write(ctx, []byte("blocked")); err != context.DeadlineExceeded {
		t.Fatalf("write(blocked) error = %v, want DeadlineExceeded", err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}
	input.abort()
}
