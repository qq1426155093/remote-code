package process

import (
	"bytes"
	"encoding/binary"
	"io"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestFrameWriterEncodesTimestampLengthAndBinaryPayload(t *testing.T) {
	var output bytes.Buffer
	writer := newFrameWriter(&output)
	writer.now = func() time.Time { return time.Unix(123, 456) }
	payload := []byte{'a', 0, 'b', 0xff}
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v", n, err)
	}
	want := make([]byte, logFrameHeaderBytes+len(payload))
	binary.BigEndian.PutUint64(want[0:8], uint64(time.Unix(123, 456).UnixNano()))
	binary.BigEndian.PutUint32(want[8:12], uint32(len(payload)))
	copy(want[12:], payload)
	if !bytes.Equal(output.Bytes(), want) {
		t.Fatalf("encoded frame = %x, want %x", output.Bytes(), want)
	}
	frames, err := ReadLogFrames(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatalf("ReadLogFrames() error = %v", err)
	}
	if len(frames) != 1 || !frames[0].Timestamp.Equal(time.Unix(123, 456)) || !bytes.Equal(frames[0].Payload, payload) {
		t.Fatalf("ReadLogFrames() = %+v", frames)
	}
}

func TestFrameWriterSplitsLargeWritesAndHandlesShortWrites(t *testing.T) {
	payload := bytes.Repeat([]byte{0xa5}, maxLogFrameBytes+17)
	short := &shortWriter{maximum: 3}
	writer := newFrameWriter(short)
	if n, err := writer.Write(payload); err != nil || n != len(payload) {
		t.Fatalf("Write() = %d, %v", n, err)
	}
	frames, err := ReadLogFrames(bytes.NewReader(short.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != 2 || len(frames[0].Payload) != maxLogFrameBytes || len(frames[1].Payload) != 17 {
		t.Fatalf("frame sizes = %v, want %d and 17", frameSizes(frames), maxLogFrameBytes)
	}
	joined := append(append([]byte(nil), frames[0].Payload...), frames[1].Payload...)
	if !bytes.Equal(joined, payload) {
		t.Fatal("split frames changed payload")
	}
}

func TestFrameWriterConcurrentWritesRemainWhole(t *testing.T) {
	var output bytes.Buffer
	writer := newFrameWriter(&output)
	values := [][]byte{bytes.Repeat([]byte("a"), 100), bytes.Repeat([]byte("b"), 100), bytes.Repeat([]byte("c"), 100)}
	var group sync.WaitGroup
	for _, value := range values {
		group.Add(1)
		go func() {
			defer group.Done()
			if _, err := writer.Write(value); err != nil {
				t.Errorf("Write() error = %v", err)
			}
		}()
	}
	group.Wait()
	frames, err := ReadLogFrames(bytes.NewReader(output.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) != len(values) {
		t.Fatalf("got %d frames, want %d", len(frames), len(values))
	}
	got := make([][]byte, 0, len(frames))
	for _, frame := range frames {
		got = append(got, frame.Payload)
	}
	if !sameByteSlicesIgnoringOrder(got, values) {
		t.Fatalf("payloads = %q, want %q", got, values)
	}
}

func TestReadLogFramesIgnoresIncompleteTailAndRejectsOversize(t *testing.T) {
	var complete bytes.Buffer
	writer := newFrameWriter(&complete)
	if _, err := writer.Write([]byte("complete")); err != nil {
		t.Fatal(err)
	}
	data := append(append([]byte(nil), complete.Bytes()...), []byte{1, 2, 3}...)
	frames, err := ReadLogFrames(bytes.NewReader(data))
	if err != nil || len(frames) != 1 || string(frames[0].Payload) != "complete" {
		t.Fatalf("ReadLogFrames(incomplete) = %+v, %v", frames, err)
	}
	header := make([]byte, logFrameHeaderBytes)
	binary.BigEndian.PutUint32(header[8:12], maxLogFrameBytes+1)
	if _, err := ReadLogFrames(bytes.NewReader(header)); err == nil {
		t.Fatal("ReadLogFrames(oversize) succeeded")
	}
}

type shortWriter struct {
	bytes.Buffer
	maximum int
}

func (w *shortWriter) Write(value []byte) (int, error) {
	if len(value) > w.maximum {
		value = value[:w.maximum]
	}
	return w.Buffer.Write(value)
}

func frameSizes(frames []LogFrame) []int {
	result := make([]int, len(frames))
	for index := range frames {
		result[index] = len(frames[index].Payload)
	}
	return result
}

func sameByteSlicesIgnoringOrder(left, right [][]byte) bool {
	if len(left) != len(right) {
		return false
	}
	used := make([]bool, len(right))
	for _, value := range left {
		found := false
		for index, candidate := range right {
			if !used[index] && reflect.DeepEqual(value, candidate) {
				used[index] = true
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

var _ io.Writer = (*shortWriter)(nil)
