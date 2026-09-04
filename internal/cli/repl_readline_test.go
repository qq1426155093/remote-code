package cli

import (
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/charmbracelet/x/ansi"
	"github.com/charmbracelet/x/vt"
	"github.com/ergochat/readline"
)

const (
	readlineTestColumns = 40
	readlineTestRows    = 24
	readlineTestPrompt  = "remote-code:/> "
)

// readlineHarness drives a readline instance against a virtual terminal:
// rendered output feeds the emulator, and the emulator's query replies
// (cursor position reports) are forwarded back to stdin, so line-editing
// behavior is observed exactly as a real terminal would see it.
type readlineHarness struct {
	emulator *vt.Emulator
	stdin    io.Writer
	mu       sync.Mutex
}

// readlineTerminalWriter renders readline output into the virtual terminal.
// It emulates ONLCR output processing: readline's raw mode deliberately keeps
// OPOST enabled, so a bare "\n" moves the real cursor to the next column 0.
type readlineTerminalWriter struct {
	harness        *readlineHarness
	carriageReturn bool
}

func (w *readlineTerminalWriter) Write(data []byte) (int, error) {
	translated := make([]byte, 0, len(data)+len(data)/8)
	for _, value := range data {
		if value == '\n' && !w.carriageReturn {
			translated = append(translated, '\r')
		}
		translated = append(translated, value)
		w.carriageReturn = value == '\r'
	}
	w.harness.mu.Lock()
	defer w.harness.mu.Unlock()
	return w.harness.emulator.Write(translated)
}

func newReadlineHarness(t *testing.T) *readlineHarness {
	t.Helper()
	reader, writer := io.Pipe()
	harness := &readlineHarness{emulator: vt.NewEmulator(readlineTestColumns, readlineTestRows), stdin: writer}
	// The emulator answers device status reports while rendering and blocks
	// until the reply is consumed, so drain continuously onto stdin.
	go func() {
		buffer := make([]byte, 64)
		for {
			n, err := harness.emulator.Read(buffer)
			if n > 0 {
				if _, writeErr := writer.Write(buffer[:n]); writeErr != nil {
					return
				}
			}
			if err != nil {
				return
			}
		}
	}()
	line, err := readline.NewFromConfig(&readline.Config{
		Prompt:          readlineTestPrompt,
		InterruptPrompt: "^C",
		EOFPrompt:       "exit",
		HistoryLimit:    500,
		Stdin:           reader,
		Stdout:          &readlineTerminalWriter{harness: harness},
		Stderr:          io.Discard,
		FuncIsTerminal:  func() bool { return true },
		FuncMakeRaw:     func() error { return nil },
		FuncExitRaw:     func() error { return nil },
		FuncGetSize:     func() (int, int) { return readlineTestColumns, readlineTestRows },
	})
	if err != nil {
		writer.Close()
		reader.Close()
		t.Fatalf("initialize readline: %v", err)
	}
	go func() {
		for {
			if _, err := line.Readline(); err != nil {
				writer.Close()
				return
			}
		}
	}()
	t.Cleanup(func() {
		writer.Close()
		line.Close()
	})
	return harness
}

// send writes keystrokes and waits for the rendered screen to settle.
func (h *readlineHarness) send(keys string) {
	timer := time.NewTimer(2 * time.Second)
	defer timer.Stop()
	if _, err := h.stdin.Write([]byte(keys)); err != nil {
		return
	}
	previous := h.screen()
	for {
		select {
		case <-timer.C:
			return
		case <-time.After(20 * time.Millisecond):
		}
		if current := h.screen(); current == previous {
			return
		} else {
			previous = current
		}
	}
}

func (h *readlineHarness) screen() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.emulator.Render()
}

// row returns the rendered text of one screen row.
func (h *readlineHarness) row(index int) string {
	rows := strings.Split(h.screen(), "\n")
	if index >= len(rows) {
		return ""
	}
	return rows[index]
}

func (h *readlineHarness) cursor() (column, row int) {
	h.mu.Lock()
	defer h.mu.Unlock()
	position := h.emulator.CursorPosition()
	return position.X, position.Y
}

// promptColumn returns the expected cursor column for text of the given
// display width typed after the prompt.
func promptColumn(text string) int {
	return ansi.StringWidth(readlineTestPrompt) + ansi.StringWidth(text)
}

// TestReadlineEditRecalledHistoryWideRunes verifies that arrow-key editing
// after recalling history positions the cursor by display columns: wide runes
// (CJK) occupy two columns, so moving across them must shift the cursor by
// two columns, not one. The retired chzyer/readline dependency emitted one
// backspace per rune, which left the cursor inside the right half of a wide
// glyph and garbled subsequent edits.
func TestReadlineEditRecalledHistoryWideRunes(t *testing.T) {
	harness := newReadlineHarness(t)
	harness.send("echo 你好\r")
	harness.send("\x1b[A") // recall the previous command

	harness.send("\x1b[D") // left arrow: move before 好 (two columns wide)
	column, row := harness.cursor()
	if row != 1 || column != promptColumn("echo 你") {
		t.Fatalf("cursor after left arrow over wide rune = (column %d, row %d), want (%d, 1)",
			column, row, promptColumn("echo 你"))
	}

	harness.send("X")
	if got, want := harness.row(1), readlineTestPrompt+"echo 你X好"; got != want {
		t.Fatalf("line after edit = %q, want %q", got, want)
	}
	column, _ = harness.cursor()
	if want := promptColumn("echo 你X"); column != want {
		t.Fatalf("cursor column after insert = %d, want %d", column, want)
	}
}

// TestReadlineEditRecalledHistoryASCII pins the single-width path: cursor
// movement inside a recalled ASCII line stays column-accurate.
func TestReadlineEditRecalledHistoryASCII(t *testing.T) {
	harness := newReadlineHarness(t)
	harness.send("ls -l /data\r")
	harness.send("\x1b[A") // recall
	harness.send("\x1b[D\x1b[D\x1b[D")

	if got, want := harness.row(1), readlineTestPrompt+"ls -l /data"; got != want {
		t.Fatalf("recalled line = %q, want %q", got, want)
	}
	column, _ := harness.cursor()
	if want := promptColumn("ls -l /data") - 3; column != want {
		t.Fatalf("cursor column after left arrows = %d, want %d", column, want)
	}
}

// TestReadlineEditRecalledHistoryWrappedLine verifies editing on a recalled
// line long enough to wrap: 一二三四五六七八九十甲乙 is 24 columns, so it plus
// the prompt fills the prompt row exactly (the space lands in the last column)
// and "ab" renders on the next row. Editing must keep the cursor on the
// wrapped row. The first entry occupies rows 0-1, so the recalled line is
// edited on rows 2-3.
func TestReadlineEditRecalledHistoryWrappedLine(t *testing.T) {
	harness := newReadlineHarness(t)
	harness.send("一二三四五六七八九十甲乙 ab\r")
	harness.send("\x1b[A") // recall
	harness.send("\x1b[D\x1b[D")
	harness.send("X")

	if got, want := harness.row(3), "Xab"; got != want {
		t.Fatalf("wrapped row after edit = %q, want %q", got, want)
	}
	column, row := harness.cursor()
	if row != 3 || column != len("X") {
		t.Fatalf("cursor after insert = (column %d, row %d), want (1, 3)", column, row)
	}
}
