package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestParseProcessWindowOptions(t *testing.T) {
	tests := []struct {
		name       string
		arguments  []string
		wantTail   uint64
		wantRefs   []string
		wantErrSub string
	}{
		{name: "defaults", wantTail: processWindowDefaultTailLines},
		{name: "tail and references", arguments: []string{"-n", "0", "designer", "pid:42"}, wantRefs: []string{"name:designer", "pid:42"}},
		{name: "maximum tail", arguments: []string{"--tail-lines", "100000", "id:abc"}, wantTail: 100_000, wantRefs: []string{"id:abc"}},
		{name: "option terminator", arguments: []string{"--", "-agent"}, wantTail: processWindowDefaultTailLines, wantRefs: []string{"name:-agent"}},
		{name: "missing tail", arguments: []string{"-n"}, wantErrSub: "requires a value"},
		{name: "invalid tail", arguments: []string{"-n", "nope"}, wantErrSub: "tail lines"},
		{name: "tail too large", arguments: []string{"-n", "100001"}, wantErrSub: "tail lines"},
		{name: "unknown option", arguments: []string{"--wat"}, wantErrSub: "unknown windows option"},
		{name: "too many", arguments: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "10"}, wantErrSub: "at most 9"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			options, err := parseProcessWindowOptions(test.arguments)
			if test.wantErrSub != "" {
				if err == nil || !strings.Contains(err.Error(), test.wantErrSub) {
					t.Fatalf("parseProcessWindowOptions() error = %v, want containing %q", err, test.wantErrSub)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if options.tailLines != test.wantTail {
				t.Fatalf("tail lines = %d, want %d", options.tailLines, test.wantTail)
			}
			gotRefs := make([]string, 0, len(options.references))
			for _, reference := range options.references {
				gotRefs = append(gotRefs, displayProcessWindowReference(reference))
			}
			if len(gotRefs) != len(test.wantRefs) || (len(gotRefs) > 0 && !reflect.DeepEqual(gotRefs, test.wantRefs)) {
				t.Fatalf("references = %#v, want %#v", gotRefs, test.wantRefs)
			}
		})
	}
}

func TestProcessWindowInputRoutesPrefixCommandsAcrossReads(t *testing.T) {
	var input processWindowInput
	actions := input.feed([]byte{'a', attachEscapeByte})
	assertWindowActions(t, actions, []processWindowInputAction{{kind: processWindowInputForward, data: []byte{'a'}}})
	if !input.prefix {
		t.Fatal("trailing prefix was not retained")
	}

	actions = input.feed([]byte{attachEscapeByte, 'b', attachEscapeByte, 'n', 'c', attachEscapeByte, 'z'})
	assertWindowActions(t, actions, []processWindowInputAction{
		{kind: processWindowInputForward, data: []byte{attachEscapeByte, 'b'}},
		{kind: processWindowInputNext},
		{kind: processWindowInputForward, data: []byte{'c', attachEscapeByte, 'z'}},
	})

	actions = input.feed([]byte{attachEscapeByte, '3', attachEscapeByte, 'x', attachEscapeByte, 'p', attachEscapeByte, '?', attachEscapeByte, 'q'})
	assertWindowActions(t, actions, []processWindowInputAction{
		{kind: processWindowInputSelect, index: 2},
		{kind: processWindowInputClose},
		{kind: processWindowInputPrevious},
		{kind: processWindowInputToggleHelp},
		{kind: processWindowInputQuit},
	})
}

func TestProcessWindowInputEditsOpenPrompt(t *testing.T) {
	var input processWindowInput
	if actions := input.feed([]byte{attachEscapeByte, 'o'}); len(actions) != 0 || !input.prompting {
		t.Fatalf("open prompt actions = %#v, prompting = %v", actions, input.prompting)
	}
	input.feed([]byte("设计者"))
	input.feed([]byte{0x7f})
	if got := input.promptText(); got != "设计" {
		t.Fatalf("prompt after UTF-8 backspace = %q", got)
	}
	input.feed([]byte{0x15})
	actions := input.feed([]byte("agent\r\nZ"))
	assertWindowActions(t, actions, []processWindowInputAction{
		{kind: processWindowInputOpen, value: "agent"},
		{kind: processWindowInputForward, data: []byte{'Z'}},
	})
	if input.prompting {
		t.Fatal("prompt remained active after enter")
	}

	input.feed([]byte{attachEscapeByte, 'o'})
	input.feed(bytes.Repeat([]byte{'a'}, processWindowPromptMaxBytes))
	actions = input.feed([]byte{'b'})
	assertWindowActions(t, actions, []processWindowInputAction{{kind: processWindowInputPromptLimit}})
	if len(input.prompt) != processWindowPromptMaxBytes {
		t.Fatalf("prompt length = %d, want %d", len(input.prompt), processWindowPromptMaxBytes)
	}
	actions = input.feed([]byte{0x03})
	assertWindowActions(t, actions, []processWindowInputAction{{kind: processWindowInputPromptCanceled}})
}

func assertWindowActions(t *testing.T, got, want []processWindowInputAction) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("actions = %#v, want %#v", got, want)
	}
}

func TestProcessWindowLayoutStaysWithinScreen(t *testing.T) {
	for _, size := range []struct{ columns, rows int }{{80, 24}, {121, 37}, {40, 12}} {
		for count := 1; count <= processWindowMaximumPanes; count++ {
			rectangles := calculateProcessWindowLayout(size.columns, size.rows, count)
			if len(rectangles) != count {
				t.Fatalf("layout %dx%d count %d returned %d rectangles", size.columns, size.rows, count, len(rectangles))
			}
			for first, rectangle := range rectangles {
				if rectangle.x < 0 || rectangle.y < 0 || rectangle.width <= 0 || rectangle.height <= 0 ||
					rectangle.x+rectangle.width > size.columns || rectangle.y+rectangle.height > size.rows-1 {
					t.Fatalf("layout %dx%d count %d rectangle %d out of bounds: %+v", size.columns, size.rows, count, first, rectangle)
				}
				for second := first + 1; second < len(rectangles); second++ {
					if rectanglesOverlap(rectangle, rectangles[second]) {
						t.Fatalf("layout %dx%d count %d overlaps: %+v and %+v", size.columns, size.rows, count, rectangle, rectangles[second])
					}
				}
			}
		}
	}
	if !processWindowLayoutFits(80, 24, 9) {
		t.Fatal("80x24 should support nine windows")
	}
	if processWindowLayoutFits(20, 6, 9) {
		t.Fatal("20x6 should not support nine usable windows")
	}
	if rectangles := calculateProcessWindowLayout(10, 1, 3); len(rectangles) != 3 {
		t.Fatalf("one-row layout returned %d rectangles, want 3", len(rectangles))
	}
}

func rectanglesOverlap(first, second windowRectangle) bool {
	return first.x < second.x+second.width && second.x < first.x+first.width &&
		first.y < second.y+second.height && second.y < first.y+first.height
}

func TestProcessWindowTextSanitizesControlsAndFitsWidth(t *testing.T) {
	got := sanitizeWindowText("bad\x1b[2J\nname\t")
	if strings.Contains(got, "\x1b") || strings.ContainsAny(got, "\r\n\t") {
		t.Fatalf("sanitizeWindowText() left controls in %q", got)
	}
	if got != "bad name " {
		t.Fatalf("sanitizeWindowText() = %q", got)
	}
	for width := 1; width <= 8; width++ {
		value := fitWindowText("设计-agent", width)
		if gotWidth := displayWindowWidth(value); gotWidth > width {
			t.Fatalf("fitWindowText width = %d, want <= %d: %q", gotWidth, width, value)
		}
	}
}

func TestVirtualProcessWindowTerminalsIsolateANSIState(t *testing.T) {
	first := newProcessWindowTerminal(12, 3)
	second := newProcessWindowTerminal(12, 3)
	defer first.Close()
	defer second.Close()
	if _, err := first.Write([]byte("before\x1b[2Jafter")); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Write([]byte("second")); err != nil {
		t.Fatal(err)
	}
	firstFrame := first.Render()
	secondFrame := second.Render()
	if !strings.Contains(firstFrame, "after") || strings.Contains(firstFrame, "second") {
		t.Fatalf("first terminal frame = %q", firstFrame)
	}
	if !strings.Contains(secondFrame, "second") || strings.Contains(secondFrame, "after") {
		t.Fatalf("second terminal frame = %q", secondFrame)
	}
	if strings.Contains(firstFrame, "\x1b[2J") {
		t.Fatalf("virtual terminal leaked screen-clear sequence: %q", firstFrame)
	}
}

func TestProcessWindowManagerKeepsFinalFrameAfterAttachmentEnds(t *testing.T) {
	localTerminal := &fakeTerminalController{}
	var created *fakeProcessWindowTerminal
	manager := newProcessWindowManager(localTerminal, io.Discard, nil, func(columns, rows int) processWindowTerminal {
		created = newFakeProcessWindowTerminal(columns, rows).(*fakeProcessWindowTerminal)
		return created
	}, 10)
	manager.columns, manager.rows = 80, 24
	attachment := newFakeProcessWindowAttachment("designer")
	_, attachmentCancel := context.WithCancel(manager.ctx)
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: attachment,
		cancel:     attachmentCancel,
		reference:  "name:designer",
	})
	if len(manager.panes) != 1 || manager.active != 0 {
		manager.shutdown()
		t.Fatalf("accepted panes = %d, active = %d", len(manager.panes), manager.active)
	}
	attachment.output <- []byte("working\r\ndone")
	attachment.finish(nil)
	deadline := time.After(2 * time.Second)
	ended := false
	for !ended {
		select {
		case event := <-manager.events:
			if err := manager.handleEvent(event); err != nil {
				manager.shutdown()
				t.Fatal(err)
			}
			_, ended = event.(processWindowAttachmentEnd)
		case <-deadline:
			manager.shutdown()
			t.Fatal("attachment did not finish")
		}
	}
	if manager.panes[0].state != processWindowPaneDone {
		manager.shutdown()
		t.Fatalf("pane state = %s, want done", manager.panes[0].state)
	}
	if got := created.Render(); !strings.Contains(got, "working") || !strings.Contains(got, "done") {
		manager.shutdown()
		t.Fatalf("retained terminal frame = %q", got)
	}
	manager.shutdown()
	if attachment.detachCount.Load() == 0 {
		t.Fatal("shutdown did not detach attachment")
	}
}

func TestProcessWindowManagerCloseSelectsAdjacentPane(t *testing.T) {
	manager := newProcessWindowManager(&fakeTerminalController{}, io.Discard, nil, newFakeProcessWindowTerminal, 10)
	manager.columns, manager.rows = 80, 24
	for _, name := range []string{"one", "two", "three"} {
		paneContext, paneCancel := context.WithCancel(manager.ctx)
		_, attachmentCancel := context.WithCancel(manager.ctx)
		manager.panes = append(manager.panes, &processWindowPane{
			id: uint64(len(manager.panes) + 1), ctx: paneContext, cancel: paneCancel,
			process:    &codev1.ProcessInfo{Id: name, Name: name},
			attachment: newFakeProcessWindowAttachment(name),
			terminal:   newFakeProcessWindowTerminal(10, 3), state: processWindowPaneActive,
			operations: make(chan processWindowOperation, processWindowOperationBuffer),
			attachStop: attachmentCancel,
		})
	}
	manager.applyLayout()
	manager.active = 1
	closed := manager.panes[1].attachment.(*fakeProcessWindowAttachment)
	manager.closeActivePane()
	if len(manager.panes) != 2 || manager.active != 1 || manager.panes[manager.active].process.GetName() != "three" {
		manager.detachWorkers.Wait()
		manager.cancel()
		manager.workers.Wait()
		t.Fatalf("after close panes = %d, active = %d", len(manager.panes), manager.active)
	}
	manager.detachWorkers.Wait()
	manager.cancel()
	manager.workers.Wait()
	if closed.detachCount.Load() != 1 {
		t.Fatalf("detach count = %d, want 1", closed.detachCount.Load())
	}
}

func TestProcessWindowManagerPreservesOpenRequestOrder(t *testing.T) {
	manager := newProcessWindowManager(&fakeTerminalController{}, io.Discard, nil, newFakeProcessWindowTerminal, 10)
	manager.columns, manager.rows = 80, 24
	_, betaCancel := context.WithCancel(manager.ctx)
	_, alphaCancel := context.WithCancel(manager.ctx)
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: newFakeProcessWindowAttachment("beta"), cancel: betaCancel, order: 2, reference: "name:beta",
	})
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: newFakeProcessWindowAttachment("alpha"), cancel: alphaCancel, order: 1, reference: "name:alpha",
	})
	if got := []string{manager.panes[0].process.GetName(), manager.panes[1].process.GetName()}; !reflect.DeepEqual(got, []string{"alpha", "beta"}) {
		manager.shutdown()
		t.Fatalf("pane order = %v, want [alpha beta]", got)
	}
	if manager.active != 1 || manager.panes[manager.active].process.GetName() != "beta" {
		manager.shutdown()
		t.Fatalf("active pane = %d, want beta at index 1", manager.active)
	}
	manager.shutdown()
}

func TestProcessWindowManagerRoutesInputOnlyToActivePane(t *testing.T) {
	manager := newProcessWindowManager(&fakeTerminalController{}, io.Discard, nil, newFakeProcessWindowTerminal, 10)
	manager.columns, manager.rows = 80, 24
	alpha := newFakeProcessWindowAttachment("alpha")
	beta := newFakeProcessWindowAttachment("beta")
	_, alphaCancel := context.WithCancel(manager.ctx)
	_, betaCancel := context.WithCancel(manager.ctx)
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: alpha, cancel: alphaCancel, order: 1, reference: "name:alpha",
	})
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: beta, cancel: betaCancel, order: 2, reference: "name:beta",
	})
	manager.handleInput([]byte("for-beta"))
	manager.handleInput(append([]byte{attachEscapeByte, '1'}, []byte("for-alpha")...))
	assertAttachmentInput(t, beta, "for-beta")
	assertAttachmentInput(t, alpha, "for-alpha")
	manager.shutdown()
}

func assertAttachmentInput(t *testing.T, attachment *fakeProcessWindowAttachment, want string) {
	t.Helper()
	select {
	case got := <-attachment.input:
		if string(got) != want {
			t.Fatalf("attachment input = %q, want %q", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("attachment did not receive %q", want)
	}
}

func TestProcessWindowManagerDetachesBeforeCancelingAttachmentContext(t *testing.T) {
	manager := newProcessWindowManager(&fakeTerminalController{}, io.Discard, nil, newFakeProcessWindowTerminal, 10)
	manager.columns, manager.rows = 80, 24
	attachmentContext, attachmentCancel := context.WithCancel(manager.ctx)
	attachment := newFakeProcessWindowAttachment("agent")
	attachment.detachContext = attachmentContext
	manager.acceptOpenResult(processWindowOpenResult{
		attachment: attachment, cancel: attachmentCancel, order: 1, reference: "name:agent",
	})
	manager.closeActivePane()
	manager.detachWorkers.Wait()
	if attachment.detachedAfterCancel.Load() {
		manager.shutdown()
		t.Fatal("attachment context was canceled before Detach")
	}
	if attachmentContext.Err() == nil {
		manager.shutdown()
		t.Fatal("attachment context was not canceled after Detach")
	}
	manager.shutdown()
}

func TestProcessWindowManagerShutdownWithFullEventQueue(t *testing.T) {
	manager := newProcessWindowManager(&fakeTerminalController{}, io.Discard, nil, newFakeProcessWindowTerminal, 10)
	attachment := newFakeProcessWindowAttachment("agent")
	paneContext, paneCancel := context.WithCancel(manager.ctx)
	_, attachmentCancel := context.WithCancel(manager.ctx)
	manager.panes = []*processWindowPane{{
		id: 1, ctx: paneContext, cancel: paneCancel, attachStop: attachmentCancel,
		process: attachment.Process(), attachment: attachment,
		terminal: newFakeProcessWindowTerminal(10, 3), state: processWindowPaneActive,
		operations: make(chan processWindowOperation, processWindowOperationBuffer),
	}}
	for len(manager.events) < cap(manager.events) {
		manager.events <- struct{}{}
	}
	done := make(chan struct{})
	go func() {
		manager.shutdown()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked with a full event queue")
	}
	if attachment.detachCount.Load() != 1 {
		t.Fatalf("detach count = %d, want 1", attachment.detachCount.Load())
	}
}

func TestProcessWindowManagerRunRestoresTerminalOnQuit(t *testing.T) {
	terminal := &scriptedTerminalController{input: []byte{attachEscapeByte, 'q'}}
	var output bytes.Buffer
	manager := newProcessWindowManager(terminal, &output, nil, newFakeProcessWindowTerminal, 0)
	if err := manager.run(nil); err != nil {
		t.Fatal(err)
	}
	if terminal.restoreCount.Load() != 1 {
		t.Fatalf("restore count = %d, want 1", terminal.restoreCount.Load())
	}
	if !strings.Contains(output.String(), processWindowsEnterScreenSequence) ||
		!strings.Contains(output.String(), processWindowsLeaveScreenSequence) {
		t.Fatalf("terminal output did not contain enter and leave sequences: %q", output.String())
	}
}

type fakeTerminalController struct{}

func (*fakeTerminalController) available() bool { return true }
func (*fakeTerminalController) makeRaw() (func() error, error) {
	return func() error { return nil }, nil
}
func (*fakeTerminalController) size() (uint32, uint32, error) { return 24, 80, nil }
func (*fakeTerminalController) read(ctx context.Context, _ []byte) (int, error) {
	<-ctx.Done()
	return 0, ctx.Err()
}
func (*fakeTerminalController) resizeEvents() (<-chan struct{}, func()) {
	return make(chan struct{}), func() {}
}

type scriptedTerminalController struct {
	input        []byte
	once         sync.Once
	restoreCount atomic.Int32
}

func (*scriptedTerminalController) available() bool { return true }
func (t *scriptedTerminalController) makeRaw() (func() error, error) {
	return func() error {
		t.restoreCount.Add(1)
		return nil
	}, nil
}
func (*scriptedTerminalController) size() (uint32, uint32, error) { return 24, 80, nil }
func (t *scriptedTerminalController) read(ctx context.Context, buffer []byte) (int, error) {
	count := 0
	t.once.Do(func() {
		count = copy(buffer, t.input)
	})
	if count > 0 {
		return count, nil
	}
	<-ctx.Done()
	return 0, ctx.Err()
}
func (*scriptedTerminalController) resizeEvents() (<-chan struct{}, func()) {
	return make(chan struct{}), func() {}
}

type fakeProcessWindowTerminal struct {
	mu      sync.Mutex
	buffer  bytes.Buffer
	columns int
	rows    int
	closed  chan struct{}
	once    sync.Once
}

func newFakeProcessWindowTerminal(columns, rows int) processWindowTerminal {
	return &fakeProcessWindowTerminal{columns: columns, rows: rows, closed: make(chan struct{})}
}

func (t *fakeProcessWindowTerminal) Read(_ []byte) (int, error) {
	<-t.closed
	return 0, io.EOF
}
func (t *fakeProcessWindowTerminal) Write(data []byte) (int, error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buffer.Write(data)
}
func (t *fakeProcessWindowTerminal) Close() error {
	t.once.Do(func() { close(t.closed) })
	return nil
}
func (t *fakeProcessWindowTerminal) Resize(columns, rows int) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.columns, t.rows = columns, rows
}
func (t *fakeProcessWindowTerminal) Render() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.buffer.String()
}
func (t *fakeProcessWindowTerminal) Cursor() (int, int, bool) { return 0, 0, true }

type fakeProcessWindowAttachment struct {
	process             *codev1.ProcessInfo
	output              chan []byte
	input               chan []byte
	done                chan struct{}
	finishOnce          sync.Once
	detachCount         atomic.Int32
	detachContext       context.Context
	detachedAfterCancel atomic.Bool
	mu                  sync.Mutex
	err                 error
}

func newFakeProcessWindowAttachment(name string) *fakeProcessWindowAttachment {
	return &fakeProcessWindowAttachment{
		process: &codev1.ProcessInfo{Id: name + "-id", Name: name},
		output:  make(chan []byte, 8), input: make(chan []byte, 8), done: make(chan struct{}),
	}
}

func (a *fakeProcessWindowAttachment) Process() *codev1.ProcessInfo { return a.process }
func (a *fakeProcessWindowAttachment) Output() <-chan []byte        { return a.output }
func (a *fakeProcessWindowAttachment) Write(data []byte) (int, error) {
	select {
	case <-a.done:
		return 0, io.ErrClosedPipe
	default:
		a.input <- append([]byte(nil), data...)
		return len(data), nil
	}
}
func (a *fakeProcessWindowAttachment) Resize(uint32, uint32) error {
	select {
	case <-a.done:
		return io.ErrClosedPipe
	default:
		return nil
	}
}
func (a *fakeProcessWindowAttachment) Detach() error {
	a.detachCount.Add(1)
	if a.detachContext != nil && a.detachContext.Err() != nil {
		a.detachedAfterCancel.Store(true)
	}
	a.finish(nil)
	return nil
}
func (a *fakeProcessWindowAttachment) Wait() error {
	<-a.done
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}
func (a *fakeProcessWindowAttachment) finish(err error) {
	a.finishOnce.Do(func() {
		a.mu.Lock()
		a.err = err
		a.mu.Unlock()
		close(a.output)
		close(a.done)
	})
}

var _ processWindowAttachment = (*fakeProcessWindowAttachment)(nil)

func TestProcessWindowErrorSummaryPreservesGRPCCode(t *testing.T) {
	err := processWindowErrorSummary(errors.New("plain"))
	if err != "plain" {
		t.Fatalf("plain summary = %q", err)
	}
	err = processWindowErrorSummary(status.Error(codes.AlreadyExists, "writer busy"))
	if err != "writer busy (AlreadyExists)" {
		t.Fatalf("gRPC summary = %q", err)
	}
}
