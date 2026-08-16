package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"
	"unicode/utf8"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	processWindowDefaultTailLines = uint64(2_000)
	processWindowMaximumTailLines = uint64(100_000)
	processWindowMaximumPanes     = 9
	processWindowFrameInterval    = time.Second / 30
	processWindowEventBuffer      = 256
	processWindowOperationBuffer  = 64
	processWindowDetachTimeout    = 3 * time.Second

	processWindowsEnterScreenSequence = "\x1b[?1049h\x1b[?7l\x1b[?25l\x1b[2J\x1b[H"
	processWindowsLeaveScreenSequence = "\x1b[0m\x1b[?25h\x1b[?7h\x1b[?1049l"
)

var errProcessWindowTerminalClosed = errors.New("multi-window terminal input closed")

type processWindowOptions struct {
	tailLines  uint64
	references []*codev1.ProcessReference
}

type processWindowAttachment interface {
	Process() *codev1.ProcessInfo
	Output() <-chan []byte
	Write([]byte) (int, error)
	Resize(rows, columns uint32) error
	Detach() error
	Wait() error
}

type processWindowAttachmentOpener func(
	context.Context,
	*codev1.ProcessReference,
	remoteclient.ProcessAttachOptions,
) (processWindowAttachment, error)

type processWindowTerminalFactory func(columns, rows int) processWindowTerminal

type processWindowPaneState uint8

const (
	processWindowPaneActive processWindowPaneState = iota + 1
	processWindowPaneDone
	processWindowPaneError
)

func (s processWindowPaneState) String() string {
	switch s {
	case processWindowPaneActive:
		return "running"
	case processWindowPaneDone:
		return "done"
	case processWindowPaneError:
		return "error"
	default:
		return "unknown"
	}
}

type processWindowOperation struct {
	data    []byte
	rows    uint32
	columns uint32
}

type processWindowPane struct {
	id         uint64
	order      uint64
	ctx        context.Context
	cancel     context.CancelFunc
	attachStop context.CancelFunc
	process    *codev1.ProcessInfo
	attachment processWindowAttachment
	terminal   processWindowTerminal
	rectangle  windowRectangle
	state      processWindowPaneState
	operations chan processWindowOperation
}

type processWindowManager struct {
	ctx             context.Context
	cancel          context.CancelFunc
	terminal        terminalController
	stdout          io.Writer
	opener          processWindowAttachmentOpener
	terminalFactory processWindowTerminalFactory
	tailLines       uint64
	events          chan any
	inputEvents     chan []byte
	workers         sync.WaitGroup
	detachWorkers   sync.WaitGroup

	panes      []*processWindowPane
	active     int
	opening    int
	nextPaneID uint64
	nextOrder  uint64
	rows       int
	columns    int
	status     string
	showHelp   bool
	input      processWindowInput
}

type processWindowOpenResult struct {
	attachment processWindowAttachment
	cancel     context.CancelFunc
	order      uint64
	reference  string
	err        error
}

type processWindowOutput struct {
	paneID uint64
	data   []byte
}

type processWindowAttachmentEnd struct {
	paneID uint64
	err    error
}

type processWindowTerminalReply struct {
	paneID uint64
	data   []byte
}

type processWindowOperationError struct {
	paneID uint64
	err    error
}

type processWindowCloseResult struct {
	name string
	err  error
}

type processWindowTerminalReadResult struct {
	err error
}

func (r *REPL) processWindows(arguments []string) error {
	options, err := parseProcessWindowOptions(arguments)
	if err != nil {
		return err
	}
	if r.terminal == nil || !r.terminal.available() {
		return errors.New("windows requires a supported interactive local terminal")
	}
	manager := newProcessWindowManager(
		r.terminal,
		r.stdout,
		func(ctx context.Context, reference *codev1.ProcessReference, options remoteclient.ProcessAttachOptions) (processWindowAttachment, error) {
			return r.client.OpenProcessAttachment(ctx, reference, options)
		},
		newProcessWindowTerminal,
		options.tailLines,
	)
	return manager.run(options.references)
}

func parseProcessWindowOptions(arguments []string) (processWindowOptions, error) {
	options := processWindowOptions{tailLines: processWindowDefaultTailLines}
	optionsEnded := false
	for index := 0; index < len(arguments); index++ {
		argument := arguments[index]
		if !optionsEnded {
			switch argument {
			case "--":
				optionsEnded = true
				continue
			case "-n", "--tail-lines":
				if index+1 >= len(arguments) {
					return processWindowOptions{}, usageErrorf("%s requires a value", argument)
				}
				index++
				value, err := strconv.ParseUint(arguments[index], 10, 64)
				if err != nil || value > processWindowMaximumTailLines {
					return processWindowOptions{}, usageErrorf("tail lines must be between 0 and %d", processWindowMaximumTailLines)
				}
				options.tailLines = value
				continue
			default:
				if strings.HasPrefix(argument, "-") {
					return processWindowOptions{}, usageErrorf("unknown windows option %q", argument)
				}
			}
		}
		reference, err := parseProcessReference(argument)
		if err != nil {
			return processWindowOptions{}, err
		}
		options.references = append(options.references, reference)
	}
	if len(options.references) > processWindowMaximumPanes {
		return processWindowOptions{}, usageErrorf("windows supports at most %d processes", processWindowMaximumPanes)
	}
	return options, nil
}

func newProcessWindowManager(
	terminal terminalController,
	stdout io.Writer,
	opener processWindowAttachmentOpener,
	terminalFactory processWindowTerminalFactory,
	tailLines uint64,
) *processWindowManager {
	ctx, cancel := context.WithCancel(context.Background())
	return &processWindowManager{
		ctx: ctx, cancel: cancel, terminal: terminal, stdout: stdout,
		opener: opener, terminalFactory: terminalFactory, tailLines: tailLines,
		events:      make(chan any, processWindowEventBuffer),
		inputEvents: make(chan []byte, processWindowEventBuffer),
		active:      -1, nextPaneID: 1, nextOrder: 1,
	}
}

func (m *processWindowManager) run(initial []*codev1.ProcessReference) error {
	rows, columns, err := m.terminal.size()
	if err != nil {
		m.cancel()
		return fmt.Errorf("get local terminal size: %w", err)
	}
	if columns < processWindowMinimumColumns || rows < processWindowMinimumRows {
		m.cancel()
		return fmt.Errorf("windows requires a terminal of at least %dx%d; current size is %dx%d",
			processWindowMinimumColumns, processWindowMinimumRows, columns, rows)
	}
	m.rows, m.columns = int(rows), int(columns)

	restore, err := m.terminal.makeRaw()
	if err != nil {
		m.cancel()
		return fmt.Errorf("enter raw terminal mode: %w", err)
	}
	if err := writeTerminalSequence(m.stdout, processWindowsEnterScreenSequence); err != nil {
		m.cancel()
		return errors.Join(fmt.Errorf("enter multi-window terminal screen: %w", err), restore())
	}

	resizeEvents, stopResize := m.terminal.resizeEvents()
	m.startTerminalReader()
	for _, reference := range initial {
		m.startOpen(reference)
	}
	result := m.eventLoop(resizeEvents)
	stopResize()
	m.shutdown()
	leaveErr := writeTerminalSequence(m.stdout, processWindowsLeaveScreenSequence)
	restoreErr := restore()
	if leaveErr != nil {
		leaveErr = fmt.Errorf("leave multi-window terminal screen: %w", leaveErr)
	}
	if restoreErr != nil {
		restoreErr = fmt.Errorf("restore local terminal mode: %w", restoreErr)
	}
	return errors.Join(result, leaveErr, restoreErr)
}

func (m *processWindowManager) eventLoop(resizeEvents <-chan struct{}) error {
	if err := m.render(); err != nil {
		return err
	}
	ticker := time.NewTicker(processWindowFrameInterval)
	defer ticker.Stop()
	dirty := false
	for {
		// Keep keyboard commands responsive even when several output streams are
		// continuously filling the shared event queue.
		select {
		case data := <-m.inputEvents:
			quit := m.handleInput(data)
			dirty = true
			if quit {
				return nil
			}
		default:
		}

		select {
		case data := <-m.inputEvents:
			quit := m.handleInput(data)
			dirty = true
			if quit {
				return nil
			}
		case event := <-m.events:
			terminalErr := m.handleEvent(event)
			dirty = true
			if terminalErr != nil {
				if errors.Is(terminalErr, errProcessWindowTerminalClosed) {
					return nil
				}
				return terminalErr
			}
		case _, ok := <-resizeEvents:
			if !ok {
				resizeEvents = nil
				continue
			}
			if err := m.handleResize(); err != nil {
				return err
			}
			dirty = true
		case <-ticker.C:
			if dirty {
				if err := m.render(); err != nil {
					return err
				}
				dirty = false
			}
		case <-m.ctx.Done():
			return nil
		}
	}
}

func (m *processWindowManager) handleInput(data []byte) bool {
	actions := m.input.feed(data)
	for _, action := range actions {
		switch action.kind {
		case processWindowInputForward:
			m.enqueueActive(processWindowOperation{data: action.data})
		case processWindowInputOpen:
			if !utf8.ValidString(action.value) || strings.IndexFunc(action.value, unicode.IsControl) >= 0 {
				m.status = "open: process reference must be valid UTF-8 without control characters"
				continue
			}
			reference, err := parseProcessReference(action.value)
			if err != nil {
				m.status = "open: " + err.Error()
				continue
			}
			m.showHelp = false
			m.startOpen(reference)
		case processWindowInputClose:
			m.showHelp = false
			m.closeActivePane()
		case processWindowInputNext:
			m.showHelp = false
			m.moveActive(1)
		case processWindowInputPrevious:
			m.showHelp = false
			m.moveActive(-1)
		case processWindowInputSelect:
			m.showHelp = false
			m.selectPane(action.index)
		case processWindowInputToggleHelp:
			m.showHelp = !m.showHelp
		case processWindowInputQuit:
			return true
		case processWindowInputPromptCanceled:
			m.status = "open canceled"
		case processWindowInputPromptLimit:
			m.status = fmt.Sprintf("process reference is limited to %d bytes", processWindowPromptMaxBytes)
		}
	}
	return false
}

func (m *processWindowManager) handleEvent(event any) error {
	switch event := event.(type) {
	case processWindowOpenResult:
		m.acceptOpenResult(event)
	case processWindowOutput:
		pane := m.findPane(event.paneID)
		if pane == nil {
			return nil
		}
		written, err := pane.terminal.Write(event.data)
		if err != nil || written != len(event.data) {
			if err == nil {
				err = io.ErrShortWrite
			}
			pane.state = processWindowPaneError
			m.status = fmt.Sprintf("%s terminal: %s", pane.process.GetName(), processWindowErrorSummary(err))
		}
	case processWindowAttachmentEnd:
		pane := m.findPane(event.paneID)
		if pane == nil {
			return nil
		}
		pane.cancel()
		pane.attachStop()
		_ = pane.terminal.Close()
		if event.err == nil || errors.Is(event.err, context.Canceled) {
			pane.state = processWindowPaneDone
			m.status = fmt.Sprintf("%s finished; close the window with Ctrl-] x", pane.process.GetName())
		} else {
			pane.state = processWindowPaneError
			m.status = fmt.Sprintf("%s: %s", pane.process.GetName(), processWindowErrorSummary(event.err))
		}
	case processWindowTerminalReply:
		pane := m.findPane(event.paneID)
		if pane != nil && pane.state == processWindowPaneActive {
			m.enqueuePane(pane, processWindowOperation{data: event.data})
		}
	case processWindowOperationError:
		pane := m.findPane(event.paneID)
		if pane != nil && pane.state == processWindowPaneActive {
			pane.state = processWindowPaneError
			m.status = fmt.Sprintf("%s input: %s", pane.process.GetName(), processWindowErrorSummary(event.err))
		}
	case processWindowCloseResult:
		if event.err != nil && !errors.Is(event.err, context.Canceled) && !errors.Is(event.err, io.EOF) {
			m.status = fmt.Sprintf("detach %s: %s", event.name, processWindowErrorSummary(event.err))
		}
	case processWindowTerminalReadResult:
		if event.err == nil || errors.Is(event.err, io.EOF) || errors.Is(event.err, context.Canceled) {
			return errProcessWindowTerminalClosed
		}
		return fmt.Errorf("read local terminal input: %w", event.err)
	}
	return nil
}

func (m *processWindowManager) startOpen(reference *codev1.ProcessReference) {
	if len(m.panes)+m.opening >= processWindowMaximumPanes {
		m.status = fmt.Sprintf("at most %d windows can be open", processWindowMaximumPanes)
		return
	}
	futureCount := len(m.panes) + m.opening + 1
	if !processWindowLayoutFits(m.columns, m.rows, futureCount) {
		m.status = "terminal is too small for another usable window"
		return
	}
	rectangles := calculateProcessWindowLayout(m.columns, m.rows, futureCount)
	content := rectangles[len(rectangles)-1].content()
	tailLines := m.tailLines
	referenceName := displayProcessWindowReference(reference)
	attachmentContext, cancel := context.WithCancel(m.ctx)
	order := m.nextOrder
	m.nextOrder++
	m.opening++
	m.status = "opening " + referenceName
	m.workers.Add(1)
	go func() {
		defer m.workers.Done()
		attachment, err := m.opener(attachmentContext, reference, remoteclient.ProcessAttachOptions{
			Rows: uint32(content.height), Columns: uint32(content.width), TailLines: &tailLines,
		})
		result := processWindowOpenResult{
			attachment: attachment, cancel: cancel, order: order, reference: referenceName, err: err,
		}
		if m.ctx.Err() != nil {
			_ = detachProcessWindowAttachment(attachment, cancel)
			return
		}
		select {
		case m.events <- result:
		case <-m.ctx.Done():
			_ = detachProcessWindowAttachment(attachment, cancel)
		}
	}()
}

func (m *processWindowManager) acceptOpenResult(result processWindowOpenResult) {
	m.opening = max(0, m.opening-1)
	if result.err != nil {
		if result.attachment != nil {
			m.detachAsync(result.attachment, result.reference, result.cancel)
		} else {
			result.cancel()
		}
		m.status = fmt.Sprintf("open %s: %s", result.reference, processWindowErrorSummary(result.err))
		return
	}
	if result.attachment == nil || result.attachment.Process() == nil || result.attachment.Process().GetId() == "" {
		if result.attachment != nil {
			m.detachAsync(result.attachment, result.reference, result.cancel)
		} else {
			result.cancel()
		}
		m.status = fmt.Sprintf("open %s: attachment returned no process identity", result.reference)
		return
	}
	process := result.attachment.Process()
	for _, pane := range m.panes {
		if pane.process.GetId() == process.GetId() {
			m.detachAsync(result.attachment, process.GetName(), result.cancel)
			m.status = fmt.Sprintf("%s is already open", process.GetName())
			return
		}
	}
	if len(m.panes) >= processWindowMaximumPanes {
		m.detachAsync(result.attachment, process.GetName(), result.cancel)
		m.status = fmt.Sprintf("at most %d windows can be open", processWindowMaximumPanes)
		return
	}

	rectangles := calculateProcessWindowLayout(m.columns, m.rows, len(m.panes)+1)
	content := rectangles[len(rectangles)-1].content()
	pane := &processWindowPane{
		id: m.nextPaneID, order: result.order,
		process: process, attachment: result.attachment,
		terminal:   m.terminalFactory(content.width, content.height),
		state:      processWindowPaneActive,
		operations: make(chan processWindowOperation, processWindowOperationBuffer),
		attachStop: result.cancel,
	}
	pane.ctx, pane.cancel = context.WithCancel(m.ctx)
	m.nextPaneID++
	activeID := uint64(0)
	if m.active >= 0 && m.active < len(m.panes) {
		activeID = m.panes[m.active].id
	}
	insertAt := sort.Search(len(m.panes), func(index int) bool {
		return m.panes[index].order > pane.order
	})
	m.panes = append(m.panes, nil)
	copy(m.panes[insertAt+1:], m.panes[insertAt:])
	m.panes[insertAt] = pane
	if insertAt == len(m.panes)-1 {
		m.active = insertAt
	} else {
		m.active = m.paneIndex(activeID)
	}
	m.startPaneWorkers(pane)
	m.applyLayout()
	for _, size := range attachmentRedrawSizes(uint32(content.height), uint32(content.width)) {
		m.enqueuePane(pane, processWindowOperation{rows: size.rows, columns: size.columns})
	}
	m.status = fmt.Sprintf("opened %s (%s)", process.GetName(), process.GetId())
}

func (m *processWindowManager) startPaneWorkers(pane *processWindowPane) {
	m.workers.Add(3)
	go func() {
		defer m.workers.Done()
		for data := range pane.attachment.Output() {
			m.sendEvent(processWindowOutput{paneID: pane.id, data: append([]byte(nil), data...)})
		}
		m.sendEvent(processWindowAttachmentEnd{paneID: pane.id, err: pane.attachment.Wait()})
	}()
	go func() {
		defer m.workers.Done()
		buffer := make([]byte, 4_096)
		for {
			count, err := pane.terminal.Read(buffer)
			if count > 0 {
				m.sendEvent(processWindowTerminalReply{paneID: pane.id, data: append([]byte(nil), buffer[:count]...)})
			}
			if err != nil {
				return
			}
		}
	}()
	go func() {
		defer m.workers.Done()
		for {
			select {
			case operation := <-pane.operations:
				var err error
				if operation.data != nil {
					var written int
					written, err = pane.attachment.Write(operation.data)
					if err == nil && written != len(operation.data) {
						err = io.ErrShortWrite
					}
				} else {
					err = pane.attachment.Resize(operation.rows, operation.columns)
				}
				if err != nil {
					m.sendEvent(processWindowOperationError{paneID: pane.id, err: err})
					return
				}
			case <-pane.ctx.Done():
				return
			}
		}
	}()
}

func (m *processWindowManager) startTerminalReader() {
	m.workers.Add(1)
	go func() {
		defer m.workers.Done()
		buffer := make([]byte, 4_096)
		for {
			count, err := m.terminal.read(m.ctx, buffer)
			if count > 0 {
				data := append([]byte(nil), buffer[:count]...)
				select {
				case m.inputEvents <- data:
				case <-m.ctx.Done():
					return
				}
			}
			if err != nil {
				m.sendEvent(processWindowTerminalReadResult{err: err})
				return
			}
		}
	}()
}

func (m *processWindowManager) sendEvent(event any) {
	select {
	case m.events <- event:
	case <-m.ctx.Done():
	}
}

func (m *processWindowManager) enqueueActive(operation processWindowOperation) {
	if m.active < 0 || m.active >= len(m.panes) {
		m.status = "no active window; use Ctrl-] o to open one"
		return
	}
	pane := m.panes[m.active]
	if pane.state != processWindowPaneActive {
		m.status = fmt.Sprintf("%s is no longer accepting input", pane.process.GetName())
		return
	}
	m.enqueuePane(pane, operation)
}

func (m *processWindowManager) enqueuePane(pane *processWindowPane, operation processWindowOperation) {
	if operation.data != nil {
		operation.data = append([]byte(nil), operation.data...)
	}
	select {
	case pane.operations <- operation:
	case <-pane.ctx.Done():
		m.status = fmt.Sprintf("%s is no longer accepting input", pane.process.GetName())
	}
}

func (m *processWindowManager) moveActive(delta int) {
	if len(m.panes) == 0 {
		m.status = "no windows are open"
		return
	}
	m.active = (m.active + delta + len(m.panes)) % len(m.panes)
	m.status = fmt.Sprintf("selected %s", m.panes[m.active].process.GetName())
}

func (m *processWindowManager) selectPane(index int) {
	if index < 0 || index >= len(m.panes) {
		m.status = fmt.Sprintf("window %d is not open", index+1)
		return
	}
	m.active = index
	m.status = fmt.Sprintf("selected %s", m.panes[m.active].process.GetName())
}

func (m *processWindowManager) closeActivePane() {
	if m.active < 0 || m.active >= len(m.panes) {
		m.status = "no windows are open"
		return
	}
	index := m.active
	pane := m.panes[index]
	pane.cancel()
	_ = pane.terminal.Close()
	m.panes = append(m.panes[:index], m.panes[index+1:]...)
	if len(m.panes) == 0 {
		m.active = -1
	} else if index >= len(m.panes) {
		m.active = len(m.panes) - 1
	} else {
		m.active = index
	}
	m.applyLayout()
	m.detachAsync(pane.attachment, pane.process.GetName(), pane.attachStop)
	m.status = fmt.Sprintf("closed %s; remote process continues", pane.process.GetName())
}

func (m *processWindowManager) detachAsync(attachment processWindowAttachment, name string, cancel context.CancelFunc) {
	m.workers.Add(1)
	m.detachWorkers.Add(1)
	go func() {
		defer m.workers.Done()
		err := detachProcessWindowAttachment(attachment, cancel)
		m.detachWorkers.Done()
		m.sendEvent(processWindowCloseResult{name: name, err: err})
	}()
}

func detachProcessWindowAttachment(attachment processWindowAttachment, cancel context.CancelFunc) error {
	if cancel == nil {
		cancel = func() {}
	}
	if attachment == nil {
		cancel()
		return nil
	}
	timer := time.AfterFunc(processWindowDetachTimeout, cancel)
	err := attachment.Detach()
	timer.Stop()
	cancel()
	return err
}

func (m *processWindowManager) applyLayout() {
	rectangles := calculateProcessWindowLayout(m.columns, m.rows, len(m.panes))
	for index, pane := range m.panes {
		content := rectangles[index].content()
		previous := pane.rectangle.content()
		pane.rectangle = rectangles[index]
		if previous.width == content.width && previous.height == content.height {
			continue
		}
		pane.terminal.Resize(content.width, content.height)
		if pane.state == processWindowPaneActive {
			m.enqueuePane(pane, processWindowOperation{rows: uint32(content.height), columns: uint32(content.width)})
		}
	}
}

func (m *processWindowManager) handleResize() error {
	rows, columns, err := m.terminal.size()
	if err != nil {
		return fmt.Errorf("get resized terminal dimensions: %w", err)
	}
	if rows == 0 || columns == 0 {
		return errors.New("resized terminal dimensions must be positive")
	}
	m.rows, m.columns = int(rows), int(columns)
	m.applyLayout()
	if columns < processWindowMinimumColumns || rows < processWindowMinimumRows {
		m.status = fmt.Sprintf("terminal is smaller than the recommended %dx%d", processWindowMinimumColumns, processWindowMinimumRows)
	} else {
		m.status = fmt.Sprintf("resized to %dx%d", columns, rows)
	}
	return nil
}

func (m *processWindowManager) render() error {
	frame := renderProcessWindowFrame(m)
	written, err := io.WriteString(m.stdout, frame)
	if err != nil {
		return fmt.Errorf("render multi-window terminal: %w", err)
	}
	if written != len(frame) {
		return io.ErrShortWrite
	}
	return nil
}

func (m *processWindowManager) findPane(id uint64) *processWindowPane {
	for _, pane := range m.panes {
		if pane.id == id {
			return pane
		}
	}
	return nil
}

func (m *processWindowManager) paneIndex(id uint64) int {
	for index, pane := range m.panes {
		if pane.id == id {
			return index
		}
	}
	return -1
}

func (m *processWindowManager) shutdown() {
	for _, pane := range m.panes {
		pane.cancel()
		_ = pane.terminal.Close()
		m.detachAsync(pane.attachment, pane.process.GetName(), pane.attachStop)
	}
	m.detachWorkers.Wait()
	m.cancel()
	m.workers.Wait()
	m.discardQueuedOpenResults()
}

func (m *processWindowManager) discardQueuedOpenResults() {
	for {
		select {
		case event := <-m.events:
			if result, ok := event.(processWindowOpenResult); ok {
				_ = detachProcessWindowAttachment(result.attachment, result.cancel)
			}
		default:
			return
		}
	}
}

func displayProcessWindowReference(reference *codev1.ProcessReference) string {
	if reference == nil {
		return "<nil>"
	}
	switch value := reference.GetValue().(type) {
	case *codev1.ProcessReference_Id:
		return "id:" + value.Id
	case *codev1.ProcessReference_Name:
		return "name:" + value.Name
	case *codev1.ProcessReference_Pid:
		return "pid:" + strconv.FormatInt(value.Pid, 10)
	default:
		return "<invalid>"
	}
}

func processWindowErrorSummary(err error) string {
	if err == nil {
		return "unknown error"
	}
	code := status.Code(err)
	if code != codes.Unknown {
		return fmt.Sprintf("%s (%s)", status.Convert(err).Message(), code)
	}
	return err.Error()
}
