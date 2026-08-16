package cli

import (
	"fmt"
	"math"
	"strings"
	"unicode"

	"github.com/charmbracelet/x/ansi"
)

const (
	processWindowMinimumColumns = 20
	processWindowMinimumRows    = 6
	processWindowTargetRatio    = 3.0
)

type windowRectangle struct {
	x      int
	y      int
	width  int
	height int
}

func (r windowRectangle) content() windowRectangle {
	return windowRectangle{
		x: r.x + 1, y: r.y + 1,
		width: max(1, r.width-2), height: max(1, r.height-2),
	}
}

func calculateProcessWindowLayout(columns, rows, count int) []windowRectangle {
	if columns <= 0 || rows <= 0 || count <= 0 {
		return nil
	}
	usableRows := max(0, rows-1)
	gridColumns, gridRows := chooseProcessWindowGrid(columns, usableRows, count)
	columnWidths := splitWindowDimension(columns, gridColumns)
	rowHeights := splitWindowDimension(usableRows, gridRows)
	columnOffsets := dimensionOffsets(columnWidths)
	rowOffsets := dimensionOffsets(rowHeights)

	result := make([]windowRectangle, 0, count)
	for index := 0; index < count; index++ {
		row := index / gridColumns
		column := index % gridColumns
		result = append(result, windowRectangle{
			x: columnOffsets[column], y: rowOffsets[row],
			width: columnWidths[column], height: rowHeights[row],
		})
	}
	return result
}

func chooseProcessWindowGrid(columns, rows, count int) (int, int) {
	bestColumns, bestRows := 1, count
	bestScore := math.Inf(1)
	for candidateColumns := 1; candidateColumns <= count; candidateColumns++ {
		candidateRows := (count + candidateColumns - 1) / candidateColumns
		minimumWidth := columns / candidateColumns
		minimumHeight := rows / candidateRows
		contentWidth := max(1, minimumWidth-2)
		contentHeight := max(1, minimumHeight-2)
		ratio := float64(contentWidth) / float64(contentHeight)
		score := math.Abs(math.Log(ratio / processWindowTargetRatio))
		score += float64(candidateColumns*candidateRows-count) * 0.20
		if minimumWidth < 4 || minimumHeight < 3 {
			score += 1_000
		}
		if score < bestScore {
			bestScore = score
			bestColumns = candidateColumns
			bestRows = candidateRows
		}
	}
	return bestColumns, bestRows
}

func processWindowLayoutFits(columns, rows, count int) bool {
	if columns < processWindowMinimumColumns || rows < processWindowMinimumRows || count <= 0 {
		return false
	}
	for _, rectangle := range calculateProcessWindowLayout(columns, rows, count) {
		if rectangle.width < 4 || rectangle.height < 3 {
			return false
		}
	}
	return true
}

func splitWindowDimension(total, parts int) []int {
	if parts <= 0 {
		return nil
	}
	result := make([]int, parts)
	base := total / parts
	extra := total % parts
	for index := range result {
		result[index] = base
		if index < extra {
			result[index]++
		}
	}
	return result
}

func dimensionOffsets(sizes []int) []int {
	result := make([]int, len(sizes))
	for index := 1; index < len(result); index++ {
		result[index] = result[index-1] + sizes[index-1]
	}
	return result
}

func renderProcessWindowFrame(manager *processWindowManager) string {
	var frame strings.Builder
	frame.Grow(manager.columns * manager.rows * 2)
	frame.WriteString("\x1b[?25l\x1b[0m\x1b[2J\x1b[H")

	if len(manager.panes) == 0 {
		message := "No windows. Press Ctrl-] o to open a PTY process."
		drawWindowText(&frame, max(0, (manager.rows-2)/2), max(0, (manager.columns-displayWindowWidth(message))/2),
			fitWindowText(message, manager.columns))
	}
	for index, pane := range manager.panes {
		drawProcessWindowPane(&frame, pane, index, index == manager.active)
	}
	if manager.showHelp && !manager.input.prompting {
		drawProcessWindowHelp(&frame, manager.columns, manager.rows)
	}

	status := manager.processWindowStatusLine()
	drawWindowText(&frame, manager.rows-1, 0, "\x1b[7m"+padWindowText(status, manager.columns)+"\x1b[0m")
	drawProcessWindowCursor(&frame, manager)
	return frame.String()
}

func drawProcessWindowPane(frame *strings.Builder, pane *processWindowPane, index int, active bool) {
	rectangle := pane.rectangle
	if rectangle.width <= 0 || rectangle.height <= 0 {
		return
	}
	style := "\x1b[2m"
	if active {
		style = "\x1b[1;36m"
	}
	if pane.state == processWindowPaneDone {
		style = "\x1b[33m"
	} else if pane.state == processWindowPaneError {
		style = "\x1b[1;31m"
	}

	title := fmt.Sprintf("[%d] %s (%s)", index+1, pane.process.GetName(), pane.state.String())
	drawWindowText(frame, rectangle.y, rectangle.x, style+processWindowTopBorder(rectangle.width, title)+"\x1b[0m")
	if rectangle.height > 1 {
		drawWindowText(frame, rectangle.y+rectangle.height-1, rectangle.x,
			style+processWindowPlainBorder(rectangle.width)+"\x1b[0m")
	}
	for row := 1; row < rectangle.height-1; row++ {
		drawWindowText(frame, rectangle.y+row, rectangle.x, style+"|\x1b[0m")
		if rectangle.width > 1 {
			drawWindowText(frame, rectangle.y+row, rectangle.x+rectangle.width-1, style+"|\x1b[0m")
		}
	}
	if rectangle.width < 3 || rectangle.height < 3 {
		return
	}

	content := rectangle.content()
	lines := strings.Split(pane.terminal.Render(), "\n")
	for row := 0; row < content.height && row < len(lines); row++ {
		line := strings.TrimSuffix(lines[row], "\r")
		if line != "" {
			drawWindowText(frame, content.y+row, content.x, line+"\x1b[0m")
		}
	}
}

func processWindowTopBorder(width int, title string) string {
	if width <= 1 {
		return "+"
	}
	if width == 2 {
		return "++"
	}
	insideWidth := width - 2
	title = "-" + fitWindowText(sanitizeWindowText(title), max(0, insideWidth-1))
	used := displayWindowWidth(title)
	if used < insideWidth {
		title += strings.Repeat("-", insideWidth-used)
	}
	return "+" + title + "+"
}

func processWindowPlainBorder(width int) string {
	if width <= 1 {
		return "+"
	}
	return "+" + strings.Repeat("-", max(0, width-2)) + "+"
}

func (m *processWindowManager) processWindowStatusLine() string {
	if m.input.prompting {
		return " open process: " + sanitizeWindowText(m.input.promptText())
	}
	parts := make([]string, 0, 4)
	if m.status != "" {
		parts = append(parts, sanitizeWindowText(m.status))
	}
	if m.opening > 0 {
		parts = append(parts, fmt.Sprintf("opening: %d", m.opening))
	}
	if len(m.panes) > 0 && m.active >= 0 && m.active < len(m.panes) {
		parts = append(parts, fmt.Sprintf("active: %d %s", m.active+1, sanitizeWindowText(m.panes[m.active].process.GetName())))
	}
	if len(parts) == 0 {
		parts = append(parts, "Ctrl-] o: open")
	}
	parts = append(parts, "Ctrl-] ?: help")
	return " " + strings.Join(parts, " | ")
}

func drawProcessWindowHelp(frame *strings.Builder, columns, rows int) {
	lines := []string{
		"Multi-window keys",
		"Ctrl-] o       open process",
		"Ctrl-] x       close active window",
		"Ctrl-] n / p   next / previous window",
		"Ctrl-] 1..9    select window",
		"Ctrl-] q / d   detach all and return",
		"Ctrl-] Ctrl-]  send literal Ctrl-]",
		"Ctrl-] ?       close this help",
	}
	boxWidth := min(columns-2, 52)
	boxHeight := len(lines) + 2
	if boxWidth < 20 || rows-1 < boxHeight {
		return
	}
	x := (columns - boxWidth) / 2
	y := (rows - 1 - boxHeight) / 2
	drawWindowText(frame, y, x, "\x1b[1;35m"+processWindowPlainBorder(boxWidth)+"\x1b[0m")
	for index, line := range lines {
		content := padWindowText(line, boxWidth-2)
		drawWindowText(frame, y+index+1, x, "\x1b[35m|\x1b[0m"+content+"\x1b[35m|\x1b[0m")
	}
	drawWindowText(frame, y+boxHeight-1, x, "\x1b[1;35m"+processWindowPlainBorder(boxWidth)+"\x1b[0m")
}

func drawProcessWindowCursor(frame *strings.Builder, manager *processWindowManager) {
	if manager.input.prompting {
		prefix := " open process: " + sanitizeWindowText(manager.input.promptText())
		column := min(manager.columns, displayWindowWidth(prefix)+1)
		drawWindowText(frame, manager.rows-1, max(0, column-1), "\x1b[?25h")
		return
	}
	if manager.showHelp || manager.active < 0 || manager.active >= len(manager.panes) {
		return
	}
	pane := manager.panes[manager.active]
	if pane.state != processWindowPaneActive || pane.rectangle.width < 3 || pane.rectangle.height < 3 {
		return
	}
	column, row, visible := pane.terminal.Cursor()
	content := pane.rectangle.content()
	if !visible || column < 0 || row < 0 || column >= content.width || row >= content.height {
		return
	}
	drawWindowText(frame, content.y+row, content.x+column, "\x1b[?25h")
}

func drawWindowText(frame *strings.Builder, row, column int, value string) {
	if row < 0 || column < 0 {
		return
	}
	fmt.Fprintf(frame, "\x1b[%d;%dH%s", row+1, column+1, value)
}

func sanitizeWindowText(value string) string {
	value = ansi.Strip(value)
	return strings.Map(func(character rune) rune {
		if unicode.IsControl(character) {
			return ' '
		}
		return character
	}, value)
}

func fitWindowText(value string, width int) string {
	if width <= 0 {
		return ""
	}
	value = sanitizeWindowText(value)
	if displayWindowWidth(value) <= width {
		return value
	}
	if width == 1 {
		return ansi.Truncate(value, width, "")
	}
	return ansi.Truncate(value, width, "…")
}

func padWindowText(value string, width int) string {
	value = fitWindowText(value, width)
	if padding := width - displayWindowWidth(value); padding > 0 {
		value += strings.Repeat(" ", padding)
	}
	return value
}

func displayWindowWidth(value string) int {
	return ansi.StringWidth(value)
}
