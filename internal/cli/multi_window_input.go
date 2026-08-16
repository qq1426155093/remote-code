package cli

import (
	"strings"
	"unicode/utf8"
)

const processWindowPromptMaxBytes = 256

type processWindowInputActionKind uint8

const (
	processWindowInputForward processWindowInputActionKind = iota + 1
	processWindowInputOpen
	processWindowInputClose
	processWindowInputNext
	processWindowInputPrevious
	processWindowInputSelect
	processWindowInputToggleHelp
	processWindowInputQuit
	processWindowInputPromptCanceled
	processWindowInputPromptLimit
)

type processWindowInputAction struct {
	kind  processWindowInputActionKind
	data  []byte
	value string
	index int
}

type processWindowInput struct {
	prefix    bool
	prompt    []byte
	prompting bool
	ignoreLF  bool
}

func (i *processWindowInput) feed(data []byte) []processWindowInputAction {
	actions := make([]processWindowInputAction, 0, 2)
	forward := make([]byte, 0, len(data))
	flush := func() {
		if len(forward) == 0 {
			return
		}
		actions = append(actions, processWindowInputAction{
			kind: processWindowInputForward,
			data: append([]byte(nil), forward...),
		})
		forward = forward[:0]
	}

	for _, value := range data {
		if i.ignoreLF {
			i.ignoreLF = false
			if value == '\n' {
				continue
			}
		}
		if i.prompting {
			switch value {
			case '\r', '\n':
				candidate := strings.TrimSpace(string(i.prompt))
				i.prompt = nil
				i.prompting = false
				i.ignoreLF = value == '\r'
				if candidate != "" {
					actions = append(actions, processWindowInputAction{kind: processWindowInputOpen, value: candidate})
				} else {
					actions = append(actions, processWindowInputAction{kind: processWindowInputPromptCanceled})
				}
			case 0x1b, 0x03:
				i.prompt = nil
				i.prompting = false
				actions = append(actions, processWindowInputAction{kind: processWindowInputPromptCanceled})
			case 0x08, 0x7f:
				i.deletePromptRune()
			case 0x15:
				i.prompt = nil
			default:
				if value < 0x20 || value == 0x7f {
					continue
				}
				if len(i.prompt) >= processWindowPromptMaxBytes {
					actions = append(actions, processWindowInputAction{kind: processWindowInputPromptLimit})
					continue
				}
				i.prompt = append(i.prompt, value)
			}
			continue
		}

		if !i.prefix {
			if value == attachEscapeByte {
				i.prefix = true
				continue
			}
			forward = append(forward, value)
			continue
		}

		i.prefix = false
		switch value {
		case attachEscapeByte:
			forward = append(forward, attachEscapeByte)
		case 'o':
			flush()
			i.prompt = nil
			i.prompting = true
		case 'x':
			flush()
			actions = append(actions, processWindowInputAction{kind: processWindowInputClose})
		case 'n':
			flush()
			actions = append(actions, processWindowInputAction{kind: processWindowInputNext})
		case 'p':
			flush()
			actions = append(actions, processWindowInputAction{kind: processWindowInputPrevious})
		case '?':
			flush()
			actions = append(actions, processWindowInputAction{kind: processWindowInputToggleHelp})
		case 'q', 'd':
			flush()
			actions = append(actions, processWindowInputAction{kind: processWindowInputQuit})
		case '1', '2', '3', '4', '5', '6', '7', '8', '9':
			flush()
			actions = append(actions, processWindowInputAction{
				kind:  processWindowInputSelect,
				index: int(value - '1'),
			})
		default:
			forward = append(forward, attachEscapeByte, value)
		}
	}
	flush()
	return actions
}

func (i *processWindowInput) deletePromptRune() {
	if len(i.prompt) == 0 {
		return
	}
	_, size := utf8.DecodeLastRune(i.prompt)
	if size <= 0 {
		size = 1
	}
	i.prompt = i.prompt[:len(i.prompt)-size]
}

func (i *processWindowInput) promptText() string {
	return string(i.prompt)
}
