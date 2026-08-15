//go:build !linux

package cli

import (
	"context"
	"errors"
	"os"
)

type unsupportedTerminal struct{}

func newLocalTerminal(*os.File) terminalController { return unsupportedTerminal{} }
func (unsupportedTerminal) available() bool        { return false }
func (unsupportedTerminal) makeRaw() (func() error, error) {
	return nil, errors.New("interactive process attachment is not supported on this platform")
}
func (unsupportedTerminal) size() (uint32, uint32, error) {
	return 0, 0, errors.New("interactive process attachment is not supported on this platform")
}
func (unsupportedTerminal) read(context.Context, []byte) (int, error) {
	return 0, errors.New("interactive process attachment is not supported on this platform")
}
func (unsupportedTerminal) resizeEvents() (<-chan struct{}, func()) {
	return nil, func() {}
}
