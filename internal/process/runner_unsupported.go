//go:build !linux

package process

import (
	"io"
	"os"
	"os/exec"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

type runningCommand struct {
	cmd     *exec.Cmd
	closers []io.Closer
}

func startCommand(*os.File, string, []string, codev1.ProcessIOMode) (*runningCommand, error) {
	return nil, errUnsupportedPlatform
}

func (c *runningCommand) close() {
	for _, closer := range c.closers {
		_ = closer.Close()
	}
}

func nativeSignal(codev1.ProcessSignal) (int, error) {
	return 0, errUnsupportedPlatform
}

func signalProcessGroup(int, codev1.ProcessSignal) error {
	return errUnsupportedPlatform
}

func processExit(*os.ProcessState, error) (*int32, *int32) {
	return nil, nil
}
