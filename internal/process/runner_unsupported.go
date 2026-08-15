//go:build !linux

package process

import (
	"os"
	"os/exec"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

type runningCommand struct {
	cmd   *exec.Cmd
	input *processInput
}

func startCommand(*os.File, string, []string, []string, codev1.ProcessIOMode, codev1.ProcessInputMode, *codev1.TerminalSize, *recordOutput) (*runningCommand, error) {
	return nil, errUnsupportedPlatform
}

func (c *runningCommand) wait() error                 { return errUnsupportedPlatform }
func (c *runningCommand) close()                      {}
func (c *runningCommand) resize(uint32, uint32) error { return errUnsupportedPlatform }

func nativeSignal(codev1.ProcessSignal) (int, error) {
	return 0, errUnsupportedPlatform
}

func signalProcessGroup(int, codev1.ProcessSignal) error {
	return errUnsupportedPlatform
}

func processExit(*os.ProcessState, error) (*int32, *int32) {
	return nil, nil
}
