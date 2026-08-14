//go:build linux

package process

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strconv"
	"syscall"
	"time"

	"github.com/creack/pty"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

const ptyDrainWait = 250 * time.Millisecond

type runningCommand struct {
	cmd      *exec.Cmd
	terminal *os.File
	copyDone chan struct{}
}

func startCommand(directory *os.File, executable string, arguments, environment []string, ioMode codev1.ProcessIOMode, output *recordOutput) (*runningCommand, error) {
	command := exec.Command(executable, arguments...)
	command.Dir = "/proc/self/fd/" + strconv.FormatUint(uint64(directory.Fd()), 10)
	command.Env = append([]string(nil), environment...)
	switch ioMode {
	case codev1.ProcessIOMode_PROCESS_IO_MODE_PTY:
		terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 24, Cols: 80})
		if err != nil {
			return nil, err
		}
		copyDone := make(chan struct{})
		go func() {
			_, _ = io.CopyBuffer(output.stdout, terminal, make([]byte, maxLogFrameBytes))
			close(copyDone)
		}()
		return &runningCommand{cmd: command, terminal: terminal, copyDone: copyDone}, nil
	case codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE:
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		command.Stdout = output.stdout
		command.Stderr = output.stderr
		stdin, err := command.StdinPipe()
		if err != nil {
			return nil, err
		}
		if err := command.Start(); err != nil {
			_ = stdin.Close()
			return nil, err
		}
		// v1 has no attach/input RPC. Closing the pipe gives the child immediate
		// EOF while preserving the requested pipe I/O semantics.
		_ = stdin.Close()
		return &runningCommand{cmd: command}, nil
	default:
		return nil, fmt.Errorf("%w: I/O mode %s", errUnsupportedPlatform, ioMode)
	}
}

func (c *runningCommand) wait() error {
	err := c.cmd.Wait()
	if c.copyDone != nil {
		timer := time.NewTimer(ptyDrainWait)
		select {
		case <-c.copyDone:
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
		case <-timer.C:
			_ = c.terminal.Close()
			<-c.copyDone
		}
	}
	return err
}

func (c *runningCommand) close() {
	if c != nil && c.terminal != nil {
		_ = c.terminal.Close()
	}
}

func nativeSignal(signal codev1.ProcessSignal) (syscall.Signal, error) {
	switch signal {
	case codev1.ProcessSignal_PROCESS_SIGNAL_HUP:
		return syscall.SIGHUP, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_INT:
		return syscall.SIGINT, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_QUIT:
		return syscall.SIGQUIT, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_TERM:
		return syscall.SIGTERM, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_KILL:
		return syscall.SIGKILL, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_USR1:
		return syscall.SIGUSR1, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_USR2:
		return syscall.SIGUSR2, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_STOP:
		return syscall.SIGSTOP, nil
	case codev1.ProcessSignal_PROCESS_SIGNAL_CONT:
		return syscall.SIGCONT, nil
	default:
		return 0, errors.New("unsupported process signal")
	}
}

func signalProcessGroup(pid int, signal codev1.ProcessSignal) error {
	native, err := nativeSignal(signal)
	if err != nil {
		return err
	}
	return syscall.Kill(-pid, native)
}

func processExit(state *os.ProcessState, _ error) (exitCode, exitSignal *int32) {
	if state == nil {
		return nil, nil
	}
	waitStatus, ok := state.Sys().(syscall.WaitStatus)
	if ok && waitStatus.Signaled() {
		value := int32(waitStatus.Signal())
		return nil, &value
	}
	value := int32(state.ExitCode())
	return &value, nil
}
