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

	"github.com/creack/pty"
	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

type runningCommand struct {
	cmd     *exec.Cmd
	closers []io.Closer
}

func startCommand(directory *os.File, executable string, arguments []string, ioMode codev1.ProcessIOMode) (*runningCommand, error) {
	command := exec.Command(executable, arguments...)
	command.Dir = "/proc/self/fd/" + strconv.FormatUint(uint64(directory.Fd()), 10)
	switch ioMode {
	case codev1.ProcessIOMode_PROCESS_IO_MODE_PTY:
		command.Env = os.Environ()
		if os.Getenv("TERM") == "" {
			command.Env = append(command.Env, "TERM=xterm-256color")
		}
		terminal, err := pty.StartWithSize(command, &pty.Winsize{Rows: 24, Cols: 80})
		if err != nil {
			return nil, err
		}
		go func() {
			_, _ = io.Copy(io.Discard, terminal)
		}()
		return &runningCommand{cmd: command, closers: []io.Closer{terminal}}, nil
	case codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE:
		command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
		stdin, err := command.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := command.StdoutPipe()
		if err != nil {
			_ = stdin.Close()
			return nil, err
		}
		stderr, err := command.StderrPipe()
		if err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			return nil, err
		}
		if err := command.Start(); err != nil {
			_ = stdin.Close()
			_ = stdout.Close()
			_ = stderr.Close()
			return nil, err
		}
		go func() {
			_, _ = io.Copy(io.Discard, stdout)
		}()
		go func() {
			_, _ = io.Copy(io.Discard, stderr)
		}()
		return &runningCommand{cmd: command, closers: []io.Closer{stdin, stdout, stderr}}, nil
	default:
		return nil, fmt.Errorf("%w: I/O mode %s", errUnsupportedPlatform, ioMode)
	}
}

func (c *runningCommand) close() {
	for _, closer := range c.closers {
		_ = closer.Close()
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
