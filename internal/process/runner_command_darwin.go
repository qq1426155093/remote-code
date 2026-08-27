//go:build darwin

package process

import (
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"

	"golang.org/x/sys/unix"
)

const (
	darwinExecHelperArgument    = "--remote-code-internal-fchdir-exec-v1"
	darwinExecHelperEnvironment = "REMOTE_CODE_INTERNAL_FCHDIR_EXEC_V1=1"
	darwinWorkingDirectoryFD    = 3
	darwinExecStatusFD          = 4
	darwinExecStatusMaxBytes    = 4096
)

// Darwin's os/exec interface only accepts a path for Cmd.Dir. Use a short-lived
// copy of this executable to fchdir to the already validated workspace directory
// and then replace itself with the requested command. Exec preserves the PID,
// process group and controlling terminal established for the helper.
func init() {
	if len(os.Args) < 4 || os.Args[1] != darwinExecHelperArgument || !hasDarwinExecHelperEnvironment(os.Environ()) {
		return
	}
	if _, err := unix.FcntlInt(darwinExecStatusFD, unix.F_SETFD, unix.FD_CLOEXEC); err != nil {
		darwinExecHelperExit("secure launch status handle", err)
	}
	if err := unix.Fchdir(darwinWorkingDirectoryFD); err != nil {
		darwinExecHelperExit("change working directory", err)
	}
	if err := unix.Close(darwinWorkingDirectoryFD); err != nil {
		darwinExecHelperExit("close working directory handle", err)
	}
	targetPath := os.Args[2]
	targetArguments := os.Args[3:]
	if err := unix.Exec(targetPath, targetArguments, withoutDarwinExecHelperEnvironment(os.Environ())); err != nil {
		darwinExecHelperExit("execute target", err)
	}
}

func newProcessCommand(directory *os.File, executable string, arguments, environment []string) (*commandLaunch, error) {
	target := exec.Command(executable, arguments...)
	if target.Err != nil {
		return nil, target.Err
	}
	helperPath, err := os.Executable()
	if err != nil {
		return nil, fmt.Errorf("resolve controller executable for process launch: %w", err)
	}
	statusReader, statusWriter, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("create process launch status pipe: %w", err)
	}
	helperArguments := make([]string, 0, 2+len(target.Args))
	helperArguments = append(helperArguments, darwinExecHelperArgument, target.Path)
	helperArguments = append(helperArguments, target.Args...)
	command := exec.Command(helperPath, helperArguments...)
	command.Env = append(append([]string(nil), environment...), darwinExecHelperEnvironment)
	command.ExtraFiles = []*os.File{directory, statusWriter}
	return &commandLaunch{
		command: command,
		finishStart: func(startErr error) error {
			_ = statusWriter.Close()
			defer statusReader.Close()
			if startErr != nil {
				return startErr
			}
			statusBytes, readErr := io.ReadAll(io.LimitReader(statusReader, darwinExecStatusMaxBytes+1))
			if readErr != nil {
				return fmt.Errorf("read process launch status: %w", readErr)
			}
			if len(statusBytes) > darwinExecStatusMaxBytes {
				return errors.New("process launcher returned an oversized error")
			}
			if message := strings.TrimSpace(string(statusBytes)); message != "" {
				return errors.New(message)
			}
			return nil
		},
	}, nil
}

func hasDarwinExecHelperEnvironment(environment []string) bool {
	for _, entry := range environment {
		if entry == darwinExecHelperEnvironment {
			return true
		}
	}
	return false
}

func withoutDarwinExecHelperEnvironment(environment []string) []string {
	marker := -1
	for index := len(environment) - 1; index >= 0; index-- {
		if environment[index] == darwinExecHelperEnvironment {
			marker = index
			break
		}
	}
	if marker < 0 {
		return append([]string(nil), environment...)
	}
	result := make([]string, 0, len(environment)-1)
	result = append(result, environment[:marker]...)
	result = append(result, environment[marker+1:]...)
	return result
}

func darwinExecHelperExit(operation string, err error) {
	message := fmt.Sprintf("remote-code process launcher: %s: %v", operation, err)
	if status := os.NewFile(darwinExecStatusFD, "remote-code-launch-status"); status != nil {
		_, _ = io.WriteString(status, message)
		_ = status.Close()
	}
	_, _ = fmt.Fprintln(os.Stderr, message)
	os.Exit(127)
}
