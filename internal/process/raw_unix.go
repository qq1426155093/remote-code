//go:build linux || darwin

package process

import (
	"io"
	"os"
	"syscall"
)

// startRawCommand launches one pipe-mode child whose stdout is an exclusive
// pipe held by the caller instead of the segmented log writer. Stderr still
// flows into the record output. The caller owns the returned pipe ends: write
// requests to Stdin and read protocol frames from Stdout; both are closed by
// the registry once the process has been reaped.
func startRawCommand(directory *os.File, executable string, arguments, environment []string, output *recordOutput) (*runningCommand, io.WriteCloser, io.ReadCloser, error) {
	launch, err := newProcessCommand(directory, executable, arguments, environment)
	if err != nil {
		return nil, nil, nil, err
	}
	command := launch.command
	command.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	command.Stderr = output.stderr
	stdinRead, stdinWrite, err := os.Pipe()
	if err != nil {
		return nil, nil, nil, launch.started(err)
	}
	stdoutRead, stdoutWrite, err := os.Pipe()
	if err != nil {
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		return nil, nil, nil, launch.started(err)
	}
	command.Stdin = stdinRead
	command.Stdout = stdoutWrite
	startErr := command.Start()
	if err := launch.started(startErr); err != nil {
		_ = stdinRead.Close()
		_ = stdinWrite.Close()
		_ = stdoutRead.Close()
		_ = stdoutWrite.Close()
		if startErr == nil {
			discardStartedCommand(command)
		}
		return nil, nil, nil, err
	}
	// The child owns its ends now; dropping the parent copies is what makes
	// EOF observable on Stdout when the child exits.
	_ = stdinRead.Close()
	_ = stdoutWrite.Close()
	running := &runningCommand{cmd: command, rawStdin: stdinWrite, rawStdout: stdoutRead}
	return running, stdinWrite, stdoutRead, nil
}

// closeRawPipes releases the parent ends of a raw child's stdio. It runs after
// the process has been waited on, so a reader blocked in Stdout.Read sees EOF
// from the closed child end even before this runs; closing here only releases
// the descriptors.
func (c *runningCommand) closeRawPipes() {
	if c.rawStdin != nil {
		_ = c.rawStdin.Close()
	}
	if c.rawStdout != nil {
		_ = c.rawStdout.Close()
	}
}
