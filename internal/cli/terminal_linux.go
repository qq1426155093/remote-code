//go:build linux

package cli

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"golang.org/x/sys/unix"
	"golang.org/x/term"
)

type localTerminal struct {
	file *os.File
}

func newLocalTerminal(file *os.File) terminalController {
	return &localTerminal{file: terminalFile(file)}
}

func (t *localTerminal) available() bool {
	return t.file != nil && term.IsTerminal(int(t.file.Fd()))
}

func (t *localTerminal) makeRaw() (func() error, error) {
	state, err := term.MakeRaw(int(t.file.Fd()))
	if err != nil {
		return nil, err
	}
	return func() error { return term.Restore(int(t.file.Fd()), state) }, nil
}

func (t *localTerminal) size() (uint32, uint32, error) {
	columns, rows, err := term.GetSize(int(t.file.Fd()))
	if err != nil {
		return 0, 0, err
	}
	if rows <= 0 || columns <= 0 || rows > 65535 || columns > 65535 {
		return 0, 0, errors.New("local terminal size is outside the supported range")
	}
	return uint32(rows), uint32(columns), nil
}

func (t *localTerminal) read(ctx context.Context, buffer []byte) (int, error) {
	descriptors := []unix.PollFd{{Fd: int32(t.file.Fd()), Events: unix.POLLIN}}
	for {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		ready, err := unix.Poll(descriptors, 250)
		if err != nil {
			if errors.Is(err, syscall.EINTR) {
				continue
			}
			return 0, err
		}
		if ready == 0 {
			continue
		}
		if descriptors[0].Revents&(unix.POLLERR|unix.POLLHUP|unix.POLLNVAL) != 0 {
			return 0, os.ErrClosed
		}
		if descriptors[0].Revents&unix.POLLIN != 0 {
			if err := ctx.Err(); err != nil {
				return 0, err
			}
			return unix.Read(int(t.file.Fd()), buffer)
		}
	}
}

func (t *localTerminal) resizeEvents() (<-chan struct{}, func()) {
	notifications := make(chan os.Signal, 1)
	events := make(chan struct{}, 1)
	stop := make(chan struct{})
	signal.Notify(notifications, syscall.SIGWINCH)
	go func() {
		defer close(events)
		for {
			select {
			case <-notifications:
				select {
				case events <- struct{}{}:
				default:
				}
			case <-stop:
				return
			}
		}
	}()
	var stopped bool
	return events, func() {
		if stopped {
			return
		}
		stopped = true
		signal.Stop(notifications)
		close(stop)
	}
}
