//go:build linux

package controllerlog

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

type runtimeLock struct{ file *os.File }

func acquireRuntimeLock(name string) (*runtimeLock, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE|unix.O_NOFOLLOW, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open controller runtime lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure controller runtime lock: %w", err)
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		_ = file.Close()
		if err == unix.EWOULDBLOCK || err == unix.EAGAIN {
			return nil, fmt.Errorf("controller runtime directory is already in use")
		}
		return nil, fmt.Errorf("lock controller runtime directory: %w", err)
	}
	return &runtimeLock{file: file}, nil
}

func (l *runtimeLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := unix.Flock(int(l.file.Fd()), unix.LOCK_UN)
	closeErr := l.file.Close()
	if err != nil {
		return err
	}
	return closeErr
}
