//go:build !linux

package controllerlog

import (
	"fmt"
	"os"
)

// Non-Linux builds retain the same file ownership semantics. Platforms without
// a portable non-blocking flock use the process-local file handle as a safe
// fallback; the controller runtime remains protected by its directory mode.
type runtimeLock struct{ file *os.File }

func acquireRuntimeLock(name string) (*runtimeLock, error) {
	file, err := os.OpenFile(name, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open controller runtime lock: %w", err)
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, fmt.Errorf("secure controller runtime lock: %w", err)
	}
	return &runtimeLock{file: file}, nil
}

func (l *runtimeLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	return l.file.Close()
}
