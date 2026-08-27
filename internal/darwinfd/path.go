//go:build darwin

// Package darwinfd contains Darwin-only helpers for identifying open files.
package darwinfd

import (
	"bytes"
	"errors"
	"runtime"
	"unsafe"

	"golang.org/x/sys/unix"
)

// Path returns the kernel-reported path for an open Darwin file descriptor.
func Path(fd int) (string, error) {
	buffer := make([]byte, unix.PathMax)
	_, err := unix.FcntlInt(uintptr(fd), unix.F_GETPATH, int(uintptr(unsafe.Pointer(&buffer[0]))))
	runtime.KeepAlive(buffer)
	if err != nil {
		return "", err
	}
	end := bytes.IndexByte(buffer, 0)
	if end <= 0 {
		return "", errors.New("fcntl F_GETPATH returned an invalid path")
	}
	return string(buffer[:end]), nil
}
