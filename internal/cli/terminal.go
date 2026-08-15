package cli

import (
	"context"
	"os"
)

type terminalController interface {
	available() bool
	makeRaw() (func() error, error)
	size() (rows, columns uint32, err error)
	read(context.Context, []byte) (int, error)
	resizeEvents() (<-chan struct{}, func())
}

func terminalFile(configured *os.File) *os.File {
	if configured != nil {
		return configured
	}
	return os.Stdin
}
