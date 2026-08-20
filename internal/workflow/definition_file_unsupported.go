//go:build !linux

package workflow

import (
	"errors"
)

func readDefinitionFile(string) ([]byte, physicalDefinitionFile, error) {
	return nil, physicalDefinitionFile{}, errors.New("secure workflow definition loading is unsupported on this platform")
}
