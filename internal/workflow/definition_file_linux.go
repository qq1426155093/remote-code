//go:build linux

package workflow

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func readDefinitionFile(name string) ([]byte, physicalDefinitionFile, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, physicalDefinitionFile{}, fmt.Errorf("open workflow definition %q: %w", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, physicalDefinitionFile{}, fmt.Errorf("open workflow definition %q: invalid file handle", name)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, physicalDefinitionFile{}, fmt.Errorf("stat workflow definition %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return nil, physicalDefinitionFile{}, fmt.Errorf("workflow definition %q is not a regular file", name)
	}
	if info.Size() <= 0 || info.Size() > maxDefinitionBytes {
		return nil, physicalDefinitionFile{}, fmt.Errorf("workflow definition %q must be between 1 and %d bytes", name, maxDefinitionBytes)
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxDefinitionBytes+1))
	if err != nil {
		return nil, physicalDefinitionFile{}, fmt.Errorf("read workflow definition %q: %w", name, err)
	}
	if int64(len(contents)) > maxDefinitionBytes {
		return nil, physicalDefinitionFile{}, fmt.Errorf("workflow definition %q exceeds the %d byte limit", name, maxDefinitionBytes)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, physicalDefinitionFile{}, fmt.Errorf("identify workflow definition %q: %w", name, err)
	}
	physicalPath, err := filepath.EvalSymlinks(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		physicalPath, err = filepath.Abs(name)
		if err != nil {
			return nil, physicalDefinitionFile{}, fmt.Errorf("resolve workflow definition %q: %w", name, err)
		}
	}
	return contents, physicalDefinitionFile{path: filepath.Clean(physicalPath), dev: uint64(stat.Dev), ino: stat.Ino}, nil
}
