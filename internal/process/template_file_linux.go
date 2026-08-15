//go:build linux

package process

import (
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/sys/unix"
)

func readProcessTemplateDefinitionFile(name string) ([]byte, processTemplatePhysicalFile, error) {
	fd, err := unix.Open(name, unix.O_RDONLY|unix.O_CLOEXEC|unix.O_NOFOLLOW, 0)
	if err != nil {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("open process template definition %q: %w", name, err)
	}
	file := os.NewFile(uintptr(fd), name)
	if file == nil {
		_ = unix.Close(fd)
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("open process template definition %q: invalid file handle", name)
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("stat process template definition %q: %w", name, err)
	}
	if !info.Mode().IsRegular() {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("process template definition %q is not a regular file", name)
	}
	if info.Size() <= 0 || info.Size() > maxProcessTemplateDefinitionBytes {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("process template definition %q must be between 1 and %d bytes", name, maxProcessTemplateDefinitionBytes)
	}
	contents, err := io.ReadAll(io.LimitReader(file, maxProcessTemplateDefinitionBytes+1))
	if err != nil {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("read process template definition %q: %w", name, err)
	}
	if int64(len(contents)) > maxProcessTemplateDefinitionBytes {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("process template definition %q exceeds the %d byte limit", name, maxProcessTemplateDefinitionBytes)
	}
	var stat unix.Stat_t
	if err := unix.Fstat(fd, &stat); err != nil {
		return nil, processTemplatePhysicalFile{}, fmt.Errorf("identify process template definition %q: %w", name, err)
	}
	physicalPath, err := filepath.EvalSymlinks(fmt.Sprintf("/proc/self/fd/%d", fd))
	if err != nil {
		physicalPath, err = filepath.Abs(name)
		if err != nil {
			return nil, processTemplatePhysicalFile{}, fmt.Errorf("resolve process template definition %q: %w", name, err)
		}
	}
	return contents, processTemplatePhysicalFile{
		path: filepath.Clean(physicalPath), dev: uint64(stat.Dev), ino: stat.Ino,
	}, nil
}
