//go:build linux

package process

import (
	"os"
	"os/exec"
	"strconv"
)

func newProcessCommand(directory *os.File, executable string, arguments, environment []string) (*commandLaunch, error) {
	command := exec.Command(executable, arguments...)
	command.Dir = "/proc/self/fd/" + strconv.FormatUint(uint64(directory.Fd()), 10)
	command.Env = append([]string(nil), environment...)
	return &commandLaunch{command: command}, nil
}
