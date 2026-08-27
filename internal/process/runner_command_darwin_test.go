//go:build darwin

package process

import (
	"os"
	"testing"
)

func TestNewProcessCommandUsesFDLauncher(t *testing.T) {
	directory, err := os.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	defer directory.Close()
	command, err := newProcessCommand(directory, "/bin/echo", []string{"hello"}, []string{
		"PATH=/usr/bin:/bin", "REMOTE_CODE_INTERNAL_FCHDIR_EXEC_V1=spoofed",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(command.command.ExtraFiles) != 2 || command.command.ExtraFiles[0] != directory {
		t.Fatalf("ExtraFiles = %+v, want working directory and launch status handles", command.command.ExtraFiles)
	}
	defer command.started(os.ErrInvalid)
	if len(command.command.Args) != 5 || command.command.Args[1] != darwinExecHelperArgument || command.command.Args[2] != "/bin/echo" || command.command.Args[3] != "/bin/echo" || command.command.Args[4] != "hello" {
		t.Fatalf("Args = %q, want launcher marker, path and target argv", command.command.Args)
	}
	if !hasDarwinExecHelperEnvironment(command.command.Env) {
		t.Fatalf("Env = %q, want internal launcher marker", command.command.Env)
	}
	filtered := withoutDarwinExecHelperEnvironment(command.command.Env)
	if len(filtered) != 2 || filtered[1] != "REMOTE_CODE_INTERNAL_FCHDIR_EXEC_V1=spoofed" {
		t.Fatalf("filtered Env = %q, want caller environment without appended marker", filtered)
	}
}
