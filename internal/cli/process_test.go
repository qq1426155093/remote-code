package cli

import (
	"reflect"
	"strings"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestParseProcessStartOptions(t *testing.T) {
	options, err := parseProcessStartOptions([]string{"--name", "worker", "--pty", "--cwd", "docs", "-e", "MODE=test", "helper", "--flag", "value"})
	if err != nil {
		t.Fatalf("parseProcessStartOptions() error = %v", err)
	}
	if options.name != "worker" || options.command != "helper" || options.workingDirectory != "docs" || options.ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY || !reflect.DeepEqual(options.arguments, []string{"--flag", "value"}) || !reflect.DeepEqual(options.environment, map[string]string{"MODE": "test"}) {
		t.Fatalf("parseProcessStartOptions() = %+v", options)
	}

	options, err = parseProcessStartOptions([]string{"--name", "worker", "--pipe", "--", "helper", "-leading-option"})
	if err != nil {
		t.Fatalf("parseProcessStartOptions(-- separator) error = %v", err)
	}
	if options.ioMode != codev1.ProcessIOMode_PROCESS_IO_MODE_PIPE || !reflect.DeepEqual(options.arguments, []string{"-leading-option"}) {
		t.Errorf("parseProcessStartOptions(-- separator) = %+v", options)
	}

	for _, arguments := range [][]string{
		{},
		{"--name", "worker"},
		{"--unknown", "worker", "helper"},
		{"-e", "invalid", "helper"},
		{"--pipe", "--pty", "helper"},
		{"--name", "", "helper"},
		{"--name", "one", "--name", "two", "helper"},
	} {
		if _, err := parseProcessStartOptions(arguments); err == nil {
			t.Errorf("parseProcessStartOptions(%q) succeeded", arguments)
		}
	}
}

func TestParseProcessSignalOptions(t *testing.T) {
	options, err := parseProcessSignalOptions([]string{"-s", "SIGKILL", "-w", "pid:123"})
	if err != nil {
		t.Fatalf("parseProcessSignalOptions() error = %v", err)
	}
	pid, ok := options.reference.GetValue().(*codev1.ProcessReference_Pid)
	if !ok || pid.Pid != 123 || options.signal != codev1.ProcessSignal_PROCESS_SIGNAL_KILL || !options.wait {
		t.Fatalf("parseProcessSignalOptions() = %+v", options)
	}

	name, err := parseProcessReference("worker")
	if err != nil {
		t.Fatal(err)
	}
	if got := name.GetName(); got != "worker" {
		t.Errorf("parseProcessReference(worker).Name = %q", got)
	}
	idValue := "7aa5daab-e886-4889-9ec3-92d461883091"
	id, err := parseProcessReference(idValue)
	if err != nil {
		t.Fatal(err)
	}
	if got := id.GetId(); got != idValue {
		t.Errorf("parseProcessReference(uuid).Id = %q", got)
	}
	if signal, err := parseProcessSignal("15"); err != nil || signal != codev1.ProcessSignal_PROCESS_SIGNAL_TERM {
		t.Errorf("parseProcessSignal(15) = %s, %v", signal, err)
	}
	if signal, err := parseProcessSignal("sigterm"); err != nil || signal != codev1.ProcessSignal_PROCESS_SIGNAL_TERM {
		t.Errorf("parseProcessSignal(sigterm) = %s, %v", signal, err)
	}
	if _, err := parseProcessSignal("BOGUS"); err == nil {
		t.Error("parseProcessSignal(BOGUS) succeeded")
	}
}

func TestParseProcessLogOptions(t *testing.T) {
	id := "7aa5daab-e886-4889-9ec3-92d461883091"
	options, err := parseProcessLogOptions([]string{"-f", "-n", "100", "--stdout", id})
	if err != nil {
		t.Fatal(err)
	}
	if options.processID != id || !options.follow || options.tail == nil || *options.tail != 100 || options.offset != nil || !reflect.DeepEqual(options.streams, []codev1.ProcessLogStream{codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDOUT}) {
		t.Fatalf("parseProcessLogOptions() = %+v", options)
	}
	options, err = parseProcessLogOptions([]string{"--offset", "42", "--stderr", strings.ToUpper(id)})
	if err != nil {
		t.Fatal(err)
	}
	if options.processID != id || options.offset == nil || *options.offset != 42 || !reflect.DeepEqual(options.streams, []codev1.ProcessLogStream{codev1.ProcessLogStream_PROCESS_LOG_STREAM_STDERR}) {
		t.Fatalf("parseProcessLogOptions(offset) = %+v", options)
	}
	for _, arguments := range [][]string{
		{},
		{"not-a-uuid"},
		{"-n", "1", "--offset", "2", id},
		{"-n", "-1", id},
		{"--stdout", "--stdout", id},
	} {
		if _, err := parseProcessLogOptions(arguments); err == nil {
			t.Errorf("parseProcessLogOptions(%q) succeeded", arguments)
		}
	}
}
