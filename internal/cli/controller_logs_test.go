package cli

import (
	"bytes"
	"testing"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
)

func TestParseControllerLogOptions(t *testing.T) {
	options, err := parseControllerLogOptions([]string{"-f", "-n", "12"})
	if err != nil || !options.follow || options.tail == nil || *options.tail != 12 || options.offset != nil {
		t.Fatalf("parseControllerLogOptions() = %+v, %v", options, err)
	}
	if _, err := parseControllerLogOptions([]string{"-n", "1", "--offset", "2"}); err == nil {
		t.Fatal("accepted mutually exclusive tail and offset")
	}
	if _, err := parseControllerLogOptions([]string{"unexpected"}); err == nil {
		t.Fatal("accepted positional controller log argument")
	}
}

func TestWriteControllerLogEntry(t *testing.T) {
	var output bytes.Buffer
	entry := &codev1.ControllerLogEntry{Offset: 7, NextOffset: 8, BootId: "boot", Level: codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_ERROR, Component: "test", Event: "failed", Message: "safe", Fields: map[string]string{"key": "value"}}
	if err := writeControllerLogEntry(&output, entry); err != nil {
		t.Fatal(err)
	}
	if got := output.String(); got == "" || !bytes.Contains([]byte(got), []byte(`"offset":7`)) || !bytes.Contains([]byte(got), []byte(`"next_offset":8`)) || !bytes.Contains([]byte(got), []byte(`"boot_id":"boot"`)) {
		t.Fatalf("controller log output = %q", got)
	}
}
