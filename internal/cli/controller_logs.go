package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

type controllerLogOptions struct {
	offset *uint64
	tail   *uint64
	follow bool
}

func parseControllerLogOptions(arguments []string) (controllerLogOptions, error) {
	var options controllerLogOptions
	for index := 0; index < len(arguments); index++ {
		switch arguments[index] {
		case "-f", "--follow":
			if options.follow {
				return controllerLogOptions{}, errors.New("--follow is specified more than once")
			}
			options.follow = true
		case "-n", "--tail":
			if index+1 >= len(arguments) || options.tail != nil || options.offset != nil {
				return controllerLogOptions{}, usageError()
			}
			index++
			value, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return controllerLogOptions{}, errors.New("controller log tail lines must be a non-negative integer")
			}
			options.tail = &value
		case "--offset":
			if index+1 >= len(arguments) || options.offset != nil || options.tail != nil {
				return controllerLogOptions{}, usageError()
			}
			index++
			value, err := strconv.ParseUint(arguments[index], 10, 64)
			if err != nil {
				return controllerLogOptions{}, errors.New("controller log offset must be a non-negative integer")
			}
			options.offset = &value
		case "--":
			if index+1 != len(arguments) {
				return controllerLogOptions{}, usageError()
			}
		default:
			if strings.HasPrefix(arguments[index], "-") {
				return controllerLogOptions{}, usageErrorf("unknown controller-logs option %q", arguments[index])
			}
			return controllerLogOptions{}, usageError()
		}
	}
	return options, nil
}

func (r *REPL) observeControllerLogs(arguments []string) error {
	options, err := parseControllerLogOptions(arguments)
	if err != nil {
		return err
	}
	commandContext, cancelCommand := r.commandContext()
	defer cancelCommand()
	streamContext := commandContext
	stopInterrupt := func() {}
	if options.follow {
		streamContext, stopInterrupt = r.interruptContext(commandContext)
	}
	defer stopInterrupt()
	interrupted := func() bool {
		return options.follow && streamContext.Err() != nil && commandContext.Err() == nil
	}
	stream, err := r.client.ObserveControllerLogs(streamContext, remoteclient.ControllerLogOptions{
		Offset: options.offset, TailLines: options.tail, Follow: options.follow,
	})
	if err != nil {
		if interrupted() {
			return nil
		}
		return err
	}
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			if interrupted() {
				return nil
			}
			return err
		}
		if entry := response.GetEntry(); entry != nil {
			if err := writeControllerLogEntry(r.stdout, entry); err != nil {
				return err
			}
		}
	}
}

type controllerLogOutput struct {
	Offset        uint64            `json:"offset"`
	NextOffset    uint64            `json:"next_offset"`
	BootID        string            `json:"boot_id"`
	Timestamp     string            `json:"timestamp"`
	Level         string            `json:"level"`
	Component     string            `json:"component"`
	Event         string            `json:"event"`
	Message       string            `json:"message,omitempty"`
	Fields        map[string]string `json:"fields,omitempty"`
	LineTruncated bool              `json:"line_truncated,omitempty"`
}

func writeControllerLogEntry(output io.Writer, entry *codev1.ControllerLogEntry) error {
	if entry == nil {
		return nil
	}
	timestamp := ""
	if value := entry.GetTimestamp(); value != nil && value.IsValid() {
		timestamp = value.AsTime().UTC().Format(time.RFC3339Nano)
	}
	line, err := json.Marshal(controllerLogOutput{
		Offset: entry.GetOffset(), NextOffset: entry.GetNextOffset(), BootID: entry.GetBootId(), Timestamp: timestamp, Level: controllerLogLevelName(entry.GetLevel()),
		Component: entry.GetComponent(), Event: entry.GetEvent(), Message: entry.GetMessage(), Fields: entry.GetFields(), LineTruncated: entry.GetLineTruncated(),
	})
	if err != nil {
		return fmt.Errorf("encode controller log output: %w", err)
	}
	line = append(line, '\n')
	written, err := output.Write(line)
	if err != nil {
		return fmt.Errorf("write controller log output: %w", err)
	}
	if written != len(line) {
		return io.ErrShortWrite
	}
	return nil
}

func controllerLogLevelName(level codev1.ControllerLogLevel) string {
	switch level {
	case codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_DEBUG:
		return "DEBUG"
	case codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_WARN:
		return "WARN"
	case codev1.ControllerLogLevel_CONTROLLER_LOG_LEVEL_ERROR:
		return "ERROR"
	default:
		return "INFO"
	}
}
