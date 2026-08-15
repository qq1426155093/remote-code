package cli

import (
	"context"
	"errors"
	"fmt"
	"io"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
)

const attachEscapeByte = byte(0x1d)

var errDetachRequested = errors.New("process detach requested")

func (r *REPL) attachProcess(arguments []string) error {
	if len(arguments) != 1 {
		return errors.New(commandUsage["attach"])
	}
	reference, err := parseProcessReference(arguments[0])
	if err != nil {
		return err
	}
	return r.attach(reference)
}

func (r *REPL) attach(reference *codev1.ProcessReference) error {
	if r.terminal == nil || !r.terminal.available() {
		return errors.New("attach requires a supported interactive local terminal")
	}
	rows, columns, err := r.terminal.size()
	if err != nil {
		return fmt.Errorf("get local terminal size: %w", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attachment, err := r.client.OpenProcessAttachment(ctx, reference, remoteclient.ProcessAttachOptions{
		Rows: rows, Columns: columns,
	})
	if err != nil {
		return err
	}
	process := attachment.Process()
	fmt.Fprintf(r.stdout, "attached to %s (%s); Ctrl-] d detaches, Ctrl-] Ctrl-] sends Ctrl-]\n", process.GetName(), process.GetId())

	restore, err := r.terminal.makeRaw()
	if err != nil {
		_ = attachment.Detach()
		return fmt.Errorf("enter raw terminal mode: %w", err)
	}
	restored := false
	defer func() {
		if !restored {
			_ = restore()
		}
	}()

	resizeEvents, stopResize := r.terminal.resizeEvents()
	defer stopResize()
	inputResult := make(chan error, 1)
	outputResult := make(chan error, 1)
	go func() { inputResult <- r.forwardAttachmentInput(ctx, attachment) }()
	go func() { outputResult <- r.copyAttachmentOutput(attachment) }()

	detached := false
	inputDone := false
	var result error
	finished := false
	for !finished {
		select {
		case inputErr := <-inputResult:
			inputDone = true
			detached = errors.Is(inputErr, errDetachRequested) || errors.Is(inputErr, io.EOF)
			if !detached && inputErr != nil && !errors.Is(inputErr, context.Canceled) {
				result = inputErr
			}
			if detachErr := attachment.Detach(); result == nil {
				result = detachErr
			}
			if outputErr := <-outputResult; result == nil {
				result = outputErr
			}
			finished = true
		case outputErr := <-outputResult:
			if outputErr != nil {
				result = outputErr
				_ = attachment.Detach()
			} else {
				result = attachment.Wait()
			}
			finished = true
		case <-attachment.Done():
			result = attachment.Wait()
			if outputErr := <-outputResult; result == nil {
				result = outputErr
			}
			finished = true
		case _, ok := <-resizeEvents:
			if !ok {
				resizeEvents = nil
				continue
			}
			newRows, newColumns, sizeErr := r.terminal.size()
			if sizeErr != nil {
				result = fmt.Errorf("get resized terminal dimensions: %w", sizeErr)
				_ = attachment.Detach()
				<-outputResult
				finished = true
				continue
			}
			if resizeErr := attachment.Resize(newRows, newColumns); resizeErr != nil {
				result = resizeErr
				_ = attachment.Detach()
				<-outputResult
				finished = true
			}
		}
	}
	cancel()
	if !inputDone {
		<-inputResult
	}
	if restoreErr := restore(); result == nil {
		result = restoreErr
	}
	restored = true
	if detached && result == nil {
		fmt.Fprintf(r.stdout, "\r\ndetached from %s (%s)\n", process.GetName(), process.GetId())
	} else {
		fmt.Fprint(r.stdout, "\r\n")
	}
	return result
}

func (r *REPL) forwardAttachmentInput(ctx context.Context, attachment *remoteclient.ProcessAttachment) error {
	buffer := make([]byte, 4096)
	prefix := false
	for {
		count, err := r.terminal.read(ctx, buffer)
		if count > 0 {
			forward, detach := filterAttachInput(buffer[:count], &prefix)
			if len(forward) > 0 {
				if _, writeErr := attachment.Write(forward); writeErr != nil {
					return writeErr
				}
			}
			if detach {
				return errDetachRequested
			}
		}
		if err != nil {
			return err
		}
	}
}

func filterAttachInput(input []byte, prefix *bool) ([]byte, bool) {
	output := make([]byte, 0, len(input))
	for _, value := range input {
		if !*prefix {
			if value == attachEscapeByte {
				*prefix = true
			} else {
				output = append(output, value)
			}
			continue
		}
		*prefix = false
		switch value {
		case 'd':
			return output, true
		case attachEscapeByte:
			output = append(output, attachEscapeByte)
		default:
			output = append(output, attachEscapeByte, value)
		}
	}
	return output, false
}

func (r *REPL) copyAttachmentOutput(attachment *remoteclient.ProcessAttachment) error {
	for data := range attachment.Output() {
		written, err := r.stdout.Write(data)
		if err != nil {
			return fmt.Errorf("write attached process output: %w", err)
		}
		if written != len(data) {
			return io.ErrShortWrite
		}
	}
	return nil
}
