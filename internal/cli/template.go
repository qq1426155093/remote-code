package cli

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"text/tabwriter"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	remoteclient "github.com/qq1426155093/remote-code/pkg/client"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
)

const maxCLIProcessTemplateParameters = 1 << 20

type processTemplateStartOptions struct {
	templateName   string
	processName    string
	parametersJSON string
	parametersFile string
	attach         bool
}

func (r *REPL) listProcessTemplates(arguments []string) error {
	if len(arguments) > 1 {
		return usageError()
	}
	ctx, cancel := r.commandContext()
	defer cancel()
	if len(arguments) == 0 {
		templates, err := r.client.ListProcessTemplates(ctx)
		if err != nil {
			return err
		}
		writer := tabwriter.NewWriter(r.stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(writer, "NAME\tMODE\tINPUT\tREVISION\tDESCRIPTION")
		for _, template := range templates {
			fmt.Fprintf(writer, "%s\t%s\t%s\t%s\t%s\n", template.GetName(), processIOModeName(template.GetIoMode()),
				processInputModeName(template.GetInputMode()), shortTemplateRevision(template.GetRevision()), template.GetDescription())
		}
		return writer.Flush()
	}
	template, err := r.client.GetProcessTemplate(ctx, arguments[0])
	if err != nil {
		return err
	}
	summary := template.GetSummary()
	fmt.Fprintf(r.stdout, "Name: %s\nDescription: %s\nRevision: %s\nMode: %s\nInput: %s\nParameters schema:\n",
		summary.GetName(), summary.GetDescription(), summary.GetRevision(), processIOModeName(summary.GetIoMode()), processInputModeName(summary.GetInputMode()))
	encoded, err := json.MarshalIndent(template.GetParametersSchema().AsMap(), "", "  ")
	if err != nil {
		return fmt.Errorf("format process template schema: %w", err)
	}
	if _, err := r.stdout.Write(append(encoded, '\n')); err != nil {
		return fmt.Errorf("write process template schema: %w", err)
	}
	return nil
}

func (r *REPL) startProcessFromTemplate(arguments []string) error {
	options, err := parseProcessTemplateStartOptions(arguments)
	if err != nil {
		return err
	}
	parameters, err := readProcessTemplateParameters(options)
	if err != nil {
		return err
	}
	var terminalSize *codev1.TerminalSize
	expectedRevision := ""
	if options.attach {
		if r.terminal == nil || !r.terminal.available() {
			return errors.New("exec-template --attach requires a supported interactive local terminal")
		}
		ctx, cancel := r.commandContext()
		template, getErr := r.client.GetProcessTemplate(ctx, options.templateName)
		cancel()
		if getErr != nil {
			return getErr
		}
		summary := template.GetSummary()
		if summary.GetIoMode() != codev1.ProcessIOMode_PROCESS_IO_MODE_PTY || summary.GetInputMode() != codev1.ProcessInputMode_PROCESS_INPUT_MODE_MANAGED {
			return errors.New("exec-template --attach requires a PTY template with managed input")
		}
		rows, columns, sizeErr := r.terminal.size()
		if sizeErr != nil {
			return fmt.Errorf("get local terminal size: %w", sizeErr)
		}
		terminalSize = &codev1.TerminalSize{Rows: rows, Columns: columns}
		expectedRevision = summary.GetRevision()
	}
	ctx, cancel := r.commandContext()
	info, err := r.client.StartProcessFromTemplate(ctx, remoteclient.ProcessTemplateStartOptions{
		TemplateName: options.templateName, Parameters: parameters, ProcessName: options.processName,
		TerminalSize: terminalSize, ExpectedTemplateRevision: expectedRevision,
	})
	cancel()
	if err != nil {
		return err
	}
	fmt.Fprintf(r.stdout, "started %s (%s) from template %s@%s, pid %d, %s mode, input %s, cwd %s\n",
		info.GetName(), info.GetId(), info.GetTemplateName(), shortTemplateRevision(info.GetTemplateRevision()), info.GetPid(),
		processIOModeName(info.GetIoMode()), processInputStateName(info.GetInputState()), info.GetWorkingDirectory())
	if options.attach {
		return r.attach(&codev1.ProcessReference{Value: &codev1.ProcessReference_Id{Id: info.GetId()}})
	}
	return nil
}

func parseProcessTemplateStartOptions(arguments []string) (processTemplateStartOptions, error) {
	var options processTemplateStartOptions
	nameSet := false
	parametersSet := false
	attachSet := false
	for index := 0; index < len(arguments); {
		switch argument := arguments[index]; argument {
		case "--name":
			if index+1 >= len(arguments) || arguments[index+1] == "" || nameSet {
				return processTemplateStartOptions{}, usageError()
			}
			options.processName = arguments[index+1]
			nameSet = true
			index += 2
		case "--params":
			if index+1 >= len(arguments) || arguments[index+1] == "" || parametersSet {
				return processTemplateStartOptions{}, usageError()
			}
			options.parametersJSON = arguments[index+1]
			parametersSet = true
			index += 2
		case "--params-file":
			if index+1 >= len(arguments) || arguments[index+1] == "" || parametersSet {
				return processTemplateStartOptions{}, usageError()
			}
			options.parametersFile = arguments[index+1]
			parametersSet = true
			index += 2
		case "--attach":
			if attachSet {
				return processTemplateStartOptions{}, errors.New("--attach is specified more than once")
			}
			options.attach = true
			attachSet = true
			index++
		case "--":
			index++
			if index >= len(arguments) || options.templateName != "" || len(arguments[index:]) != 1 {
				return processTemplateStartOptions{}, usageError()
			}
			options.templateName = arguments[index]
			index = len(arguments)
		default:
			if strings.HasPrefix(argument, "-") {
				return processTemplateStartOptions{}, usageErrorf("unknown exec-template option %q", argument)
			}
			if options.templateName != "" {
				return processTemplateStartOptions{}, usageError()
			}
			options.templateName = argument
			index++
		}
	}
	if options.templateName == "" {
		return processTemplateStartOptions{}, usageError()
	}
	return options, nil
}

func readProcessTemplateParameters(options processTemplateStartOptions) (*structpb.Struct, error) {
	contents := []byte("{}")
	if options.parametersJSON != "" {
		contents = []byte(options.parametersJSON)
	}
	if options.parametersFile != "" {
		file, err := os.Open(options.parametersFile)
		if err != nil {
			return nil, fmt.Errorf("open process template parameters: %w", err)
		}
		defer file.Close()
		info, err := file.Stat()
		if err != nil {
			return nil, fmt.Errorf("stat process template parameters: %w", err)
		}
		if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maxCLIProcessTemplateParameters {
			return nil, fmt.Errorf("process template parameters file must be a regular file between 1 and %d bytes", maxCLIProcessTemplateParameters)
		}
		contents, err = io.ReadAll(io.LimitReader(file, maxCLIProcessTemplateParameters+1))
		if err != nil {
			return nil, fmt.Errorf("read process template parameters: %w", err)
		}
	}
	if len(contents) > maxCLIProcessTemplateParameters {
		return nil, fmt.Errorf("process template parameters exceed %d bytes", maxCLIProcessTemplateParameters)
	}
	if trimmed := bytes.TrimSpace(contents); len(trimmed) == 0 || trimmed[0] != '{' {
		return nil, errors.New("process template parameters must be one JSON object")
	}
	parameters := &structpb.Struct{}
	if err := protojson.Unmarshal(contents, parameters); err != nil {
		return nil, errors.New("process template parameters must be one valid JSON object")
	}
	return parameters, nil
}

func shortTemplateRevision(revision string) string {
	if len(revision) <= 12 {
		return revision
	}
	return revision[:12]
}
