package process

import (
	"context"

	codev1 "github.com/qq1426155093/remote-code/api/remote/code/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ListProcessTemplates returns immutable summaries sorted by template name.
func (s *Service) ListProcessTemplates(ctx context.Context, _ *codev1.ListProcessTemplatesRequest) (*codev1.ListProcessTemplatesResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	return &codev1.ListProcessTemplatesResponse{Templates: s.templates.summaries()}, nil
}

// GetProcessTemplate returns one public template definition without its
// executable, render source, or environment values.
func (s *Service) GetProcessTemplate(ctx context.Context, request *codev1.GetProcessTemplateRequest) (*codev1.GetProcessTemplateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || !identifierPattern.MatchString(request.GetName()) {
		return nil, status.Errorf(codes.InvalidArgument, "process template name must match %s", identifierPattern)
	}
	template, ok := s.templates.lookup(request.GetName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "process template %q was not found", request.GetName())
	}
	return &codev1.GetProcessTemplateResponse{Template: template.publicDefinition()}, nil
}

// StartProcessFromTemplate validates dynamic parameters, runs the immutable
// pure Expr renderer, and starts the normalized request through the same core
// used by StartProcess.
func (s *Service) StartProcessFromTemplate(ctx context.Context, request *codev1.StartProcessFromTemplateRequest) (*codev1.StartProcessFromTemplateResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, status.FromContextError(err).Err()
	}
	if request == nil || !identifierPattern.MatchString(request.GetTemplateName()) {
		return nil, status.Errorf(codes.InvalidArgument, "process template name must match %s", identifierPattern)
	}
	template, ok := s.templates.lookup(request.GetTemplateName())
	if !ok {
		return nil, status.Errorf(codes.NotFound, "process template %q was not found", request.GetTemplateName())
	}
	expectedRevision := request.GetExpectedTemplateRevision()
	if expectedRevision != "" {
		if !templateRevisionPattern.MatchString(expectedRevision) {
			return nil, status.Error(codes.InvalidArgument, "expected template revision must be a lowercase SHA-256 hexadecimal value")
		}
		if expectedRevision != template.summary.GetRevision() {
			return nil, status.Errorf(codes.FailedPrecondition, "process template %q revision does not match the requested revision", request.GetTemplateName())
		}
	}
	startRequest, err := template.render(ctx, request.GetParameters())
	if err != nil {
		return nil, err
	}
	startRequest.Name = request.GetProcessName()
	if size := request.GetTerminalSize(); size != nil {
		startRequest.TerminalSize = &codev1.TerminalSize{Rows: size.GetRows(), Columns: size.GetColumns()}
	}
	response, err := s.startProcess(ctx, startRequest, startOrigin{
		templateName: request.GetTemplateName(), templateRevision: template.summary.GetRevision(), redactArguments: true,
	})
	if err != nil {
		return nil, err
	}
	return &codev1.StartProcessFromTemplateResponse{Process: response.GetProcess()}, nil
}
