package rpcerror

import (
	"errors"
	"testing"

	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestErrorfCarriesCodeMessageAndReason(t *testing.T) {
	err := Errorf(codes.FailedPrecondition, ProcessInputClosed, "process %q is closed", "designer")
	if got := status.Code(err); got != codes.FailedPrecondition {
		t.Fatalf("code = %s, want FailedPrecondition", got)
	}
	if got := status.Convert(err).Message(); got != `process "designer" is closed` {
		t.Fatalf("message = %q", got)
	}
	if got := ReasonOf(err); got != ProcessInputClosed {
		t.Fatalf("reason = %q, want %q", got, ProcessInputClosed)
	}
}

func TestErrorfWithMetadataRoundTripsValues(t *testing.T) {
	err := ErrorfWithMetadata(codes.OutOfRange, LogOffsetOutOfRange, map[string]string{
		"earliest_offset": "12", "next_offset": "40",
	}, "offset out of range")
	if got := ReasonOf(err); got != LogOffsetOutOfRange {
		t.Fatalf("reason = %q", got)
	}
	details := status.Convert(err).Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want 1", len(details))
	}
	info, ok := details[0].(*errdetails.ErrorInfo)
	if !ok {
		t.Fatalf("detail type = %T", details[0])
	}
	if info.GetDomain() != DomainName {
		t.Fatalf("domain = %q, want %q", info.GetDomain(), DomainName)
	}
	if info.GetMetadata()["earliest_offset"] != "12" || info.GetMetadata()["next_offset"] != "40" {
		t.Fatalf("metadata = %v", info.GetMetadata())
	}
}

func TestReasonOfIgnoresErrorsWithoutAReason(t *testing.T) {
	foreign := &errdetails.ErrorInfo{Reason: "SOMETHING", Domain: "other.example"}
	base := status.New(codes.Internal, "boom")
	withForeign, err := base.WithDetails(foreign)
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]error{
		"nil":              nil,
		"plain error":      errors.New("boom"),
		"undetailed":       status.Error(codes.NotFound, "missing"),
		"different domain": withForeign.Err(),
	}
	for name, candidate := range tests {
		t.Run(name, func(t *testing.T) {
			if got := ReasonOf(candidate); got != "" {
				t.Fatalf("ReasonOf() = %q, want empty", got)
			}
		})
	}
}

// Reasons are API surface, so a duplicate value would silently make two
// conditions indistinguishable to a client.
func TestReasonValuesAreUniqueAndWellFormed(t *testing.T) {
	all := []Reason{
		ProcessServiceShuttingDown, ActiveProcessLimitReached, ProcessNameInUse,
		ProcessHistoryLimitReached, ProcessNotRunning, ProcessNotTerminal,
		ProcessAlreadyExited, WorkingDirectoryOpenFailed, WorkingDirectoryNotDirectory,
		ProcessNotPTY, PTYInputCloseUnsupported, ProcessInputDisabled,
		ProcessInputClosed, ProcessInputAttached, ProcessLogsUnavailable,
		ProcessLogsObserved, ProcessLogObserverLimitReached, LogOffsetOutOfRange,
		ControllerLogOffsetOutOfRange, ControllerLogsUnavailable,
		TemplateRenderFailed, TemplateRevisionMismatch,
		TransferOffsetMismatch, TransferFileChanged, TransferPrefixMismatch,
		TransferSessionState, TransferActiveTransfer,
	}
	seen := make(map[Reason]struct{}, len(all))
	for _, reason := range all {
		if reason == "" {
			t.Fatal("empty reason constant")
		}
		for _, character := range reason {
			if (character < 'A' || character > 'Z') && character != '_' {
				t.Fatalf("reason %q must be upper snake case", reason)
			}
		}
		if _, exists := seen[reason]; exists {
			t.Fatalf("duplicate reason %q", reason)
		}
		seen[reason] = struct{}{}
	}
}
