package client

import (
	"errors"
	"testing"

	"github.com/qq1426155093/remote-code/internal/rpcerror"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestReasonReadsControllerReasons(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want string
	}{
		{name: "reasoned error", err: rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ProcessInputClosed, "closed"), want: ReasonProcessInputClosed},
		{name: "same code different condition", err: rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ProcessNotRunning, "exited"), want: ReasonProcessNotRunning},
		{name: "undetailed status", err: status.Error(codes.FailedPrecondition, "closed")},
		{name: "plain error", err: errors.New("closed")},
		{name: "nil"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := Reason(test.err); got != test.want {
				t.Fatalf("Reason() = %q, want %q", got, test.want)
			}
		})
	}
}

// Two conditions that share FailedPrecondition must stay distinguishable, which
// is the property callers rely on instead of matching the message.
func TestReasonSeparatesConditionsSharingOneCode(t *testing.T) {
	closed := rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ProcessInputClosed, "process input is closed")
	disabled := rpcerror.Errorf(codes.FailedPrecondition, rpcerror.ProcessInputDisabled, "process input was not enabled at startup")
	if status.Code(closed) != status.Code(disabled) {
		t.Fatal("test premise broken: the two errors no longer share a code")
	}
	if Reason(closed) == Reason(disabled) {
		t.Fatalf("reasons collided: %q", Reason(closed))
	}
}
