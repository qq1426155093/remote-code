package auth

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestAuthenticate(t *testing.T) {
	tests := []struct {
		name   string
		values []string
		code   codes.Code
	}{
		{name: "valid", values: []string{"Bearer secret"}, code: codes.OK},
		{name: "missing", code: codes.Unauthenticated},
		{name: "wrong scheme", values: []string{"Basic secret"}, code: codes.Unauthenticated},
		{name: "wrong token", values: []string{"Bearer other"}, code: codes.Unauthenticated},
		{name: "duplicate", values: []string{"Bearer secret", "Bearer secret"}, code: codes.Unauthenticated},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx := metadata.NewIncomingContext(context.Background(), metadata.MD{"authorization": test.values})
			if got := status.Code(authenticate(ctx, "secret")); got != test.code {
				t.Fatalf("authenticate() code = %s, want %s", got, test.code)
			}
		})
	}
}

func TestReadTokenFile(t *testing.T) {
	filename := filepath.Join(t.TempDir(), "token")
	if err := os.WriteFile(filename, []byte("  secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if token, err := ReadTokenFile(filename); err != nil || token != "secret" {
		t.Fatalf("ReadTokenFile() = %q, %v", token, err)
	}
	if err := os.WriteFile(filename, []byte(" \n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := ReadTokenFile(filename); err == nil {
		t.Fatal("ReadTokenFile() accepted an empty token")
	}
}
