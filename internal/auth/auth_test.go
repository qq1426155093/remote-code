package auth

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

func TestBearerHTTPMiddleware(t *testing.T) {
	handler := BearerHTTPMiddleware("secret", http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if principal, ok := PrincipalFromContext(request.Context()); !ok || principal.ID == "" {
			t.Error("authenticated principal missing")
		}
		response.WriteHeader(http.StatusNoContent)
	}))
	for _, test := range []struct {
		name, authorization string
		want                int
	}{
		{name: "valid", authorization: "Bearer secret", want: http.StatusNoContent},
		{name: "missing", want: http.StatusUnauthorized},
		{name: "wrong scheme", authorization: "bearer secret", want: http.StatusUnauthorized},
		{name: "wrong token", authorization: "Bearer other", want: http.StatusUnauthorized},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := httptest.NewRequest(http.MethodPost, "http://example.test/mcp", nil)
			if test.authorization != "" {
				request.Header.Set("Authorization", test.authorization)
			}
			response := httptest.NewRecorder()
			handler.ServeHTTP(response, request)
			if response.Code != test.want {
				t.Fatalf("status = %d, want %d", response.Code, test.want)
			}
		})
	}
}

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
