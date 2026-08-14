package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ReadTokenFile reads a token without exposing its value in an error.
func ReadTokenFile(filename string) (string, error) {
	contents, err := os.ReadFile(filename)
	if err != nil {
		return "", fmt.Errorf("read token file: %w", err)
	}
	token := strings.TrimSpace(string(contents))
	if token == "" {
		return "", errors.New("token file is empty")
	}
	return token, nil
}

// UnaryServerInterceptor authenticates unary RPC metadata.
func UnaryServerInterceptor(token string) grpc.UnaryServerInterceptor {
	return func(ctx context.Context, request any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		if err := authenticate(ctx, token); err != nil {
			return nil, err
		}
		return handler(ctx, request)
	}
}

// StreamServerInterceptor authenticates streaming RPC metadata.
func StreamServerInterceptor(token string) grpc.StreamServerInterceptor {
	return func(server any, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authenticate(stream.Context(), token); err != nil {
			return err
		}
		return handler(server, stream)
	}
}

func authenticate(ctx context.Context, expected string) error {
	values := metadata.ValueFromIncomingContext(ctx, "authorization")
	if len(values) != 1 {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	const prefix = "Bearer "
	if !strings.HasPrefix(values[0], prefix) {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	actual := strings.TrimPrefix(values[0], prefix)
	if len(actual) != len(expected) || subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) != 1 {
		return status.Error(codes.Unauthenticated, "valid bearer token required")
	}
	return nil
}
