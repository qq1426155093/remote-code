package auth

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"net/http"
	"strings"
)

// Principal is a stable, non-secret identity derived from the configured token.
type Principal struct{ ID string }

type principalContextKey struct{}

// BearerHTTPMiddleware requires exactly one bearer Authorization header.
func BearerHTTPMiddleware(expected string, next http.Handler) http.Handler {
	digest := sha256.Sum256([]byte(expected))
	principal := Principal{ID: "token:" + strings.ToLower(stringHex(digest[:8]))}
	return http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		values := request.Header.Values("Authorization")
		if len(values) != 1 || !validBearer(values[0], expected) {
			response.Header().Set("WWW-Authenticate", "Bearer")
			http.Error(response, "valid bearer token required", http.StatusUnauthorized)
			return
		}
		next.ServeHTTP(response, request.WithContext(context.WithValue(request.Context(), principalContextKey{}, principal)))
	})
}

// PrincipalFromContext returns the authenticated HTTP principal.
func PrincipalFromContext(ctx context.Context) (Principal, bool) {
	principal, ok := ctx.Value(principalContextKey{}).(Principal)
	return principal, ok
}

func validBearer(value, expected string) bool {
	const prefix = "Bearer "
	if !strings.HasPrefix(value, prefix) {
		return false
	}
	actual := strings.TrimPrefix(value, prefix)
	return len(actual) == len(expected) && subtle.ConstantTimeCompare([]byte(actual), []byte(expected)) == 1
}

func stringHex(value []byte) string {
	const digits = "0123456789abcdef"
	result := make([]byte, len(value)*2)
	for index, item := range value {
		result[index*2] = digits[item>>4]
		result[index*2+1] = digits[item&15]
	}
	return string(result)
}
