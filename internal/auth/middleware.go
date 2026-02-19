package auth

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"

	"github.com/duckmesh/duckmesh/internal/observability"
)

type contextKey string

const identityKey contextKey = "auth_identity"

func WithIdentity(ctx context.Context, identity Identity) context.Context {
	return context.WithValue(ctx, identityKey, identity)
}

func IdentityFromContext(ctx context.Context) (Identity, bool) {
	identity, ok := ctx.Value(identityKey).(Identity)
	return identity, ok
}

func Middleware(logger *slog.Logger, validator APIKeyValidator) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			apiKey := extractAPIKey(r)
			if apiKey == "" {
				writeUnauthorized(w, r, "missing API key")
				return
			}

			identity, ok := validator.Validate(r.Context(), apiKey)
			if !ok {
				if logger != nil {
					logger.WarnContext(r.Context(), "authentication failed",
						slog.String("trace_id", observability.TraceIDFromContext(r.Context())),
						slog.String("path", r.URL.Path),
					)
				}
				writeUnauthorized(w, r, "invalid API key")
				return
			}

			next.ServeHTTP(w, r.WithContext(WithIdentity(r.Context(), identity)))
		})
	}
}

func extractAPIKey(r *http.Request) string {
	if key := strings.TrimSpace(r.Header.Get("X-API-Key")); key != "" {
		return key
	}
	authorization := strings.TrimSpace(r.Header.Get("Authorization"))
	if authorization == "" {
		return ""
	}
	const bearerPrefix = "Bearer "
	if strings.HasPrefix(authorization, bearerPrefix) {
		return strings.TrimSpace(strings.TrimPrefix(authorization, bearerPrefix))
	}
	return ""
}

func writeUnauthorized(w http.ResponseWriter, r *http.Request, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusUnauthorized)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"error_code": "UNAUTHORIZED",
		"message":    message,
		"retryable":  false,
		"trace_id":   observability.TraceIDFromContext(r.Context()),
	})
}
