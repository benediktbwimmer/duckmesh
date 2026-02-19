package observability

import (
	"context"
	"io"
	"log/slog"

	"github.com/duckmesh/duckmesh/internal/config"
)

type ctxKey string

const traceIDKey ctxKey = "trace_id"

func NewLogger(cfg config.Config, writer io.Writer) *slog.Logger {
	if writer == nil {
		writer = io.Discard
	}
	var handler slog.Handler
	if cfg.Observability.LogJSON {
		handler = slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: cfg.Observability.LogLevel})
	} else {
		handler = slog.NewTextHandler(writer, &slog.HandlerOptions{Level: cfg.Observability.LogLevel})
	}
	return slog.New(handler).With(
		slog.String("service", cfg.Service.Name),
		slog.String("profile", string(cfg.Profile)),
	)
}

func ContextWithTraceID(ctx context.Context, traceID string) context.Context {
	return context.WithValue(ctx, traceIDKey, traceID)
}

func TraceIDFromContext(ctx context.Context) string {
	value, ok := ctx.Value(traceIDKey).(string)
	if !ok {
		return ""
	}
	return value
}
