package observability

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestTraceMiddlewarePreservesIncomingTraceID(t *testing.T) {
	h := TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := TraceIDFromContext(r.Context()); got != "trace-1" {
			t.Fatalf("TraceIDFromContext() = %q", got)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	req.Header.Set(traceHeader, "trace-1")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)

	if got := rr.Header().Get(traceHeader); got != "trace-1" {
		t.Fatalf("trace header = %q", got)
	}
}

func TestTraceMiddlewareGeneratesTraceID(t *testing.T) {
	h := TraceMiddleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if TraceIDFromContext(r.Context()) == "" {
			t.Fatal("expected generated trace id")
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/health", nil))

	if rr.Header().Get(traceHeader) == "" {
		t.Fatal("expected X-Trace-ID header")
	}
}

func TestTraceIDContextHelpers(t *testing.T) {
	ctx := ContextWithTraceID(context.Background(), "abc123")
	if got := TraceIDFromContext(ctx); got != "abc123" {
		t.Fatalf("TraceIDFromContext() = %q", got)
	}
}

func TestLoggingMiddlewareDoesNotPanic(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(io.Discard, nil))
	h := LoggingMiddleware(logger)(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
	}))
	h.ServeHTTP(httptest.NewRecorder(), httptest.NewRequest(http.MethodGet, "/x", nil))
}
