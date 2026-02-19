package auth

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestStaticAPIKeyValidatorParsing(t *testing.T) {
	validator, err := NewStaticAPIKeyValidator("k1:t1:ingest_writer|query_reader")
	if err != nil {
		t.Fatalf("NewStaticAPIKeyValidator() error = %v", err)
	}
	identity, ok := validator.Validate(context.Background(), "k1")
	if !ok {
		t.Fatal("expected key to be valid")
	}
	if identity.TenantID != "t1" {
		t.Fatalf("TenantID = %q", identity.TenantID)
	}
	if !identity.HasRole("ingest_writer") {
		t.Fatal("expected ingest_writer role")
	}
}

func TestStaticAPIKeyValidatorRejectsBadSpec(t *testing.T) {
	_, err := NewStaticAPIKeyValidator("invalid")
	if err == nil {
		t.Fatal("expected parse error")
	}
}

func TestMiddlewareRequiresKey(t *testing.T) {
	validator, err := NewStaticAPIKeyValidator("k1:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup: %v", err)
	}

	mw := Middleware(slog.New(slog.NewJSONHandler(io.Discard, nil)), validator)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))

	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/tables", nil))

	if rr.Code != http.StatusUnauthorized {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
	}
}

func TestMiddlewareInjectsIdentity(t *testing.T) {
	validator, err := NewStaticAPIKeyValidator("k1:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup: %v", err)
	}

	mw := Middleware(nil, validator)
	handler := mw(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		identity, ok := IdentityFromContext(r.Context())
		if !ok {
			t.Fatal("expected identity in context")
		}
		if identity.TenantID != "t1" {
			t.Fatalf("TenantID = %q", identity.TenantID)
		}
		w.WriteHeader(http.StatusNoContent)
	}))

	req := httptest.NewRequest(http.MethodGet, "/v1/tables", nil)
	req.Header.Set("X-API-Key", "k1")
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d", rr.Code)
	}
}
