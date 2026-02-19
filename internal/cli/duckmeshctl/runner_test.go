package duckmeshctl

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRunLagCommand(t *testing.T) {
	var gotMethod, gotPath, gotAPIKey, gotTenant string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		gotAPIKey = r.Header.Get("X-API-Key")
		gotTenant = r.Header.Get("X-Tenant-ID")
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"pending_events":2}`))
	}))
	defer srv.Close()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{
		"-base-url", srv.URL,
		"-api-key", "k1",
		"-tenant-id", "tenant-a",
		"lag",
	}, Options{
		Stdout:  &stdout,
		Stderr:  &stderr,
		Timeout: 2 * time.Second,
	})
	if code != 0 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
	if gotMethod != http.MethodGet || gotPath != "/v1/lag" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
	if gotAPIKey != "k1" || gotTenant != "tenant-a" {
		t.Fatalf("headers api_key=%q tenant=%q", gotAPIKey, gotTenant)
	}
	if stdout.Len() == 0 {
		t.Fatal("expected command output")
	}
}

func TestRunCompactionCommand(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"completed"}`))
	}))
	defer srv.Close()

	code := Run(context.Background(), []string{"-base-url", srv.URL, "compaction-run"}, Options{})
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/compaction/run" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
}

func TestRunIntegrityCommand(t *testing.T) {
	var gotMethod, gotPath string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotPath = r.URL.Path
		_, _ = w.Write([]byte(`{"status":"completed"}`))
	}))
	defer srv.Close()

	code := Run(context.Background(), []string{"-base-url", srv.URL, "integrity-run"}, Options{})
	if code != 0 {
		t.Fatalf("exit code = %d", code)
	}
	if gotMethod != http.MethodPost || gotPath != "/v1/integrity/run" {
		t.Fatalf("request = %s %s", gotMethod, gotPath)
	}
}

func TestRunReturnsErrorOnHTTPFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error_code":"FORBIDDEN"}`))
	}))
	defer srv.Close()

	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"-base-url", srv.URL, "lag"}, Options{Stderr: &stderr})
	if code != 1 {
		t.Fatalf("exit code = %d, stderr=%s", code, stderr.String())
	}
}

func TestRunUnknownCommand(t *testing.T) {
	var stderr bytes.Buffer
	code := Run(context.Background(), []string{"unknown"}, Options{Stderr: &stderr})
	if code != 2 {
		t.Fatalf("exit code = %d", code)
	}
	if stderr.Len() == 0 {
		t.Fatal("expected usage output")
	}
}
