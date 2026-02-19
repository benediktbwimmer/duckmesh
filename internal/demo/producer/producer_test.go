package producer

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestEnsureTableCreatesWhenMissing(t *testing.T) {
	var (
		getCalls  int
		postCalls int
	)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/tables/events":
			getCalls++
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error_code":"NOT_FOUND"}`))
		case r.Method == http.MethodPost && r.URL.Path == "/v1/tables":
			postCalls++
			if tenant := r.Header.Get("X-Tenant-ID"); tenant != "tenant-dev" {
				t.Fatalf("X-Tenant-ID = %q, want tenant-dev", tenant)
			}
			var req createTableRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Fatalf("decode create table request: %v", err)
			}
			if req.TableName != "events" {
				t.Fatalf("table_name = %q", req.TableName)
			}
			w.WriteHeader(http.StatusCreated)
			_, _ = w.Write([]byte(`{"table_name":"events"}`))
		default:
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIBaseURL = server.URL
	cfg.CreateTable = true

	svc, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), server.Client())
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}

	if err := svc.ensureTable(context.Background()); err != nil {
		t.Fatalf("ensureTable() error = %v", err)
	}
	if getCalls != 1 {
		t.Fatalf("getCalls = %d, want 1", getCalls)
	}
	if postCalls != 1 {
		t.Fatalf("postCalls = %d, want 1", postCalls)
	}
}

func TestProduceOncePostsBatch(t *testing.T) {
	var gotWaitForVisibility bool

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/v1/ingest/events" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if apiKey := r.Header.Get("X-API-Key"); apiKey != "k1" {
			t.Fatalf("X-API-Key = %q, want k1", apiKey)
		}
		var req ingestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode ingest request: %v", err)
		}
		if len(req.Records) != 3 {
			t.Fatalf("record count = %d, want 3", len(req.Records))
		}
		gotWaitForVisibility = req.WaitForVisibility
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"accepted_count":3,"duplicate_count":0,"max_visibility_token":7}`))
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIBaseURL = server.URL
	cfg.TableName = "events"
	cfg.APIKey = "k1"
	cfg.BatchSize = 3
	cfg.WaitForVisibility = true
	cfg.VisibilityTimeoutMS = 2200
	cfg.Seed = 123

	svc, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), server.Client())
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	svc.generator.now = func() time.Time { return time.Unix(1, 0).UTC() }

	if err := svc.produceOnce(context.Background()); err != nil {
		t.Fatalf("produceOnce() error = %v", err)
	}
	if !gotWaitForVisibility {
		t.Fatal("wait_for_visibility not set on ingest request")
	}
}

func TestProduceOnceReportsHTTPFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"error_code":"INTERNAL_ERROR","message":"failed"}`))
	}))
	defer server.Close()

	cfg := DefaultConfig()
	cfg.APIBaseURL = server.URL

	svc, err := NewService(cfg, slog.New(slog.NewTextHandler(io.Discard, nil)), server.Client())
	if err != nil {
		t.Fatalf("NewService() error = %v", err)
	}
	err = svc.produceOnce(context.Background())
	if err == nil {
		t.Fatal("produceOnce() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "status 500") {
		t.Fatalf("error = %v, want status in message", err)
	}
}
