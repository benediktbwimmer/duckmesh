package api

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

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/bus"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
)

func TestIngestEndpointAcceptsAndCountsDuplicates(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	repo := &fakeCatalogRepo{table: catalog.TableDef{TableID: 42, TenantID: "tenant-1", TableName: "events"}}
	busStub := &fakeIngestBus{publishResults: []bus.PublishResult{
		{EventID: "100", VisibilityToken: 100, Inserted: true},
		{EventID: "101", VisibilityToken: 101, Inserted: false},
	}}

	h := NewHandler(cfg, Dependencies{CatalogRepo: repo, IngestBus: busStub})

	body := `{"records":[{"idempotency_key":"k1","op":"insert","payload":{"id":1}},{"idempotency_key":"k2","op":"insert","payload":{"id":2}}]}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/events", strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("response decode error: %v", err)
	}
	if response["accepted_count"] != float64(1) {
		t.Fatalf("accepted_count = %v", response["accepted_count"])
	}
	if response["duplicate_count"] != float64(1) {
		t.Fatalf("duplicate_count = %v", response["duplicate_count"])
	}
	if response["status"] != "partial_duplicate" {
		t.Fatalf("status = %v", response["status"])
	}
	if response["max_visibility_token"] != float64(101) {
		t.Fatalf("max_visibility_token = %v", response["max_visibility_token"])
	}

	if len(busStub.publishedEvents) != 2 {
		t.Fatalf("published events = %d, want 2", len(busStub.publishedEvents))
	}
	if busStub.publishedEvents[0].TenantID != "tenant-1" {
		t.Fatalf("tenant propagated = %q", busStub.publishedEvents[0].TenantID)
	}
}

func TestIngestEndpointRequiresIngestWriterRole(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{
		"DUCKMESH_AUTH_REQUIRED": "true",
	}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	validator, err := auth.NewStaticAPIKeyValidator("key:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(slog.New(slog.NewJSONHandler(io.Discard, nil)), validator),
		CatalogRepo:    &fakeCatalogRepo{table: catalog.TableDef{TableID: 1, TenantID: "t1", TableName: "events"}},
		IngestBus:      &fakeIngestBus{publishResults: []bus.PublishResult{{EventID: "1", VisibilityToken: 1, Inserted: true}}},
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/events", strings.NewReader(`{"records":[{"idempotency_key":"k1","op":"insert","payload":{"a":1}}]}`))
	req.Header.Set("X-API-Key", "key")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusForbidden)
	}
}

func TestIngestEndpointHandlesTableNotFound(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		CatalogRepo: &fakeCatalogRepo{err: catalog.ErrNotFound},
		IngestBus:   &fakeIngestBus{},
	})
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/missing", strings.NewReader(`{"records":[{"idempotency_key":"k1","op":"insert","payload":{"a":1}}]}`))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

type fakeCatalogRepo struct {
	table      catalog.TableDef
	err        error
	latest     catalog.Snapshot
	latestErr  error
	listTables []catalog.TableDef
}

func (f *fakeCatalogRepo) GetTableByName(_ context.Context, _, _ string) (catalog.TableDef, error) {
	if f.err != nil {
		return catalog.TableDef{}, f.err
	}
	return f.table, nil
}

func (f *fakeCatalogRepo) ListTables(context.Context, string) ([]catalog.TableDef, error) {
	if f.err != nil {
		return nil, f.err
	}
	if f.listTables != nil {
		return append([]catalog.TableDef(nil), f.listTables...), nil
	}
	return []catalog.TableDef{f.table}, nil
}

func (f *fakeCatalogRepo) GetLatestSnapshot(context.Context, string) (catalog.Snapshot, error) {
	if f.latestErr != nil {
		return catalog.Snapshot{}, f.latestErr
	}
	if f.latest.SnapshotID == 0 {
		return catalog.Snapshot{}, catalog.ErrNotFound
	}
	return f.latest, nil
}

func (f *fakeCatalogRepo) GetSnapshotByID(context.Context, string, int64) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (f *fakeCatalogRepo) GetSnapshotByTime(context.Context, string, time.Time) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (f *fakeCatalogRepo) ListSnapshotFiles(context.Context, string, int64) ([]catalog.SnapshotFileEntry, error) {
	return nil, catalog.ErrNotFound
}

func TestIngestEndpointWaitForVisibilityReturnsVisible(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	repo := &fakeCatalogRepo{
		table: catalog.TableDef{TableID: 42, TenantID: "tenant-1", TableName: "events"},
		latest: catalog.Snapshot{
			SnapshotID:         700,
			TenantID:           "tenant-1",
			MaxVisibilityToken: 101,
		},
	}
	busStub := &fakeIngestBus{publishResults: []bus.PublishResult{
		{EventID: "101", VisibilityToken: 101, Inserted: true},
	}}
	h := NewHandler(cfg, Dependencies{CatalogRepo: repo, IngestBus: busStub})

	body := `{"records":[{"idempotency_key":"k1","op":"insert","payload":{"id":1}}],"wait_for_visibility":true,"visibility_timeout_ms":200}`
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/events", strings.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()

	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("response decode error: %v", err)
	}
	if response["status"] != "visible" {
		t.Fatalf("status = %v, want visible", response["status"])
	}
	if response["visible_snapshot_id"] != float64(700) {
		t.Fatalf("visible_snapshot_id = %v", response["visible_snapshot_id"])
	}
}

type fakeIngestBus struct {
	publishResults  []bus.PublishResult
	publishedEvents []bus.Envelope
}

func (f *fakeIngestBus) Publish(_ context.Context, events []bus.Envelope) ([]bus.PublishResult, error) {
	f.publishedEvents = append(f.publishedEvents, events...)
	if len(f.publishResults) == 0 {
		results := make([]bus.PublishResult, 0, len(events))
		for _, event := range events {
			results = append(results, bus.PublishResult{EventID: event.IdempotencyKey, VisibilityToken: 1, Inserted: true})
		}
		return results, nil
	}
	return f.publishResults, nil
}

func (f *fakeIngestBus) ClaimBatch(context.Context, string, int, int) (bus.Batch, error) {
	return bus.Batch{}, nil
}

func (f *fakeIngestBus) Ack(context.Context, string, []string) error {
	return nil
}

func (f *fakeIngestBus) Nack(context.Context, string, []string, string) error {
	return nil
}

func (f *fakeIngestBus) ExtendLease(context.Context, string, int) error {
	return nil
}

func (f *fakeIngestBus) RequeueExpired(context.Context) (int, error) {
	return 0, nil
}
