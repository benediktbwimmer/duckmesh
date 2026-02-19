package api

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/query"
)

func TestQueryEndpointReturnsResults(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	repo := &fakeQueryCatalogRepo{
		snapshot: catalog.Snapshot{SnapshotID: 7, TenantID: "tenant-1", MaxVisibilityToken: 20, CreatedAt: time.Now().UTC()},
		files:    []catalog.SnapshotFileEntry{{TableName: "events", Path: "k1", FileSizeBytes: 10}},
	}
	engine := &fakeQueryEngine{result: query.Result{Columns: []string{"c"}, Rows: [][]any{{int64(2)}}, Duration: 20 * time.Millisecond, ScannedFiles: 1, ScannedBytes: 10}}
	service := NewHandler(cfg, Dependencies{CatalogRepo: repo, QueryEngine: engine})

	req := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{"sql":"SELECT 2 AS c"}`))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()

	service.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("json decode failed: %v", err)
	}
	if body["snapshot_id"] != float64(7) {
		t.Fatalf("snapshot_id = %v", body["snapshot_id"])
	}
	if len(engine.requests) != 1 {
		t.Fatalf("engine request count = %d", len(engine.requests))
	}
}

func TestQueryEndpointConsistencyTimeout(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	repo := &fakeQueryCatalogRepo{errLatest: catalog.ErrNotFound}
	service := NewHandler(cfg, Dependencies{CatalogRepo: repo, QueryEngine: &fakeQueryEngine{}})

	req := httptest.NewRequest(http.MethodPost, "/v1/query", strings.NewReader(`{"sql":"SELECT 1","min_visibility_token":5,"consistency_timeout_ms":50}`))
	req.Header.Set("X-Tenant-ID", "tenant-1")
	rr := httptest.NewRecorder()

	service.ServeHTTP(rr, req)
	if rr.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusGatewayTimeout)
	}
}

type fakeQueryCatalogRepo struct {
	table     catalog.TableDef
	snapshot  catalog.Snapshot
	files     []catalog.SnapshotFileEntry
	errLatest error
	errByID   error
	errByTime error
	errByName error
	errFiles  error
}

func (f *fakeQueryCatalogRepo) GetTableByName(context.Context, string, string) (catalog.TableDef, error) {
	if f.errByName != nil {
		return catalog.TableDef{}, f.errByName
	}
	return f.table, nil
}

func (f *fakeQueryCatalogRepo) ListTables(context.Context, string) ([]catalog.TableDef, error) {
	if f.errByName != nil {
		return nil, f.errByName
	}
	if f.table.TableName == "" {
		return nil, nil
	}
	return []catalog.TableDef{f.table}, nil
}

func (f *fakeQueryCatalogRepo) GetLatestSnapshot(context.Context, string) (catalog.Snapshot, error) {
	if f.errLatest != nil {
		return catalog.Snapshot{}, f.errLatest
	}
	return f.snapshot, nil
}

func (f *fakeQueryCatalogRepo) GetSnapshotByID(context.Context, string, int64) (catalog.Snapshot, error) {
	if f.errByID != nil {
		return catalog.Snapshot{}, f.errByID
	}
	return f.snapshot, nil
}

func (f *fakeQueryCatalogRepo) GetSnapshotByTime(context.Context, string, time.Time) (catalog.Snapshot, error) {
	if f.errByTime != nil {
		return catalog.Snapshot{}, f.errByTime
	}
	return f.snapshot, nil
}

func (f *fakeQueryCatalogRepo) ListSnapshotFiles(context.Context, string, int64) ([]catalog.SnapshotFileEntry, error) {
	if f.errFiles != nil {
		return nil, f.errFiles
	}
	return f.files, nil
}

type fakeQueryEngine struct {
	requests []query.Request
	result   query.Result
	err      error
}

func (f *fakeQueryEngine) Execute(_ context.Context, request query.Request) (query.Result, error) {
	f.requests = append(f.requests, request)
	if f.err != nil {
		return query.Result{}, f.err
	}
	return f.result, nil
}
