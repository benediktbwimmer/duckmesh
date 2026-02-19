package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/duckmesh/duckmesh/internal/auth"
	"github.com/duckmesh/duckmesh/internal/catalog"
	"github.com/duckmesh/duckmesh/internal/config"
)

func TestTableLifecycleEndpoints(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	repo := newInMemoryTableCatalog()
	h := NewHandler(cfg, Dependencies{CatalogRepo: repo})

	createBody := `{"table_name":"events","primary_key_cols":["id"],"partition_spec":{"date":"day"},"schema_json":{"id":"bigint"}}`
	createReq := httptest.NewRequest(http.MethodPost, "/v1/tables", strings.NewReader(createBody))
	createReq.Header.Set("X-Tenant-ID", "tenant-1")
	createRR := httptest.NewRecorder()
	h.ServeHTTP(createRR, createReq)
	if createRR.Code != http.StatusCreated {
		t.Fatalf("create status = %d, body=%s", createRR.Code, createRR.Body.String())
	}

	listReq := httptest.NewRequest(http.MethodGet, "/v1/tables", nil)
	listReq.Header.Set("X-Tenant-ID", "tenant-1")
	listRR := httptest.NewRecorder()
	h.ServeHTTP(listRR, listReq)
	if listRR.Code != http.StatusOK {
		t.Fatalf("list status = %d, body=%s", listRR.Code, listRR.Body.String())
	}

	getReq := httptest.NewRequest(http.MethodGet, "/v1/tables/events", nil)
	getReq.Header.Set("X-Tenant-ID", "tenant-1")
	getRR := httptest.NewRecorder()
	h.ServeHTTP(getRR, getReq)
	if getRR.Code != http.StatusOK {
		t.Fatalf("get status = %d, body=%s", getRR.Code, getRR.Body.String())
	}

	patchReq := httptest.NewRequest(http.MethodPatch, "/v1/tables/events", bytes.NewBufferString(`{"schema_json":{"id":"bigint","value":"double"}}`))
	patchReq.Header.Set("X-Tenant-ID", "tenant-1")
	patchRR := httptest.NewRecorder()
	h.ServeHTTP(patchRR, patchReq)
	if patchRR.Code != http.StatusOK {
		t.Fatalf("patch status = %d, body=%s", patchRR.Code, patchRR.Body.String())
	}

	var patched map[string]any
	if err := json.Unmarshal(patchRR.Body.Bytes(), &patched); err != nil {
		t.Fatalf("decode patch response error: %v", err)
	}
	if patched["schema_version"] != float64(2) {
		t.Fatalf("schema_version = %v, want 2", patched["schema_version"])
	}

	deleteReq := httptest.NewRequest(http.MethodDelete, "/v1/tables/events", nil)
	deleteReq.Header.Set("X-Tenant-ID", "tenant-1")
	deleteRR := httptest.NewRecorder()
	h.ServeHTTP(deleteRR, deleteReq)
	if deleteRR.Code != http.StatusOK {
		t.Fatalf("delete status = %d, body=%s", deleteRR.Code, deleteRR.Body.String())
	}
}

func TestTableCreateRequiresTableAdminRole(t *testing.T) {
	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{
		"DUCKMESH_AUTH_REQUIRED": "true",
	}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	validator, err := auth.NewStaticAPIKeyValidator("reader:t1:query_reader")
	if err != nil {
		t.Fatalf("validator setup failed: %v", err)
	}

	repo := newInMemoryTableCatalog()
	h := NewHandler(cfg, Dependencies{
		AuthMiddleware: auth.Middleware(nil, validator),
		CatalogRepo:    repo,
	})

	req := httptest.NewRequest(http.MethodPost, "/v1/tables", strings.NewReader(`{"table_name":"events"}`))
	req.Header.Set("X-API-Key", "reader")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Fatalf("status = %d, body=%s", rr.Code, rr.Body.String())
	}
}

type inMemoryTableCatalog struct {
	nextID int64
	tables map[string]catalog.TableDef
}

func newInMemoryTableCatalog() *inMemoryTableCatalog {
	return &inMemoryTableCatalog{
		nextID: 1,
		tables: map[string]catalog.TableDef{},
	}
}

func tableKey(tenantID, tableName string) string {
	return tenantID + "/" + tableName
}

func (r *inMemoryTableCatalog) CreateTable(_ context.Context, in catalog.CreateTableInput) (catalog.TableDef, error) {
	key := tableKey(in.TenantID, in.TableName)
	now := time.Now().UTC()
	table := catalog.TableDef{
		TableID:        r.nextID,
		TenantID:       in.TenantID,
		TableName:      in.TableName,
		PrimaryKeyCols: in.PrimaryKeyCols,
		PartitionSpec:  in.PartitionSpec,
		SchemaVersion:  in.SchemaVersion,
		CreatedAt:      now,
	}
	if table.SchemaVersion <= 0 {
		table.SchemaVersion = 1
	}
	r.nextID++
	r.tables[key] = table
	return table, nil
}

func (r *inMemoryTableCatalog) GetTableByName(_ context.Context, tenantID, tableName string) (catalog.TableDef, error) {
	table, ok := r.tables[tableKey(tenantID, tableName)]
	if !ok {
		return catalog.TableDef{}, catalog.ErrNotFound
	}
	return table, nil
}

func (r *inMemoryTableCatalog) ListTables(_ context.Context, tenantID string) ([]catalog.TableDef, error) {
	out := make([]catalog.TableDef, 0)
	for _, table := range r.tables {
		if table.TenantID == tenantID {
			out = append(out, table)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].TableName < out[j].TableName })
	return out, nil
}

func (r *inMemoryTableCatalog) DeleteTableByName(_ context.Context, tenantID, tableName string) (bool, error) {
	key := tableKey(tenantID, tableName)
	if _, ok := r.tables[key]; !ok {
		return false, nil
	}
	delete(r.tables, key)
	return true, nil
}

func (r *inMemoryTableCatalog) SetTableSchemaVersion(_ context.Context, tableID int64, schemaVersion int) error {
	for key, table := range r.tables {
		if table.TableID == tableID {
			table.SchemaVersion = schemaVersion
			r.tables[key] = table
			return nil
		}
	}
	return catalog.ErrNotFound
}

func (r *inMemoryTableCatalog) UpsertTableSchemaVersion(_ context.Context, in catalog.UpsertTableSchemaVersionInput) (catalog.TableSchemaVersion, error) {
	return catalog.TableSchemaVersion{
		TableID:           in.TableID,
		SchemaVersion:     in.SchemaVersion,
		SchemaJSON:        in.SchemaJSON,
		CompatibilityMode: in.CompatibilityMode,
		CreatedAt:         time.Now().UTC(),
	}, nil
}

func (r *inMemoryTableCatalog) GetLatestSnapshot(context.Context, string) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (r *inMemoryTableCatalog) GetSnapshotByID(context.Context, string, int64) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (r *inMemoryTableCatalog) GetSnapshotByTime(context.Context, string, time.Time) (catalog.Snapshot, error) {
	return catalog.Snapshot{}, catalog.ErrNotFound
}

func (r *inMemoryTableCatalog) ListSnapshotFiles(context.Context, string, int64) ([]catalog.SnapshotFileEntry, error) {
	return nil, catalog.ErrNotFound
}
