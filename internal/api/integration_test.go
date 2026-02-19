//go:build integration

package api

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/duckmesh/duckmesh/internal/bus"
	buspostgres "github.com/duckmesh/duckmesh/internal/bus/postgres"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/config"
	"github.com/duckmesh/duckmesh/internal/coordinator"
	"github.com/duckmesh/duckmesh/internal/migrations"
	duckdbengine "github.com/duckmesh/duckmesh/internal/query/duckdb"
	s3store "github.com/duckmesh/duckmesh/internal/storage/s3"
)

func TestIngestEndpointIdempotencyWithPostgresBus(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN)
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}
	seedTenantAndTable(t, db, "tenant-api", "events")

	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}

	h := NewHandler(cfg, Dependencies{
		CatalogRepo: catalogpostgres.NewRepository(db),
		IngestBus:   buspostgres.NewIngestBus(db),
	})

	first := postIngest(t, h, "tenant-api", "events", []map[string]any{
		{"idempotency_key": "idem-1", "op": "insert", "payload": map[string]any{"id": 1}},
		{"idempotency_key": "idem-2", "op": "insert", "payload": map[string]any{"id": 2}},
	})
	if first["accepted_count"].(float64) != 2 {
		t.Fatalf("first accepted_count = %v", first["accepted_count"])
	}
	if first["duplicate_count"].(float64) != 0 {
		t.Fatalf("first duplicate_count = %v", first["duplicate_count"])
	}
	if first["status"] != "accepted" {
		t.Fatalf("first status = %v", first["status"])
	}

	second := postIngest(t, h, "tenant-api", "events", []map[string]any{
		{"idempotency_key": "idem-1", "op": "insert", "payload": map[string]any{"id": 1}},
	})
	if second["accepted_count"].(float64) != 0 {
		t.Fatalf("second accepted_count = %v", second["accepted_count"])
	}
	if second["duplicate_count"].(float64) != 1 {
		t.Fatalf("second duplicate_count = %v", second["duplicate_count"])
	}
	if second["status"] != "partial_duplicate" {
		t.Fatalf("second status = %v", second["status"])
	}
}

func TestQueryVisibilityBarrierWaitsUntilCoordinatorPublishes(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN)
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}
	seedTenantAndTable(t, db, "tenant-api", "events")

	store, err := s3store.New(ctx, s3store.Config{
		Endpoint:         envOr("DUCKMESH_TEST_S3_ENDPOINT", "localhost:9000"),
		Region:           envOr("DUCKMESH_TEST_S3_REGION", "us-east-1"),
		Bucket:           envOr("DUCKMESH_TEST_S3_BUCKET", "duckmesh-it"),
		AccessKeyID:      envOr("DUCKMESH_TEST_S3_ACCESS_KEY", "minio"),
		SecretAccessKey:  envOr("DUCKMESH_TEST_S3_SECRET_KEY", "miniostorage"),
		UseSSL:           false,
		Prefix:           "api-query-tests",
		AutoCreateBucket: true,
	})
	if err != nil {
		t.Fatalf("s3store.New() error = %v", err)
	}

	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	catalogRepo := catalogpostgres.NewRepository(db)
	ingestBus := buspostgres.NewIngestBus(db)
	queryEngine := duckdbengine.NewEngine(store)
	h := NewHandler(cfg, Dependencies{
		CatalogRepo: catalogRepo,
		IngestBus:   ingestBus,
		QueryEngine: queryEngine,
	})

	ingest := postIngest(t, h, "tenant-api", "events", []map[string]any{
		{"idempotency_key": "idem-1", "op": "insert", "payload": map[string]any{"id": int64(1), "value": "a"}},
	})
	token := int64(ingest["max_visibility_token"].(float64))

	timeoutResp := postQuery(t, h, "tenant-api", map[string]any{
		"sql":                    "SELECT COUNT(*) AS c FROM events",
		"min_visibility_token":   token,
		"consistency_timeout_ms": 100,
	}, http.StatusGatewayTimeout)
	if timeoutResp["error_code"] != "CONSISTENCY_TIMEOUT" {
		t.Fatalf("error_code = %v", timeoutResp["error_code"])
	}

	coord := &coordinator.Service{
		Bus:         ingestBus,
		Publisher:   catalogRepo,
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   "api-test-coordinator",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "api-test-coordinator",
		},
	}

	done := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		done <- coord.ProcessOnce(context.Background())
	}()

	successResp := postQuery(t, h, "tenant-api", map[string]any{
		"sql":                    "SELECT COUNT(*) AS c FROM events",
		"min_visibility_token":   token,
		"consistency_timeout_ms": 3000,
	}, http.StatusOK)
	if err := <-done; err != nil {
		t.Fatalf("coordinator ProcessOnce() error = %v", err)
	}

	rows, ok := successResp["rows"].([]any)
	if !ok || len(rows) != 1 {
		t.Fatalf("rows = %#v", successResp["rows"])
	}
	firstRow, ok := rows[0].([]any)
	if !ok || len(firstRow) != 1 {
		t.Fatalf("first row = %#v", rows[0])
	}
	if firstRow[0] != float64(1) && firstRow[0] != int64(1) {
		t.Fatalf("count value = %#v", firstRow[0])
	}
}

func TestLagEndpointReturnsPendingCounts(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN)
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}
	tenantID := "tenant-lag"
	tableName := "events"
	seedTenantAndTable(t, db, tenantID, tableName)
	tableID := fetchTableID(t, db, tenantID, tableName)

	ingestBus := buspostgres.NewIngestBus(db)
	if _, err := ingestBus.Publish(ctx, []bus.Envelope{
		{TenantID: tenantID, TableID: fmt.Sprintf("%d", tableID), IdempotencyKey: "lag-1", Op: "insert", PayloadJSON: []byte(`{"x":1}`), EventTimeUnixMs: time.Now().UTC().UnixMilli()},
		{TenantID: tenantID, TableID: fmt.Sprintf("%d", tableID), IdempotencyKey: "lag-2", Op: "insert", PayloadJSON: []byte(`{"x":2}`), EventTimeUnixMs: time.Now().UTC().UnixMilli()},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if _, err := ingestBus.ClaimBatch(ctx, "lag-claimer", 1, 30); err != nil {
		t.Fatalf("ClaimBatch() error = %v", err)
	}
	if _, err := db.Exec(`INSERT INTO snapshot (tenant_id, created_by, max_visibility_token) VALUES ($1, $2, $3)`, tenantID, "lag-test", int64(1)); err != nil {
		t.Fatalf("insert snapshot error = %v", err)
	}

	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	h := NewHandler(cfg, Dependencies{
		CatalogRepo: catalogpostgres.NewRepository(db),
		IngestBus:   ingestBus,
	})

	req := httptest.NewRequest(http.MethodGet, "/v1/lag", nil)
	req.Header.Set("X-Tenant-ID", tenantID)
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("lag status = %d, body=%s", rr.Code, rr.Body.String())
	}

	var body map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode lag response error = %v", err)
	}
	if body["accepted_events"] != float64(1) {
		t.Fatalf("accepted_events = %v", body["accepted_events"])
	}
	if body["claimed_events"] != float64(1) {
		t.Fatalf("claimed_events = %v", body["claimed_events"])
	}
	if body["pending_events"] != float64(2) {
		t.Fatalf("pending_events = %v", body["pending_events"])
	}
}

func TestIngestWaitForVisibilityReturnsVisibleSnapshot(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN)
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}
	seedTenantAndTable(t, db, "tenant-wait", "events")

	store, err := s3store.New(ctx, s3store.Config{
		Endpoint:         envOr("DUCKMESH_TEST_S3_ENDPOINT", "localhost:9000"),
		Region:           envOr("DUCKMESH_TEST_S3_REGION", "us-east-1"),
		Bucket:           envOr("DUCKMESH_TEST_S3_BUCKET", "duckmesh-it"),
		AccessKeyID:      envOr("DUCKMESH_TEST_S3_ACCESS_KEY", "minio"),
		SecretAccessKey:  envOr("DUCKMESH_TEST_S3_SECRET_KEY", "miniostorage"),
		UseSSL:           false,
		Prefix:           "api-ingest-wait-tests",
		AutoCreateBucket: true,
	})
	if err != nil {
		t.Fatalf("s3store.New() error = %v", err)
	}

	cfg, err := config.Load("duckmesh-api", mapLookup(map[string]string{}))
	if err != nil {
		t.Fatalf("config load failed: %v", err)
	}
	catalogRepo := catalogpostgres.NewRepository(db)
	ingestBus := buspostgres.NewIngestBus(db)
	queryEngine := duckdbengine.NewEngine(store)
	h := NewHandler(cfg, Dependencies{
		CatalogRepo: catalogRepo,
		IngestBus:   ingestBus,
		QueryEngine: queryEngine,
	})

	coord := &coordinator.Service{
		Bus:         ingestBus,
		Publisher:   catalogRepo,
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   "api-wait-test-coordinator",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "api-wait-test-coordinator",
		},
	}
	done := make(chan error, 1)
	go func() {
		time.Sleep(200 * time.Millisecond)
		done <- coord.ProcessOnce(context.Background())
	}()

	body, err := json.Marshal(map[string]any{
		"records": []map[string]any{
			{"idempotency_key": "wait-1", "op": "insert", "payload": map[string]any{"id": 1, "value": "a"}},
		},
		"wait_for_visibility":   true,
		"visibility_timeout_ms": 3000,
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/events", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", "tenant-wait")
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest status = %d, body=%s", rr.Code, rr.Body.String())
	}
	if err := <-done; err != nil {
		t.Fatalf("coordinator ProcessOnce() error = %v", err)
	}

	var response map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	if response["status"] != "visible" {
		t.Fatalf("status = %v, want visible", response["status"])
	}
	if _, ok := response["visible_snapshot_id"]; !ok || response["visible_snapshot_id"] == nil {
		t.Fatalf("visible_snapshot_id missing: %#v", response)
	}
}

func postIngest(t *testing.T, handler http.Handler, tenantID, table string, records []map[string]any) map[string]any {
	t.Helper()
	body, err := json.Marshal(map[string]any{"records": records})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}

	req := httptest.NewRequest(http.MethodPost, "/v1/ingest/"+table, bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenantID)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("ingest status = %d, body = %s", rr.Code, rr.Body.String())
	}

	var response map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode response error = %v", err)
	}
	return response
}

func seedTenantAndTable(t *testing.T, db *sql.DB, tenantID, tableName string) {
	t.Helper()
	if _, err := db.Exec(`INSERT INTO tenant (tenant_id, name, status) VALUES ($1, $2, 'active')`, tenantID, tenantID); err != nil {
		t.Fatalf("insert tenant error = %v", err)
	}
	if _, err := db.Exec(`
INSERT INTO table_def (tenant_id, table_name, primary_key_cols, partition_spec, schema_version)
VALUES ($1, $2, '[]'::jsonb, '{}'::jsonb, 1)`, tenantID, tableName); err != nil {
		t.Fatalf("insert table_def error = %v", err)
	}
}

func fetchTableID(t *testing.T, db *sql.DB, tenantID, tableName string) int64 {
	t.Helper()
	var tableID int64
	if err := db.QueryRow(`SELECT table_id FROM table_def WHERE tenant_id = $1 AND table_name = $2`, tenantID, tableName).Scan(&tableID); err != nil {
		t.Fatalf("fetch table id error = %v", err)
	}
	return tableID
}

func createTemporaryDatabase(t *testing.T, adminDSN string) (string, func()) {
	t.Helper()

	parsed, err := url.Parse(adminDSN)
	if err != nil {
		t.Fatalf("url.Parse(adminDSN) error = %v", err)
	}
	adminDBName := strings.TrimPrefix(parsed.Path, "/")
	if adminDBName == "" {
		t.Fatal("admin DSN must include a database name")
	}

	adminDB, err := sql.Open("pgx", adminDSN)
	if err != nil {
		t.Fatalf("sql.Open(adminDSN) error = %v", err)
	}

	name := fmt.Sprintf("duckmesh_it_api_%d", time.Now().UnixNano())
	if _, err := adminDB.Exec(`CREATE DATABASE ` + name); err != nil {
		t.Fatalf("CREATE DATABASE failed: %v", err)
	}

	testURL := *parsed
	testURL.Path = "/" + name
	testDSN := testURL.String()

	cleanup := func() {
		defer func() { _ = adminDB.Close() }()
		if _, err := adminDB.Exec(`SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1`, name); err != nil {
			t.Fatalf("terminate test db sessions: %v", err)
		}
		if _, err := adminDB.Exec(`DROP DATABASE ` + name); err != nil {
			t.Fatalf("DROP DATABASE failed: %v", err)
		}
	}
	return testDSN, cleanup
}

func postQuery(t *testing.T, handler http.Handler, tenantID string, payload map[string]any, expectedStatus int) map[string]any {
	t.Helper()
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/v1/query", bytes.NewReader(body))
	req.Header.Set("X-Tenant-ID", tenantID)
	rr := httptest.NewRecorder()
	handler.ServeHTTP(rr, req)
	if rr.Code != expectedStatus {
		t.Fatalf("query status = %d, want %d, body=%s", rr.Code, expectedStatus, rr.Body.String())
	}
	var response map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &response); err != nil {
		t.Fatalf("decode query response error = %v", err)
	}
	return response
}

func envOr(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}
