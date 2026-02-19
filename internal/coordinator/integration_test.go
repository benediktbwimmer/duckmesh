//go:build integration

package coordinator

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/duckmesh/duckmesh/internal/bus"
	buspostgres "github.com/duckmesh/duckmesh/internal/bus/postgres"
	catalogpostgres "github.com/duckmesh/duckmesh/internal/catalog/postgres"
	"github.com/duckmesh/duckmesh/internal/migrations"
	s3store "github.com/duckmesh/duckmesh/internal/storage/s3"
)

func TestServiceProcessOncePublishesSnapshot(t *testing.T) {
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
	seedTenantAndTable(t, db, "tenant-coord", "events")
	tableID := fetchTableID(t, db, "tenant-coord", "events")

	ingestBus := buspostgres.NewIngestBus(db)
	if _, err := ingestBus.Publish(ctx, []bus.Envelope{
		{TenantID: "tenant-coord", TableID: fmt.Sprintf("%d", tableID), IdempotencyKey: "k1", Op: "insert", PayloadJSON: []byte(`{"x":1}`), EventTimeUnixMs: time.Now().UTC().UnixMilli()},
		{TenantID: "tenant-coord", TableID: fmt.Sprintf("%d", tableID), IdempotencyKey: "k2", Op: "insert", PayloadJSON: []byte(`{"x":2}`), EventTimeUnixMs: time.Now().UTC().UnixMilli()},
	}); err != nil {
		t.Fatalf("Publish() error = %v", err)
	}

	store, err := s3store.New(ctx, s3store.Config{
		Endpoint:         envOr("DUCKMESH_TEST_S3_ENDPOINT", "localhost:9000"),
		Region:           envOr("DUCKMESH_TEST_S3_REGION", "us-east-1"),
		Bucket:           envOr("DUCKMESH_TEST_S3_BUCKET", "duckmesh-it"),
		AccessKeyID:      envOr("DUCKMESH_TEST_S3_ACCESS_KEY", "minio"),
		SecretAccessKey:  envOr("DUCKMESH_TEST_S3_SECRET_KEY", "miniostorage"),
		UseSSL:           false,
		Prefix:           "coordinator-tests",
		AutoCreateBucket: true,
	})
	if err != nil {
		t.Fatalf("s3store.New() error = %v", err)
	}

	service := &Service{
		Bus:         ingestBus,
		Publisher:   catalogpostgres.NewRepository(db),
		ObjectStore: store,
		Config: Config{
			ConsumerID:   "coord-it",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "coord-it",
		},
	}

	if err := service.ProcessOnce(ctx); err != nil {
		t.Fatalf("ProcessOnce() error = %v", err)
	}

	assertCount(t, db, `SELECT COUNT(*) FROM snapshot WHERE tenant_id = 'tenant-coord'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM data_file WHERE tenant_id = 'tenant-coord'`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM snapshot_file`, 1)
	assertCount(t, db, `SELECT COUNT(*) FROM ingest_event WHERE state = 'committed'`, 2)
	assertCount(t, db, `SELECT COUNT(*) FROM ingest_event WHERE state = 'claimed'`, 0)

	var path string
	if err := db.QueryRow(`SELECT path FROM data_file WHERE tenant_id = 'tenant-coord' LIMIT 1`).Scan(&path); err != nil {
		t.Fatalf("select data file path error = %v", err)
	}
	if _, err := store.Stat(ctx, path); err != nil {
		t.Fatalf("store.Stat(path) error = %v", err)
	}
}

func assertCount(t *testing.T, db *sql.DB, query string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(query).Scan(&got); err != nil {
		t.Fatalf("query %q error = %v", query, err)
	}
	if got != want {
		t.Fatalf("query %q count = %d, want %d", query, got, want)
	}
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

func envOr(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
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

	name := fmt.Sprintf("duckmesh_it_coord_%d", time.Now().UnixNano())
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
