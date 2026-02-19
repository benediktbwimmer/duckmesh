//go:build integration

package postgres

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
	"github.com/duckmesh/duckmesh/internal/migrations"
)

func TestIngestBusPublishClaimAckAndRequeue(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN)
	defer cleanup()

	db := openDB(t, testDSN)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	runner := migrations.NewRunner()
	if _, err := runner.Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}

	seedTenantAndTable(t, db, "tenant-a", "events")
	tableID := fetchTableID(t, db, "tenant-a", "events")

	ingestBus := NewIngestBus(db)

	published, err := ingestBus.Publish(ctx, []bus.Envelope{
		{
			TenantID:       "tenant-a",
			TableID:        fmt.Sprintf("%d", tableID),
			IdempotencyKey: "idem-1",
			Op:             "insert",
			PayloadJSON:    []byte(`{"id":1}`),
		},
		{
			TenantID:       "tenant-a",
			TableID:        fmt.Sprintf("%d", tableID),
			IdempotencyKey: "idem-2",
			Op:             "insert",
			PayloadJSON:    []byte(`{"id":2}`),
		},
		{
			TenantID:       "tenant-a",
			TableID:        fmt.Sprintf("%d", tableID),
			IdempotencyKey: "idem-1",
			Op:             "insert",
			PayloadJSON:    []byte(`{"id":1}`),
		},
	})
	if err != nil {
		t.Fatalf("Publish() error = %v", err)
	}
	if len(published) != 3 {
		t.Fatalf("len(Publish()) = %d, want 3", len(published))
	}
	if !published[0].Inserted || !published[1].Inserted {
		t.Fatal("first two publish results should be inserted")
	}
	if published[2].Inserted {
		t.Fatal("third publish result should be duplicate")
	}
	if published[2].VisibilityToken != published[0].VisibilityToken {
		t.Fatalf("duplicate visibility token = %d, want %d", published[2].VisibilityToken, published[0].VisibilityToken)
	}

	claimed, err := ingestBus.ClaimBatch(ctx, "worker-1", 10, 10)
	if err != nil {
		t.Fatalf("ClaimBatch() error = %v", err)
	}
	if claimed.BatchID == "" {
		t.Fatal("ClaimBatch() returned empty batch id")
	}
	if len(claimed.EventIDs) != 2 {
		t.Fatalf("len(claimed.EventIDs) = %d, want 2", len(claimed.EventIDs))
	}

	if err := ingestBus.Ack(ctx, claimed.BatchID, claimed.EventIDs); err != nil {
		t.Fatalf("Ack() error = %v", err)
	}

	assertEventState(t, db, published[0].EventID, "committed")
	assertEventState(t, db, published[1].EventID, "committed")

	published2, err := ingestBus.Publish(ctx, []bus.Envelope{
		{
			TenantID:       "tenant-a",
			TableID:        fmt.Sprintf("%d", tableID),
			IdempotencyKey: "idem-3",
			Op:             "insert",
			PayloadJSON:    []byte(`{"id":3}`),
		},
	})
	if err != nil {
		t.Fatalf("Publish(second) error = %v", err)
	}

	claimed2, err := ingestBus.ClaimBatch(ctx, "worker-2", 10, 10)
	if err != nil {
		t.Fatalf("ClaimBatch(second) error = %v", err)
	}
	if len(claimed2.EventIDs) != 1 {
		t.Fatalf("len(claimed2.EventIDs) = %d, want 1", len(claimed2.EventIDs))
	}

	if err := forceEventLeaseExpired(ctx, db, published2[0].EventID); err != nil {
		t.Fatalf("forceEventLeaseExpired() error = %v", err)
	}

	requeued, err := ingestBus.RequeueExpired(ctx)
	if err != nil {
		t.Fatalf("RequeueExpired() error = %v", err)
	}
	if requeued < 1 {
		t.Fatalf("RequeueExpired() = %d, want >= 1", requeued)
	}
	assertEventState(t, db, published2[0].EventID, "accepted")
}

func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	return db
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

func forceEventLeaseExpired(ctx context.Context, db *sql.DB, eventID string) error {
	_, err := db.ExecContext(ctx, `
UPDATE ingest_event
SET lease_until = NOW() - INTERVAL '5 second'
WHERE event_id = $1`, eventID)
	return err
}

func assertEventState(t *testing.T, db *sql.DB, eventID, expectedState string) {
	t.Helper()
	var state string
	if err := db.QueryRow(`SELECT state::text FROM ingest_event WHERE event_id = $1`, eventID).Scan(&state); err != nil {
		t.Fatalf("query event state error = %v", err)
	}
	if state != expectedState {
		t.Fatalf("event %s state = %s, want %s", eventID, state, expectedState)
	}
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

	name := fmt.Sprintf("duckmesh_it_bus_%d", time.Now().UnixNano())
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
