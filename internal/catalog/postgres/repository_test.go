package postgres

import (
	"context"
	"database/sql"
	"errors"
	"regexp"
	"testing"
	"time"

	sqlmock "github.com/DATA-DOG/go-sqlmock"

	"github.com/duckmesh/duckmesh/internal/catalog"
)

func TestCreateTenant(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)
	now := time.Now()

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO tenant (tenant_id, name, status)
VALUES ($1, $2, $3)
RETURNING created_at`)).
		WithArgs("tenant-1", "Tenant One", "active").
		WillReturnRows(sqlmock.NewRows([]string{"created_at"}).AddRow(now))

	tenant, err := repo.CreateTenant(context.Background(), catalog.CreateTenantInput{
		TenantID: "tenant-1",
		Name:     "Tenant One",
	})
	if err != nil {
		t.Fatalf("CreateTenant() error = %v", err)
	}
	if tenant.TenantID != "tenant-1" {
		t.Fatalf("TenantID = %q", tenant.TenantID)
	}
	if !tenant.CreatedAt.Equal(now) {
		t.Fatalf("CreatedAt = %v, want %v", tenant.CreatedAt, now)
	}
	assertSQLMock(t, mock)
}

func TestInsertIngestEventConflictReturnsInsertedFalse(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
INSERT INTO ingest_event (tenant_id, table_id, idempotency_key, op, payload_json, event_time, state)
VALUES ($1, $2, $3, $4::duckmesh_ingest_op, $5::jsonb, $6, 'accepted')
ON CONFLICT (tenant_id, table_id, idempotency_key)
DO UPDATE SET idempotency_key = ingest_event.idempotency_key
RETURNING event_id, (xmax = 0) AS inserted, ingested_at`)).
		WithArgs("tenant-1", int64(22), "idem-1", "upsert", `{"a":1}`, nil).
		WillReturnRows(sqlmock.NewRows([]string{"event_id", "inserted", "ingested_at"}).AddRow(int64(33), false, now))

	result, err := repo.InsertIngestEvent(context.Background(), catalog.InsertIngestEventInput{
		TenantID:       "tenant-1",
		TableID:        22,
		IdempotencyKey: "idem-1",
		Op:             "upsert",
		PayloadJSON:    []byte(`{"a":1}`),
	})
	if err != nil {
		t.Fatalf("InsertIngestEvent() error = %v", err)
	}
	if result.EventID != 33 {
		t.Fatalf("EventID = %d", result.EventID)
	}
	if result.Inserted {
		t.Fatal("Inserted should be false for conflict path")
	}
	assertSQLMock(t, mock)
}

func TestGetTenantReturnsNotFound(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT tenant_id, name, status, created_at
FROM tenant
WHERE tenant_id = $1`)).
		WithArgs("missing-tenant").
		WillReturnError(sql.ErrNoRows)

	_, err := repo.GetTenant(context.Background(), "missing-tenant")
	if err == nil {
		t.Fatal("expected not found error")
	}
	if err != catalog.ErrNotFound {
		t.Fatalf("error = %v, want %v", err, catalog.ErrNotFound)
	}
	assertSQLMock(t, mock)
}

func TestDeleteTableByName(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)

	mock.ExpectExec(regexp.QuoteMeta(`
DELETE FROM table_def
WHERE tenant_id = $1 AND table_name = $2`)).
		WithArgs("tenant-1", "events").
		WillReturnResult(sqlmock.NewResult(0, 1))

	deleted, err := repo.DeleteTableByName(context.Background(), "tenant-1", "events")
	if err != nil {
		t.Fatalf("DeleteTableByName() error = %v", err)
	}
	if !deleted {
		t.Fatal("expected deleted=true")
	}
	assertSQLMock(t, mock)
}

func TestSetTableSchemaVersionNotFound(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)

	mock.ExpectExec(regexp.QuoteMeta(`
UPDATE table_def
SET schema_version = $2
WHERE table_id = $1`)).
		WithArgs(int64(77), 3).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err := repo.SetTableSchemaVersion(context.Background(), 77, 3)
	if err == nil {
		t.Fatal("expected error")
	}
	if !errors.Is(err, catalog.ErrNotFound) {
		t.Fatalf("error = %v, want %v", err, catalog.ErrNotFound)
	}
	assertSQLMock(t, mock)
}

func TestWithTxCommitsOnSuccess(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)

	mock.ExpectBegin()
	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO snapshot_table_watermark (snapshot_id, table_id, max_visibility_token)
VALUES ($1, $2, $3)
ON CONFLICT (snapshot_id, table_id)
DO UPDATE SET max_visibility_token = GREATEST(
    snapshot_table_watermark.max_visibility_token,
    EXCLUDED.max_visibility_token
)`)).
		WithArgs(int64(10), int64(20), int64(99)).
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectCommit()

	err := repo.WithTx(context.Background(), func(tx *TxRepository) error {
		return tx.UpsertSnapshotTableWatermark(context.Background(), catalog.UpsertSnapshotTableWatermarkInput{
			SnapshotID:         10,
			TableID:            20,
			MaxVisibilityToken: 99,
		})
	})
	if err != nil {
		t.Fatalf("WithTx() error = %v", err)
	}
	assertSQLMock(t, mock)
}

func TestRecordGCRun(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)

	mock.ExpectExec(regexp.QuoteMeta(`
INSERT INTO gc_run (tenant_id, status, details_json, completed_at)
VALUES ($1, $2, $3::jsonb, NOW())`)).
		WithArgs("tenant-1", "completed", `{"deleted":4}`).
		WillReturnResult(sqlmock.NewResult(1, 1))

	if err := repo.RecordGCRun(context.Background(), RecordGCRunInput{
		TenantID:    "tenant-1",
		Status:      "completed",
		DetailsJSON: []byte(`{"deleted":4}`),
	}); err != nil {
		t.Fatalf("RecordGCRun() error = %v", err)
	}
	assertSQLMock(t, mock)
}

func TestListSnapshots(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)
	now := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT snapshot_id, tenant_id, created_by, max_visibility_token, parent_snapshot_id, created_at
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT $2`)).
		WithArgs("tenant-1", 2).
		WillReturnRows(sqlmock.NewRows([]string{
			"snapshot_id", "tenant_id", "created_by", "max_visibility_token", "parent_snapshot_id", "created_at",
		}).
			AddRow(int64(11), "tenant-1", "coord", int64(100), nil, now).
			AddRow(int64(10), "tenant-1", "coord", int64(90), int64(9), now.Add(-time.Second)))

	snapshots, err := repo.ListSnapshots(context.Background(), "tenant-1", 2)
	if err != nil {
		t.Fatalf("ListSnapshots() error = %v", err)
	}
	if len(snapshots) != 2 {
		t.Fatalf("snapshot count = %d, want 2", len(snapshots))
	}
	if snapshots[0].SnapshotID != 11 || snapshots[0].MaxVisibilityToken != 100 {
		t.Fatalf("snapshot[0] = %#v", snapshots[0])
	}
	if snapshots[1].ParentSnapshotID == nil || *snapshots[1].ParentSnapshotID != 9 {
		t.Fatalf("snapshot[1] parent = %#v", snapshots[1].ParentSnapshotID)
	}
	assertSQLMock(t, mock)
}

func TestGetIngestLagStats(t *testing.T) {
	db, mock := newSQLMock(t)
	repo := NewRepository(db)
	oldest := time.Now().Add(-2 * time.Minute).UTC()
	snapshotAt := time.Now().UTC()

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT
    COALESCE(SUM(CASE WHEN state = 'accepted' THEN 1 ELSE 0 END), 0) AS accepted_events,
    COALESCE(SUM(CASE WHEN state = 'claimed' THEN 1 ELSE 0 END), 0) AS claimed_events,
    MIN(CASE WHEN state IN ('accepted', 'claimed') THEN ingested_at END) AS oldest_pending_ingested_at,
    COALESCE(MAX(CASE WHEN state IN ('accepted', 'claimed') THEN event_id ELSE NULL END), 0) AS max_pending_token
FROM ingest_event
WHERE tenant_id = $1`)).
		WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"accepted_events", "claimed_events", "oldest_pending_ingested_at", "max_pending_token"}).
			AddRow(int64(4), int64(1), oldest, int64(42)))

	mock.ExpectQuery(regexp.QuoteMeta(`
SELECT snapshot_id, max_visibility_token, created_at
FROM snapshot
WHERE tenant_id = $1
ORDER BY snapshot_id DESC
LIMIT 1`)).
		WithArgs("tenant-1").
		WillReturnRows(sqlmock.NewRows([]string{"snapshot_id", "max_visibility_token", "created_at"}).
			AddRow(int64(9), int64(39), snapshotAt))

	stats, err := repo.GetIngestLagStats(context.Background(), "tenant-1")
	if err != nil {
		t.Fatalf("GetIngestLagStats() error = %v", err)
	}
	if stats.AcceptedEvents != 4 || stats.ClaimedEvents != 1 {
		t.Fatalf("counts = accepted:%d claimed:%d", stats.AcceptedEvents, stats.ClaimedEvents)
	}
	if stats.MaxPendingToken != 42 || stats.LatestVisibilityToken != 39 {
		t.Fatalf("tokens = pending:%d latest:%d", stats.MaxPendingToken, stats.LatestVisibilityToken)
	}
	if stats.LatestSnapshotID == nil || *stats.LatestSnapshotID != 9 {
		t.Fatalf("latest snapshot id = %#v", stats.LatestSnapshotID)
	}
	if stats.OldestPendingIngestAt == nil || !stats.OldestPendingIngestAt.Equal(oldest) {
		t.Fatalf("oldest pending at = %#v", stats.OldestPendingIngestAt)
	}
	assertSQLMock(t, mock)
}

func newSQLMock(t *testing.T) (*sql.DB, sqlmock.Sqlmock) {
	t.Helper()
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherRegexp))
	if err != nil {
		t.Fatalf("sqlmock.New() error = %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db, mock
}

func assertSQLMock(t *testing.T, mock sqlmock.Sqlmock) {
	t.Helper()
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Fatalf("unmet sql expectations: %v", err)
	}
}
