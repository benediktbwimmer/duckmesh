//go:build integration

package maintenance

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
	"github.com/duckmesh/duckmesh/internal/coordinator"
	"github.com/duckmesh/duckmesh/internal/migrations"
	"github.com/duckmesh/duckmesh/internal/query"
	duckdbengine "github.com/duckmesh/duckmesh/internal/query/duckdb"
	"github.com/duckmesh/duckmesh/internal/storage"
	s3store "github.com/duckmesh/duckmesh/internal/storage/s3"
)

func TestCompactionMergesActiveFilesAndPreservesRows(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN, "maintenance")
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}

	tenantID := "tenant-maint"
	tableName := "events"
	seedTenantAndTable(t, db, tenantID, tableName)
	tableID := fetchTableID(t, db, tenantID, tableName)

	store := newTestStore(t, ctx, "maintenance-compaction")
	repo := catalogpostgres.NewRepository(db)
	ingestBus := buspostgres.NewIngestBus(db)
	coord := &coordinator.Service{
		Bus:         ingestBus,
		Publisher:   repo,
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   "maintenance-compaction-coord",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "maintenance-compaction-coord",
		},
	}

	publishAndMaterializeEvents(t, ctx, ingestBus, coord, tenantID, tableID, 4, 100)

	svc := &Service{
		Catalog:     repo,
		ObjectStore: store,
		Config: Config{
			CompactionMinInputFiles: 2,
			KeepSnapshots:           3,
			GCSafetyAge:             time.Nanosecond,
			CreatedBy:               "maintenance-test",
		},
	}

	summary, err := svc.RunCompactionOnce(ctx, tenantID)
	if err != nil {
		t.Fatalf("RunCompactionOnce() error = %v (summary=%+v)", err, summary)
	}
	if summary.TablesCompacted != 1 {
		t.Fatalf("TablesCompacted = %d, want 1", summary.TablesCompacted)
	}
	if summary.InputFilesCompacted != 4 {
		t.Fatalf("InputFilesCompacted = %d, want 4", summary.InputFilesCompacted)
	}

	latest, err := repo.GetLatestSnapshot(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetLatestSnapshot() error = %v", err)
	}
	latestFiles, err := repo.ListSnapshotFilesForTable(ctx, tenantID, latest.SnapshotID, tableID)
	if err != nil {
		t.Fatalf("ListSnapshotFilesForTable() error = %v", err)
	}
	if len(latestFiles) != 1 {
		t.Fatalf("latest active file count = %d, want 1", len(latestFiles))
	}

	engine := duckdbengine.NewEngine(store)
	result, err := engine.Execute(ctx, query.Request{
		SQL: "SELECT COUNT(*) AS c FROM events",
		Files: []query.TableFile{
			{
				TableName:     tableName,
				ObjectPath:    latestFiles[0].Path,
				FileSizeBytes: latestFiles[0].FileSizeBytes,
			},
		},
	})
	if err != nil {
		t.Fatalf("query engine execute error = %v", err)
	}
	if len(result.Rows) != 1 || len(result.Rows[0]) != 1 {
		t.Fatalf("query rows = %#v", result.Rows)
	}
	if got := normalizeCountValue(t, result.Rows[0][0]); got != 4 {
		t.Fatalf("row count = %d, want 4", got)
	}
}

func TestRetentionRespectsKeepSnapshotsSafetyWindow(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN, "maintenance")
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}

	tenantID := "tenant-retention"
	tableName := "events"
	seedTenantAndTable(t, db, tenantID, tableName)
	tableID := fetchTableID(t, db, tenantID, tableName)

	store := newTestStore(t, ctx, "maintenance-retention")
	repo := catalogpostgres.NewRepository(db)
	ingestBus := buspostgres.NewIngestBus(db)
	coord := &coordinator.Service{
		Bus:         ingestBus,
		Publisher:   repo,
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   "maintenance-retention-coord",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "maintenance-retention-coord",
		},
	}

	publishAndMaterializeEvents(t, ctx, ingestBus, coord, tenantID, tableID, 4, 200)
	preCompactionSnapshot, err := repo.GetLatestSnapshot(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetLatestSnapshot() before compaction error = %v", err)
	}
	removedCandidates, err := repo.ListSnapshotFilesForTable(ctx, tenantID, preCompactionSnapshot.SnapshotID, tableID)
	if err != nil {
		t.Fatalf("ListSnapshotFilesForTable() before compaction error = %v", err)
	}
	if len(removedCandidates) != 4 {
		t.Fatalf("pre-compaction active files = %d, want 4", len(removedCandidates))
	}

	svc := &Service{
		Catalog:     repo,
		ObjectStore: store,
		Config: Config{
			CompactionMinInputFiles: 2,
			KeepSnapshots:           2,
			GCSafetyAge:             time.Nanosecond,
			CreatedBy:               "maintenance-test",
		},
	}

	if _, err := svc.RunCompactionOnce(ctx, tenantID); err != nil {
		t.Fatalf("RunCompactionOnce() error = %v", err)
	}

	// Create one more snapshot so removed files become eligible when keepSnapshots=1.
	publishAndMaterializeEvents(t, ctx, ingestBus, coord, tenantID, tableID, 1, 300)

	firstRetention, err := svc.RunRetentionOnce(ctx, tenantID)
	if err != nil {
		t.Fatalf("RunRetentionOnce() keepSnapshots=2 error = %v (summary=%+v)", err, firstRetention)
	}
	if firstRetention.FilesDeleted != 0 {
		t.Fatalf("FilesDeleted with keepSnapshots=2 = %d, want 0", firstRetention.FilesDeleted)
	}

	svc.Config.KeepSnapshots = 1
	secondRetention, err := svc.RunRetentionOnce(ctx, tenantID)
	if err != nil {
		t.Fatalf("RunRetentionOnce() keepSnapshots=1 error = %v (summary=%+v)", err, secondRetention)
	}
	if secondRetention.FilesDeleted != len(removedCandidates) {
		t.Fatalf("FilesDeleted with keepSnapshots=1 = %d, want %d", secondRetention.FilesDeleted, len(removedCandidates))
	}

	for _, removed := range removedCandidates {
		if _, err := store.Stat(ctx, removed.Path); err == nil {
			t.Fatalf("removed object still exists: %s", removed.Path)
		} else if err != storage.ErrObjectNotFound {
			t.Fatalf("store.Stat(%q) error = %v", removed.Path, err)
		}
	}
	for _, removed := range removedCandidates {
		var exists int
		if err := db.QueryRow(`SELECT COUNT(*) FROM data_file WHERE file_id = $1`, removed.FileID).Scan(&exists); err != nil {
			t.Fatalf("query data_file for %d error = %v", removed.FileID, err)
		}
		if exists != 0 {
			t.Fatalf("data_file %d still exists", removed.FileID)
		}
	}
}

func TestIntegrityCheckDetectsMissingVisibleFile(t *testing.T) {
	adminDSN := strings.TrimSpace(os.Getenv("DUCKMESH_TEST_CATALOG_DSN"))
	if adminDSN == "" {
		t.Skip("DUCKMESH_TEST_CATALOG_DSN is not set")
	}

	testDSN, cleanup := createTemporaryDatabase(t, adminDSN, "maintenance")
	defer cleanup()

	db, err := sql.Open("pgx", testDSN)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	if _, err := migrations.NewRunner().Up(ctx, db, 0); err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}

	tenantID := "tenant-integrity"
	tableName := "events"
	seedTenantAndTable(t, db, tenantID, tableName)
	tableID := fetchTableID(t, db, tenantID, tableName)

	store := newTestStore(t, ctx, "maintenance-integrity")
	repo := catalogpostgres.NewRepository(db)
	ingestBus := buspostgres.NewIngestBus(db)
	coord := &coordinator.Service{
		Bus:         ingestBus,
		Publisher:   repo,
		ObjectStore: store,
		Config: coordinator.Config{
			ConsumerID:   "maintenance-integrity-coord",
			ClaimLimit:   100,
			LeaseSeconds: 30,
			CreatedBy:    "maintenance-integrity-coord",
		},
	}

	publishAndMaterializeEvents(t, ctx, ingestBus, coord, tenantID, tableID, 1, 500)
	latest, err := repo.GetLatestSnapshot(ctx, tenantID)
	if err != nil {
		t.Fatalf("GetLatestSnapshot() error = %v", err)
	}
	files, err := repo.ListSnapshotFilesForTable(ctx, tenantID, latest.SnapshotID, tableID)
	if err != nil {
		t.Fatalf("ListSnapshotFilesForTable() error = %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("active files = %d, want 1", len(files))
	}
	if err := store.Delete(ctx, files[0].Path); err != nil {
		t.Fatalf("store.Delete(%q) error = %v", files[0].Path, err)
	}

	svc := &Service{
		Catalog:     repo,
		ObjectStore: store,
		Config: Config{
			IntegritySnapshotLimit: 10,
			CreatedBy:              "maintenance-test",
		},
	}
	summary, err := svc.RunIntegrityCheckOnce(ctx, tenantID)
	if err == nil {
		t.Fatalf("expected integrity check error (summary=%+v)", summary)
	}
	if summary.MissingFiles != 1 {
		t.Fatalf("MissingFiles = %d, want 1", summary.MissingFiles)
	}
	if summary.UniqueFilesChecked != 1 {
		t.Fatalf("UniqueFilesChecked = %d, want 1", summary.UniqueFilesChecked)
	}
}

func publishAndMaterializeEvents(t *testing.T, ctx context.Context, ingestBus bus.IngestBus, coord *coordinator.Service, tenantID string, tableID int64, count int, base int) {
	t.Helper()
	for i := 0; i < count; i++ {
		_, err := ingestBus.Publish(ctx, []bus.Envelope{
			{
				TenantID:        tenantID,
				TableID:         fmt.Sprintf("%d", tableID),
				IdempotencyKey:  fmt.Sprintf("idem-%d", base+i),
				Op:              "insert",
				PayloadJSON:     []byte(fmt.Sprintf(`{"id":%d,"value":"v%d"}`, base+i, base+i)),
				EventTimeUnixMs: time.Now().UTC().UnixMilli(),
			},
		})
		if err != nil {
			t.Fatalf("ingest publish error = %v", err)
		}
		if err := coord.ProcessOnce(ctx); err != nil {
			t.Fatalf("coord.ProcessOnce() error = %v", err)
		}
	}
}

func newTestStore(t *testing.T, ctx context.Context, prefix string) *s3store.Store {
	t.Helper()
	store, err := s3store.New(ctx, s3store.Config{
		Endpoint:         envOr("DUCKMESH_TEST_S3_ENDPOINT", "localhost:9000"),
		Region:           envOr("DUCKMESH_TEST_S3_REGION", "us-east-1"),
		Bucket:           envOr("DUCKMESH_TEST_S3_BUCKET", "duckmesh-it"),
		AccessKeyID:      envOr("DUCKMESH_TEST_S3_ACCESS_KEY", "minio"),
		SecretAccessKey:  envOr("DUCKMESH_TEST_S3_SECRET_KEY", "miniostorage"),
		UseSSL:           false,
		Prefix:           fmt.Sprintf("%s-%d", prefix, time.Now().UnixNano()),
		AutoCreateBucket: true,
	})
	if err != nil {
		t.Fatalf("s3store.New() error = %v", err)
	}
	return store
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

func createTemporaryDatabase(t *testing.T, adminDSN, prefix string) (string, func()) {
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

	name := fmt.Sprintf("duckmesh_it_%s_%d", prefix, time.Now().UnixNano())
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

func envOr(key, fallback string) string {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return fallback
	}
	return value
}

func normalizeCountValue(t *testing.T, value any) int64 {
	t.Helper()
	switch typed := value.(type) {
	case int64:
		return typed
	case int32:
		return int64(typed)
	case int:
		return int64(typed)
	case uint64:
		return int64(typed)
	case uint32:
		return int64(typed)
	default:
		t.Fatalf("unexpected count type %T", value)
		return 0
	}
}
