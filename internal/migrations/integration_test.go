//go:build integration

package migrations

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
)

func TestRunnerAppliesAndRollsBackCatalogSchema(t *testing.T) {
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

	runner := NewRunner()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	applied, err := runner.Up(ctx, db, 0)
	if err != nil {
		t.Fatalf("runner.Up() error = %v", err)
	}
	if applied < 1 {
		t.Fatalf("runner.Up() applied %d migrations, want at least 1", applied)
	}

	assertTableExists(t, db, "tenant", true)
	assertTableExists(t, db, "ingest_event", true)
	assertTableExists(t, db, "snapshot", true)
	assertTableExists(t, db, "data_file", true)

	rolledBack, err := runner.Down(ctx, db, 1)
	if err != nil {
		t.Fatalf("runner.Down() error = %v", err)
	}
	if rolledBack != 1 {
		t.Fatalf("runner.Down() rolled back %d migrations, want 1", rolledBack)
	}

	assertTableExists(t, db, "tenant", false)
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

	name := fmt.Sprintf("duckmesh_it_%d", time.Now().UnixNano())
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

func assertTableExists(t *testing.T, db *sql.DB, table string, expected bool) {
	t.Helper()

	var count int
	query := `SELECT COUNT(*) FROM pg_tables WHERE schemaname = 'public' AND tablename = $1`
	if err := db.QueryRow(query, table).Scan(&count); err != nil {
		t.Fatalf("query table %q existence failed: %v", table, err)
	}
	exists := count > 0
	if exists != expected {
		t.Fatalf("table %q exists = %v, want %v", table, exists, expected)
	}
}
