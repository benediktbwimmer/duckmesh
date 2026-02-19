package migrations

import (
	"context"
	"database/sql"
	"embed"
	"fmt"
	"io/fs"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed sql/*.sql
var embeddedFS embed.FS

const migrationTable = "duckmesh_schema_migrations"

var migrationNamePattern = regexp.MustCompile(`^([0-9]+)_.+\.(up|down)\.sql$`)

type Runner struct {
	fsys fs.FS
}

func NewRunner() *Runner {
	return &Runner{fsys: embeddedFS}
}

type migration struct {
	Version int64
	UpSQL   string
	DownSQL string
}

func (r *Runner) Up(ctx context.Context, db *sql.DB, steps int) (int, error) {
	migrations, err := loadMigrations(r.fsys)
	if err != nil {
		return 0, err
	}
	if err := ensureMigrationTable(ctx, db); err != nil {
		return 0, err
	}
	applied, err := listAppliedVersions(ctx, db)
	if err != nil {
		return 0, err
	}

	appliedSet := make(map[int64]struct{}, len(applied))
	for _, version := range applied {
		appliedSet[version] = struct{}{}
	}

	runCount := 0
	for _, item := range migrations {
		if _, ok := appliedSet[item.Version]; ok {
			continue
		}
		if steps > 0 && runCount >= steps {
			break
		}
		if err := applyMigration(ctx, db, item.Version, item.UpSQL); err != nil {
			return runCount, err
		}
		runCount++
	}
	return runCount, nil
}

func (r *Runner) Down(ctx context.Context, db *sql.DB, steps int) (int, error) {
	if steps <= 0 {
		steps = 1
	}

	migrations, err := loadMigrations(r.fsys)
	if err != nil {
		return 0, err
	}
	if err := ensureMigrationTable(ctx, db); err != nil {
		return 0, err
	}

	applied, err := listAppliedVersionsDesc(ctx, db)
	if err != nil {
		return 0, err
	}

	lookup := make(map[int64]migration, len(migrations))
	for _, item := range migrations {
		lookup[item.Version] = item
	}

	runCount := 0
	for _, version := range applied {
		if runCount >= steps {
			break
		}
		item, ok := lookup[version]
		if !ok {
			return runCount, fmt.Errorf("applied migration %d is missing from source", version)
		}
		if strings.TrimSpace(item.DownSQL) == "" {
			return runCount, fmt.Errorf("migration %d has empty down SQL", version)
		}
		if err := rollbackMigration(ctx, db, item.Version, item.DownSQL); err != nil {
			return runCount, err
		}
		runCount++
	}

	return runCount, nil
}

func ensureMigrationTable(ctx context.Context, db *sql.DB) error {
	query := `
CREATE TABLE IF NOT EXISTS ` + migrationTable + ` (
	version BIGINT PRIMARY KEY,
	applied_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
)`
	_, err := db.ExecContext(ctx, query)
	if err != nil {
		return fmt.Errorf("ensure migration table: %w", err)
	}
	return nil
}

func applyMigration(ctx context.Context, db *sql.DB, version int64, script string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("apply migration %d: %w", version, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO `+migrationTable+` (version) VALUES ($1)`, version); err != nil {
		return fmt.Errorf("mark migration %d: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit migration %d: %w", version, err)
	}
	return nil
}

func rollbackMigration(ctx context.Context, db *sql.DB, version int64, script string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, script); err != nil {
		return fmt.Errorf("rollback migration %d: %w", version, err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM `+migrationTable+` WHERE version = $1`, version); err != nil {
		return fmt.Errorf("unmark migration %d: %w", version, err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit rollback %d: %w", version, err)
	}
	return nil
}

func listAppliedVersions(ctx context.Context, db *sql.DB) ([]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM `+migrationTable+` ORDER BY version ASC`)
	if err != nil {
		return nil, fmt.Errorf("query applied versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return versions, nil
}

func listAppliedVersionsDesc(ctx context.Context, db *sql.DB) ([]int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT version FROM `+migrationTable+` ORDER BY version DESC`)
	if err != nil {
		return nil, fmt.Errorf("query applied versions: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var versions []int64
	for rows.Next() {
		var version int64
		if err := rows.Scan(&version); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		versions = append(versions, version)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows error: %w", err)
	}
	return versions, nil
}

func loadMigrations(fsys fs.FS) ([]migration, error) {
	entries, err := fs.ReadDir(fsys, "sql")
	if err != nil {
		return nil, fmt.Errorf("read migration dir: %w", err)
	}

	items := map[int64]migration{}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		base := path.Base(entry.Name())
		matches := migrationNamePattern.FindStringSubmatch(base)
		if len(matches) != 3 {
			continue
		}
		version, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse migration version for %q: %w", base, err)
		}
		direction := matches[2]

		script, err := fs.ReadFile(fsys, path.Join("sql", entry.Name()))
		if err != nil {
			return nil, fmt.Errorf("read migration %q: %w", entry.Name(), err)
		}

		item := items[version]
		item.Version = version
		switch direction {
		case "up":
			item.UpSQL = string(script)
		case "down":
			item.DownSQL = string(script)
		default:
			return nil, fmt.Errorf("unsupported migration direction %q", direction)
		}
		items[version] = item
	}

	versions := make([]int64, 0, len(items))
	for version := range items {
		versions = append(versions, version)
	}
	sort.Slice(versions, func(i, j int) bool { return versions[i] < versions[j] })

	migrations := make([]migration, 0, len(versions))
	for _, version := range versions {
		item := items[version]
		if strings.TrimSpace(item.UpSQL) == "" {
			return nil, fmt.Errorf("migration %d missing up SQL", version)
		}
		if strings.TrimSpace(item.DownSQL) == "" {
			return nil, fmt.Errorf("migration %d missing down SQL", version)
		}
		migrations = append(migrations, item)
	}
	return migrations, nil
}
