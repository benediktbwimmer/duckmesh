package migrations

import (
	"strings"
	"testing"
	"testing/fstest"
)

func TestLoadMigrationsSortsAndPairsUpDown(t *testing.T) {
	fsys := fstest.MapFS{
		"sql/000002_two.up.sql":   {Data: []byte("SELECT 2;")},
		"sql/000002_two.down.sql": {Data: []byte("SELECT -2;")},
		"sql/000001_one.up.sql":   {Data: []byte("SELECT 1;")},
		"sql/000001_one.down.sql": {Data: []byte("SELECT -1;")},
	}

	items, err := loadMigrations(fsys)
	if err != nil {
		t.Fatalf("loadMigrations() error = %v", err)
	}
	if len(items) != 2 {
		t.Fatalf("len(items) = %d", len(items))
	}
	if items[0].Version != 1 || items[1].Version != 2 {
		t.Fatalf("unexpected migration order: %+v", items)
	}
}

func TestLoadMigrationsErrorsWhenDownMissing(t *testing.T) {
	fsys := fstest.MapFS{
		"sql/000001_one.up.sql": {Data: []byte("SELECT 1;")},
	}
	_, err := loadMigrations(fsys)
	if err == nil {
		t.Fatal("expected error for missing down migration")
	}
	if !strings.Contains(err.Error(), "missing down SQL") {
		t.Fatalf("unexpected error: %v", err)
	}
}
