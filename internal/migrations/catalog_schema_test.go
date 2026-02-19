package migrations

import (
	"strings"
	"testing"
)

func TestCatalogMigrationContainsRequiredTablesAndIndexes(t *testing.T) {
	body, err := embeddedFS.ReadFile("sql/000001_catalog.up.sql")
	if err != nil {
		t.Fatalf("ReadFile() error = %v", err)
	}

	sql := string(body)
	requiredSnippets := []string{
		"CREATE TABLE tenant",
		"CREATE TABLE api_key",
		"CREATE TABLE table_def",
		"CREATE TABLE table_schema_version",
		"CREATE TABLE ingest_event",
		"CREATE TABLE ingest_claim_batch",
		"CREATE TABLE ingest_claim_item",
		"CREATE TABLE snapshot",
		"CREATE TABLE snapshot_table_watermark",
		"CREATE TABLE data_file",
		"CREATE TABLE snapshot_file",
		"CREATE TABLE compaction_run",
		"CREATE TABLE gc_run",
		"CREATE TABLE query_audit",
		"CREATE TABLE incident_audit",
		"CREATE INDEX idx_ingest_event_state_lease_table",
		"CREATE UNIQUE INDEX idx_ingest_event_idempotency",
		"CREATE INDEX idx_snapshot_tenant_id_desc",
		"CREATE INDEX idx_snapshot_table_watermark_table_snapshot_desc",
		"CREATE INDEX idx_snapshot_file_snapshot_table",
	}

	for _, snippet := range requiredSnippets {
		if !strings.Contains(sql, snippet) {
			t.Fatalf("migration missing required snippet: %s", snippet)
		}
	}
}
