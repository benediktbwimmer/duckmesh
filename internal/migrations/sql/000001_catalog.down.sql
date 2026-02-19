DROP INDEX IF EXISTS idx_snapshot_file_snapshot_table;
DROP INDEX IF EXISTS idx_snapshot_table_watermark_table_snapshot_desc;
DROP INDEX IF EXISTS idx_snapshot_tenant_id_desc;
DROP INDEX IF EXISTS idx_ingest_event_idempotency;
DROP INDEX IF EXISTS idx_ingest_event_state_lease_table;

DROP TABLE IF EXISTS incident_audit;
DROP TABLE IF EXISTS query_audit;
DROP TABLE IF EXISTS gc_run;
DROP TABLE IF EXISTS compaction_run;
DROP TABLE IF EXISTS snapshot_file;
DROP TABLE IF EXISTS data_file;
DROP TABLE IF EXISTS snapshot_table_watermark;
DROP TABLE IF EXISTS snapshot;
DROP TABLE IF EXISTS ingest_claim_item;
DROP TABLE IF EXISTS ingest_claim_batch;
DROP TABLE IF EXISTS ingest_event;
DROP TABLE IF EXISTS table_schema_version;
DROP TABLE IF EXISTS table_def;
DROP TABLE IF EXISTS api_key;
DROP TABLE IF EXISTS tenant;

DROP TYPE IF EXISTS duckmesh_change_type;
DROP TYPE IF EXISTS duckmesh_ingest_state;
DROP TYPE IF EXISTS duckmesh_ingest_op;
DROP TYPE IF EXISTS duckmesh_tenant_status;
