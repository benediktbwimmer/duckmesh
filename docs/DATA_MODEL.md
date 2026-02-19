# Data Model and Storage Specification

## 1. Persistence layers

1. **Catalog DB (PostgreSQL)**
   - control metadata, ingest state, snapshots, manifests, leases
2. **Object storage (S3/MinIO)**
   - Parquet data files and delete files

## 2. Catalog schema (logical)

### 2.1 Tenancy + auth

- `tenant`
  - `tenant_id` (pk)
  - `name`
  - `status`
  - `created_at`

- `api_key`
  - `key_id` (pk)
  - `tenant_id` (fk)
  - `key_hash`
  - `role`
  - `created_at`
  - `revoked_at`

### 2.2 Table metadata

- `table_def`
  - `table_id` (pk)
  - `tenant_id` (fk)
  - `table_name`
  - `primary_key_cols` (jsonb)
  - `partition_spec` (jsonb)
  - `schema_version`
  - `created_at`
  - unique (`tenant_id`, `table_name`)

- `table_schema_version`
  - `table_id` (fk)
  - `schema_version`
  - `schema_json`
  - `compatibility_mode`
  - `created_at`
  - pk (`table_id`, `schema_version`)

### 2.3 Ingest bus (Postgres implementation)

- `ingest_event`
  - `event_id` (pk, bigserial/ulid)
  - `tenant_id`
  - `table_id`
  - `idempotency_key`
  - `op`
  - `payload_json`
  - `event_time`
  - `ingested_at`
  - `lease_owner` (nullable)
  - `lease_until` (nullable)
  - `state` (`accepted|claimed|committed|failed`)
  - unique (`tenant_id`, `table_id`, `idempotency_key`)

- `ingest_claim_batch`
  - `batch_id` (pk)
  - `consumer_id`
  - `claimed_at`
  - `lease_until`
  - `state`

- `ingest_claim_item`
  - `batch_id` (fk)
  - `event_id` (fk)
  - pk (`batch_id`, `event_id`)

### 2.4 Snapshots + manifests

- `snapshot`
  - `snapshot_id` (pk, bigserial)
  - `tenant_id`
  - `created_at`
  - `created_by`
  - `max_visibility_token`
  - `parent_snapshot_id`

- `snapshot_table_watermark`
  - `snapshot_id`
  - `table_id`
  - `max_visibility_token`
  - pk (`snapshot_id`, `table_id`)

- `data_file`
  - `file_id` (pk)
  - `tenant_id`
  - `table_id`
  - `path`
  - `format` (`parquet`)
  - `record_count`
  - `file_size_bytes`
  - `min_event_time`
  - `max_event_time`
  - `stats_json`
  - `created_at`

- `snapshot_file`
  - `snapshot_id`
  - `table_id`
  - `file_id`
  - `change_type` (`add|remove`)
  - pk (`snapshot_id`, `table_id`, `file_id`, `change_type`)

### 2.5 Maintenance + audit

- `compaction_run`
- `gc_run`
- `query_audit`
- `incident_audit`

## 3. Object storage layout

```text
s3://bucket/{tenant}/{table}/
  date=YYYY-MM-DD/hour=HH/
    part-{snapshot_id}-{seq}.parquet
  deletes/
    delete-{snapshot_id}-{seq}.parquet
```

## 4. Invariants

1. Snapshot publication is atomic in catalog transaction.
2. Unpublished files must not be query-visible.
3. Watermark monotonically increases.
4. Idempotency key uniqueness must prevent duplicate logical writes.
5. GC never removes files reachable from unexpired snapshots.

## 5. Indexing requirements

- `ingest_event(state, lease_until, table_id)`
- `ingest_event(tenant_id, table_id, idempotency_key)` unique
- `snapshot(tenant_id, snapshot_id desc)`
- `snapshot_table_watermark(table_id, snapshot_id desc)`
- `snapshot_file(snapshot_id, table_id)`

## 6. Migration strategy

- SQL migrations versioned and reversible when feasible
- online-safe schema changes preferred
- schema compatibility checks enforced in API layer
