# API Specification (Functional)

This document defines external behavior. Canonical machine-readable contract should live in `api/openapi.yaml`.

## 1. Auth

- API key or bearer token
- tenant context required
- all write/query operations tenant-scoped

## 2. Ingest endpoints

### `POST /v1/ingest/{table}`

Ingest one or more events.

Request:

- `records[]`
  - `idempotency_key` (required)
  - `op` (`insert|upsert|delete`)
  - `payload` (object)
  - `event_time` (optional)
- `wait_for_visibility` (bool, default false)
- `visibility_timeout_ms` (optional)

Response:

- `accepted_count`
- `duplicate_count`
- `max_visibility_token`
- `visible_snapshot_id` (when waited)
- `status` (`accepted|visible|partial_duplicate`)

## 3. Query endpoint

### `POST /v1/query`

Request:

- `sql` (required)
- `params` (optional)
- one of:
  - `snapshot_id`
  - `snapshot_time`
  - latest + optional `min_visibility_token`
- `consistency_timeout_ms` (optional)
- `row_limit` (optional guardrail)

Response:

- `columns[]`
- `rows[]`
- `snapshot_id`
- `snapshot_time`
- `max_visibility_token`
- `stats` (duration, scanned_files, scanned_bytes)

### `POST /v1/query/translate`

Translate natural language into SQL for DuckDB.

Request:

- `prompt` (required)

Response:

- `sql`
- `provider`
- `model`

Auth/role:

- tenant-scoped
- requires `query_reader` role when auth is enabled

### `GET /v1/ui/schema`

Fetch table metadata + sample rows used by frontend autocomplete and AI-assist context.

Auth/role:

- tenant-scoped
- requires `query_reader` role when auth is enabled

## 4. Metadata/table management

- `POST /v1/tables`
  - create logical table metadata
  - requires `table_admin` role
- `GET /v1/tables`
  - list tables for tenant
  - requires `query_reader` or `table_admin`
- `GET /v1/tables/{table}`
  - fetch one table definition
  - requires `query_reader` or `table_admin`
- `PATCH /v1/tables/{table}` (schema evolution)
  - creates next schema version and bumps active table schema version
  - requires `table_admin`
- `DELETE /v1/tables/{table}`
  - removes table definition
  - requires `table_admin`

## 5. Snapshot endpoints

- `GET /v1/snapshots`
- `GET /v1/snapshots/{id}`
- `POST /v1/snapshots/pin`
- `POST /v1/snapshots/restore` (admin)

## 6. Operations endpoints

- `GET /v1/health`
- `GET /v1/ready`
- `GET /v1/metrics`
- `GET /v1/lag`
- `POST /v1/compaction/run`
- `POST /v1/retention/run`
- `POST /v1/integrity/run`

`POST /v1/compaction/run`, `POST /v1/retention/run`, and `POST /v1/integrity/run` are admin operations:

- require `ops_admin` role
- operate on the caller tenant scope
- return `{ status: "completed", summary: {...} }`

`POST /v1/integrity/run` validates snapshot-visible file references against object storage:

- checks object presence
- checks object size against catalog metadata
- fails with `INTEGRITY_CHECK_FAILED` when missing or mismatched files are found

`GET /v1/lag` is an admin operation:

- requires `ops_admin` role
- returns pending ingest queue depth and visibility lag signals for the caller tenant

## 7. Error contract

Error body:

- `error_code`
- `message`
- `retryable` (bool)
- `context` (map)
- `trace_id`

## 8. Compatibility rules

- additive fields are backward compatible
- no semantic changes to consistency behavior without version bump + ADR
- deprecations require documented grace period
