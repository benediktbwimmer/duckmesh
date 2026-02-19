# Milestone Report: Phase 1 Step B Data Model + Storage Plumbing

## 1. What was implemented

- Replaced placeholder migration with full catalog schema migration from `docs/DATA_MODEL.md`:
  - tenancy/auth: `tenant`, `api_key`
  - table metadata: `table_def`, `table_schema_version`
  - ingest bus tables: `ingest_event`, `ingest_claim_batch`, `ingest_claim_item`
  - snapshot/manifests: `snapshot`, `snapshot_table_watermark`, `data_file`, `snapshot_file`
  - maintenance/audit: `compaction_run`, `gc_run`, `query_audit`, `incident_audit`
  - required indexes for ingest/snapshot lookups
- Added typed catalog repository contracts in `internal/catalog`.
- Implemented PostgreSQL repository layer in `internal/catalog/postgres`:
  - tenant CRUD-read operations
  - API key creation
  - table create/list/get
  - schema version upsert
  - idempotent ingest-event insert (`ON CONFLICT ... RETURNING inserted flag`)
  - snapshot creation and watermark/file manifest registration primitives
  - transaction helper for future atomic snapshot publish flows
- Added storage abstraction and path builders in `internal/storage`.
- Implemented MinIO/S3 adapter in `internal/storage/s3` with:
  - config-driven client creation
  - bucket auto-create support
  - safe object-key normalization (prevents traversal)
  - put/get/stat/delete operations
  - not-found error mapping contract
- Extended config for:
  - catalog connection pool settings
  - object store endpoint/region/bucket/credentials/prefix/SSL/autocreate
- Added integration test harness (`go test -tags=integration ...`) and `make test-integration` target.
- Normalized local docker defaults for reproducible local integration (`postgres/postgres` and `postgres` DB).

## 2. Spec sections satisfied

- `docs/DATA_MODEL.md`
  - catalog schema and indexing requirements implemented in SQL migration
  - object storage layout contract supported via path builders
- `docs/ARCHITECTURE.md`
  - catalog-repo and object-store plumbing layers implemented with explicit boundaries
- `docs/OPERATIONS.md`
  - local one-command dependency stack remains available and now supports integration test workflow
- `docs/ROADMAP.md`
  - Phase 1 prerequisite plumbing completed to unblock Step C ingest endpoint and Postgres ingest bus behavior

## 3. Test evidence

Unit and static checks:

- `go test ./...` passed
- `go test -race ./...` passed
- `make lint` passed
- `make spec-check` passed
- `make build` passed

Integration checks (Docker-backed):

- `go test -tags=integration ./internal/migrations ./internal/storage/s3` passed
- `make test-integration` target now reproduces:
  - migration up/down against real Postgres
  - object put/get/stat/delete round-trip against real MinIO

## 4. Known gaps and risks

- Postgres `IngestBus` adapter (claim/lease/ack/nack/requeue) is not implemented yet.
- `POST /v1/ingest/{table}` still returns `NOT_IMPLEMENTED`.
- Repository APIs are currently focused on Step B/Step C needs; additional catalog operations (retention/compaction/query audit workflows) remain for later phases.
- Full connectivity checks for catalog/object store in runtime readiness are currently config-level checks; live dependency probes will be strengthened when services begin active dependency usage.

## 5. Next milestone plan

Step C (Ingest path) implementation sequence:

1. Implement `internal/bus/postgres` adapter satisfying `IngestBus` contract:
   - publish
   - claim/lease
   - ack/nack
   - lease extension
   - expired requeue
2. Wire idempotency behavior through ingest bus + catalog repos.
3. Implement `POST /v1/ingest/{table}` request validation and response contract from `api/openapi.yaml`.
4. Return monotonic `max_visibility_token` for accepted batches.
5. Add integration tests for concurrent writes, duplicate retries, and lease recovery basics.
