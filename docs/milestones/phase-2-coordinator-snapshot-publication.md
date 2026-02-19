# Milestone Report: Phase 2 Coordinator + Snapshot Publication

## 1. What was implemented

- Implemented coordinator runtime service in `internal/coordinator`:
  - batch claim loop (`ProcessOnce` + `Run`)
  - grouping claimed events by tenant/table
  - Parquet encoding of claimed events
  - object store write for micro-batch data file
  - snapshot publication orchestration
- Added Parquet writer integration using `github.com/parquet-go/parquet-go`:
  - event rows encoded as typed parquet schema
  - min/max event-time extraction for file metadata
- Implemented atomic snapshot publication in catalog Postgres layer:
  - snapshot row insert
  - data file registration
  - table watermark insert
  - snapshot file manifest insert
  - event state transition to `committed`
  - claim-batch state transition with multi-group safety
- Extended Postgres ingest bus ack state updates for multi-group batch correctness.
- Wired `duckmesh-coordinator` command to real dependencies:
  - catalog DB pooling
  - Postgres ingest bus
  - S3/MinIO object store adapter
  - configurable claim/lease/poll settings from config.
- Extended config with coordinator settings:
  - consumer ID
  - claim limit
  - lease seconds
  - poll interval
  - created-by metadata

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 2:
  - batch claim + grouping logic
  - Parquet writer integration
  - atomic snapshot publication transaction
  - watermark update logic
- `docs/ARCHITECTURE.md` materialization path:
  - coordinator claims events
  - writes parquet to object storage
  - publishes snapshot metadata atomically
- `docs/DATA_MODEL.md` invariants support:
  - snapshot publication atomicity in catalog transaction
  - watermark progression per published batch
  - unpublished/failed writes remain outside snapshot manifests

## 3. Test evidence

Unit/static checks:

- `go test ./...` passed
- `go test -race ./...` passed
- `make lint` passed
- `make ci` passed
- `make build` passed

Integration checks (Docker-backed):

- `make test-integration` passed
- integration suites now include:
  - migrations (`internal/migrations`)
  - object store (`internal/storage/s3`)
  - ingest bus (`internal/bus/postgres`)
  - ingest API (`internal/api`)
  - coordinator publication (`internal/coordinator`)

Coordinator integration assertions validate:

- claimed events become committed after processing
- snapshot/data_file/snapshot_file rows are created
- emitted data file path is readable from MinIO

## 4. Known gaps and risks

- Query path and consistency barrier are still pending (Phase 3).
- `wait_for_visibility=true` on ingest remains non-blocking until query barrier/snapshot visibility waiting is implemented.
- Current coordinator service processes in-memory per batch/group and does not yet expose detailed lag/commit metrics.
- Compaction/retention/GC and integrity validators are not yet implemented.

## 5. Next milestone plan

Phase 3 (Query service + consistency barrier):

1. Implement `POST /v1/query` execution path and snapshot resolution (`latest`, `snapshot_id`, `snapshot_time`).
2. Add visibility barrier wait semantics for `min_visibility_token` with explicit timeout errors.
3. Integrate DuckDB query engine over snapshot manifests.
4. Add integration tests for strict read-after-write behavior under normal load and coordinator restart.
5. Add query guardrails (row/time limits) and error contract alignment with OpenAPI.
