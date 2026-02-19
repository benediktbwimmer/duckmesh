# Milestone Report: Phase 4 Maintenance Plane

## 1. What was implemented

- Implemented maintenance runtime in `internal/maintenance`:
  - compaction execution (`RunCompactionOnce`) with tenant/table scan
  - parquet merge via DuckDB (`read_parquet` + `COPY ... TO PARQUET`)
  - atomic compaction snapshot publish through catalog transaction (`PublishCompaction`)
  - retention/GC execution (`RunRetentionOnce`) with keep-snapshot and safety-age filters
  - GC run audit writes to `gc_run`
- Replaced compactor skeleton with production worker wiring in `cmd/duckmesh-compactor`:
  - catalog DB connection
  - object-store adapter
  - periodic compaction + retention loops
- Added maintenance admin APIs:
  - `POST /v1/compaction/run`
  - `POST /v1/retention/run`
  - tenant-scoped + `ops_admin` role requirement
- Extended config system for maintenance controls:
  - `DUCKMESH_MAINTENANCE_COMPACTION_INTERVAL`
  - `DUCKMESH_MAINTENANCE_COMPACTION_MIN_INPUT_FILES`
  - `DUCKMESH_MAINTENANCE_RETENTION_INTERVAL`
  - `DUCKMESH_MAINTENANCE_KEEP_SNAPSHOTS`
  - `DUCKMESH_MAINTENANCE_GC_SAFETY_AGE`
  - `DUCKMESH_MAINTENANCE_CREATED_BY`
- Added maintenance metrics:
  - `duckmesh_compaction_runs_total`
  - `duckmesh_compaction_bytes_rewritten_total`
  - `duckmesh_gc_files_deleted_total`
- Added GC catalog plumbing:
  - `RecordGCRun` repository method
- Updated API contract/docs (`api/openapi.yaml`, `docs/API.md`) and test documentation.

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 4:
  - compaction policies
  - retention/GC with safety window
  - compaction correctness validation coverage
- `docs/DATA_MODEL.md` invariants:
  - GC avoids deleting files before retention/snapshot windows
  - snapshot publication remains atomic
- `docs/ARCHITECTURE.md` maintenance components:
  - compactor worker and gc worker behavior implemented in one maintenance service
- `docs/API.md` operations endpoints:
  - `/v1/compaction/run` and `/v1/retention/run` implemented
- `docs/OPERATIONS.md` storage/maintenance metrics:
  - compaction run, bytes rewritten, gc files deleted metrics exported.

## 3. Test evidence

Validation commands executed successfully:

- `go test ./...`
- `make ci`
- `make test-integration`
- `make build`

New automated coverage includes:

- `internal/maintenance/integration_test.go`
  - compaction preserves row counts and replaces active file set with compacted file
  - retention respects keep-snapshot safety behavior, then deletes old removed files when eligible
- `internal/api/maintenance_test.go`
  - ops role enforcement and endpoint responses
- `internal/config/config_test.go`
  - maintenance config defaults and env overrides
- `internal/catalog/postgres/repository_test.go`
  - `RecordGCRun` SQL behavior.

## 4. Known gaps and risks

- Integrity validator jobs are not implemented yet (Phase 4 roadmap item partially open).
- `GET /v1/lag` and broader remediation endpoints/runbooks remain pending.
- Query engine still lacks parameter binding support and advanced execution guardrails.
- Race-test linker warnings appear on current macOS toolchain (`LC_DYSYMTAB`), but tests still pass.

## 5. Next milestone plan

Phase 5 (Ops + security hardening):

1. Add lag/remediation endpoints (`/v1/lag`) and operational control surfaces.
2. Implement runbooks for stuck leases, ingest lag, snapshot publish failures, and restore procedures.
3. Add backup/restore + integrity validation workflows and tests.
4. Expand SLO instrumentation/dashboards and alert-friendly counters.
5. Harden authz paths and tenant isolation tests for admin/ops surfaces.
