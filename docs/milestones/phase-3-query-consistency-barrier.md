# Milestone Report: Phase 3 Query Path + Consistency Barrier

## 1. What was implemented

- Implemented `POST /v1/query` endpoint behavior in API plane:
  - request validation (`sql`, selector constraints)
  - tenant scoping and role enforcement (`query_reader` when authenticated)
  - snapshot resolution by:
    - latest
    - `snapshot_id`
    - `snapshot_time`
  - visibility barrier wait for `min_visibility_token`
  - explicit `CONSISTENCY_TIMEOUT` error with structured context
- Added catalog snapshot query support in Postgres repository:
  - latest snapshot lookup
  - snapshot by ID lookup
  - snapshot by timestamp lookup
  - resolved snapshot file listing with add/remove semantics
- Implemented DuckDB query engine package:
  - materializes snapshot objects from object store to temp local parquet files
  - creates DuckDB views per logical table
  - executes read-only SQL (`SELECT`/`WITH`)
  - returns columns/rows plus scanned file/byte stats and duration
- Wired API runtime to instantiate and use object store + DuckDB query engine.
- Added strict read-after-write integration test path:
  - ingest write returns token
  - query with `min_visibility_token` times out before coordinator publish
  - query succeeds after coordinator publish with same token

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 3:
  - SQL endpoint over DuckDB
  - snapshot resolver (`latest`/`id`/`time`)
  - `min_visibility_token` barrier wait logic
  - explicit timeout semantics
- `docs/CONSISTENCY.md`:
  - barrier algorithm implemented with polling + timeout
  - `CONSISTENCY_TIMEOUT` returned with latest token context
- `docs/API.md` + `api/openapi.yaml`:
  - query endpoint contract fields and core behavior implemented

## 3. Test evidence

Unit/static checks:

- `go test ./...` passed
- `go test -race ./...` passed
- `make lint` passed
- `make ci` passed
- `make build` passed

Integration checks (Docker-backed):

- `make test-integration` passed
- integration coverage now includes:
  - migration runner
  - S3 adapter round-trip
  - Postgres ingest bus claim/ack/requeue
  - ingest API idempotency
  - coordinator snapshot publication
  - query barrier strict-read flow with coordinator-delayed visibility

## 4. Known gaps and risks

- Query parameter binding (`params`) is currently rejected as unsupported.
- Query guardrails are basic (read-only SQL + optional row limit) and need more hardening (execution timeout/memory controls).
- Snapshot pin/restore APIs and full time-travel management operations are still pending.
- Maintenance plane (compaction/retention/GC) not implemented yet.

## 5. Next milestone plan

Phase 4 (Maintenance plane):

1. Implement compaction workflows and manifest rewrites.
2. Implement retention and GC with safety windows.
3. Add integrity validator jobs and orphan detection.
4. Expose lag/remediation endpoints and runbook-backed operational controls.
5. Add correctness tests for compaction and GC invariants.
