# Milestone Report: Phase 1 Step C Ingest Path

## 1. What was implemented

- Implemented `IngestBus` contract methods in Postgres backend:
  - `Publish`
  - `ClaimBatch`
  - `Ack`
  - `Nack`
  - `ExtendLease`
  - `RequeueExpired`
- Added duplicate/idempotent ingest semantics in bus publish path via `ON CONFLICT` and explicit inserted-vs-duplicate return metadata.
- Updated `IngestBus` contract to return publish results (`event_id`, `visibility_token`, `inserted`) so API can report `accepted_count`, `duplicate_count`, and `max_visibility_token`.
- Implemented `POST /v1/ingest/{table}` endpoint behavior:
  - tenant resolution from auth identity or `X-Tenant-ID`
  - role enforcement (`ingest_writer`) when authenticated
  - request validation (`records`, `idempotency_key`, `op`, `payload`)
  - tenant table resolution via catalog
  - publish to `IngestBus`
  - response mapping to OpenAPI fields (`accepted_count`, `duplicate_count`, `max_visibility_token`, `status`)
- Wired API runtime dependencies:
  - catalog DB connection + pooling
  - catalog Postgres repo
  - Postgres ingest bus
  - readiness check using live catalog ping and object-store config validation
- Added integration tests for Step C behavior:
  - Postgres ingest bus end-to-end claim/ack/requeue behavior
  - API ingest idempotency behavior against real Postgres
- Extended `make test-integration` to include Step C integration packages.

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 1:
  - `IngestBus` implementation completed (Postgres backend)
  - idempotency handling implemented
  - ingest API endpoint implemented
- `docs/ARCHITECTURE.md`:
  - backend-agnostic bus contract preserved; API/coordinator can depend on interface
  - Postgres backend provided as initial adapter
- `docs/API.md` + `api/openapi.yaml`:
  - ingest endpoint implemented with required request/response core fields

## 3. Test evidence

Unit/static checks:

- `go test ./...` passed
- `go test -race ./...` passed
- `make lint` passed
- `make ci` passed

Integration checks (Docker-backed):

- `make test-integration` passed
- Integration suites now include:
  - `internal/migrations` (catalog migration up/down)
  - `internal/storage/s3` (MinIO round-trip)
  - `internal/bus/postgres` (publish/claim/ack/requeue)
  - `internal/api` (ingest idempotency via real Postgres bus)

## 4. Known gaps and risks

- `wait_for_visibility=true` is parsed but true visibility waiting is deferred to query-barrier milestone (Phase 3).
- Coordinator publication path is not implemented yet, so tokens represent accepted/queued order, not yet visible snapshot inclusion.
- Table lifecycle endpoints are still partial; ingest requires pre-existing table metadata in catalog.
- Query path and read-after-write barrier semantics are pending later milestones.

## 5. Next milestone plan

Phase 2 (Coordinator + snapshot publication):

1. Implement coordinator claim loop over `IngestBus` with lease extension and crash-safe retries.
2. Group claimed events by tenant/table and write Parquet files to object storage.
3. Add atomic snapshot publication transaction (file registration + watermark advancement + snapshot row + bus ack).
4. Add restart and lease recovery integration tests for write-to-visible durability.
5. Surface coordinator lag/commit metrics required by operations spec.
