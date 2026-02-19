# tests/

Automated tests currently include:

- unit tests for config profiles and env overrides
- unit tests for auth middleware behavior
- unit tests for API baseline endpoints
- unit tests for migration loader guarantees
- unit tests for Postgres catalog repository SQL behavior (sqlmock)
- unit tests for object-store key/path behavior and S3 adapter semantics
- unit tests for ingest API validation, tenant scoping, authz role enforcement, and duplicate accounting
- unit tests for coordinator grouping logic and Parquet encoding output
- unit tests for query endpoint validation/barrier behavior and DuckDB engine execution over Parquet
- unit tests for `duckmeshctl` CLI command routing (including integrity runs), auth header forwarding, and error handling
- unit tests for demo ingest producer config parsing, deterministic record generation, and ingest/table bootstrap HTTP behavior
- unit tests for restore drill script dry-run/argument validation (`scripts/restore_drill_test.go`)
- unit tests for stack lifecycle script dry-run/argument validation (`scripts/stack_test.go`)
- unit tests validating committed observability assets (Prometheus alert+recording rules, Alertmanager routing example, scrape example, Grafana dashboard JSON) in `deployments/observability_assets_test.go`
- unit tests for UI-assist endpoints (`/v1/ui/schema`, `/v1/query/translate`) and static UI route serving
- integration tests (tag `integration`) for migration runner against real Postgres and S3 adapter round-trip against MinIO
- integration tests (tag `integration`) for Postgres ingest bus claim/lease/ack/requeue and API ingest idempotency behavior
- integration tests (tag `integration`) for `/v1/lag` ingest pending-depth and visibility lag reporting
- integration tests (tag `integration`) for coordinator `ProcessOnce` end-to-end snapshot publication and committed state transitions
- integration tests (tag `integration`) for strict read-after-write via `/v1/query` with `min_visibility_token` barrier waits
- integration tests (tag `integration`) for maintenance plane compaction correctness and retention/GC safety-window behavior

Integration/e2e/load suites will be added in roadmap phases 1-6.

Run integration tests locally:

```bash
make test-integration
```
