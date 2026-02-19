# Delivery Roadmap (toward refined production release)

## Phase 0 — Foundations

- repo scaffolding, build/test tooling
- config system + environment profiles
- migration framework
- structured logging + tracing base
- auth middleware skeleton

Exit criteria:

- CI pipelines pass
- local dev stack boots reliably

## Phase 1 — Ingest core (Postgres bus)

- implement IngestBus interface
- Postgres bus adapter with claims/leases/acks
- idempotency handling
- ingest API endpoint

Exit criteria:

- concurrent ingest correctness tests pass
- duplicate ingestion prevented

## Phase 2 — Commit coordinator + snapshot publication

- batch claim and grouping logic
- Parquet writer integration
- atomic snapshot publication tx
- watermark advancement

Exit criteria:

- write-to-visible path validated
- crash recovery tests pass

## Phase 3 — Query service + consistency barrier

- SQL endpoint over DuckDB
- snapshot resolver (latest/id/time)
- `min_visibility_token` barrier wait logic
- explicit timeout/error semantics

Exit criteria:

- read-after-write integration tests pass under load

## Phase 4 — Maintenance plane

- compaction policies
- retention and GC with safety windows
- data integrity validator jobs

Exit criteria:

- no orphaned visible references
- compaction correctness tests pass

## Phase 5 — Ops + security hardening

- SLO dashboards
- runbooks and alerts
- security controls and penetration test checklist
- backup/restore drills

Exit criteria:

- incident response dry-run complete
- restore test verified

## Phase 6 — Product refinement

- CLI tooling
- admin APIs
- optional web admin UI
- docs and onboarding polish

Exit criteria:

- clean onboarding path for external users
- release candidate tagged

## Phase 7 — Kafka adapter implementation

- Kafka bus adapter built against existing interface
- semantic parity test suite reused
- throughput benchmarks and tuning guide

Exit criteria:

- no API/consistency contract changes required
- parity suite green for both bus backends

## Release policy

- no production claim before Phase 5 exit criteria are met
- no compatibility break without versioning + migration docs
