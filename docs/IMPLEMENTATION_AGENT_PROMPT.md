# Implementation Agent Prompt (Copy/Paste)

You are the implementation agent for **DuckMesh**.

Your goal is to build a **production-grade, refined application** from the specs in this repository.
This is explicitly **not** a quick MVP or throwaway prototype.

## Mission

Implement DuckMesh as a self-hostable analytics ingestion/query plane around DuckDB with:

- concurrent multi-writer ingest,
- micro-batch materialization,
- snapshot-consistent queries,
- strict read-after-write guarantees via visibility tokens,
- pluggable ingest bus architecture (`postgres` now, `kafka` compatible by contract).

## Read this first (in order)

1. `docs/HANDOFF.md`
2. `docs/PRD.md`
3. `docs/ARCHITECTURE.md`
4. `docs/CONSISTENCY.md`
5. `docs/API.md`
6. `api/openapi.yaml`
7. `docs/DATA_MODEL.md`
8. `docs/OPERATIONS.md`
9. `docs/SECURITY.md`
10. `docs/ROADMAP.md`
11. `docs/adr/*.md`

## Non-negotiable constraints

1. **Language:** Go for core services.
2. Implement **IngestBus abstraction from day one**.
3. Implement only **Postgres bus backend** initially.
4. Keep architecture ready for **future Kafka backend** without semantic changes.
5. Enforce read-after-write with **visibility tokens + query barrier**.
6. Do not add query-time merge of uncommitted queue events unless explicitly approved by ADR.
7. No weakening of consistency semantics for convenience.
8. No feature is “done” without tests + docs.

## Quality bar

- Clean architecture and explicit boundaries.
- Deterministic behavior under retries/failures.
- Restart-safe workers and idempotent processing.
- Full structured observability (logs, metrics, traces).
- Secure defaults (tenant isolation, authz checks, input limits).

## Implementation sequence

Follow `docs/ROADMAP.md` phases. Start with:

### Step A — Foundation

- Go module + Makefile + lint + CI skeleton
- config system and profiles
- structured logging and trace IDs
- migration framework

### Step B — Data model and storage plumbing

- catalog schema migrations from `docs/DATA_MODEL.md`
- Postgres repositories
- object store adapter (MinIO/S3)

### Step C — Ingest path

- `IngestBus` interface
- `postgres` bus implementation (claim/lease/ack/nack/requeue)
- idempotency enforcement
- `POST /v1/ingest/{table}`

### Step D — Coordinator + snapshot publication

- micro-batch claim loop
- parquet writer
- atomic snapshot publish tx
- watermark update logic

### Step E — Query path + consistency barrier

- `POST /v1/query`
- snapshot resolution (latest/id/time)
- `min_visibility_token` wait semantics
- explicit timeout errors

### Step F — Maintenance + hardening

- compaction + retention + GC
- lag/remediation endpoints
- backup/restore + integrity checks
- load/failure tests

## Test requirements (must be automated)

- idempotency under retried writes
- strict read-after-write (normal + during coordinator restart)
- lease recovery after worker crash
- snapshot reproducibility/time travel correctness
- tenant isolation and authz enforcement
- performance regression baseline

## Operational requirements

Expose and validate:

- `/v1/health`, `/v1/ready`, `/v1/metrics`
- ingest lag metrics
- write-to-visible latency metrics
- consistency timeout counters

Create runbooks for:

- stuck leases
- high ingest lag
- snapshot publish failures
- restore from backup

## Delivery rules

For each implementation milestone provide:

1. What was implemented
2. Which spec sections were satisfied
3. Test evidence
4. Known gaps/risks
5. Next milestone plan

If any spec ambiguity blocks progress, create/update an ADR before diverging.

## Definition of done (release candidate)

A release candidate is complete only when:

- PRD functional/non-functional requirements are met,
- OpenAPI contract matches behavior,
- consistency guarantees are proven by integration tests,
- operational dashboards and runbooks are in place,
- security checks pass,
- failure recovery scenarios are validated,
- docs are up to date.
