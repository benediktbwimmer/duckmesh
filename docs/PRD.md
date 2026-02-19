# Product Requirements Document (PRD)

## 1. Product summary

DuckMesh is a self-hostable analytics ingestion and query plane that combines:

- concurrent multi-producer writes,
- DuckDB query speed,
- snapshot isolation,
- optional strict read-after-write consistency.

## 2. Target users

1. B2B SaaS teams embedding customer-facing analytics
2. Platform/data teams needing fresh event analytics without heavyweight warehouse ops
3. Engineering teams that like local-first/open formats and self-hosting

## 3. Problems solved

- DuckDB file-level multi-process write limitations in production
- Stale analytics experiences for event-heavy products
- Operational burden of full warehouse stacks for small/mid teams
- Lack of explicit, application-level read-after-write guarantees in analytics systems

## 4. Goals

### Functional goals

- Accept high-concurrency ingest from many writers
- Materialize data into queryable Parquet-backed snapshots
- Support SQL queries against latest or chosen snapshot
- Guarantee read-after-write when requested (`min_visibility_token` or write-wait mode)
- Provide idempotent ingestion
- Provide schema evolution and table lifecycle management

### Non-functional goals

- Predictable correctness semantics
- Operable with small-team infra footprint
- Observable and debuggable in production
- Secure multi-tenant operation

## 5. Non-goals

- OLTP transactional database replacement
- Millisecond-latency stream processor for arbitrary joins over uncommitted data
- Full BI suite replacement

## 6. Personas + key use cases

### Persona A: App platform engineer

Needs tenant-level dashboards that reflect recent events quickly.

- Sends events via API
- Gets visibility token
- Queries with `min_visibility_token`
- Receives consistent SQL result that includes known writes

### Persona B: Data engineer (small team)

Needs simple, low-ops event warehouse.

- Ingests from services/CDC
- Runs ad-hoc SQL and scheduled reports
- Uses snapshot/time-travel for reproducibility

### Persona C: SRE/ops owner

Needs reliability and predictability.

- Tracks ingest lag, query freshness, commit errors
- Runs compaction/retention safely
- Restores from backup with integrity checks

## 7. Feature requirements

### FR-1 Ingestion API

- Append/upsert/delete operations via API
- Batch ingest endpoint
- Idempotency key required per logical write
- Returns write receipt including visibility token

### FR-2 Visibility and consistency

- Query endpoint accepts `min_visibility_token`
- Query blocks until token visible or timeout
- Optional ingest mode `wait_for_visibility=true`

### FR-3 Snapshot catalog

- Monotonic snapshot IDs
- Per-table and global visibility watermarks
- Time-travel query by snapshot ID or timestamp

### FR-4 Query execution

- SQL endpoint using DuckDB engine
- Parameterized query support
- Row/size/time limits

### FR-5 Schema/table lifecycle

- Create/alter/drop table APIs
- Schema registry + compatibility checks
- Event schema version tracking

### FR-6 Storage management

- Parquet file writing and registration
- Compaction policy
- Retention + garbage collection with safety windows

### FR-7 Multi-tenant controls

- Tenant-scoped auth
- Tenant-scoped data visibility and quotas

### FR-8 Observability

- Metrics for ingest rate, lag, commit latency, query latency
- Structured logs + trace IDs
- Health/readiness endpoints

### FR-9 Reliability operations

- Lease recovery for crashed workers
- Replay/backfill support
- Snapshot integrity checker

## 8. Acceptance criteria

- A write acknowledged with token `T` is guaranteed visible in any query with `min_visibility_token >= T` (or clear timeout error).
- Under concurrent writes, no duplicate logical records are produced when idempotency keys are reused.
- System survives coordinator crash without data loss or orphaned invisible writes.
- Query results are snapshot-consistent and reproducible.
- Postgres ingest bus can be replaced by alternate implementation without API-level semantic changes.

## 9. Success metrics

- p95 ingest acknowledgment latency (non-wait mode)
- p95 write-to-visible latency
- p95 query latency by query class
- ingest-to-visible timeout rate
- compaction amplification ratio
- snapshot publication failure rate

## 10. Release criteria (v1.0 quality)

- All FRs implemented
- Security baseline complete
- SLO dashboards live
- Backup/restore tested
- Load + failure tests passed
- Developer/operator docs complete
