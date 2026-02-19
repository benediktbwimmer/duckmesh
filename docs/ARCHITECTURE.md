# Architecture Specification

## 1. Overview

DuckMesh is composed of four planes:

1. **Control/API plane**: auth, table lifecycle, ingest acceptance, query orchestration
2. **Ingest bus plane**: pluggable event transport (`postgres` now, `kafka` later)
3. **Commit/materialization plane**: micro-batch workers write Parquet + publish snapshots
4. **Query plane**: DuckDB executes SQL on snapshot-selected file sets

## 2. High-level component model

- `api-gateway` (Go service)
  - AuthN/AuthZ
  - Ingest endpoints
  - Query endpoints
  - Admin endpoints
- `catalog-repo` (PostgreSQL)
  - metadata, schemas, snapshots, file manifests, leases
- `ingest-bus` abstraction
  - `postgres-bus` implementation (required)
  - `kafka-bus` implementation (planned)
- `commit-coordinator` (Go worker)
  - claims events
  - validates + groups by table
  - writes Parquet data files
  - atomically publishes snapshot
- `query-executor` (DuckDB runtime)
  - resolves snapshot
  - waits for visibility barrier
  - executes SQL with guardrails
- `compactor` (Go worker)
  - merges small files
  - rewrites manifests
  - advances snapshot
- `gc-worker`
  - deletes files no longer referenced beyond retention window

## 3. Core interfaces

### 3.1 IngestBus interface (concept)

- `Publish(ctx, events []Envelope) error`
- `ClaimBatch(ctx, consumerID string, limit int, lease time.Duration) (Batch, error)`
- `Ack(ctx, batchID string, eventIDs []string) error`
- `Nack(ctx, batchID string, eventIDs []string, reason string) error`
- `ExtendLease(ctx, batchID string, lease time.Duration) error`
- `RequeueExpired(ctx) (int, error)`

**Constraint:** Commit coordinator and API layers must not depend on backend-specific constructs.

### 3.2 CatalogRepo interface (concept)

- table/schema metadata
- snapshot creation/publication
- file registration
- watermark management
- idempotency lookups

## 4. Ingest path sequence

1. Client sends events with idempotency keys.
2. API validates schema + auth + quotas.
3. API writes events to ingest bus and returns receipt with `visibility_token`.
4. If `wait_for_visibility=true`, API blocks until snapshot watermark includes token or timeout.

## 5. Materialization path sequence

1. Coordinator claims event batch with lease.
2. Deduplicates by idempotency key semantics.
3. Transforms events to row groups per table/partition.
4. Writes Parquet data file(s) to object storage.
5. In one catalog transaction:
   - registers files,
   - advances table/global watermarks,
   - creates snapshot,
   - marks events committed.
6. Acks claimed bus events.

## 6. Query path sequence

1. Client submits SQL with optional consistency constraints.
2. API resolves target snapshot strategy:
   - explicit snapshot ID
   - timestamp mapping
   - latest with optional `min_visibility_token`
3. If min token specified, wait for barrier until satisfied or timeout.
4. Query executor creates relation bindings over snapshot manifest.
5. DuckDB executes query and returns result metadata + rows.

## 7. Deployment model

### Initial target

- Docker Compose:
  - `duckmesh-api`
  - `duckmesh-coordinator`
  - `duckmesh-compactor`
  - `postgres`
  - `minio`
  - `otel-collector` (optional)

### Production target

- Kubernetes with separated deployments and HPA per component
- Postgres HA managed service
- S3-compatible object storage

## 8. Scaling model

- API scales horizontally
- coordinator can scale with partitioned claim strategy (single-writer per table partition contract)
- query service scales horizontally with stateless workers
- compactor runs per table/tenant policy windows

## 9. Failure model

- Worker crash: leases expire, unacked events reclaimed
- Partial file write: uncommitted files ignored until snapshot publication
- Snapshot tx failure: no visibility watermark advancement
- Object store latency: backpressure + retry policy

## 10. Evolution path

1. Postgres bus only
2. Kafka bus adapter implementation
3. advanced partitioned coordinators
4. materialized aggregate acceleration
5. multi-region read replicas
