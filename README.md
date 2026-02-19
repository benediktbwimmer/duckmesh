# DuckMesh

Production-grade analytics ingestion and query plane built around DuckDB.

## Mission

Make DuckDB usable for broader production workloads with:

- many concurrent writers,
- near-real-time micro-batch ingestion,
- strict read-after-write consistency when requested,
- snapshot-based query isolation,
- pluggable ingest bus backends (`postgres` first, `kafka` ready by architecture).

## Status

- ✅ Product + architecture specs drafted
- ✅ API contracts drafted
- ✅ Data model drafted
- ✅ Ops/security/testing requirements drafted
- ✅ ADRs drafted for key decisions
- ✅ Phase 0 foundation scaffold implemented
- ✅ Phase 1 Step B data model + storage plumbing implemented
- ✅ Phase 1 Step C ingest path implemented
- ✅ Phase 2 coordinator + snapshot publication implemented
- ✅ Phase 3 query path + consistency barrier implemented
- ✅ Phase 4 maintenance plane implemented
- ✅ Phase 5 ops + security hardening implemented (runbooks + lag endpoint + SLO metrics)
- ✅ Phase 6 product refinement implemented (CLI + onboarding + web query UI + contract parity tests)

## Core Decisions (locked for initial build)

1. **Language:** Go
2. **Ingest bus abstraction:** required from day one
3. **Initial ingest bus implementation:** PostgreSQL
4. **Future ingest bus implementation:** Kafka-compatible (contract-defined, implementation deferred)
5. **Consistency target:** read-after-write via visibility tokens + query barrier
6. **Write strategy:** micro-batching with commit coordinator

## Quick start (foundation)

```bash
cp .env.example .env
make tidy
make stack-up
```

Ops CLI examples:

```bash
go run ./cmd/duckmeshctl -tenant-id tenant-dev lag
go run ./cmd/duckmeshctl -tenant-id tenant-dev compaction-run
go run ./cmd/duckmeshctl -tenant-id tenant-dev retention-run
go run ./cmd/duckmeshctl -tenant-id tenant-dev integrity-run
```

Validate basic endpoints:

```bash
curl -s localhost:8080/v1/health
curl -s localhost:8080/v1/ready
curl -s localhost:8080/v1/metrics | head
```

Open the query console:

- [http://localhost:8080](http://localhost:8080)

Generate continuous demo traffic (optional):

```bash
make demo-up
make demo-status
tail -f .tmp/duckmesh-demo-producer.log
```

Stop full local stack:

```bash
make stack-down
```

## Development commands

- `make build`
- `make test`
- `make test-race`
- `make lint`
- `make migrate-up`
- `make migrate-down`
- `make restore-drill`
- `make run-demo-producer`
- `make demo-up`
- `make demo-down`
- `make demo-status`
- `make stack-up`
- `make stack-down`
- `make stack-status`
- `make web-dev`
- `make web-build`

Observability definitions:

- `deployments/observability/prometheus/duckmesh_recording_rules.yaml`
- `deployments/observability/prometheus/duckmesh_rules.yaml`
- `deployments/observability/alertmanager/alertmanager.example.yaml`
- `deployments/observability/grafana/duckmesh_slo_dashboard.json`

## Where to start

Read in order:

1. `docs/HANDOFF.md`
2. `docs/PRD.md`
3. `docs/ARCHITECTURE.md`
4. `docs/CONSISTENCY.md`
5. `docs/API.md`
6. `docs/DATA_MODEL.md`
7. `docs/ROADMAP.md`

## Repository layout

```text
api/                OpenAPI and protocol specs
cmd/                service entrypoints
internal/           domain/application packages
deployments/        docker compose and deployment assets
tests/              integration/e2e/perf test harnesses
docs/               product, architecture, and delivery specs
```

## Non-goal

This is **not** a generic OLTP replacement. DuckMesh is a fresh analytics plane with explicit consistency semantics.
