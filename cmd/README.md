# cmd/

Current Go entrypoints:

- `duckmesh-api`: API plane process with health/readiness/metrics, ingest endpoint, and DuckDB-backed query endpoint with visibility barrier
- `duckmesh-coordinator`: coordinator worker with Postgres claim loop, Parquet materialization, object-store writes, and snapshot publication
- `duckmesh-compactor`: maintenance worker running compaction and retention/GC loops
- `duckmesh-migrate`: SQL migration runner for catalog schema
- `duckmeshctl`: operator CLI for health/readiness/lag checks and maintenance/integrity triggers
- `duckmesh-demo-producer`: sample workload generator that continuously ingests synthetic events through `POST /v1/ingest/{table}`
