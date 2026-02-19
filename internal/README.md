# internal/

Current package status:

- `config`: environment-driven config with profile defaults (`dev|test|prod`)
- `observability`: structured logging, trace middleware, HTTP metrics middleware
- `auth`: API key validator + auth middleware skeleton
- `api`: HTTP handler wiring with health/readiness/metrics and ingest endpoint
- `migrations`: embedded SQL migration framework (up/down)
- `bus`: ingest bus contract interface for pluggable backends
- `bus/postgres`: Postgres ingest bus implementation (publish/claim/ack/nack/requeue)
- `catalog`: catalog repository contracts and models
- `catalog/postgres`: PostgreSQL repository implementation for tenants/tables/ingest/snapshots/files
- `coordinator`: micro-batch claim service, Parquet encoding, and snapshot publish orchestration
- `query`: query engine contracts
- `query/duckdb`: DuckDB execution engine over snapshot-resolved Parquet files
- `storage`: object store contract and path builders
- `storage/s3`: MinIO/S3 object store adapter
