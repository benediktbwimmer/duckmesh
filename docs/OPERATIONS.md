# Operations, Reliability, and SRE Specification

## 1. SLOs (initial targets)

- API availability: 99.9%
- p95 ingest ack latency (no wait): < 200 ms
- p95 write-to-visible latency: < 3 s
- p95 strict query barrier wait success: > 99%
- p95 query latency (interactive class): < 2 s

## 2. Key metrics

### Ingest

- `ingest_requests_total`
- `ingest_records_total`
- `ingest_duplicates_total`
- `ingest_ack_latency_ms`
- `duckmesh_ingest_requests_total`
- `duckmesh_ingest_records_total`
- `duckmesh_ingest_duplicates_total`
- `duckmesh_ingest_ack_latency_ms`

### Freshness

- `visibility_lag_ms`
- `latest_visibility_token`
- `consistency_timeout_total`
- `duckmesh_visibility_lag_ms`
- `duckmesh_latest_visibility_token`
- `duckmesh_consistency_timeout_total`
- `duckmesh_write_to_visible_latency_ms`
- `duckmesh_ingest_lag_events`

### Coordinator

- `claim_batch_size`
- `claim_retries_total`
- `commit_batch_latency_ms`
- `snapshot_publish_failures_total`

### Query

- `query_requests_total`
- `query_latency_ms`
- `query_scanned_files`
- `query_scanned_bytes`

### Storage/maintenance

- `compaction_runs_total`
- `compaction_bytes_rewritten`
- `gc_files_deleted_total`
- `integrity_runs_total`
- `integrity_files_checked_total`
- `integrity_missing_files_total`
- `integrity_size_mismatch_files_total`
- `duckmesh_compaction_runs_total`
- `duckmesh_compaction_bytes_rewritten_total`
- `duckmesh_gc_files_deleted_total`
- `duckmesh_integrity_runs_total`
- `duckmesh_integrity_files_checked_total`
- `duckmesh_integrity_missing_files_total`
- `duckmesh_integrity_size_mismatch_files_total`

## 3. Logging and tracing

- JSON structured logs
- required fields: `timestamp`, `service`, `tenant_id`, `trace_id`, `error_code`
- distributed tracing for ingest->commit->query paths

## 4. Health model

- `/v1/health`: process liveness only
- `/v1/ready`: dependency readiness (catalog, object store, schema cache)

## 5. Backpressure and load shedding

- per-tenant ingest rate limits
- max pending ingest queue threshold
- query concurrency limit + queue
- reject/429 before catastrophic failure

## 6. Recovery scenarios (must support)

1. API restart during high ingest
2. coordinator crash with leased but unacked events
3. object store transient failure
4. catalog lock contention spikes
5. compaction interrupted mid-run

## 7. Backup/restore

- periodic catalog DB backups with point-in-time recovery
- object storage versioning recommended
- snapshot consistency validator after restore
- automated local drill command: `make restore-drill`
- optional API integrity endpoint check during drill:
  `DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL=http://localhost:8080 make restore-drill`

## 8. Runbooks required

- ingest lag remediation
- stuck lease cleanup
- snapshot publication failure handling
- compaction rollback strategy
- emergency read-only mode activation

Implemented runbooks:

- `docs/runbooks/stuck-leases.md`
- `docs/runbooks/high-ingest-lag.md`
- `docs/runbooks/snapshot-publish-failures.md`
- `docs/runbooks/restore-from-backup.md`

## 9. Deployment requirements

- reproducible container images
- env-driven config with safe defaults
- one-command local environment via docker compose
- IaC-managed production deployment

## 10. Dashboard and Alerts

Committed observability artifacts:

- Prometheus alert rules:
  - `deployments/observability/prometheus/duckmesh_rules.yaml`
- Prometheus recording rules:
  - `deployments/observability/prometheus/duckmesh_recording_rules.yaml`
- Prometheus scrape/rule wiring example:
  - `deployments/observability/prometheus/prometheus-scrape.example.yaml`
- Alertmanager routing example:
  - `deployments/observability/alertmanager/alertmanager.example.yaml`
- Grafana dashboard definition:
  - `deployments/observability/grafana/duckmesh_slo_dashboard.json`
