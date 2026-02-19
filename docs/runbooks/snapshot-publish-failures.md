# Runbook: Snapshot Publish Failures

## Symptoms

- coordinator logs include publish transaction errors
- `snapshot_publish_failures_total` increases
- `duckmesh_consistency_timeout_total` starts increasing

## Triage

1. Inspect coordinator logs for failing tenant/table and error class.
2. Check Postgres for lock waits, disk pressure, or failed transactions.
3. Check object store health and write latency.

## Remediation

1. Restart coordinator after resolving dependency health issues.
2. If failures are tenant-specific payload/schema issues, pause ingest for impacted tenant and fix producer-side data.
3. Re-run coordinator and verify new snapshots publish.
4. Run retention/compaction only after publish path is stable.

## Validation

1. Latest snapshot IDs advance for impacted tenant(s).
2. `GET /v1/lag` pending depth decreases.
3. Queries with new visibility tokens succeed without timeout.
