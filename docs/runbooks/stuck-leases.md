# Runbook: Stuck Leases

## Symptoms

- `duckmesh_ingest_lag_events` grows steadily
- `claimed_events` remains high in `GET /v1/lag`
- coordinator logs show no successful commits

## Immediate checks

1. Verify coordinator process health.
2. Verify catalog DB connectivity and lock contention.
3. Verify object store availability for writes.

## Remediation steps

1. Restart coordinator worker.
2. Force expired claims to requeue by running one coordinator cycle.
3. Validate `claimed_events` drops and `accepted_events` drains.
4. Confirm new snapshots are advancing (`latest_visibility_token` increases).

## Validation

1. `GET /v1/lag` returns decreasing `pending_events`.
2. Queries with recent `min_visibility_token` stop timing out.
3. No repeated `claim batch` failures in coordinator logs.

## Escalation

- If queue does not drain after restart and DB/object store are healthy, place ingest in maintenance mode and investigate lock chains and failing tenant/table payloads.
