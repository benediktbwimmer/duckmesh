# Runbook: High Ingest Lag

## Symptoms

- `duckmesh_visibility_lag_ms` above SLO target
- `duckmesh_write_to_visible_latency_ms` p95 increases
- `duckmesh_consistency_timeout_total` rising

## Triage

1. Check `GET /v1/lag` for affected tenant(s).
2. Compare `accepted_events` vs `claimed_events`:
   - mostly `accepted`: coordinator throughput issue
   - mostly `claimed`: stuck leases or object store/catalog publish bottleneck
3. Check compactor activity and DB load for contention.

## Remediation

1. Increase coordinator replicas or claim limits cautiously.
2. Reduce per-tenant ingest burst if backpressure controls are available.
3. Temporarily disable/slow compaction if it causes write-path contention.
4. Verify object store latency and retry pressure.

## Validation

1. `pending_events` trends down.
2. `duckmesh_write_to_visible_latency_ms` and `duckmesh_visibility_lag_ms` trend to baseline.
3. `duckmesh_consistency_timeout_total` rate returns to near-zero.
