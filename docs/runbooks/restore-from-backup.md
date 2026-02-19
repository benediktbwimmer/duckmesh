# Runbook: Restore From Backup

## Goal

Restore catalog metadata and object data with snapshot consistency verification.

## Prerequisites

1. Catalog backup artifact (full + WAL/PITR range or latest dump).
2. Object-store backup/versioned bucket for DuckMesh paths.
3. Maintenance window or read-only mode enabled.

## Procedure

0. Optional preflight/local rehearsal:
   - run `make restore-drill` on a staging/local environment.
   - to include API integrity endpoint validation in the drill:
     `DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL=http://localhost:8080 make restore-drill`
1. Stop API, coordinator, and compactor services.
2. Restore Postgres catalog to target timestamp.
3. Restore object-store bucket/prefix to matching point-in-time.
4. Run migrations (`duckmesh-migrate -direction up`) to ensure schema compatibility.
5. Start API in read-only validation mode (if available) and run integrity checks:
   - run `duckmeshctl -tenant-id <tenant> integrity-run`
   - latest snapshot resolves
   - referenced data files exist in object storage
   - sample strict read-after-write flow succeeds on test tenant
6. Start coordinator and compactor after validation passes.

## Validation checklist

1. `/v1/ready` healthy.
2. `GET /v1/lag` returns reasonable pending depth.
3. Query on latest snapshot succeeds for known tables.
4. No sustained growth in `duckmesh_consistency_timeout_total`.

## Rollback

- If integrity checks fail, stop services and restore from last known-good backup set.
