# Milestone Report: Phase 5 Step B Integrity Validation

## 1. What was implemented

- Added maintenance integrity validation flow:
  - `RunIntegrityCheckOnce` in `internal/maintenance/service.go`
  - validates snapshot-visible file references against object storage
  - checks missing objects and file-size mismatches
- Added maintenance integrity metrics:
  - `duckmesh_integrity_runs_total`
  - `duckmesh_integrity_files_checked_total`
  - `duckmesh_integrity_missing_files_total`
  - `duckmesh_integrity_size_mismatch_files_total`
- Added catalog snapshot listing support:
  - `ListSnapshots` to `internal/catalog/catalog.go`
  - Postgres implementation in `internal/catalog/postgres/repository.go`
- Added admin endpoint:
  - `POST /v1/integrity/run` (tenant scoped, `ops_admin` role)
- Added CLI command:
  - `duckmeshctl integrity-run`
- Expanded OpenAPI table/admin coverage:
  - detailed `/v1/tables` and `/v1/tables/{table}` schemas
  - added `/v1/integrity/run`

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 4/5 hardening:
  - closes integrity validator job gap from maintenance plane
  - extends ops/admin controls with integrity run surface
- `docs/OPERATIONS.md`:
  - snapshot consistency validator capability is now implemented
  - integrity metrics exported for dashboards/alerts
- `docs/API.md`:
  - admin integrity operation documented
  - table lifecycle contract clarified with role requirements

## 3. Test evidence

New/updated automated tests:

- `internal/maintenance/service_integrity_test.go`
  - success path and missing-file detection
- `internal/maintenance/integration_test.go`
  - real Postgres + object-store missing-file detection (`TestIntegrityCheckDetectsMissingVisibleFile`)
- `internal/api/maintenance_test.go`
  - `/v1/integrity/run` endpoint behavior
- `internal/cli/duckmeshctl/runner_test.go`
  - `integrity-run` command routing
- `internal/catalog/postgres/repository_test.go`
  - `ListSnapshots` query behavior
- `internal/api/openapi_parity_test.go`
  - parity checks include table and integrity paths

## 4. Known gaps and risks

- Integrity run currently returns aggregated summary and first-level issue details in error text; it does not persist a dedicated integrity audit table yet.
- Dashboard/alert manifests are still not committed (metrics exist and are queryable).
- Backup/restore drill automation remains partially manual, though restore runbook now includes integrity command usage.

## 5. Next milestone plan

1. Add dashboard/alert definition artifacts for ingest lag, write-to-visible latency, consistency timeouts, and integrity failures.
2. Add scripted backup/restore drill automation to reduce manual runbook steps.
3. Decide whether to persist integrity run history in catalog (`integrity_run`) via ADR.
