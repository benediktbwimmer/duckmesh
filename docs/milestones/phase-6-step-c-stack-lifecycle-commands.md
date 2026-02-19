# Milestone Report: Phase 6 Step C Stack Lifecycle Commands

## 1. What was implemented

- Added one-command lifecycle controls for full local environment:
  - `make stack-up`
  - `make stack-down`
  - `make stack-status`
- Implemented orchestration script:
  - `scripts/stack.sh`
  - supports commands `up|down|status`
  - supports `--dry-run`
  - manages docker dependencies, migrations, tenant bootstrap, web build, API/coordinator/compactor processes, and demo producer startup
  - stores process pid/log files under `.tmp/`
- Updated Makefile command surface and help text.
- Updated onboarding and deployment docs to use stack lifecycle commands.

## 2. Spec sections satisfied

- `docs/ROADMAP.md` Phase 6:
  - improved onboarding and operator ergonomics for starting/stopping full product stack.
- `docs/OPERATIONS.md` deployment requirements:
  - one-command local environment workflow improved to include core services and optional demo producer.

## 3. Test evidence

Validation commands executed successfully:

- `go test ./scripts`
- `go test ./...`
- `make stack-down`
- `make stack-status`

New automated coverage:

- `scripts/stack_test.go`
  - dry-run `up`
  - dry-run `down`
  - unknown command handling

## 4. Known gaps and risks

- `stack-up` defaults to `localhost:5432` for catalog DSN unless overridden; hosts with conflicting local Postgres services should set `DUCKMESH_STACK_CATALOG_DSN`.
- `stack-down` intentionally keeps docker volumes (uses `docker compose down` without `-v`) to avoid accidental data loss.

## 5. Next milestone plan

Phase 7 (Kafka adapter implementation):

1. Implement Kafka backend for `IngestBus`.
2. Reuse semantic parity tests across Postgres and Kafka.
3. Verify unchanged consistency and visibility-token semantics.
4. Publish Kafka tuning and benchmark guidance.
