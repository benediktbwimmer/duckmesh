#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${DUCKMESH_RESTORE_DRILL_COMPOSE_FILE:-$ROOT_DIR/deployments/local/docker-compose.yml}"
PG_SERVICE="${DUCKMESH_RESTORE_DRILL_PG_SERVICE:-postgres}"
PG_USER="${DUCKMESH_RESTORE_DRILL_PG_USER:-postgres}"
PG_DB="${DUCKMESH_RESTORE_DRILL_PG_DB:-postgres}"

INTEGRITY_API_URL="${DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL:-}"
INTEGRITY_TENANT_ID="${DUCKMESH_RESTORE_DRILL_TENANT_ID:-tenant-dev}"
INTEGRITY_API_KEY="${DUCKMESH_RESTORE_DRILL_API_KEY:-${DUCKMESH_API_KEY:-}}"

DUMP_DIR="${DUCKMESH_RESTORE_DRILL_DUMP_DIR:-$ROOT_DIR/.tmp/restore-drill}"
KEEP_DUMP="${DUCKMESH_RESTORE_DRILL_KEEP_DUMP:-false}"
DRY_RUN=false

usage() {
  cat <<USAGE
Usage: scripts/restore_drill.sh [--dry-run]

Runs a local backup/restore verification drill:
1. Takes a pg_dump backup of the source catalog database.
2. Restores backup into a temporary verification database.
3. Compares key catalog table existence/counts source vs restored DB.
4. Verifies migration version metadata parity.
5. Runs relational consistency checks on restored catalog.
6. Optionally runs API integrity check when DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL is set.

Environment overrides are supported via DUCKMESH_RESTORE_DRILL_* variables.
USAGE
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

mkdir -p "$DUMP_DIR"

timestamp="$(date +%Y%m%d%H%M%S)"
restore_db="duckmesh_restore_drill_${timestamp}"
dump_path="$DUMP_DIR/catalog_${timestamp}.dump"
restore_db_created=false

log() {
  printf '[restore-drill] %s\n' "$*"
}

run_cmd() {
  local cmd="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "[dry-run] $cmd"
    return 0
  fi
  bash -lc "$cmd"
}

pg_exec_scalar() {
  local db_name="$1"
  local query="$2"
  if [[ "$DRY_RUN" == "true" ]]; then
    printf '[restore-drill] [dry-run] scalar query on %s: %s\n' "$db_name" "$query" >&2
    echo "0"
    return 0
  fi
  docker compose -f "$COMPOSE_FILE" exec -T "$PG_SERVICE" \
    psql -U "$PG_USER" -d "$db_name" -tA -v ON_ERROR_STOP=1 -c "$query" | tr -d '[:space:]'
}

pg_migration_versions() {
  local db_name="$1"
  if [[ "$(pg_exec_scalar "$db_name" "SELECT CASE WHEN to_regclass('public.duckmesh_schema_migrations') IS NULL THEN 0 ELSE 1 END;")" != "1" ]]; then
    echo ""
    return 0
  fi
  pg_exec_scalar "$db_name" "SELECT COALESCE(string_agg(version::text, ',' ORDER BY version), '') FROM duckmesh_schema_migrations;"
}

cleanup() {
  if [[ "$restore_db_created" == "true" && "$DRY_RUN" != "true" ]]; then
    docker compose -f "$COMPOSE_FILE" exec -T "$PG_SERVICE" \
      psql -U "$PG_USER" -d postgres -v ON_ERROR_STOP=1 -c "DROP DATABASE IF EXISTS \"$restore_db\" WITH (FORCE);" >/dev/null 2>&1 || true
  fi
  if [[ "$KEEP_DUMP" != "true" && "$DRY_RUN" != "true" ]]; then
    rm -f "$dump_path" || true
  fi
}
trap cleanup EXIT

if [[ "$DRY_RUN" != "true" ]]; then
  if ! docker compose -f "$COMPOSE_FILE" ps --services --status running | grep -qx "$PG_SERVICE"; then
    log "postgres service '$PG_SERVICE' is not running (compose: $COMPOSE_FILE)"
    exit 1
  fi
  ready=false
  for _ in $(seq 1 60); do
    if docker compose -f "$COMPOSE_FILE" exec -T "$PG_SERVICE" pg_isready -U "$PG_USER" -d "$PG_DB" >/dev/null 2>&1; then
      ready=true
      break
    fi
    sleep 1
  done
  if [[ "$ready" != "true" ]]; then
    log "postgres service '$PG_SERVICE' did not become ready in time"
    exit 1
  fi
fi

log "creating catalog backup: $dump_path"
run_cmd "docker compose -f \"$COMPOSE_FILE\" exec -T \"$PG_SERVICE\" pg_dump -U \"$PG_USER\" -d \"$PG_DB\" -Fc > \"$dump_path\""

log "creating restore verification database: $restore_db"
run_cmd "docker compose -f \"$COMPOSE_FILE\" exec -T \"$PG_SERVICE\" psql -U \"$PG_USER\" -d postgres -v ON_ERROR_STOP=1 -c \"CREATE DATABASE \\\"$restore_db\\\";\""
restore_db_created=true

log "restoring backup into verification database"
run_cmd "cat \"$dump_path\" | docker compose -f \"$COMPOSE_FILE\" exec -T \"$PG_SERVICE\" pg_restore -U \"$PG_USER\" -d \"$restore_db\" --clean --if-exists --no-owner --no-privileges"

log "comparing key catalog counts source vs restored"
for table in tenant table_def ingest_event snapshot data_file snapshot_file; do
  source_exists="$(pg_exec_scalar "$PG_DB" "SELECT CASE WHEN to_regclass('public.${table}') IS NULL THEN 0 ELSE 1 END;")"
  restore_exists="$(pg_exec_scalar "$restore_db" "SELECT CASE WHEN to_regclass('public.${table}') IS NULL THEN 0 ELSE 1 END;")"
  if [[ "$source_exists" != "$restore_exists" ]]; then
    log "table existence mismatch for '${table}' (source=${source_exists} restored=${restore_exists})"
    exit 1
  fi
  source_count="0"
  restore_count="0"
  if [[ "$source_exists" == "1" ]]; then
    source_count="$(pg_exec_scalar "$PG_DB" "SELECT COUNT(*) FROM ${table};")"
    restore_count="$(pg_exec_scalar "$restore_db" "SELECT COUNT(*) FROM ${table};")"
  fi
  log "table=${table} source=${source_count} restored=${restore_count}"
  if [[ "$source_count" != "$restore_count" ]]; then
    log "count mismatch for table '${table}'"
    exit 1
  fi
done

log "verifying migration version metadata parity"
source_versions="$(pg_migration_versions "$PG_DB")"
restore_versions="$(pg_migration_versions "$restore_db")"
log "migration_versions source='${source_versions}' restored='${restore_versions}'"
if [[ "$source_versions" != "$restore_versions" ]]; then
  log "migration version mismatch"
  exit 1
fi

log "running restored catalog consistency checks"
check_zero() {
  local label="$1"
  local query="$2"
  local value
  value="$(pg_exec_scalar "$restore_db" "$query")"
  log "check=${label} value=${value}"
  if [[ "$value" != "0" ]]; then
    log "consistency check failed: ${label}"
    exit 1
  fi
}

full_catalog_schema=true
for table in snapshot_file data_file snapshot_table_watermark snapshot table_def; do
  if [[ "$(pg_exec_scalar "$restore_db" "SELECT CASE WHEN to_regclass('public.${table}') IS NULL THEN 0 ELSE 1 END;")" != "1" ]]; then
    full_catalog_schema=false
    break
  fi
done

if [[ "$full_catalog_schema" == "true" ]]; then
  check_zero "missing_snapshot_file_data_file_refs" "SELECT COUNT(*) FROM snapshot_file sf LEFT JOIN data_file df ON df.file_id = sf.file_id WHERE df.file_id IS NULL;"
  check_zero "missing_snapshot_watermark_refs" "SELECT COUNT(*) FROM snapshot_table_watermark stw LEFT JOIN snapshot s ON s.snapshot_id = stw.snapshot_id LEFT JOIN table_def td ON td.table_id = stw.table_id WHERE s.snapshot_id IS NULL OR td.table_id IS NULL;"
  check_zero "invalid_snapshot_parents" "SELECT COUNT(*) FROM snapshot s LEFT JOIN snapshot parent ON parent.snapshot_id = s.parent_snapshot_id WHERE s.parent_snapshot_id IS NOT NULL AND parent.snapshot_id IS NULL;"
else
  log "skipping relational consistency checks (catalog schema tables not present)"
fi

if [[ -n "$INTEGRITY_API_URL" ]]; then
  log "running API integrity check via ${INTEGRITY_API_URL}"
  integrity_cmd="cd \"$ROOT_DIR\" && go run ./cmd/duckmeshctl -base-url \"$INTEGRITY_API_URL\" -tenant-id \"$INTEGRITY_TENANT_ID\""
  if [[ -n "$INTEGRITY_API_KEY" ]]; then
    integrity_cmd+=" -api-key \"$INTEGRITY_API_KEY\""
  fi
  integrity_cmd+=" integrity-run"
  run_cmd "$integrity_cmd"
else
  log "skipping API integrity check (set DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL to enable)"
fi

log "restore drill succeeded"
log "verification database cleaned up: $restore_db"
if [[ "$KEEP_DUMP" == "true" ]]; then
  log "backup retained at: $dump_path"
fi
