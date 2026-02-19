#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
COMPOSE_FILE="${DUCKMESH_STACK_COMPOSE_FILE:-$ROOT_DIR/deployments/local/docker-compose.yml}"
PID_DIR="${DUCKMESH_STACK_PID_DIR:-$ROOT_DIR/.tmp}"
API_LOG="${PID_DIR}/duckmesh-api.log"
COORDINATOR_LOG="${PID_DIR}/duckmesh-coordinator.log"
COMPACTOR_LOG="${PID_DIR}/duckmesh-compactor.log"
API_PID="${PID_DIR}/duckmesh-api.pid"
COORDINATOR_PID="${PID_DIR}/duckmesh-coordinator.pid"
COMPACTOR_PID="${PID_DIR}/duckmesh-compactor.pid"

CATALOG_DSN="${DUCKMESH_STACK_CATALOG_DSN:-${DUCKMESH_CATALOG_DSN:-postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable}}"
OBJECTSTORE_ENDPOINT="${DUCKMESH_STACK_OBJECTSTORE_ENDPOINT:-${DUCKMESH_OBJECTSTORE_ENDPOINT:-localhost:9000}}"
OBJECTSTORE_REGION="${DUCKMESH_STACK_OBJECTSTORE_REGION:-${DUCKMESH_OBJECTSTORE_REGION:-us-east-1}}"
OBJECTSTORE_BUCKET="${DUCKMESH_STACK_OBJECTSTORE_BUCKET:-${DUCKMESH_OBJECTSTORE_BUCKET:-duckmesh}}"
OBJECTSTORE_ACCESS_KEY="${DUCKMESH_STACK_OBJECTSTORE_ACCESS_KEY:-${DUCKMESH_OBJECTSTORE_ACCESS_KEY:-minio}}"
OBJECTSTORE_SECRET_KEY="${DUCKMESH_STACK_OBJECTSTORE_SECRET_KEY:-${DUCKMESH_OBJECTSTORE_SECRET_KEY:-miniostorage}}"
OBJECTSTORE_USE_SSL="${DUCKMESH_STACK_OBJECTSTORE_USE_SSL:-${DUCKMESH_OBJECTSTORE_USE_SSL:-false}}"
HTTP_ADDR="${DUCKMESH_STACK_HTTP_ADDR:-${DUCKMESH_HTTP_ADDR:-:8080}}"
ENABLE_DEMO="${DUCKMESH_STACK_ENABLE_DEMO:-true}"
TENANT_ID="${DUCKMESH_STACK_TENANT_ID:-tenant-dev}"
DRY_RUN=false

usage() {
  cat <<USAGE
Usage: scripts/stack.sh <up|down|status> [--dry-run]

Commands:
  up      Start full local stack (docker deps + API + coordinator + compactor + demo producer)
  down    Stop full local stack
  status  Print status for app processes, demo producer, and docker services
USAGE
}

if [[ $# -lt 1 ]]; then
  usage >&2
  exit 2
fi

COMMAND="$1"
shift

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

log() {
  printf '[stack] %s\n' "$*"
}

run_cmd() {
  local cmd="$1"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "[dry-run] $cmd"
    return 0
  fi
  bash -lc "$cmd"
}

start_service() {
  local name="$1"
  local pid_file="$2"
  local log_file="$3"
  local start_cmd="$4"

  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
    log "${name} already running (pid=$(cat "$pid_file"))"
    return 0
  fi

  run_cmd "nohup env DUCKMESH_CATALOG_DSN='$CATALOG_DSN' DUCKMESH_OBJECTSTORE_ENDPOINT='$OBJECTSTORE_ENDPOINT' DUCKMESH_OBJECTSTORE_REGION='$OBJECTSTORE_REGION' DUCKMESH_OBJECTSTORE_BUCKET='$OBJECTSTORE_BUCKET' DUCKMESH_OBJECTSTORE_ACCESS_KEY='$OBJECTSTORE_ACCESS_KEY' DUCKMESH_OBJECTSTORE_SECRET_KEY='$OBJECTSTORE_SECRET_KEY' DUCKMESH_OBJECTSTORE_USE_SSL='$OBJECTSTORE_USE_SSL' DUCKMESH_HTTP_ADDR='$HTTP_ADDR' DUCKMESH_LOG_JSON='false' $start_cmd > '$log_file' 2>&1 & echo \$! > '$pid_file'"
  log "started ${name} (pid_file=${pid_file})"
}

stop_service() {
  local name="$1"
  local pid_file="$2"

  if [[ ! -f "$pid_file" ]]; then
    log "${name} not running (no pid file)"
    return 0
  fi

  local pid
  pid="$(cat "$pid_file")"
  if [[ "$DRY_RUN" == "true" ]]; then
    log "[dry-run] stop ${name} pid=${pid}"
    return 0
  fi

  if kill -0 "$pid" >/dev/null 2>&1; then
    kill "$pid" >/dev/null 2>&1 || true
    log "stopped ${name} (pid=${pid})"
  else
    log "${name} pid not found (pid=${pid})"
  fi
  rm -f "$pid_file"
}

wait_for_postgres() {
  run_cmd "docker compose -f '$COMPOSE_FILE' up -d"
  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  for _ in $(seq 1 60); do
    if docker compose -f "$COMPOSE_FILE" exec -T postgres pg_isready -U postgres -d postgres >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "postgres did not become ready in time"
  exit 1
}

ensure_tenant() {
  run_cmd "docker compose -f '$COMPOSE_FILE' exec -T postgres psql -U postgres -d postgres -v ON_ERROR_STOP=1 -c \"INSERT INTO tenant (tenant_id, name, status) VALUES ('$TENANT_ID', '$TENANT_ID', 'active') ON CONFLICT (tenant_id) DO NOTHING;\""
}

wait_for_api_ready() {
  local ready_url
  if [[ "$HTTP_ADDR" == :* ]]; then
    ready_url="http://127.0.0.1${HTTP_ADDR}/v1/ready"
  else
    ready_url="http://${HTTP_ADDR}/v1/ready"
  fi

  if [[ "$DRY_RUN" == "true" ]]; then
    return 0
  fi
  for _ in $(seq 1 60); do
    if curl -sf "$ready_url" >/dev/null 2>&1; then
      return 0
    fi
    sleep 1
  done
  log "api did not become ready in time"
  exit 1
}

stack_up() {
  mkdir -p "$PID_DIR"
  wait_for_postgres
  run_cmd "cd '$ROOT_DIR' && DUCKMESH_CATALOG_DSN='$CATALOG_DSN' go run ./cmd/duckmesh-migrate -direction up"
  ensure_tenant
  run_cmd "cd '$ROOT_DIR/web' && npm run build"

  start_service "duckmesh-api" "$API_PID" "$API_LOG" "go run '$ROOT_DIR/cmd/duckmesh-api'"
  start_service "duckmesh-coordinator" "$COORDINATOR_PID" "$COORDINATOR_LOG" "go run '$ROOT_DIR/cmd/duckmesh-coordinator'"
  start_service "duckmesh-compactor" "$COMPACTOR_PID" "$COMPACTOR_LOG" "go run '$ROOT_DIR/cmd/duckmesh-compactor'"

  wait_for_api_ready

  local demo_api_url
  if [[ "$HTTP_ADDR" == :* ]]; then
    demo_api_url="http://localhost${HTTP_ADDR}"
  else
    demo_api_url="http://${HTTP_ADDR}"
  fi

  local enable_demo_normalized
  enable_demo_normalized="$(printf '%s' "$ENABLE_DEMO" | tr '[:upper:]' '[:lower:]')"
  if [[ "$enable_demo_normalized" == "true" ]]; then
    run_cmd "cd '$ROOT_DIR' && DUCKMESH_DEMO_API_URL='$demo_api_url' DUCKMESH_DEMO_TENANT_ID='$TENANT_ID' DUCKMESH_DEMO_TABLE='events' make demo-up"
  fi

  log "stack is up"
}

stack_down() {
  run_cmd "cd '$ROOT_DIR' && make demo-down"
  stop_service "duckmesh-api" "$API_PID"
  stop_service "duckmesh-coordinator" "$COORDINATOR_PID"
  stop_service "duckmesh-compactor" "$COMPACTOR_PID"
  run_cmd "docker compose -f '$COMPOSE_FILE' down"
  log "stack is down"
}

service_status() {
  local name="$1"
  local pid_file="$2"
  if [[ -f "$pid_file" ]] && kill -0 "$(cat "$pid_file")" >/dev/null 2>&1; then
    printf '%s running (pid=%s)\n' "$name" "$(cat "$pid_file")"
  else
    printf '%s not running\n' "$name"
  fi
}

stack_status() {
  service_status "duckmesh-api" "$API_PID"
  service_status "duckmesh-coordinator" "$COORDINATOR_PID"
  service_status "duckmesh-compactor" "$COMPACTOR_PID"
  run_cmd "cd '$ROOT_DIR' && make demo-status"
  run_cmd "docker compose -f '$COMPOSE_FILE' ps"
}

case "$COMMAND" in
  up)
    stack_up
    ;;
  down)
    stack_down
    ;;
  status)
    stack_status
    ;;
  *)
    echo "unknown command: $COMMAND" >&2
    usage >&2
    exit 2
    ;;
esac
