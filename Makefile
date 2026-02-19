.PHONY: help tree spec-check fmt fmt-check tidy lint test test-race test-integration build run-api run-coordinator run-compactor run-cli run-demo-producer demo-up demo-down demo-status stack-up stack-down stack-status web-install web-dev web-build migrate-up migrate-down restore-drill dev-up dev-down ci

GO ?= go
BINS := duckmesh-api duckmesh-coordinator duckmesh-compactor duckmesh-migrate duckmeshctl duckmesh-demo-producer

help:
	@echo "DuckMesh development commands"
	@echo "Targets:"
	@echo "  build         - compile all services"
	@echo "  test          - run Go unit tests"
	@echo "  test-race     - run tests with race detector"
	@echo "  test-integration - run Docker-backed integration tests"
	@echo "  lint          - run golangci-lint"
	@echo "  fmt           - format Go files"
	@echo "  migrate-up    - run DB migrations up"
	@echo "  migrate-down  - run DB migrations down one step"
	@echo "  restore-drill - run automated local backup/restore/integrity verification drill"
	@echo "  run-cli       - run duckmeshctl command-line client"
	@echo "  run-demo-producer - run sample ingest data producer in foreground"
	@echo "  demo-up       - start sample ingest data producer in background"
	@echo "  demo-down     - stop sample ingest data producer background process"
	@echo "  demo-status   - show sample ingest data producer process status"
	@echo "  stack-up      - start full local stack (deps + services + demo producer)"
	@echo "  stack-down    - stop full local stack"
	@echo "  stack-status  - show full local stack status"
	@echo "  web-install   - install frontend dependencies"
	@echo "  web-dev       - run frontend dev server"
	@echo "  web-build     - build frontend into embedded static assets"
	@echo "  dev-up        - start local Postgres + MinIO stack"
	@echo "  dev-down      - stop local dependency stack"
	@echo "  spec-check    - verify required spec files exist"
	@echo "  tree          - show repository tree"

build:
	$(GO) build ./...

test:
	$(GO) test ./...

test-race:
	$(GO) test -race ./...

test-integration:
	@set +e; \
	$(MAKE) dev-up; \
	for i in $$(seq 1 60); do \
		if docker compose -f deployments/local/docker-compose.yml exec -T postgres pg_isready -U postgres -d postgres >/dev/null 2>&1; then \
			break; \
		fi; \
		sleep 1; \
	done; \
	DUCKMESH_TEST_CATALOG_DSN='postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable' \
	DUCKMESH_TEST_S3_ENDPOINT='localhost:9000' \
	DUCKMESH_TEST_S3_REGION='us-east-1' \
	DUCKMESH_TEST_S3_BUCKET='duckmesh-it' \
	DUCKMESH_TEST_S3_ACCESS_KEY='minio' \
	DUCKMESH_TEST_S3_SECRET_KEY='miniostorage' \
	$(GO) test -tags=integration ./internal/migrations ./internal/storage/s3 ./internal/bus/postgres ./internal/api ./internal/coordinator ./internal/maintenance; \
	test_exit=$$?; \
	$(MAKE) dev-down; \
	exit $$test_exit

lint:
	@if command -v golangci-lint >/dev/null 2>&1; then \
		golangci-lint run ./...; \
	else \
		$(GO) run github.com/golangci/golangci-lint/cmd/golangci-lint@v1.63.4 run ./...; \
	fi

fmt:
	gofmt -w $$(find . -type f -name '*.go' -not -path './vendor/*')

fmt-check:
	@test -z "$$(gofmt -l $$(find . -type f -name '*.go' -not -path './vendor/*'))" || (echo "Go files are not formatted" && gofmt -l $$(find . -type f -name '*.go' -not -path './vendor/*') && false)

tidy:
	$(GO) mod tidy

run-api:
	$(GO) run ./cmd/duckmesh-api

run-coordinator:
	$(GO) run ./cmd/duckmesh-coordinator

run-compactor:
	$(GO) run ./cmd/duckmesh-compactor

run-cli:
	$(GO) run ./cmd/duckmeshctl

run-demo-producer:
	$(GO) run ./cmd/duckmesh-demo-producer

demo-up:
	@mkdir -p .tmp
	@if [ -f .tmp/duckmesh-demo-producer.pid ] && kill -0 "$$(cat .tmp/duckmesh-demo-producer.pid)" 2>/dev/null; then \
		echo "duckmesh-demo-producer already running (pid=$$(cat .tmp/duckmesh-demo-producer.pid))"; \
		exit 0; \
	fi
	@nohup env \
		DUCKMESH_DEMO_API_URL="$${DUCKMESH_DEMO_API_URL:-http://localhost:8080}" \
		DUCKMESH_DEMO_API_KEY="$${DUCKMESH_DEMO_API_KEY:-}" \
		DUCKMESH_DEMO_TENANT_ID="$${DUCKMESH_DEMO_TENANT_ID:-tenant-dev}" \
		DUCKMESH_DEMO_TABLE="$${DUCKMESH_DEMO_TABLE:-events}" \
		DUCKMESH_DEMO_PRODUCER_ID="$${DUCKMESH_DEMO_PRODUCER_ID:-demo-producer}" \
		DUCKMESH_DEMO_BATCH_SIZE="$${DUCKMESH_DEMO_BATCH_SIZE:-25}" \
		DUCKMESH_DEMO_INTERVAL="$${DUCKMESH_DEMO_INTERVAL:-1s}" \
		DUCKMESH_DEMO_HTTP_TIMEOUT="$${DUCKMESH_DEMO_HTTP_TIMEOUT:-10s}" \
		DUCKMESH_DEMO_CREATE_TABLE="$${DUCKMESH_DEMO_CREATE_TABLE:-true}" \
		DUCKMESH_DEMO_WAIT_FOR_VISIBILITY="$${DUCKMESH_DEMO_WAIT_FOR_VISIBILITY:-false}" \
		DUCKMESH_DEMO_VISIBILITY_TIMEOUT_MS="$${DUCKMESH_DEMO_VISIBILITY_TIMEOUT_MS:-3000}" \
		DUCKMESH_DEMO_USER_CARDINALITY="$${DUCKMESH_DEMO_USER_CARDINALITY:-200}" \
		DUCKMESH_DEMO_SEED="$${DUCKMESH_DEMO_SEED:-}" \
		$(GO) run ./cmd/duckmesh-demo-producer > .tmp/duckmesh-demo-producer.log 2>&1 & echo $$! > .tmp/duckmesh-demo-producer.pid
	@echo "duckmesh-demo-producer started (pid=$$(cat .tmp/duckmesh-demo-producer.pid), log=.tmp/duckmesh-demo-producer.log)"

demo-down:
	@if [ ! -f .tmp/duckmesh-demo-producer.pid ]; then \
		echo "duckmesh-demo-producer is not running"; \
	else \
		pid="$$(cat .tmp/duckmesh-demo-producer.pid)"; \
		if kill -0 "$$pid" 2>/dev/null; then \
			kill "$$pid"; \
			echo "duckmesh-demo-producer stopped (pid=$$pid)"; \
		else \
			echo "duckmesh-demo-producer process not found (pid=$$pid)"; \
		fi; \
		rm -f .tmp/duckmesh-demo-producer.pid; \
	fi

demo-status:
	@if [ -f .tmp/duckmesh-demo-producer.pid ] && kill -0 "$$(cat .tmp/duckmesh-demo-producer.pid)" 2>/dev/null; then \
		echo "duckmesh-demo-producer running (pid=$$(cat .tmp/duckmesh-demo-producer.pid))"; \
	else \
		echo "duckmesh-demo-producer not running"; \
	fi

stack-up:
	./scripts/stack.sh up

stack-down:
	./scripts/stack.sh down

stack-status:
	./scripts/stack.sh status

web-install:
	cd web && npm install

web-dev:
	cd web && npm run dev

web-build:
	cd web && npm run build

migrate-up:
	$(GO) run ./cmd/duckmesh-migrate -direction up

migrate-down:
	$(GO) run ./cmd/duckmesh-migrate -direction down -steps 1

restore-drill:
	./scripts/restore_drill.sh

dev-up:
	docker compose -f deployments/local/docker-compose.yml up -d

dev-down:
	docker compose -f deployments/local/docker-compose.yml down -v

ci: fmt-check lint test test-race spec-check

tree:
	@find . -maxdepth 3 -type f | sort

spec-check:
	@test -f README.md
	@test -f docs/HANDOFF.md
	@test -f docs/PRD.md
	@test -f docs/ARCHITECTURE.md
	@test -f docs/CONSISTENCY.md
	@test -f docs/API.md
	@test -f docs/DATA_MODEL.md
	@test -f docs/OPERATIONS.md
	@test -f docs/SECURITY.md
	@test -f docs/ROADMAP.md
	@test -f docs/ONBOARDING.md
	@test -f api/openapi.yaml
	@echo "Spec files present."
