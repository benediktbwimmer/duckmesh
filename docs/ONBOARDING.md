# DuckMesh Onboarding Guide

## 1. Prerequisites

- Go toolchain matching `go.mod`
- Docker + Docker Compose
- `make`

## 2. Start local stack

```bash
make stack-up
```

## 3. Optional frontend hot-reload dev mode

```bash
make web-dev
```

Enable natural-language SQL generation by setting:

- `DUCKMESH_AI_TRANSLATE_ENABLED=true`
- `DUCKMESH_AI_API_KEY=...`
- optional `DUCKMESH_AI_MODEL` (default `gpt-5`)

## 4. Basic smoke checks

```bash
curl -s localhost:8080/v1/health
curl -s localhost:8080/v1/ready
curl -s localhost:8080/v1/metrics | head
```

Open UI:

- [http://localhost:8080](http://localhost:8080)

## 5. Operator workflows with CLI

Use `duckmeshctl` against local API:

```bash
go run ./cmd/duckmeshctl -tenant-id tenant-dev health
go run ./cmd/duckmeshctl -tenant-id tenant-dev lag
go run ./cmd/duckmeshctl -tenant-id tenant-dev compaction-run
go run ./cmd/duckmeshctl -tenant-id tenant-dev retention-run
go run ./cmd/duckmeshctl -tenant-id tenant-dev integrity-run
```

Authentication-enabled environments:

```bash
go run ./cmd/duckmeshctl \
  -base-url https://duckmesh.example.com \
  -api-key "$DUCKMESH_API_KEY" \
  lag
```

## 6. Test and validate

```bash
make ci
make test-integration
make build
make restore-drill
```

Optional API-integrity validation in the drill:

```bash
DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL=http://localhost:8080 make restore-drill
```

## 7. Optional: manage demo ingest producer separately

Start continuous synthetic ingest for demo/testing:

```bash
make demo-up
make demo-status
```

Foreground mode (for debugging):

```bash
make run-demo-producer
```

Stop:

```bash
make demo-down
```

## 8. Shutdown

```bash
make stack-down
```
