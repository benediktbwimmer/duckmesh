# Implementation Blueprint (Go)

## Suggested package layout

```text
cmd/
  duckmesh-api/
  duckmesh-coordinator/
  duckmesh-compactor/
internal/
  app/
  config/
  auth/
  api/
  domain/
    ingest/
    snapshot/
    query/
  bus/
    postgres/
    kafka/         # contract stub initially
  catalog/
    postgres/
  storage/
    s3/
  query/
    duckdb/
  coordinator/
  compactor/
  observability/
  migrations/
```

## Domain contracts to implement first

1. `IngestBus`
2. `CatalogRepo`
3. `ObjectStore`
4. `QueryEngine`
5. `Clock` and `IDGenerator` abstractions for deterministic tests

## Testing pyramid

- Unit: business rules and edge cases
- Integration: Postgres + MinIO + DuckDB end-to-end flows
- E2E: API behavior + consistency guarantees
- Load: ingest/query concurrency and lag behavior
- Fault injection: worker crashes, lease expiry, object store errors

## Required toolchain

- Go stable toolchain
- golangci-lint
- sqlc
- goose/atlas for migrations
- testcontainers-go

## CI minimum jobs

1. lint
2. unit tests
3. integration tests (dockerized dependencies)
4. race detector tests
5. basic load regression test
6. vulnerability scan

## First implementation milestone checklist

- [ ] Bootstrap module and Makefile
- [ ] Wire config and logger
- [ ] Implement migrations and catalog schema
- [ ] Implement PostgresBus claim/ack workflow
- [ ] Implement ingest endpoint with idempotency
- [ ] Implement coordinator micro-batch commit
- [ ] Implement visibility watermark publication
- [ ] Implement query endpoint with barrier wait
- [ ] End-to-end read-after-write test green
