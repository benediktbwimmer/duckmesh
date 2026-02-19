# Penetration Test Checklist

## AuthN/AuthZ

- Verify unauthenticated access is denied when `DUCKMESH_AUTH_REQUIRED=true`.
- Attempt role escalation from `query_reader` to `ops_admin`.
- Attempt cross-tenant access by overriding tenant headers.

## API abuse

- Replay ingest requests with same idempotency key at high concurrency.
- Fuzz ingest and query payload sizes and malformed JSON.
- Validate error responses never leak secrets or DSN credentials.

## Query safety

- Attempt non-read-only SQL through `/v1/query`.
- Attempt relation access outside tenant-visible tables.
- Validate consistency timeout behavior does not leak other-tenant state.

## Operational endpoints

- Verify `/v1/lag`, `/v1/compaction/run`, `/v1/retention/run` require `ops_admin`.
- Attempt endpoint access with missing/mismatched tenant context.

## Infrastructure

- Validate TLS and credential handling in deployment.
- Run dependency/container vulnerability scan in CI.
- Validate backup artifacts are encrypted and access-controlled.
