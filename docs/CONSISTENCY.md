# Consistency and Freshness Semantics

## 1. Primary requirement

DuckMesh must provide **as-fresh-as-possible analytics** while supporting explicit read-after-write guarantees.

## 2. Write lifecycle states

1. `accepted`: event validated and accepted by ingest API
2. `durable`: event persisted in ingest bus + catalog staging
3. `visible`: event included in published snapshot watermark

Only `visible` implies query inclusion.

## 3. Visibility token contract

Each accepted write gets a monotonically comparable `visibility_token` (global or tenant/table scoped).

- token is returned in write receipt
- snapshots carry `max_visibility_token`
- query with `min_visibility_token = T` must not run on snapshot with watermark `< T`

## 4. Read-after-write modes

### Mode A: two-step strict mode

1. Write -> receive token `T`
2. Query with `min_visibility_token=T`

### Mode B: one-step strict mode

Write with `wait_for_visibility=true` and timeout; response is returned only when token is visible.

### Mode C: best-effort freshness mode

Write without wait and query without token (lowest latency, no strict guarantee).

## 5. Query barrier algorithm

Given requested token `T`:

1. fetch latest snapshot watermark `W`
2. if `W >= T`, execute query immediately
3. else poll/wait for watermark advancement until timeout
4. on timeout, return explicit `CONSISTENCY_TIMEOUT` with latest watermark

## 6. Why no query-time merge of uncommitted queue events (initially)

Not performed in first product versions due to complexity and correctness risks:

- difficult semantics for joins/aggregations/window functions,
- expensive dedup and ordering at query time,
- hard to keep deterministic behavior.

Fresh micro-batches + visibility barriers deliver clear correctness with simpler ops.

## 7. Micro-batch targets

Configurable defaults:

- batch flush interval: 200–1000 ms
- max events per batch: tune by row size and table
- max write-to-visible target (p95): service SLO-defined

## 8. Edge cases

- duplicate idempotency key: must not produce duplicate visible rows
- out-of-order arrival: ordering is by committed visibility token, not client timestamp
- coordinator crash after file write before snapshot publish: file remains invisible/orphan-candidate until cleanup

## 9. Consistency test matrix (must automate)

1. write-read strict under normal load
2. write-read strict during coordinator restart
3. concurrent writes across tenants
4. duplicate/retry ingest requests
5. timeout behavior correctness
6. snapshot time-travel reproducibility

## 10. Error model

- `CONSISTENCY_TIMEOUT`
- `TOKEN_SCOPE_MISMATCH`
- `SNAPSHOT_NOT_FOUND`
- `VISIBILITY_TOKEN_INVALID`

All consistency errors must include structured metadata for retry/debug.
