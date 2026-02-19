# ADR 0004: Read-after-write via visibility token barriers

- Status: Accepted
- Date: 2026-02-19

## Context

Product requirement prioritizes freshest practical data and explicit read-after-write correctness.

## Decision

Use visibility tokens and snapshot watermarks. Queries may request `min_visibility_token` and block until visible or timeout.

## Rationale

- clear deterministic semantics
- avoids query-time merge complexity over uncommitted queue data
- aligns with micro-batch materialization model

## Consequences

- strict reads may incur bounded wait
- requires robust watermark publication and timeout handling
