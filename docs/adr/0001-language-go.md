# ADR 0001: Use Go for core services

- Status: Accepted
- Date: 2026-02-19

## Context

DuckMesh requires long-running API + worker processes with strong concurrency behavior and production-grade deployment ergonomics.

## Decision

Implement core services in Go.

## Rationale

- robust concurrency model
- low operational footprint
- static binaries and straightforward deployment
- mature ecosystem for Postgres, Kafka, and observability

## Consequences

- team should standardize lint/test/toolchain for Go
- Python may still be used for ancillary tooling/scripts if needed
