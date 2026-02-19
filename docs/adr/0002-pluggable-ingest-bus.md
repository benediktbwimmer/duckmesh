# ADR 0002: Ingest bus must be pluggable

- Status: Accepted
- Date: 2026-02-19

## Context

Initial implementation prefers Postgres for speed of delivery, but future scaling and integration needs may require Kafka.

## Decision

Define and enforce an `IngestBus` abstraction from day one. Core services may not depend on backend-specific APIs.

## Rationale

- protects future optionality
- avoids hard-coupling business semantics to one queue technology
- enables parity testing across backends

## Consequences

- additional interface/test complexity upfront
- easier backend evolution later
