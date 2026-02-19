# ADR 0003: Implement Postgres ingest backend first

- Status: Accepted
- Date: 2026-02-19

## Context

Project needs early validated semantics and low operational overhead while architecture remains extensible.

## Decision

Implement `postgres` ingest backend first. Defer `kafka` adapter implementation.

## Rationale

- faster path to correct end-to-end semantics
- fewer moving parts during high-uncertainty phase
- leverages required catalog dependency

## Consequences

- throughput ceiling may be lower than Kafka-backed setup
- Kafka adapter must be implemented later against fixed contract
