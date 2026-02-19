# Handoff Brief for Implementation Agent

You are implementing DuckMesh from spec. This project is expected to become a **complete, refined application**, not a throwaway prototype.

## Mandatory constraints

1. Keep architecture queue-backend-agnostic.
2. Implement only `postgres` ingest bus in first pass.
3. Preserve compatibility surface for future `kafka` bus.
4. Enforce read-after-write consistency via visibility barriers.
5. Do not silently weaken consistency semantics.
6. No feature merges without tests and docs updates.

## Source of truth

- Product requirements: `docs/PRD.md`
- System architecture: `docs/ARCHITECTURE.md`
- Freshness/consistency semantics: `docs/CONSISTENCY.md`
- External API contract: `docs/API.md`
- Storage/catalog schema: `docs/DATA_MODEL.md`
- Operational and SRE requirements: `docs/OPERATIONS.md`
- Security model: `docs/SECURITY.md`
- Delivery sequence + gates: `docs/ROADMAP.md`
- Architecture decisions: `docs/adr/*.md`

## Definition of done (global)

A release candidate is done only when all are true:

- Functional requirements in PRD are met.
- OpenAPI spec matches actual behavior.
- Read-after-write guarantees are proven with automated tests.
- SLO instrumentation is present and exported.
- Failure recovery scenarios (worker crash, lease timeout, restart) are validated.
- Backfill/replay, compaction, and snapshot integrity checks exist.
- Security controls and auth flows are validated.
- Runbooks exist for deploy, rollback, restore, and incident response.

## Minimum implementation milestones

1. Foundation (repo, config, logging, migrations, auth skeleton)
2. Ingest path (Postgres bus, idempotency, leases)
3. Commit coordinator (micro-batch writes, snapshot publication)
4. Query path (snapshot selection, barrier wait, SQL execution)
5. Compaction + retention + GC
6. Observability + SLOs + load tests
7. Hardening (security, backup/restore, chaos/failure tests)
8. UX layer (CLI + admin endpoints; optional UI)

## Quality bar

- Prefer explicit contracts over convenience hacks.
- Every major behavior must have integration tests.
- Every tuning knob must have safe defaults.
- Every background process must be restart-safe and idempotent.

If spec ambiguity exists: create/update an ADR before implementation divergence.
