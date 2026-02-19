# Security Specification

## 1. Threat model scope

Protect against:

- cross-tenant data leakage
- unauthorized writes/queries
- replay abuse and key compromise
- metadata tampering
- accidental exposure of sensitive payload fields

## 2. Authentication and authorization

- API keys and/or JWT bearer tokens
- tenant identity mandatory on every request
- role model:
  - `ingest_writer`
  - `query_reader`
  - `table_admin`
  - `ops_admin`

## 3. Tenant isolation

- every catalog row tenant-scoped
- no global queries without explicit admin context
- policy checks in API + query planning layers

## 4. Secret management

- no secrets in repo
- runtime secrets via secret manager or env injection
- key hashing (never store raw API key)

## 5. Transport and at-rest security

- TLS in transit
- encrypted object storage
- encrypted Postgres volumes/backups

## 6. Data governance controls

- table-level retention policies
- optional field-level masking in query responses
- audit trail for admin/security-sensitive actions

## 7. Secure defaults

- deny-by-default role assignment
- conservative query limits
- strict input validation and payload size limits
- SQL endpoint restricted to tenant-visible relations

## 8. Security tests

- auth bypass tests
- tenant breakout tests
- replay/idempotency abuse tests
- malicious SQL payload tests
- dependency and container vulnerability scans in CI

## 9. Penetration checklist

- Execute `docs/PENETRATION_TEST_CHECKLIST.md` before release-candidate sign-off.
