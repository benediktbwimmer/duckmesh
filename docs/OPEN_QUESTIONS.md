# Open Questions (to resolve before full implementation lock)

1. Token scope model:
   - global token across tenant?
   - per table token?
   - hybrid model?

2. Upsert semantics:
   - key definition per table mandatory?
   - how to resolve conflicting updates in same batch?

3. Query safety:
   - SQL allowlist/denylist depth
   - max execution time and memory enforcement strategy

4. Snapshot granularity:
   - global tenant snapshot per commit cycle
   - per-table snapshots with composed query resolution

5. Compaction policy defaults:
   - target file size
   - trigger thresholds
   - time windows by table profile

6. Multi-region future path:
   - catalog replication strategy
   - read replica freshness semantics

7. UX surface for first public release:
   - CLI only?
   - minimal web admin?

8. Licensing/business model:
   - OSS license choice
   - hosted differentiators
