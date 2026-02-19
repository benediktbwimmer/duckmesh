# scripts/

Operational helper scripts:

- `restore_drill.sh`
  - automates local backup/restore verification
  - performs source/restored catalog count checks
  - verifies migration metadata parity on restored DB
  - runs relational consistency checks on restored DB
  - optionally executes API integrity validation when `DUCKMESH_RESTORE_DRILL_INTEGRITY_API_URL` is set
  - supports `--dry-run`
