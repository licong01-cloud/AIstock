# Production DDL Gate - Multi-Alpha - 2026-06-29

## Scope

- User authorization: explicit current-session approval to execute production DDL for exactly three multi-alpha migrations.
- Git HEAD: `70c27b8c1c347572984ec6bfdf93da8caa8f53b0`.
- Target DB: `.env` `TDX_DB_*`, database `aistock`, server `172.17.0.3/32:5432`, user `postgres`; secrets not printed.
- Production runtime touched: false; backend/frontend/TDX/worker were not started, stopped, or restarted.
- DML correction: the 2026-06-29 DDL gate's only migration-embedded `UPDATE` had `update_would_affect=0` before execution and `0` after execution, but an earlier #1709 Phase A production backfill write did occur and is recorded below.

## Applied DDL

1. `backend/migrations/strategy_pkg_multi_alpha_paper_admission_20260628.sql` -> PASS, verified table, unique constraint, broker CHECK, partial lookup index.
2. `backend/migrations/qe_archive_multi_alpha_phase_a_20260628.sql` -> PASS, verified five `qe_archive.multi_alpha_*` tables and `partial_failed` status constraints.
3. `backend/migrations/strategy_pkg_multi_alpha_combine_source_type_20260629.sql` -> PASS, verified `package_source_type_check` includes `multi_alpha_combine_run`.

## Evidence

- Handoff record: `docs/handoff/multi_alpha_ddl_apply_record_20260629.md`.
- Committed raw output: `tests/aistock_validation/history/production_ddl/20260629_multi_alpha_ddl_apply_production_output.json`.
- Scratch DB dry run: `aistock_codex_multi_alpha_ddl_20260629122032`, forward/idempotent/rollback/final-forward PASS, then dropped.
- Backfill raw evidence: `C:\Users\lc999\Documents\Codex\2026-06-28\alpha-qe-phase-a-macb-qe\work\macb_phase_a_prod_backfill.json` and `macb_phase_a_prod_post_verify.json`.

## Production Backfill Write Correction

- Exact command start: `2026-06-28T17:29:09.665Z` / `2026-06-29 01:29:09.665 +08:00`.
- Command:

```powershell
rtk python scripts/qe_archive_backfill.py --source multi-alpha --limit 500 --write --confirm-write QE_ARCHIVE_WRITE --output "C:\Users\lc999\Documents\Codex\2026-06-28\alpha-qe-phase-a-macb-qe\work\macb_phase_a_prod_backfill.json"
```

- Target DB: production `.env` `TDX_DB_*`, `127.0.0.1:5432/aistock`, `user=postgres`, server identity `172.17.0.3/32:5432`; not scratch.
- Classification: intentional #1709 Phase A `backfill_service` materialization into `qe_archive`, not a scratch mis-target; however, it was outside the later DDL-only reporting boundary and was omitted from the first 2026-06-29 DDL report.
- Rows materialized and verified:
  - `qe_archive.multi_alpha_run=28`
  - `qe_archive.multi_alpha_leg=84`
  - `qe_archive.multi_alpha_leg_source=1226`
  - `qe_archive.multi_alpha_scheme=41`
  - `qe_archive.multi_alpha_loo=79`
  - `qe_archive.run.multi_alpha_combine=28`
  - `strategy_pkg.macb_terminal=28`
- Resolution/parity: `leg_source` resolved `1226/1226`, provenance-complete legs `84/84`, business run parity mismatches `0`, scheme mismatches `0`, LOO mismatches `0`.
- Idempotency: repository writes `qe_archive.run` and `qe_archive.multi_alpha_run` via `ON CONFLICT ... DO UPDATE`, and replace-by-run deletes/reinserts child rows for leg/source/scheme/loo; production post-verify `idempotence_counts.before == after` with `stable=true`.
- Corrected summary: production DML did occur for QE archive backfill rows; no rollback is required because data parity and idempotency checks passed.

## Gates

- production_ddl_gate=applied_and_verified
- production_backend_dependency_gate=noop
- production_frontend_dependency_gate=noop
- production_backend_8001_touched=false
- production_frontend_3000_touched=false
- tdx_19080_touched=false

## User-Owned Follow-Up

Backend restart remains user-owned. After restart, verify the multi-alpha paper dry-run endpoint, QE archive worker event registration, and combine-detail strategy package export flow.
