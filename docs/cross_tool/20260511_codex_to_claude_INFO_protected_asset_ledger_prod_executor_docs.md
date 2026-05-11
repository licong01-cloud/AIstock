# Codex -> Claude Handoff: Protected Asset Ledger Prod Executor Docs

Date: 2026-05-11
From: Codex App / Task 5
To: Claude Code / release commander
Branch: `codex/qe-governance-integration-20260509`
Worktree: `F:/Dev/AIstock_worktrees/qe-governance-integration-20260509`

## Scope

Updated R6 production apply runbook section 7.3 and dry-run artifacts for the protected asset ledger production executor.

## Files

- `docs/operations/r6_prod_apply_runbook_20260511.md`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_backfill_prod_executor_fixture.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_apply_prod_dev_preview_offline.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_prod_executor_cli_assumptions.json.md`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_prod_executor_dry_run_notes.md`

## Contract Captured

- Executor path: `scripts/protected_asset_ledger_backfill_prod_executor.py`
- Confirm token: `APPLY_PROTECTED_ASSET_LEDGER_BACKFILL_PROD`
- Apply env: `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_PROD_APPLY_ENABLED`
- Mutex env: `AISTOCK_PROTECTED_ASSET_LEDGER_BACKFILL_MUTEX_HELD`
- Target guard: `--target-db prod --db-port 5432` plus non-dev/non-test DB name.

## Boundaries Preserved

- production_touched=false
- prod_db_touched=false
- dev_db_touched=false
- db_connection_opened=false
- db_writes_executed=false
- dev_locked_scripts_modified=false
- services_restarted=false
