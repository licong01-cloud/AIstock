# Codex -> Claude Review: Protected Asset Ledger Prod Executor

Date: 2026-05-11
From: Codex App
To: Claude Code / strategy
Branch: `codex/qe-governance-integration-20260509`
Task: Task 5 protected asset ledger production executor

## Status

Task 5 implementation is ready for review. Production was not touched.

## Files Added / Updated

- `scripts/protected_asset_ledger_backfill_prod_executor.py`
- `backend/tests/scripts/test_protected_asset_ledger_backfill_prod_executor.py`
- `docs/operations/r6_prod_apply_runbook_20260511.md`
- `docs/cross_tool/20260511_codex_to_claude_INFO_protected_asset_ledger_prod_executor_docs.md`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_backfill_prod_executor_fixture.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_apply_prod_dev_preview_offline.json`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_prod_executor_cli_assumptions.json.md`
- `tests/aistock_validation/dry_runs/20260511_evidence_backfill_prod_executor_dry_run/protected_asset_ledger_prod_executor_dry_run_notes.md`

## Safety Contract

- Default mode is offline dry-run and opens no DB connection.
- `--apply` is blocked before connect unless the exact token, prod env flag, mutex env, prod target triple, verified DR snapshot, plan SHA, and scoped operator confirmation are all present.
- Writes are scoped to `strategy_pkg.package_asset` only.
- Natural key is `(package_id, asset_type, asset_ref)` with `asset_type='protected_asset_ledger_evidence'` and `asset_ref='governance/protected_asset_ledger_backfill'`.
- Per-package transaction: lock/check package row, compare manifest/status, compare existing natural key payload, insert or report idempotent exact match, commit package; rollback and stop on failure.
- No DDL, no prod/dev DB execution, no production services, no `8001/3000`, no main merge.

## Dry-Run Artifacts

- `protected_asset_ledger_backfill_prod_executor_fixture.json`: offline reviewed 4-package ledger plan fixture.
- `protected_asset_ledger_apply_prod_dev_preview_offline.json`: executor output produced from fixture; `db_connection_opened=false`, `db_writes_executed=false`, `ddl=false`, `production_services_touched=false`, `rows_inserted=0`.

## Notes

- The existing dev-locked `scripts/protected_asset_ledger_backfill.py` was intentionally not modified.
- The strategy-package prod executor was intentionally not modified.
- Runbook section 7.3 now points to the protected asset ledger prod executor and exact env/token names.
