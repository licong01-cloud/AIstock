# BUG-118 Local Data Tushare Trading-Day Sync Validation

- Date: 2026-05-25
- Worktree: `F:\Dev\AIstock_worktrees\BUG-118-local-data-tushare-trade-date-datasets-still-syn-20260525`
- Branch: `bug/BUG-118-local-data-tushare-trade-date-datasets-still-syn-20260525`
- Issue: BUG-118 / GitHub #201
- Module: `local_data_management`

## Scope Fixed

- Marked Tushare `trade_date` datasets `daily_basic`, `adj_factor`, `bak_basic`, `margin_detail`, and `stk_limit` as `trading_day_only=True`.
- Kept `stock_st_events` as calendar-day `pub_date` semantics.
- Kept `suspend_d` special replace/empty-valid behavior unchanged.
- Routed Tushare engine BY_DATE trading-day sequences through the trading calendar service helper, while keeping an existing-connection variant for in-transaction sync. The helper validates complete calendar-row coverage before allowing an empty non-trading window.
- Routed Local Data scheduler and ingestion auto-range latest/next trading-day decisions through `TradingCalendarStatusService` helpers instead of local MAX/MIN trading-calendar SQL for the affected auto-range paths.
- Did not add `stk_limit` or `margin_detail` to zero-row-valid datasets, preserving fail-fast behavior for true trading-day empty-invalid results.
- Updated ingestion auto-range calendar errors to fail fast with structured `DATA_UNAVAILABLE` details instead of silently falling back when the canonical service reports missing calendar rows.

## Design Compliance Matrix

| Requirement | Implementation refs | Evidence | Status | Gap / exception |
|---|---|---|---|---|
| `stk_limit` and `margin_detail` skip weekend/holiday BY_DATE sync dates | `backend/services/tushare_dataset_specs.py`, `backend/services/tushare_sync_engine.py` | `test_sync_by_date_skips_non_trading_days_for_stk_limit_and_margin_detail` | PASS | None |
| `daily_basic`, `adj_factor`, `bak_basic`, `stk_limit`, `margin_detail` use trading-day policy | `backend/services/tushare_dataset_specs.py` | `test_trade_date_tushare_specs_use_trading_day_sequence` | PASS | None |
| `stock_st_events` calendar-day events not reclassified | `backend/services/tushare_dataset_specs.py` | `test_trade_date_tushare_specs_use_trading_day_sequence`, scheduler calendar dataset test | PASS | None |
| `suspend_d` special refresh behavior not silently reclassified | `backend/services/tushare_dataset_specs.py`, `backend/ingestion/tdx_scheduler.py` | `test_trade_date_tushare_specs_use_trading_day_sequence`, suspend_d refresh tests | PASS | None |
| Scheduler/auto-range uses canonical trading-day service for trade-date datasets | `backend/ingestion/tdx_scheduler.py`, `backend/routers/ingestion.py`, `backend/services/trading_calendar_status.py` | scheduler auto-range tests and ingestion router auto-range tests | PASS | None |
| Preserve fail-fast for true trading-day 0-row results | `backend/services/tushare_sync_engine.py`, `backend/services/trading_calendar_status.py`, `backend/routers/ingestion.py` | zero-row-valid assertions for `stk_limit` / `margin_detail`; missing calendar-row service/router fail-fast tests | PASS | None |
| Do not repair historical audit rows | N/A | No DB write commands executed | PASS | Historical failed rows remain until separately approved |

## Validation Commands

- `python -m py_compile backend/services/trading_calendar_status.py backend/services/tushare_sync_engine.py backend/services/tushare_dataset_specs.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py backend/tests/test_ingestion_data_stats_readiness_api.py`
  - Result: PASS
- `python -m pytest backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py -q`
  - Result: PASS, 46 passed in 13.09s after syncing latest `origin/main` and adding missing-calendar-row fail-fast regressions.
- `python -m pytest backend/tests/test_ingestion_data_stats_readiness_api.py -q`
  - Result: PASS, 5 passed in 16.53s

## Additional Validation Commands

- `python F:\Dev\AIstock_artifacts\bug118_query_audit.py`
  - Result: PASS, `failed_empty_invalid_since=2026-05-25 count=0` for `stk_limit` and `margin_detail`.
- `python -m nox -s data_sync_autonomy_backend`
  - Result: PASS, 77 passed in 13.75s.
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0`
  - Result: PASS, 8 passed; module ownership scan mapped 12/12 files after latest origin/main sync.
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0`
  - Result: PASS after latest origin/main sync; guardrails reported no blocking findings, with existing P2/P0 baseline findings only.
- `C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend`
  - Result: PASS, 200 passed in 45.15s; coverage line 79.39, branch 61.21 after latest origin/main sync.
- `python -m nox -s local_data_management_audit`
  - Result: PASS, 11 passed in 0.45s plus local data audit schema smoke PASS after latest origin/main sync.
- `python -m nox -s qe_read_backend`
  - Result: PASS, 11 passed in 7.75s after latest origin/main sync.
- `QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3`
  - Result: FAIL before backend tests on existing unrelated QE frontend guardrail HIGH `frontend/src/app/quantevolver/evolution/page.tsx:44`.
- `python -m nox -s paper_v2_data_quality`
  - Result: PASS after latest origin/main sync; data-quality smoke PASS with one legacy ledger WARN.
- `python -m nox -s paper_v2_backend`
  - Result: FAIL, existing unrelated `backend/tests/strategy_package/test_manifest_alpha_core_boundary.py::test_alpha_core_paper_portfolio_requires_manifest_or_explicit_execution_policy` did not raise expected `StrategyPackageValidationError`; 486 passed, 1 skipped, 2 xfailed.
- `python -m nox -s data_quality_deep`
  - Result: PASS, 10 passed, 21 skipped after latest origin/main sync.

## Latest Sync Validation

- Synced branch to latest `origin/main` commit `4bc2b89` on 2026-05-25 before final validation; backup created at `F:\Dev\AIstock_backups\BUG-118-pre-sync-20260525-160012`.
- `python -m pytest backend/tests/paper_trading_v2/test_trading_calendar_status.py backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_ingestion_router_auto_range.py -q`
  - Result: PASS, 32 passed in 12.55s.
- `python -m pytest backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py -q`
  - Result: PASS, 46 passed in 11.04s.
- `python -m py_compile backend/services/trading_calendar_status.py backend/services/tushare_sync_engine.py backend/services/tushare_dataset_specs.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py backend/tests/test_ingestion_data_stats_readiness_api.py`
  - Result: PASS.
- `python -m pytest backend/tests/test_ingestion_data_stats_readiness_api.py -q`
  - Result: PASS, 5 passed in 12.91s.

## Gates

- `production_ddl_gate=noop` - no schema or migration change.
- `production_frontend_dependency_gate=noop` - no frontend dependency change.
- `production_backend_dependency_gate=noop` - no backend dependency change.
- Production runtime: untouched; no backend/frontend restart.
- DB writes: none. Read-only DB audit query returned `failed_empty_invalid_since=2026-05-25 count=0` for `stk_limit` and `margin_detail`.

## Required Plan Notes

- `qe_read_l3` was attempted with `QE_READ_L3_SKIP_UI=1` but failed before backend tests on an existing QE frontend guardrail HIGH (`frontend/src/app/quantevolver/evolution/page.tsx:44`), unrelated to this local-data fix. The scoped backend equivalent `qe_read_backend` passed.
- Full `paper_v2_l3` was not rerun because `paper_v2_backend` currently fails on existing strategy-package regression `test_alpha_core_paper_portfolio_requires_manifest_or_explicit_execution_policy`; `paper_v2_data_quality` and `data_quality_deep` passed for data-readiness coverage.

## Post-merge-origin/main Validation - 2026-05-26

- Synced BUG-118 branch with latest `origin/main` commit `b7649f63` using a merge commit after first committing the BUG-118 task files. No merge conflicts occurred.
- Backup branch before sync: `backup/BUG-118-pre-origin-main-sync-20260526-002941`.
- Final branch head after sync and validation: `e9e58b50`.
- Temporary validation artifacts generated by broad L3 record commands were moved out of the repo to `F:\Dev\AIstock_artifacts\BUG-118\validation-generated-20260526-0045` and were not staged.

### Re-run Commands And Results

- `git diff --check`
  - Result: PASS.
- `python -m py_compile backend/services/trading_calendar_status.py backend/services/tushare_sync_engine.py backend/services/tushare_dataset_specs.py backend/ingestion/tdx_scheduler.py backend/routers/ingestion.py backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py backend/tests/test_ingestion_data_stats_readiness_api.py`
  - Result: PASS.
- `python -m pytest backend/tests/test_tushare_sync_engine.py backend/tests/ingestion/test_tdx_scheduler_state_reconciliation.py backend/tests/ingestion/test_ingestion_router_auto_range.py backend/tests/paper_trading_v2/test_trading_calendar_status.py -q`
  - Result: PASS, 46 passed in 8.50s.
- `python -m pytest backend/tests/test_ingestion_data_stats_readiness_api.py -q`
  - Result: PASS, 5 passed in 10.69s.
- `python -m nox -s data_sync_autonomy_backend`
  - Result: PASS, 77 passed in 12.26s; session successful in 15s.
- `python -m nox -s validation_module_registry_l0`
  - Result: PASS, 8 passed in 1.28s; module ownership scan mapped 12/12 files.
- `python -m nox -s local_data_management_audit`
  - Result: PASS, 11 passed in 0.33s plus local dev DB schema smoke PASS.
- `python -m nox -s validation_center_backend`
  - Result: PASS, 210 passed, coverage line 79.39 and branch 61.21.
- `python -m nox -s l0`
  - Result: PASS; blocking=0. Existing baseline/P2 guardrail findings remain outside BUG-118 scope.
- `python -m nox -s qe_read_backend`
  - Result: PASS, 11 passed in 8.33s.
- `QE_READ_L3_SKIP_UI=1 python -m nox -s qe_read_l3`
  - Result: FAIL on existing unrelated QE frontend guardrail HIGH `frontend/src/app/quantevolver/evolution/page.tsx:44`; same pre-existing blocker as earlier, before backend tests.
- `python -m nox -s paper_v2_backend`
  - Result: PASS, 495 passed, 1 skipped, 2 xfailed in 32.55s.
- `python -m nox -s paper_v2_data_quality`
  - Result: PASS; data quality smoke passed with legacy Paper v2 ledger WARN only.
- `python -m nox -s data_quality_deep`
  - Result: PASS, 10 passed, 21 skipped in 1.35s.
- `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3`
  - Result: PASS. Ran `paper_v2_l3`, `l0`, `paper_v2_backend`, `paper_v2_data_quality`, and `data_quality_deep`; UI leg intentionally skipped to avoid dev-service startup and production-port risk.

### DB Audit Result

- Read-only DB audit query for `stk_limit` and `margin_detail` empty-invalid failures since 2026-05-25:
  - Total trading-day/non-trading-day empty-invalid rows: 1.
  - Non-trading-day empty-invalid rows: 0.
  - Missing-calendar rows: 0.
  - The one remaining row is `margin_detail` on trading day `2026-05-25`; this is an intentional fail-fast case for a true trading day and is not the BUG-118 weekend/holiday false-failure case.

### Final Production Gates

- `production_ddl_gate=noop` - no migration or schema change.
- `production_frontend_dependency_gate=noop` - no frontend dependency change.
- `production_backend_dependency_gate=noop` - no backend dependency change.
- Production backend/frontend: untouched; no restart of `8001` or `3000`.
- Production DB/DDL: untouched; only read-only audit queries were executed.

