# BUG-047 factor lookback inference window

- Module: strategy_package
- Level: L0
- Date: 2026-05-18T12:02:58
- Git commit: 08ece26 (pre-commit baseline; final fix commit recorded in BUG after commit)
- Operator: lc999

## Scope

- Changed files: `backend/data_service/preprocessor.py`, `backend/inference_engine.py`, `backend/tests/test_inference_strict_scoring_alignment.py`, BUG-047 registry JSON.
- Impacted flows: StrategyPackage local live inference data-window selection before factor scoring.
- Business goal: 120d/250d factor packages load enough trading days from DB and fail only when history is truly missing.
- Out of scope: production backend/frontend restart, live order execution, StrategyPackage asset mutation, DB schema/migration.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, QE artifact, HMM snapshot, selection artifact, or paper ledger files modified.

## Environment

- Backend port: not started
- Frontend port: not started
- TDX port: not used
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`
- Database: not connected by tests; trading-calendar behavior mocked
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Factor lookback parsing | `m_turnover_percentile_250d` and `m_atr_percentile_250d` require 250 trading days; `ROC60` remains 61 | `test_factor_lookback_infers_dynamic_250d_suffix`, `test_required_data_window_uses_largest_factor_lookback` | Pass |
| Trading-calendar start-date | inference start date is resolved by N trading sessions, not natural-day multiplier | `test_resolve_inference_start_date_uses_trading_calendar_offset` | Pass |
| Strict fail-fast | strict inference fails if trading calendar cannot provide enough history | `test_resolve_inference_start_date_strict_fails_without_calendar_history` | Pass |
| Legacy compatibility | natural-day helper env validation remains covered | existing natural-day tests | Pass |
| Asset safety | no protected assets changed | `git status --short`, changed-file review | Pass |

## Commands

```bash
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_inference_strict_scoring_alignment.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/data_service/preprocessor.py backend/inference_engine.py
git diff --check
```

## Evidence

- Test result: `10 passed in 0.63s` after adding `m_roc120d` coverage.
- Compile result: `py_compile` exited 0 for `backend/data_service/preprocessor.py` and `backend/inference_engine.py`.
- Diff hygiene: `git diff --check` exited 0.
- Business output summary: local inference now derives factor window from factor names and resolves `start_date` by `market.trading_calendar`; cache entries with insufficient trading days are bypassed and reloaded.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial new regression expected Jan 6 but function returned Jan 7 | Test expectation counted one extra trading day; inclusive N=required+buffer means offset `N-1` | Corrected expected start date to Jan 7 for 7 inclusive trading sessions | Final pytest: `10 passed in 0.63s` |

## Result

- Final status: passed
- Remaining risks: full live StrategyPackage package inference against real DB was not executed in this validation slice to avoid production/runtime side effects; covered logic is unit-tested with mocked DB calendar.
- Need production backend restart: no during validation; user can restart backend later to activate merged code in runtime.
- Need dev service restart: no dev service was started.
