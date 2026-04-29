# Paper v2 catchup to live trading-hours validation

- Module: paper_v2_selection_center
- Level: L4
- Date: 2026-04-29T10:35:47+08:00
- Git commit: 59e1c80 (working tree validation changes pending)
- Operator: Codex

## Scope

- Changed files: Paper v2 live/catchup runner validation code, historical replay mark-to-market support, live selection cutoff handling, UI session polling, validation script/nox/skill/matrix.
- Impacted flows: StrategyPackage authoritative selection artifact generation, DB historical replay, catch-up-then-live session, V25 Paper v2 live tick, run console polling, Paper v2 UI regression suite.
- Business goal: prove a StrategyPackage can replay the latest completed historical trading day, switch to 2026-04-29 TDX realtime minute data, process live bars with V25, and persist meaningful orders/fills without silent fallback.
- Out of scope: QMT, Shadow/live trading, strategy/model/HMM asset mutation, production backend restart on port 8001.
- Protected assets reviewed: no StrategyPackage manifest/hash, QE/RD-Agent artifact, model weight, HMM snapshot/coefficient, validated execution-policy asset, or source strategy asset was edited. The run created isolated Paper v2 validation portfolios/runs only.

## Environment

- Backend port: 8012 dev backend, restarted after code changes; production 8001 left running and untouched.
- Frontend port: 3011 Playwright-controlled Next dev server for UI validation; existing 3012 not touched.
- TDX port: 19080.
- Conda/env: C:/Users/lc999/miniconda3/envs/AIstock/python.exe.
- Database: local AIstock PostgreSQL/TimescaleDB; market.stk_limit 2026-04-29 synced from real Tushare incremental path, 7,568 rows, audit success job db50ee75-4aa8-4815-b6d9-90ecb3671a43.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH hardcoded path/secret/fallback/asset finding | `nox -s l0`; 0 HIGH, 13 existing MEDIUM review findings | Pass |
| Backend tests | Paper v2 + Selection Center backend tests pass | `nox -s paper_v2_backend`; 107 passed | Pass |
| Targeted regression | Historical replay values existing positions from real DB minute prices, held-position snapshot prices are real minute closes, live cutoff is previous trading day | `pytest test_day_runner.py test_live_session.py test_runtime_selection.py`; 42 passed | Pass |
| Type check | Frontend TypeScript compiles after run-console polling changes | `npm exec tsc -- --noEmit` | Pass |
| UI E2E | Paper v2 + Selection Center UI flows work on dev ports without page/console/request failures | `nox -s paper_v2_ui`; 12 passed in 10m | Pass |
| Trading-hours live API | Replay previous completed trading day then switch to 2026-04-29 realtime TDX bars | `nox -s paper_v2_live -- --require-live-bars`; portfolio `paper_f58c3deef1a84eba85ad065d7fb44188`, session `psess_63d5613efcd3406f877a1d442d889c76` | Pass |
| Asset safety | No protected strategy/model/HMM/QE asset silently changed | Git staging review pending; only framework/test files intended for commit | Pass |

## Commands

```powershell
# Data readiness for live-day limit prices
Get-Content .env | ForEach-Object { if ($_ -match '^\s*([^#][^=]+)=(.*)$') { $name=$matches[1].Trim(); $value=$matches[2].Trim().Trim('"').Trim("'"); [Environment]::SetEnvironmentVariable($name,$value,'Process') } }
C:/Users/lc999/miniconda3/envs/AIstock/python.exe - <<'PY'
# Equivalent direct call executed through PowerShell pipeline:
# TushareSyncEngine().sync(STK_LIMIT, mode="incremental", start_date=2026-04-29, end_date=2026-04-29)
PY

# Regression and guardrails
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_live_session.py backend/tests/selection_center/test_runtime_selection.py -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_backend
cd frontend; npm exec tsc -- --noEmit

# Dev backend/frontend validation, production 8001 not restarted
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3011'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_live -- --require-live-bars
```

## Evidence

- API calls: `/api/v1/paper-v2/sessions/{session_id}/tick` returned `LIVE_WAITING_FOR_BAR` after catchup; latest/live processed bar `2026-04-29T10:23:00+08:00`.
- DB checks: `market.stk_limit` has 7,568 rows for `2026-04-29`; `market.dataset_date_refresh_audit` row is `success` with row_count 7,568.
- Log files: `.codex_tmp/dev_backend_8012_20260429_live.out.log`, `.codex_tmp/dev_backend_8012_20260429_live.err.log`.
- Playwright report/trace: standard Playwright output under `frontend/test-results` if retained by Playwright; no failed trace produced.
- Business output summary: package `pkg_b668f8a633c44b72a5d557a2cb8970e3` (`qe_20260416_002701`) with V25 policy `execpol_8e96a3ec3d4d414f9581c66fbf405830`; replay_start `2026-04-28`; live_date `2026-04-29`; run_count 2; order_count 21; fill_count 74; error_count 0.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Missing 2026-04-29 limit prices | `market.stk_limit` had audit success with row_count 0 and no actual rows | Ran real Tushare incremental `stk_limit` sync for 2026-04-29; no fake prices | DB row_count 7,568; later live validation passed |
| Existing position equity failed during multi-day replay | Day runner required `current_prices` but historical replay did not derive them from DB minute data | Load first observed DB minute close for existing-position equity and persist runtime trace/event | Targeted pytest + live validation passed |
| Held position snapshot failed when no same-day order loaded its market data | Snapshot price map only contained symbols with order intents | Load latest available minute close for held positions without order market data and persist event | Targeted pytest + live validation passed |
| Current-day live selection inference failed for 2026-04-29 | Strict live inference correctly refused to score current day when DB factor data latest date was 2026-04-28 | Live sessions inject previous trading day cutoff; artifacts store target trade_date with cutoff/score/reference-date metadata | `paper_v2_live` passed with live_date 2026-04-29 |
| Live validation replayed more days than intended after today's `stk_limit` arrived | Validation helper used defaults with `as_of_date=live_date`, so replay_start expanded to lookback range including 2026-04-27 | Default live validation now selects the latest completed historical trading day before live_date; optional lookback remains configurable | Final `paper_v2_live` replay_start `2026-04-28`, run_count 2 |

## Result

- Final status: Pass.
- Remaining risks: The live smoke proves bars/orders/fills/cursor with V25, but not full-day close/finalization or fill-required edge cases across all market states. Those should be separate trading-hours/after-close validations.
- Need production backend restart: yes, after review/merge, user should restart production backend 8001 to apply framework fixes; Codex did not restart 8001.
- Need dev service restart: 8012 was restarted during validation and is currently running from the updated working tree.
