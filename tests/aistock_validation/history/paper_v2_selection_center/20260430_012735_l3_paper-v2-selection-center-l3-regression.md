# Paper v2 Selection Center L3 regression - watchlist coverage

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-30T01:27:35
- Git commit before run: 009859c
- Operator: lc999

## Scope

- Changed files: `backend/routers/watchlist.py`, `backend/tests/selection_center/test_runtime_selection.py`, `frontend/src/app/watchlist/page.tsx`, `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`, `scripts/aistock_data_quality_smoke.py`, validation matrix and memory files.
- Impacted flows: Selection Center single-package selection, one-click watchlist import, watchlist category/source filtering, watchlist persistent metadata display, Paper v2 L3 non-realtime regression, data-quality smoke date readiness gate.
- Business goal: prove Selection Center watchlist import is not only a success toast; imported stocks must be persisted with source StrategyPackage name, source selection run id, rank, reference entry price, entry_as_of date, created/updated time, and visible `/watchlist` UI rows.
- Out of scope: TDX realtime/live-session trading assertions because this run used `PAPER_V2_SKIP_REALTIME=1`; permission/auth/security tests remain deferred by current product decision.
- Protected assets reviewed: no StrategyPackage manifest, model weight, HMM snapshot/coefficient asset, validated execution policy, QE/RD-Agent asset, or strategy source asset was modified.

## Environment

- Backend port: 8011 temporary dev uvicorn (`backend.main:app`)
- Frontend port: 3011 temporary Playwright Next dev server
- TDX port: skipped intentionally with `PAPER_V2_SKIP_REALTIME=1`
- Conda/env: `AIstock` (`C:/Users/lc999/miniconda3/envs/AIstock/python.exe`)
- Database: local PostgreSQL/TimescaleDB shared development DB
- Browser/headless: Playwright Chromium headless

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH path/secret/fallback/asset finding | `nox -s paper_v2_l3` ran L0; 0 HIGH, existing MEDIUM review findings only | PASS |
| Backend tests | Selection Center rejects missing watchlist reference price and exports traceable import payload | `paper_v2_backend`: 114 passed; full targeted pytest: 151 passed | PASS |
| API/DB flow | Watchlist items persist category, source, run id, rank, entry price, as-of and timestamps | Playwright checks `/selection-center/runs/{run_id}/aggregate-results`, `/watchlist/categories`, `/watchlist/items` | PASS |
| UI E2E | Operator can add selection result to watchlist and verify `/watchlist` category/source filtered rows | `paper_v2_ui`: 12 passed | PASS |
| Data quality | Daily readiness gate must not demand current trading-day daily datasets before post-close window | `paper_v2_data_quality`: PASS with existing legacy ledger WARN | PASS |
| Asset safety | Framework/test changes only; protected assets untouched | Git diff limited to code/tests/docs/history, no asset paths | PASS |

## Commands

```bash
set PYTHONIOENCODING=utf-8
set PYTHONDONTWRITEBYTECODE=1
python -m pytest backend/tests/selection_center/test_runtime_selection.py -q -p no:cacheprovider
cd frontend && npm exec tsc -- --noEmit
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_backend
# backend 8011 started with: python -m uvicorn backend.main:app --host 127.0.0.1 --port 8011
set BACKEND_PORT=8011
set FRONTEND_PORT=3011
set PAPER_V2_API_BASE=http://127.0.0.1:8011/api/v1
set NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1
set PAPER_V2_SKIP_REALTIME=1
set PAPER_V2_E2E_SKIP_REALTIME=1
cd frontend && npm run test:e2e -- tests/paper-v2/paper-v2-real-flow.spec.ts -g "Selection Center runs"
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_data_quality_smoke.py --scope paper_v2_selection_center --output tmp/paper_v2_data_quality_smoke.json
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_l3
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_data_quality_smoke.py --portfolio-name-prefix E2E --since-hours 4 --max-recent-runs 80 --json --output tmp/paper_v2_data_quality_smoke_e2e_scoped_watchlist.json
```

## Evidence

- API calls: Playwright used `http://127.0.0.1:8011/api/v1`; service probe checked `/openapi.json`; realtime TDX probe was skipped by explicit non-realtime mode.
- DB/API checks: watchlist import response, selection aggregate results, watchlist categories, and watchlist item listing were cross-checked for every imported symbol.
- Scoped E2E data quality: `tmp/paper_v2_data_quality_smoke_e2e_scoped_watchlist.json` passed with 0 warnings and 0 failures over recent `E2E` portfolios.
- Log files: `tmp/paper_v2_backend_8011_watchlist.log`, `tmp/paper_v2_backend_8011_watchlist.err.log`.
- Playwright report/trace: final UI suite passed; no failure trace in the final run.
- Screenshots: no failure screenshots in the final run.
- Business output summary: a real StrategyPackage selection run generated top candidates, imported up to top 20 into an E2E watchlist category using the selection reference price, and `/watchlist` displayed the same imported symbol with StrategyPackage source name, selection run id, rank, entry price, entry_as_of date, gain-tracking columns, and join time.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Watchlist E2E only checked success toast | Original test did not prove watchlist DB/API/UI persistence | Added API assertions and `/watchlist` UI assertions for category, source package, run id, rank, price, as-of, timestamps and gain columns | Targeted Playwright test passed; full UI suite 12 passed |
| Selection watchlist import lacked unit coverage | Backend test suite did not verify metadata payload or missing reference-price fail-fast | Added Selection Center unit tests for traceable import payload and missing reference price rejection | `paper_v2_backend`: 114 passed; full targeted pytest: 151 passed |
| Watchlist router could silently return empty data on query failure | `/watchlist/all` and `/watchlist/items/source-tasks` swallowed exceptions | Changed to raise HTTP 500 with clear context instead of fake empty success | Full UI suite and targeted tests passed |
| Data-quality smoke failed before market daily data could exist | Latest trading-day resolver treated current pre-close trading date as completed | Added local post-close readiness cutoff; before 18:00 it validates the previous completed trading day | `paper_v2_data_quality` passed; final `paper_v2_l3` passed |

## Result

- Final status: PASS for all non-realtime Paper v2 + Selection Center L3 paths including watchlist import and `/watchlist` verification.
- Remaining risks: realtime TDX/live-session trading assertions were intentionally skipped; default historical baseline still reports 3 legacy order/fill mismatches as WARN only.
- Need production backend restart: yes, if the operator wants production `8001` to load the watchlist router fail-fast and watchlist UI source/as-of display changes. Codex did not restart `8001`.
- Need dev service restart: no; temporary 8011 backend can be stopped after validation.
