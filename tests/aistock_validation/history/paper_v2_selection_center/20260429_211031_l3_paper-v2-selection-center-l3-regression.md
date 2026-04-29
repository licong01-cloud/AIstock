# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-29T21:10:31
- Git commit: 89c4bdd
- Operator: lc999

## Scope

- Changed files: `scripts/aistock_validate.py`, `noxfile.py`, `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`, validation history and memory files
- Impacted flows: StrategyPackage list, Selection Center single/multi-package selection, watchlist import, HMM UI, Paper v2 portfolio creation, replay, run console, ledger, performance, running dashboard, lifecycle, runtime/policy audit, non-realtime validation pipeline
- Business goal: validate all Paper v2 + Selection Center flows that do not require current realtime market data, and cross-check data persistence/ledger consistency
- Out of scope: live TDX realtime minute data, real live-session trading tick validation, permission/auth/security testing
- Protected assets reviewed: no StrategyPackage manifest, model weight, HMM snapshot/coefficient asset, validated execution policy, QE/RD-Agent asset, or strategy source asset was modified

## Environment

- Backend port: 8011 temporary dev uvicorn (`backend.main:app`)
- Frontend port: 3011 temporary Playwright Next dev server
- TDX port: skipped intentionally with `PAPER_V2_SKIP_REALTIME=1`
- Conda/env: `AIstock`
- Database: local PostgreSQL/TimescaleDB shared dev DB
- Browser/headless: Playwright Chromium headless

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH path/secret/fallback/asset finding | `nox -s l0`; 0 HIGH, existing MEDIUM review findings only | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `paper_v2_backend`: 112 passed; full targeted pytest: 149 passed | PASS |
| Data quality | API/DB data is fresh and ledger is traceable | default smoke PASS with legacy WARN; scoped E2E smoke PASS with 0 warnings | PASS |
| UI E2E | Non-realtime user-visible flows are usable and backed by real APIs | `paper_v2_ui`: 12 passed | PASS |
| Asset safety | No protected asset modified silently | git diff limited to validation scripts/tests/docs/history; no asset file touched | PASS |

## Commands

```bash
set PYTHONIOENCODING=utf-8
set PYTHONDONTWRITEBYTECODE=1
python scripts/aistock_validate.py services --backend-port 8011 --skip-tdx
cd frontend && npm exec tsc -- --noEmit
python -m nox -s paper_v2_backend
python -m nox -s paper_v2_data_quality
python -m nox -s l0
set BACKEND_PORT=8011
set FRONTEND_PORT=3011
set PAPER_V2_API_BASE=http://127.0.0.1:8011/api/v1
set NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1
set PAPER_V2_SKIP_REALTIME=1
set PAPER_V2_E2E_SKIP_REALTIME=1
python -m nox -s paper_v2_ui
python -m nox -s paper_v2_l3
python scripts/aistock_data_quality_smoke.py --portfolio-name-prefix E2E --since-hours 2 --max-recent-runs 80 --json --output tmp/paper_v2_data_quality_smoke_e2e_scoped.json
python -m pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
```

## Evidence

- API calls: Playwright used `http://127.0.0.1:8011/api/v1`; service probe checked `/openapi.json`
- DB checks: `tmp/paper_v2_data_quality_smoke.json`, `tmp/paper_v2_data_quality_smoke_e2e_scoped.json`
- Log files: `tmp/paper_v2_backend_8011.log`
- Playwright report/trace: no failure trace in final run; final UI suite `12 passed`
- Screenshots: no failure screenshots in final run
- Business output summary: selected packages from QE experiments, generated PIT single-package selections, aggregated multiple strategy-package runs, imported results into watchlist, created V25 replay portfolio, verified orders/fills/positions/snapshots/performance, exercised readiness/run-day/replay reject/reset, activated runtime profile and execution policy, verified HMM/model maintenance preview and daily coefficient job UI without live market data

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Realtime service probe and realtime UI checks were not suitable for a no-realtime run | Original validation assumed TDX realtime endpoint availability | Added `--skip-tdx` / `PAPER_V2_SKIP_REALTIME=1` and skipped only the live/TDX-specific assertions | `paper_v2_ui` and final `paper_v2_l3` passed |
| Multi-package historical aggregation could hang on disabled/unstable history selection | Test selected arbitrary first visible historical rows instead of known compatible setup runs | Select `ensuredRuns` by run_id-specific test ids and assert aggregate button enabled before click | UI test 5 passed |
| Model/HMM page could occasionally open unhydrated in Next dev after repeated route compilation | Next dev transient page hydration/chunk state left dropdowns with only placeholder options | Added one explicit reload-and-require-real-data helper in the E2E; no fake data or backend fallback is used | Targeted Model/HMM test and full UI suite passed |

## Result

- Final status: PASS for all non-realtime Paper v2 + Selection Center validation paths
- Remaining risks: realtime TDX/live-session validation was intentionally not run; default data-quality baseline still reports 3 legacy historical order/fill mismatches outside the scoped E2E validation, while scoped E2E ledger checks passed with 0 warnings
- Need production backend restart: no
- Need dev service restart: no
