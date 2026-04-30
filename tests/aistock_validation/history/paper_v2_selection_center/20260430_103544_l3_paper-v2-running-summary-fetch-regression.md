# Paper v2 running summary fetch regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-30T10:35:44+08:00
- Git commit: 8b79fb5 (pre-fix baseline; fix committed separately after validation)
- Operator: Codex

## Scope

- Changed files: `backend/services/paper_trading_v2/repository.py`, `backend/services/paper_trading_v2/service.py`, `backend/routers/paper_trading_v2.py`, `backend/services/strategy_package/service.py`, `frontend/src/lib/paper-v2/api.ts`, `frontend/src/app/paper-v2/running/page.tsx`, `backend/tests/strategy_package/test_repository_service.py`, `backend/tests/paper_trading_v2/test_day_runner.py`, `frontend/tests/paper-v2/paper-v2-real-flow.spec.ts`.
- Impacted flows: Paper v2 running portfolio summary page; StrategyPackage enable-paper status transition; Paper v2 UI regression suite.
- Business goal: the running summary must show real persisted portfolios/orders/fills/positions/snapshots without browser request exhaustion, and StrategyPackage paper enablement must not be blocked by an obsolete manifest-embedded V24 asset when execution policy is selected separately.
- Out of scope: realtime live trading with current market data, production port 8001 restart, protected asset mutation, TDX service restart.
- Protected assets reviewed: no StrategyPackage manifest, model weight, HMM snapshot/coefficient, validated execution policy row, QE/RD-Agent workspace, or strategy source asset was modified.

## Environment

- Backend port: 8011 dev backend, scheduler disabled with `PAPER_V2_SESSION_SCHEDULER_ENABLED=0`.
- Frontend port: 3011 dev frontend with `NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1`.
- TDX port: 19080 was not restarted; UI suite ran with realtime checks skipped.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local AIstock PostgreSQL/TimescaleDB through existing `.env` connection settings.
- Browser/headless: Playwright Chromium headless.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Backend unit regression | V24 manifest minute asset no longer blocks package paper status; Paper v2 running summary aggregates ledger data | targeted pytest 33 passed | PASS |
| Backend module regression | Trading Core / StrategyPackage / Paper v2 / Selection Center stay compatible | targeted pytest 152 passed | PASS |
| API flow | `/api/v1/paper-v2/running-summary` returns active summaries in one call | manual API probe returned HTTP 200 and `summaries=107` | PASS |
| UI E2E | `/paper-v2/running` loads without `Failed to fetch` and without browser request exhaustion | Playwright probe on 3011 and `nox -s paper_v2_ui` | PASS |
| Asset safety | Framework-only changes, no protected asset mutation | git diff review and changed-file list | PASS |
| L0 guardrails | No HIGH guardrail findings; existing MEDIUM baseline remains review-only | `nox -s l0` | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/strategy_package/test_repository_service.py backend/tests/strategy_package/test_manifest_v1.py backend/tests/paper_trading_v2/test_day_runner.py -q -p no:cacheprovider
npm exec tsc -- --noEmit
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
# dev backend 8011 + dev frontend 3011 were started separately; production 8001 was not restarted.
Invoke-WebRequest -UseBasicParsing 'http://127.0.0.1:8011/api/v1/paper-v2/running-summary?limit=300&snapshot_limit=30&position_limit=8'
node tmp_probe_running_fixed.js
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:PAPER_V2_API_BASE='http://127.0.0.1:8011/api/v1'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'; $env:PAPER_V2_SKIP_REALTIME='1'; $env:PAPER_V2_E2E_SKIP_REALTIME='1'; C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_ui
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0
```

## Evidence

- API calls: `GET http://127.0.0.1:8011/api/v1/paper-v2/running-summary?limit=300&snapshot_limit=30&position_limit=8` -> HTTP 200, `summaries=107`.
- DB checks: API response was built from persisted `paper_v2.portfolio`, `paper_v2.run`, `paper_v2.trade_session`, `paper_v2.orders`, `paper_v2.fills`, `paper_v2.positions`, `paper_v2.daily_snapshots`, and `paper_v2.errors` rows.
- Log files: uvicorn dev session on 8011 only; no production 8001 restart.
- Playwright report/trace: final `paper_v2_ui` produced 12 passed; first failed attempt trace recorded under `frontend/tmp/playwright-results/...TopK-guard-through-UI...` before test-date fix.
- Screenshots: Playwright failure screenshot in the first failed attempt; no final failure screenshot because all tests passed.
- Business output summary: running page rendered 107 active Paper v2 portfolios, 107.38M current total NAV, 7,773 fills, and persisted error count without `Failed to fetch`.
- Guardrails: `nox -s l0` passed with 0 HIGH findings and existing MEDIUM review findings only.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `/paper-v2/running` showed `Failed to fetch` / `net::ERR_INSUFFICIENT_RESOURCES` | Frontend issued seven requests per active portfolio; accumulated E2E portfolios created >700 browser requests | Added backend `running-summary` aggregation endpoint and changed UI to use one API call | Playwright probe passed; `paper_v2_ui` 12 passed |
| StrategyPackage enable-paper failed on `V24_PLAN model_path` | Package paper status transition validated manifest-embedded minute runtime asset even though Paper v2 execution policy is selected separately | `enable_paper` now validates package identity/hash/status only; selected execution policy still fail-fast validates runtime assets at portfolio/session boundary | StrategyPackage regression test passed in targeted and full backend runs |
| First `paper_v2_ui` rerun failed HMM uncovered-date assertion | Test filled `REPLAY_TRADE_DATE`, which is now covered by the HMM coefficient artifact | Test now uses `HMM_UNCOVERED_TRADE_DATE` for the fail-fast HMM coverage path | Final `paper_v2_ui` 12 passed |

## Result

- Final status: PASS.
- Remaining risks: production 8001 and currently running production frontend must be restarted/rebuilt by the user to load this framework fix; until then the old browser fan-out code may still show `Failed to fetch`.
- Need production backend restart: yes, by user only; Codex did not restart 8001.
- Need dev service restart: 8011/3011 were started for validation and can be stopped after review.

