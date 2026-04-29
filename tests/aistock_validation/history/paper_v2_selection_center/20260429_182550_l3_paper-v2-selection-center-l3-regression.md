# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3/L4-local
- Date: 2026-04-29T18:25:50+08:00
- Git commit at run start: 4b7db0c
- Operator: lc999 / Codex

## Scope

- Changed files: Paper v2 session runner/router, live/day runner no-rebalance handling, UI session creation/manual tick config, live validation script, backend regression tests.
- Impacted flows: StrategyPackage -> Selection Center -> Paper v2 replay -> live/catch-up session -> ledger/snapshot/performance UI.
- Business goal: UI/API validation must prove Selection Center and Paper v2 produce traceable selections, orders/fills/snapshots, and explicit no-rebalance states without silent fallback.
- Out of scope: production port 8001 restart, QMT/Shadow/live broker trading, protected strategy/model/HMM asset mutation.
- Protected assets reviewed: no StrategyPackage manifest/model weight/HMM snapshot/QE/RD-Agent asset files were staged or modified.

## Environment

- Backend port: 8011 (temporary dev uvicorn)
- Frontend port: 3011 (Playwright webServer)
- TDX port: 19080
- Conda/env: C:\Users\lc999\miniconda3\envs\AIstock\python.exe
- Database: local AIstock PostgreSQL/TimescaleDB shared with dev/prod processes
- Browser/headless: Playwright Chromium headless

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH hardcoded path/secret/fallback/asset finding | `nox -s paper_v2_l3` L0 section | Pass; 14 existing MEDIUM findings only |
| Backend tests | Paper v2 + Selection Center backend tests pass | `112 passed` inside L3; full pytest `149 passed` | Pass |
| API flow | API, DB, and logs agree for sessions, replay, live, no-rebalance | `paper_v2_live` result with `run_count=2`, `order_count=20`, `fill_count=74`, `error_count=0` | Pass |
| UI E2E | User-visible flow works with no blocking UI/API failures | Playwright `12 passed` | Pass |
| Asset safety | No protected asset modified silently | `git status --short -- <scoped paths>` before staging | Pass |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
npm exec tsc -- --noEmit
$env:BACKEND_PORT='8011'; $env:FRONTEND_PORT='3011'; $env:PAPER_V2_API_BASE='http://127.0.0.1:8011/api/v1'; $env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8011/api/v1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_l3
$env:BACKEND_PORT='8011'; $env:PAPER_V2_API_BASE='http://127.0.0.1:8011/api/v1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_live
```

## Evidence

- Backend pytest: `149 passed in 9.62s`.
- TypeScript: `npm exec tsc -- --noEmit` passed.
- L3 Nox: L0 pass, backend `112 passed`, Playwright `12 passed (3.6m)`.
- Live/catch-up validation: `paper_v2_live` passed on 8011/19080 with package `qe_20260416_002701`, policy `V25_TWO_STAGE`, replay_start `2026-04-28`, live_date `2026-04-29`, final_status `LIVE_WAITING_NEXT_TRADING_DAY`, run_count `2`, order_count `20`, fill_count `74`, error_count `0`.
- Playwright report/trace: `frontend/../tmp/playwright-report` and `frontend/../tmp/playwright-results` (retained by Playwright config).

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| L3 UI first failed on 8012 with `/selection-center/pit-cutoff` 404 | 8012 was an old dev backend process not loaded with current commit | Started fresh 8011 dev backend from current workspace and reran | PIT endpoint returned 200; subsequent L3 proceeded |
| UI replay portfolio had a duplicate replay error while production 8001 scheduler was running old code | Sessions created by UI were visible as tickable while the UI was about to manually tick them, allowing another backend scheduler to race | Added explicit `manual_tick_only` session mode for UI/validation-created sessions; manual ticks require `allow_paused=true`; long replay/catch-up keeps session non-tickable until the explicit tick completes | L3 UI passed; no duplicate persisted errors |
| After-hours `paper_v2_live` failed with `rebalance produced no order intents` | Current target positions could equal existing holdings after catch-up; no-op rebalance was not explicitly modeled | Rebalance now returns an empty diff for exact target/current match; Paper v2 day/live runners persist explicit `NO_REBALANCE_REQUIRED` events and mark-to-market snapshots using real minute prices | `paper_v2_live` passed with `error_count=0` |

## Result

- Final status: Pass.
- Remaining risks: existing MEDIUM guardrail findings remain as pre-existing review items; production 8001 still returned 404 for the new PIT endpoint during this run, so it needs restart to load current code.
- Need production backend restart: yes, for 8001 to load this commit and avoid old scheduler behavior.
- Need dev service restart: 8011 was restarted during validation and is currently running current workspace code.
