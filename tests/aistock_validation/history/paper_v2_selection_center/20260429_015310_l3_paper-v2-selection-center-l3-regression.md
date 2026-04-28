# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-04-29T01:53:10
- Git commit: b8c7e10
- Operator: lc999

## Scope

- Changed files: `noxfile.py`, `scripts/aistock_validate.py`, `frontend/playwright.config.ts`, `.codex/skills/verify-aistock-feature/SKILL.md`, `tests/aistock_validation/modules/paper_v2_selection_center.md`, `docs/codex_project_memory.md`.
- Impacted flows: local validation runner, Paper v2 + Selection Center backend regression, Paper v2/Selection Center Playwright UI E2E, backend/TDX service readiness probe.
- Business goal: prove the Paper v2 + Selection Center first-stage flow can run against real local backend/DB/TDX services on development ports, including selection, aggregation, watchlist import, V25 replay portfolio, ledger/performance, lifecycle, run console, HMM maintenance preview, negative fail-fast errors, and TDX realtime minute reachability.
- Out of scope: production backend `8001`, cloud CI, QMT/Shadow/live trading, modification of strategy/model/HMM/QE assets, full trading-hours live stream validation.
- Protected assets reviewed: Git diff was limited to validation tooling, Playwright config, skill docs, and this run record. No StrategyPackage manifests, model weights, HMM snapshots, validated execution policies, QE/RD-Agent artifacts, or source strategy assets were modified by the code changes.

## Environment

- Backend port: `8012` (existing FastAPI dev service, `/openapi.json` returned HTTP 200).
- Frontend port: `3011` (free before each Playwright run; Playwright started a controlled Next.js dev server).
- TDX port: `19080` (existing `tdx-api` service, realtime minute endpoint returned HTTP 200 with non-empty body).
- Conda/env: `C:\Users\lc999\miniconda3\envs\AIstock\python.exe`, `PYTHONIOENCODING=utf-8`, `PYTHONDONTWRITEBYTECODE=1`.
- Database: local AIstock dev database used by backend port `8012`.
- Browser/headless: Playwright Chromium, headless, one worker.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `nox -s l0`: skill YAML valid; guardrail scan reported 13 MEDIUM review findings and 0 HIGH findings | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `nox -s paper_v2_backend`: `103 passed in 8.78s` during full L3 run | PASS |
| API flow | Required local services are reachable before UI starts | `scripts/aistock_validate.py services --backend-port 8012 --tdx-port 19080`: backend and TDX probes both HTTP 200 | PASS |
| UI E2E | User-visible flow works with no blocking console/page/request errors | Playwright Paper v2 suite: `12 passed (7.8m)` during full L3 run | PASS |
| Asset safety | No protected asset modified silently | `git diff` limited to validation code/docs/config and run record; no protected strategy/model/HMM/QE files staged or modified by this task | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0

$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_backend

$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3011'
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_ui

$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3011'
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s paper_v2_l3
```

## Evidence

- API calls: `GET http://127.0.0.1:8012/openapi.json` -> HTTP 200; `GET http://127.0.0.1:19080/api/kline-all/tdx?code=SZ000001&type=minute1` -> HTTP 200.
- DB checks: backend and UI E2E exercised persisted selection runs, watchlist import, Paper v2 portfolios, replay runs, orders, fills, positions, snapshots, lifecycle transitions, runtime config activations, and execution policy activations through API-backed assertions.
- Log files: runtime backend logs remain under `backend/logs/`; no production `8001` service was restarted or managed.
- Playwright report/trace: standard Playwright output under `frontend/test-results` / Playwright report locations when retained by the configured reporter.
- Screenshots: not retained for passing cases by current Playwright config.
- Business output summary: Selection Center generated live-data selections, displayed history, imported watchlist symbols, aggregated multi-package historical runs, validated HMM/blacklist/topK UI behavior; Paper v2 created a V25 replay portfolio and exposed ledger/performance/runs/orders/fills/positions/snapshots/errors plus lifecycle and run-console controls.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Nox UI session could not pass `cwd` to `Session.run` in current Nox | `Session.run(..., cwd=...)` is unsupported in Nox 2026.4.10 | Temporarily `os.chdir(ROOT / "frontend")` around the npm command and restore cwd in `finally` | `nox -s paper_v2_ui` PASS, `12 passed` |
| L3 did not run UI by default | Previous L3 required an extra `--with-ui` argument, so a full module regression could omit UI coverage | `paper_v2_l3` now includes UI unless explicitly disabled with `PAPER_V2_L3_SKIP_UI=1` | `nox -s paper_v2_l3` ran L0 + backend + UI and passed |
| UI validation could fail late when backend/TDX service was unavailable | No early local service readiness probe existed in validation runner | Added `scripts/aistock_validate.py services` and wired it into `paper_v2_ui` before Playwright starts | Full L3 service probes passed before UI E2E |
| Hardcoded local Codex validator path in Nox | L0 called a workstation-specific `C:/Users/...` validator path | Resolve validator from `CODEX_HOME` or `Path.home()/.codex` and fail clearly if absent | `nox -s l0` PASS, skill validation passed |
| Reusing an already-running frontend should be explicit for Playwright | Existing frontend on `3012` had previously served stale/corrupt dev chunks | Playwright config now honors `PLAYWRIGHT_SKIP_WEBSERVER=1`; validation used free `3011` so Playwright started a controlled dev server | `nox -s paper_v2_ui` and full L3 both passed on `3011` |

## Result

- Final status: PASS. Full Paper v2 + Selection Center L3 local validation completed successfully with backend tests and UI E2E.
- Remaining risks: trading-hours live stream behavior still requires a real market-time validation window; current run validates non-market-time flows and TDX realtime endpoint reachability. L0 still reports existing MEDIUM review findings in tests (raw JSON assertion patterns and one hardcoded test path), but no HIGH blocking findings.
- Need production backend restart: no. Production port `8001` was not touched.
- Need dev service restart: no required restart after this validation tooling change, but future manual production use should restart the target service to load any changed application code if applicable.
