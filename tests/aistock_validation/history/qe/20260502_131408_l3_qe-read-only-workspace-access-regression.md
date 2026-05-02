# QE read-only workspace access regression

- Module: qe
- Level: L3
- Date: 2026-05-02T13:14:08
- Git commit: 1a5e746
- Operator: lc999

## Scope

- Changed files: `backend/routers/quantevolver_evolution.py`, `frontend/src/app/quantevolver/evolution/page.tsx`, `backend/tests/unified_engine/test_qe_evolution_read_paths.py`, `frontend/tests/qe/qe-evolution-read-only.spec.ts`, `tests/aistock_validation/modules/qe.md`, `docs/architecture/qe_worker_workspace_read_refactor_validation_plan_20260502.md`, `noxfile.py`.
- Impacted flows: QE evolution task list/detail read path, selected terminal-task detail loading, no-active-task dashboard polling behavior, read-only UI observability.
- Business goal: terminal QE task details must return/display real DB/API status, loop count, loop statuses, and IC metrics without Windows-side direct worker workspace reads; when no task is active, the dashboard must stop automatic task-list/detail polling.
- Out of scope: experiment creation, dispatch/scanner scheduling logic, retry/rerun/resume/fork/append/delete/cleanup, worker workspace mutation, model/HMM/StrategyPackage assets.
- Protected assets reviewed: no QE/RD-Agent `mlruns`, model weights, HMM snapshots, StrategyPackage frozen manifests, execution-policy assets, Qlib datasets, or Paper ledger assets were modified.

## Environment

- Backend port: `8012`, fresh dev FastAPI started from current working tree with `.env` preloaded and scheduler/scanner env overrides set after preload (`PYTHON_DOTENV_DISABLED=1`, `DISABLE_INGESTION_SCHEDULER=1`, `DISABLE_STRATEGY_SCHEDULER=1`, `DISABLE_PAPER_TRADING_SCHEDULER=1`, `ENABLE_PAPER_TRADING_V2_SCHEDULER=0`, `DISABLE_NODE_HEALTH_SCHEDULER=1`, `DISABLE_HMM_SCHEDULER=1`, `DISABLE_EVOLUTION_SCANNER=1`, `DISABLE_QE_EXPERIMENT_SCANNER=1`). Production `8001` was not restarted.
- Frontend port: `3011`, Playwright webServer started/stopped Next dev for the QE E2E run.
- TDX port: skipped intentionally for this QE read-only validation (`--skip-tdx`).
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`; frontend Node/npm from local `frontend` workspace.
- Database: local PostgreSQL/TimescaleDB on `127.0.0.1:5432`, read-only probes only; `pg_locks` ungranted count was `0`.
- Browser/headless: Playwright Chromium headless; report path `tmp/playwright-report/index.html`; result marker `tmp/playwright-results/.last-run.json`.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No HIGH path/secret/fallback/asset finding | `nox -s qe_read_l3` Guardrail scan completed with 16 MEDIUM raw-json review findings only; no HIGH | Pass |
| Backend tests | QE read path no longer scans worker workspace or writes DB from task detail enrichment | `pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py -q -p no:cacheprovider` -> `3 passed` | Pass |
| API flow | Terminal task detail returns real DB/API data and no 500 for inaccessible worker artifacts | `qe_20260414_173338_d1c5` returned `completed`, `2/2`, loop IC values `0.060389` and `0.060389` on port 8012 | Pass |
| UI E2E | UI displays task id, `2/2`, loop labels, and IC metric matching API; no pageerror/console error/requestfailed/unexpected API 5xx | Playwright test `QE evolution terminal task detail is read-only, accurate, and observable` passed; mock enhanced-metrics route returned 200 in the no-active polling test | Pass |
| Polling throttle | No-active-task dashboard state must not issue list/detail requests after old 60s polling window | Mock-backed Playwright test waited 65s after terminal task selection; list/detail request counters did not increase; UI showed `手动` | Pass |
| Current WSL process diagnosis | Resource-consuming process owner is identified without killing or mutating worker state | WSL Ubuntu has two `python qrun_limit_minute.py conf.yaml` jobs in `qe_20260501_011054_c90a/Loop5` and `Loop6`, spawned by RD-Agent Results API server on port 9000 | Observed |
| DB health | Other DB pages are not blocked by PostgreSQL locks | `pg_stat_activity`: mostly idle sessions; `ungranted_locks=0` | Pass |
| Asset safety | No protected runtime assets modified silently | Git diff limited to source/test/docs/validation harness plus validation records; no worker asset paths staged | Pass |

## Commands

```powershell
# Backend syntax/unit guard.
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/routers/quantevolver_evolution.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py -q -p no:cacheprovider

# Fresh dev backend on 8012, with .env preloaded then scheduler/scanner env overrides applied.
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -c "import os, uvicorn; from dotenv import load_dotenv; load_dotenv(r'F:/Dev/AIstock/.env', override=True); os.environ['PYTHON_DOTENV_DISABLED']='1'; [os.environ.__setitem__(k, v) for k, v in {'DISABLE_INGESTION_SCHEDULER':'1','DISABLE_STRATEGY_SCHEDULER':'1','DISABLE_PAPER_TRADING_SCHEDULER':'1','ENABLE_PAPER_TRADING_V2_SCHEDULER':'0','DISABLE_NODE_HEALTH_SCHEDULER':'1','DISABLE_HMM_SCHEDULER':'1','DISABLE_EVOLUTION_SCANNER':'1','DISABLE_QE_EXPERIMENT_SCANNER':'1','PYTHONIOENCODING':'utf-8'}.items()]; uvicorn.run('backend.main:app', host='127.0.0.1', port=8012)"

# Full QE read-only L3 validation.
$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3011'
$env:QE_API_BASE='http://127.0.0.1:8012/api/v1'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8012/api/v1'
$env:QE_READ_TASK_ID='qe_20260414_173338_d1c5'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_read_l3

# API sample checks.
Invoke-RestMethod http://127.0.0.1:8012/api/v1/quantevolver/evolution/tasks/qe_20260414_173338_d1c5
Invoke-RestMethod http://127.0.0.1:8012/api/v1/quantevolver/evolution/tasks/qe_20260501_011054_c90a

# WSL process/resource diagnosis.
wsl.exe -d Ubuntu -- bash -lc "ps -eo pid,ppid,stat,pcpu,pmem,etime,cmd --sort=-pcpu | head -25; readlink -f /proc/3394583/cwd 2>/dev/null; readlink -f /proc/3394952/cwd 2>/dev/null; ss -ltnp 2>/dev/null | grep -E ':9000|:5432|:3001|:9090|:9100' || true"
```

## Evidence

- API calls: `GET /api/v1/quantevolver/evolution/tasks/qe_20260414_173338_d1c5` on port 8012 returned `status=completed`, `current_loop=2`, `max_loops=2`, two completed loops, IC values `0.060389` and `0.060389`; no HTTP 500.
- API calls: `GET /api/v1/quantevolver/evolution/tasks/qe_20260501_011054_c90a` returned `status=running`, `current_loop=28`, `max_loops=28`, with Loop5 and Loop6 still `running`; this explains why a page connected to this task is still allowed to poll.
- DB checks: `pg_stat_activity` grouping was `[('idle','Client',16), (None,'Activity',5), ('idle','Extension',2), (None,'Extension',1), ('active',None,1)]`; `ungranted_locks=0`, so the observed blank/slow DB pages are more consistent with API/resource starvation than a PostgreSQL lock.
- WSL checks: Ubuntu has two high-CPU worker jobs, PID `3394583` cwd `/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop5` and PID `3394952` cwd `/mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260501_011054_c90a/Loop6`; both run `python qrun_limit_minute.py conf.yaml`. Port `9000` is RD-Agent Results API (`uvicorn rdagent.app.results_api_server:create_app`).
- Log files: validation used console output; Playwright report is persisted at `tmp/playwright-report/index.html`.
- Playwright report/trace: `tmp/playwright-report/index.html`; success run retained no failure screenshots/videos.
- Business output summary: the terminal task is visible and accurate in UI; the controlled no-active UI state is manual-refresh only and does not poll after 65s; current real task `qe_20260501_011054_c90a` is not terminal because Loop5/Loop6 are still running in WSL.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `GET /tasks/qe_20260414_173338_d1c5` could return 500 on Windows | Detail endpoint scanned local worker workspace for `positions_normal_1day.pkl` under `mlruns`; Windows cannot access WSL/RD-Agent reparse/symlink paths reliably | Removed local worker workspace scanning and pickle enrichment from the task detail path; missing position summaries remain absent instead of fabricated | Backend pytest `3 passed`; API returned 200 for target task |
| Terminal/no-active QE dashboard kept background polling | Frontend kept list/detail intervals even when no task needed live updates | List auto-poll now starts only for `running`/`processing`; terminal/non-active task click loads once and does not open SSE/detail intervals; UI badge says `手动` | Playwright no-active test waited 65s with unchanged request counters |
| Validation harness lacked QE read-path L3 coverage | QE was not yet integrated into Paper v2-style automation | Added QE matrix, nox sessions, backend regression tests, and Playwright read-only E2E | `nox -s qe_read_l3` passed after rerun with mocked enhanced-metrics endpoint to avoid any expected/unexpected 5xx during UI validation |

## Result

- Final status: Pass for the scoped read-only QE task-detail and dashboard-polling slice.
- Remaining risks: other QE endpoints still need phased audit for direct worker workspace reads; experiment creation/dispatch/retry/rerun/resume/fork/append/delete/cleanup intentionally remain unchanged; current WSL Loop5/Loop6 are still consuming CPU/memory and should be stopped only with user confirmation.
- Need production backend restart: no Codex restart was performed; production 8001 will need a user-controlled restart to load backend changes.
- Need dev service restart: frontend tabs should be hard-refreshed to drop old intervals; the temporary 8012 validation backend was stopped after validation; ports 8012 and 3011 were free.
