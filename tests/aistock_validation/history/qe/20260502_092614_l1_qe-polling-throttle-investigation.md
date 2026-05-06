# QE polling throttle investigation

- Module: qe
- Level: L1
- Date: 2026-05-02T09:26:14
- Git commit: 4d2f9ad
- Operator: lc999

## Scope

- Changed files: `frontend/src/app/quantevolver/evolution/page.tsx`
- Impacted flows: QE evolution dashboard task-list refresh, selected-task detail refresh, terminal-task log loading.
- Business goal: when no QE task is `running`/`processing`, the QE dashboard must not automatically poll `/quantevolver/evolution/tasks` or `/tasks/{task_id}`; clicking a task should load detail once.
- Out of scope: backend payload slimming, QE/RD-Agent execution, database schema changes.
- Protected assets reviewed: no model, Qlib, StrategyPackage, HMM, workspace, ledger, or execution-policy assets modified.

## Environment

- Backend port: existing production/dev backend `127.0.0.1:8001` was queried only; not restarted.
- Frontend port: existing Next dev server `3000`; `npm run build` executed in `frontend`.
- TDX port: not involved.
- Conda/env: Windows shell, existing AIstock backend process; WSL Ubuntu checked for RDAgent processes.
- Database: TimescaleDB/PostgreSQL on `127.0.0.1:5432`, credentials loaded from `.env`.
- Browser/headless: no Playwright; static build and API/process checks only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `git diff --check -- frontend/src/app/quantevolver/evolution/page.tsx` | Pass; only existing LF/CRLF warning |
| Frontend build | QE dashboard compiles after polling-state change | `npm run build` | Pass |
| API flow | API/DB evidence supports root-cause analysis | `/tasks` 308ms/369KB; `/tasks/qe_20260501_011054_c90a` 16914ms/18.9MB; `/rdagent/tasks/local?limit=1` 37ms | Pass |
| DB health | Database is not locked or saturated | `pg_stat_activity count=20`, no active blocking queries, only AccessShareLock | Pass |
| WSL process check | Resource owner identified | WSL `uvicorn rdagent.app.results_api_server:create_app --port 9000` child RSS ~1.76GB, CPU ~4%; no GPU compute process | Pass |
| Asset safety | No protected asset modified silently | Git diff limited to frontend polling file plus validation record; build touched existing `frontend/tsconfig.tsbuildinfo` | Pass |

## Commands

```bash
npm run build
git diff --check -- frontend/src/app/quantevolver/evolution/page.tsx
Invoke-WebRequest http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks
Invoke-WebRequest http://127.0.0.1:8001/api/v1/quantevolver/evolution/tasks/qe_20260501_011054_c90a
Invoke-WebRequest http://127.0.0.1:8001/api/v1/rdagent/tasks/local?limit=1
python -  # loaded .env, queried pg_stat_activity/pg_locks/qe status counts
wsl bash -lc "ps -eo pid,ppid,stat,pcpu,pmem,rss,etime,cmd --sort=-rss | head -n 25"
wsl bash -lc "ss -ltnp 2>/dev/null | sed -n '1,80p'"
docker ps --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
```

## Evidence

- API calls: `/quantevolver/evolution/tasks` returned 200 in 308ms, length 369196; `/quantevolver/evolution/tasks/qe_20260501_011054_c90a` returned 200 in 16914ms, length 18956971; `/rdagent/tasks/local?limit=1` returned 200 in 37ms.
- DB checks: `qe_evolution_tasks` counts were `completed=25`, `failed=16`, `paused=4`, `pending=2`; `qe_experiments` had no `running`; `pg_stat_activity` showed idle backend sessions and no blocking locks.
- Log files: no backend file log used; Uvicorn access log symptom provided by user and API timings reproduced locally.
- Playwright report/trace: not run; no browser automation required for this L1 throttling fix.
- Screenshots: none.
- Business output summary: no running QE tasks; selected completed task detail is very large/slow, so automatic polling can starve the single backend worker/event loop and make other DB pages appear blank or delayed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| QE dashboard auto-refresh continued after terminal status | Frontend kept list polling at 60s and selected terminal-task detail polling at 60s; stale selected status could also keep live-refresh logic open | Stop list auto-poll when no `running`/`processing` tasks; stop selected-task detail interval for terminal/non-active statuses; only load once on click | `npm run build` passed |

## Result

- Final status: Pass for scoped frontend throttling fix and root-cause diagnostics.
- Remaining risks: backend detail endpoint still returns ~18.9MB for the large completed task and takes ~17s on one request; backend payload slimming or summary/detail split remains recommended.
- Need production backend restart: no
- Need dev service restart: frontend hot reload should pick up the change; if old browser tabs keep intervals, hard refresh or close/reopen `/quantevolver/evolution`.
