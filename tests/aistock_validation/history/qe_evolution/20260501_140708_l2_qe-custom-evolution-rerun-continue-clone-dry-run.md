# QE custom evolution rerun continue clone dry-run

- Module: qe_evolution
- Level: L2
- Date: 2026-05-01T14:07:08
- Git commit: 734430f
- Operator: lc999

## Scope

- Changed files:
- `backend/routers/quantevolver_evolution.py`
- `backend/services/quantevolver/qe_evolution_service.py`
- `backend/services/quantevolver/qe_workspace_client.py`
- `frontend/src/app/quantevolver/evolution/page.tsx`
- `frontend/src/app/quantevolver/evolution/components/TopologyPanel.tsx`
- `backend/tests/unified_engine/test_custom_evo_mutation_routes.py`
- `docs/architecture/qe_custom_evo_rerun_continue_plan_20260501.md`
- external runtime dependency: `F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py`
- Impacted flows:
- QE `custom_evo` loop full-config rerun.
- QE `custom_evo` append/continue evolution.
- QE `custom_evo` clone-to-new-task creation flow.
- Loop-level workspace cleanup before rerun.
- Business goal:
- Allow correcting a wrong Loop config by deleting old results and rerunning the same Loop index.
- Allow appending new custom Loops in the same task without rerunning historical failed Loops.
- Allow cloning all Loop configs into a new editable custom evolution task.
- Out of scope:
- No real QE training, backtest, RD-Agent Loop submission, DB mutation, or browser E2E was executed.
- Protected assets reviewed:
- No StrategyPackage manifest, model weight, HMM snapshot, validated execution policy, QE result asset, or Paper v2 ledger was intentionally modified.

## Environment

- Backend port: not started; production 8001 not restarted
- Frontend port: not started
- TDX port: not used
- Conda/env: current Codex shell Python / local npm
- Database: not touched; route-level dry-run uses monkeypatches
- Browser/headless: not used

## Matrix

```text
Case              Expected business result                                      Evidence                                      Result
----------------  ------------------------------------------------------------  --------------------------------------------  ------
Python compile    Backend and RD-Agent endpoint code is syntactically valid      py_compile returned 0                         PASS
Route dry-run     rerun requires explicit delete confirmation                    pytest case rejects missing confirmation       PASS
Route dry-run     rerun schedules only selected target loop                      pytest checks BackgroundTasks args=[2]         PASS
Route dry-run     append schedules only backend-assigned new loop indexes        pytest checks BackgroundTasks args=[4, 5]      PASS
Frontend compile  evolution page/topology types and JSX are valid               npm exec tsc -- --noEmit returned 0           PASS
Asset safety      No real QE/DB/RD-Agent execution or protected asset writes     no services started; no QE command executed    PASS
```

## Commands

```powershell
python -m py_compile backend/routers/quantevolver_evolution.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py F:/Dev/RD-Agent-main/rdagent/app/api_endpoints/qe_evolution_api.py
python -m pytest backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
cd frontend
npm exec tsc -- --noEmit
```

## Evidence

- API calls: no live HTTP calls; direct router function dry-run with monkeypatched scheduler/node preflight.
- DB checks: not applicable; no DB mutation performed.
- Log files: not applicable.
- Playwright report/trace: not run; TypeScript compile only.
- Screenshots: not applicable.
- Business output summary:
  - `rerun` route does not run without `confirm_delete_old_result=true`.
  - `rerun` route passes only the target Loop index to `submit_custom_evo_selected_loops`.
  - `append` route passes only newly assigned Loop indexes to `submit_custom_evo_selected_loops`.
  - Loop cleanup client has no task-level fallback; RD-Agent loop cleanup endpoint is required and added.

## Failures And Fixes

```text
Failure                               Root cause                         Fix                                Rerun evidence
------------------------------------  ---------------------------------  ---------------------------------  --------------
pytest async tests failed initially    pytest-asyncio not installed       use asyncio.run in sync tests       3 passed
```

## Result

- Final status: PASS for static compile, route-level dry-run, and TypeScript compile.
- Remaining risks:
  - Real QE/RD-Agent runtime behavior still requires a controlled manual run later; skipped intentionally per user requirement.
  - Production backend/frontend/RD-Agent services must be restarted by the operator before the new endpoints/UI are available.
- Need production backend restart: no
- Need dev service restart: yes, for manual UI/API testing after merge
