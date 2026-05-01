# L2 QE Custom Evolution Rerun Loop Cleanup Fallback - 2026-05-01

## Scope

- Fix custom_evo loop rerun failure when the running RD-Agent QE workspace API returns 404 for loop-scoped cleanup.
- Reported failure: `RD-Agent QE workspace API does not expose loop-level cleanup or the loop path is unavailable: qe_20260501_011054_c90a/Loop15`.
- Business goal: rerun must remove stale Loop workspace artifacts without deleting sibling Loop results, then delete old DB/result rows and queue the target Loop again.
- No real QE training or backtest was executed.

## Root Cause Evidence

- Local RD-Agent source has `DELETE /api/v1/qe_workspace/tasks/{task_id}/loops/{loop_id}`.
- The currently running RD-Agent process on `http://127.0.0.1:9000/openapi.json` does not expose that path, so AIstock received HTTP 404 during rerun cleanup.
- The affected task `qe_20260501_011054_c90a` Loop15 is on node `wsl2-5080`; its node config includes a local WSL workspace path under `/mnt/f/...`, which can be mapped safely to a Windows path for loop-only deletion.

## Fix

- Added typed `QELoopWorkspaceCleanupUnavailable` for RD-Agent loop cleanup 404.
- `delete_custom_evo_loop_result` now handles only that explicit cleanup-unavailable error by using node filesystem cleanup.
- Local node fallback maps `/mnt/<drive>/...` or Windows workspace paths to a local `Path`, validates target path containment, and deletes only `{workspace_base}/{task_id}/{LoopN}`.
- Remote node fallback uses SSH with `BatchMode=yes`, validates target containment on the remote host using Python `Path.resolve()`, and deletes only the target Loop directory.
- Other cleanup errors still fail fast; no task-level cleanup is used for rerun, so sibling loops are not deleted.
- The cleanup result is returned as `remote_cleanup` with `method`, `node_id`, `workspace_base`, `existed`, and reason fields for traceability.

## Commands Run

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py backend/routers/quantevolver_evolution.py
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_node_execution.py -q
git diff --check -- backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py
```

## Results

- Targeted rerun/append route and service tests: 10 passed.
- Wider QE custom-evo/log/node targeted suite: 20 passed.
- `py_compile`: passed for changed service/client and adjacent router.
- `git diff --check`: passed; only existing line-ending normalization warnings were reported.

## Regression Coverage Added

- Rerun old-result deletion falls back to filesystem cleanup when RD-Agent loop cleanup endpoint is missing.
- Local node filesystem cleanup deletes only the requested Loop directory and preserves sibling Loop directories.

## Residual Risks

- If a remote node lacks both the loop cleanup API and SSH access, rerun will still fail fast with the SSH cleanup error. This is intentional because stale remote Loop artifacts must not be silently retained.
- Backend restart is required for production port 8001 to load the fix. RD-Agent restart is still recommended later so the API endpoint matches source, but rerun can now proceed via the explicit filesystem cleanup path.
