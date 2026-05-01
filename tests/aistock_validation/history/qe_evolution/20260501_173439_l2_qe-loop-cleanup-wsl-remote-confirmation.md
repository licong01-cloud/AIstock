# L2 QE Loop Cleanup WSL/Remote Confirmation - 2026-05-01

## Scope

- Confirm the custom_evo rerun loop-cleanup fix works for both local WSL nodes and remote Linux nodes.
- Confirm there is no silent-success fallback in the cleanup path.
- No real QE training or backtest was executed.

## WSL Behavior Confirmed

- Local node is selected when `api_base_url` host is `127.0.0.1`, `localhost`, or `::1`.
- Local cleanup maps `workspace_base` from either Windows path or `/mnt/<drive>/...` WSL path to a Windows `Path`.
- Cleanup target is exactly `{workspace_base}/{task_id}/{LoopN}`.
- The code validates both task directory and loop directory are inside the configured workspace before deletion.
- If the target Loop does not exist, the result records `existed=false`; it is not treated as an unverified success because the path was still resolved and bounded.

## Remote Behavior Confirmed

- Remote node is selected when `api_base_url` host is not local.
- Remote cleanup requires a valid `ssh_user` or derives one only from an unambiguous `/home/<user>/...` workspace path.
- Remote cleanup executes SSH with `BatchMode=yes` and `ConnectTimeout=10`.
- The remote Python command validates `{workspace_base}/{task_id}/{LoopN}` containment using `Path.resolve()` before deleting.
- SSH non-zero exit raises `RuntimeError`; rerun submission fails fast instead of keeping stale artifacts.

## No Silent Fallback Confirmation

- Compatibility path activates only for typed `QELoopWorkspaceCleanupUnavailable`, which comes from RD-Agent HTTP 404 on the loop cleanup endpoint.
- Other RD-Agent cleanup errors still raise and abort rerun.
- Filesystem cleanup failure raises and aborts rerun.
- No task-level workspace cleanup is used for rerun, so sibling loops are not deleted.
- Cleanup method and reason are returned in `cleanup.remote_cleanup` for API traceability.

## Commands Run

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/qe_workspace_client.py backend/routers/quantevolver_evolution.py
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_node_execution.py -q
git diff --check -- backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py
python - <<openapi check via stdin>>
```

## Results

- Targeted QE custom-evo/log/node suite: 22 passed.
- `py_compile`: passed for service/client/router.
- `git diff --check`: passed; only existing line-ending normalization warning was reported.
- Current RD-Agent on `127.0.0.1:9000` still does not expose the loop delete path, so the compatibility path is necessary until RD-Agent is restarted/updated.

## Coverage Added

- WSL/local branch: deletes only the target Loop directory and preserves sibling loops.
- Remote branch: constructs the expected SSH cleanup command and reports success.
- Remote failure branch: non-zero SSH exit raises and surfaces the error.

## Residual Risk

- This verification used mocked SSH for remote cleanup to avoid deleting real remote files. Actual remote execution depends on SSH reachability and permissions; if unavailable, rerun correctly fails fast.
