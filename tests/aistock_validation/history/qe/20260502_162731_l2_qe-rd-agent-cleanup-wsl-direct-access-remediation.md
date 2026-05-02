# QE RD-Agent cleanup WSL direct access remediation

- Module: qe
- Level: L2
- Date: 2026-05-02T16:27:31
- Git commit: pending-this-run
- Operator: lc999

## Scope

- Changed files:
  - `backend/services/strategy_package/workspace_policy.py`
  - `backend/services/quantevolver/qe_evolution_service.py`
  - `backend/routers/quantevolver.py`
  - `backend/routers/rdagent.py`
  - `backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py`
  - `backend/tests/unified_engine/test_qe_cleanup_path_policy.py`
- Impacted flows:
  - QE single experiment delete local/remote cleanup.
  - QE evolution task delete local/remote cleanup.
  - QE custom_evo loop delete/rerun cleanup pre-step.
  - RD-Agent task delete local dispatch log cleanup plus remote API cleanup.
- Business goal:
  - Windows-side FastAPI cleanup paths must never directly delete, scan, or mutate WSL/RD-Agent worker workspaces.
  - Worker workspace cleanup must be performed by QE/RD-Agent node APIs only.
  - Local deletes must be limited to explicit AIstock-owned artifact roots and must refuse worker roots, path traversal, and root-directory deletion.
- Out of scope:
  - Experiment creation, scheduling, retry, rerun implementation changes beyond fail-fast cleanup pre-step behavior.
  - UI changes and live production deletion calls.
  - Restarting production backend `8001` or WSL/RD-Agent API `9000`.
- Protected assets reviewed:
  - No QE/RD-Agent worker workspace, `mlruns`, model weights, HMM snapshots, StrategyPackage frozen manifests, or production task artifacts were modified.
  - Destructive behavior validated with temp directories and mocked DB/API clients only.

## Environment

- Backend port: not started; no production/dev backend restart required for these unit/API-router mocked tests.
- Frontend port: not started; no UI code changed and cleanup endpoints are destructive.
- TDX port: not used.
- Conda/env: default local Python from Codex shell.
- Database: mocked DB connection/cursors in tests; no real DB mutation.
- Browser/headless: not used; no UI surface changed.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk WSL path, secret, silent default, or forbidden fallback finding in touched files | `scan_quality_guardrails.py --fail-on HIGH ...` => 0 findings | PASS |
| Py compile | Touched Python files compile | `python -m py_compile ...` => exit 0 | PASS |
| QE delete experiment | Calls `QEWorkspaceClient.cleanup_task_workspace` for default and assigned nodes; removes only `QE_EXPERIMENTS_ROOT` / `QE_SOTA_ASSETS_DIR`; leaves `QE_WORKSPACE_WIN` test worker dir intact | `test_qe_experiment_delete_cleans_local_assets_without_worker_workspace` | PASS |
| QE evolution task cleanup | Source no longer imports/uses `QE_WORKSPACE_WIN`; local cleanup uses explicit AIstock roots; worker cleanup remains node API | `test_evolution_delete_task_no_worker_workspace_direct_cleanup` plus read/log lifecycle regression suite | PASS |
| custom_evo loop cleanup | If loop-level RD-Agent API is unavailable, deletion fails before DB/local cleanup; no local/SSH filesystem fallback remains | `test_delete_custom_evo_loop_result_fails_fast_when_api_route_missing`, `test_custom_evo_cleanup_has_no_direct_node_filesystem_fallbacks` | PASS |
| RD-Agent task cleanup | Remote task deletion uses node API; local dispatch log delete is guarded under repo `dispatch_logs`; DB delete is mocked and observable | `test_rdagent_task_delete_cleans_dispatch_only_after_node_api` | PASS |
| Path policy | Refuses worker workspace roots and root-directory deletion; removes only explicit local child paths | `test_cleanup_policy_refuses_worker_workspace_delete`, `test_cleanup_policy_removes_only_child_under_explicit_local_root` | PASS |
| UI E2E | No UI changed; destructive cleanup endpoints were not called against real experiments/tasks | N/A by asset-safety rule | NOT RUN |

## Commands

```powershell
python -m py_compile backend/services/strategy_package/workspace_policy.py backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py backend/routers/rdagent.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py

python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py -q
# 11 passed in 9.71s

python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py -q
# 28 passed in 8.25s

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/strategy_package/workspace_policy.py backend/services/quantevolver/qe_evolution_service.py backend/routers/quantevolver.py backend/routers/rdagent.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py
# Guardrail scan completed with 0 finding(s).
```

## Evidence

- API calls:
  - No real destructive API calls were made.
  - Router-level delete flows were executed with mocked DB/API clients in pytest.
- DB checks:
  - Mocked SQL captured expected QE and RD-Agent `DELETE` statements only after remote API cleanup success.
  - custom_evo missing cleanup API path captured no DB `DELETE` statements.
- Log files:
  - No production logs modified.
- Playwright report/trace:
  - Not applicable; no UI code changed and destructive endpoints were not run against real data.
- Screenshots:
  - Not applicable.
- Business output summary:
  - QE experiment cleanup leaves a simulated worker workspace intact while local AIstock artifacts and Optuna files are removed.
  - RD-Agent cleanup calls remote API first, then removes only test-owned local dispatch logs and mocked DB rows.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Guardrail initially reported 3 `SILENT_EMPTY_SUCCESS` findings | Existing helper exception handlers returned `None` without logging in touched files | Added debug logging before benign parse failures in QE log payload parser and RD-Agent manifest parsers | Guardrail rerun: 0 findings |
| Historical tests expected local/SSH filesystem fallback for custom_evo loop cleanup | Old behavior violated the new red line that worker workspace cleanup must be node API only | Replaced fallback expectation with fail-fast tests and worker-directory-intact assertions | `28 passed in 8.25s` |

## Result

- Final status: PASS
- Remaining risks:
  - Production backend `8001` must be restarted by the user later to load code changes; no restart was performed in this run.
  - If an RD-Agent node lacks loop-level cleanup API, custom_evo loop delete/rerun cleanup now fails fast instead of deleting via local/SSH filesystem. The node API should be upgraded before using that destructive flow.
  - UI E2E was intentionally not run because there were no UI changes and real cleanup operations would be destructive to production assets.
- Need production backend restart: no action taken; required later for production to pick up backend code.
- Need dev service restart: no.
