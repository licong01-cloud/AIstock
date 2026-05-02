# QE experiment delete node API workspace cleanup

- Module: qe
- Level: L2
- Date: 2026-05-03T01:56:38
- Git commit: b6d6467
- Operator: lc999

## Scope

- Changed files: `backend/routers/quantevolver.py`, `backend/tests/unified_engine/test_qe_cleanup_path_policy.py`, `tests/aistock_validation/modules/qe.md`
- Impacted flows: QE experiment history delete, parent experiment cleanup, child Loop cleanup, DB cleanup ordering
- Business goal: deleting a QE history item must remove DB records and the real worker workspace through QE node APIs, not by Windows-side WSL filesystem access
- Out of scope: real destructive deletion against production experiments, QE task creation/retry/resume, production backend/API restart
- Protected assets reviewed: no QE/RD-Agent workspace, mlruns, model weights, HMM snapshots, StrategyPackage manifests, or paper ledgers modified

## Environment

- Backend port: not started; direct router/unit validation only
- Frontend port: not started; no UI change
- TDX port: not used
- Conda/env: local Python via `python`
- Database: mocked `get_conn` for destructive cleanup paths
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | guardrail scan on changed QE files returned 0 findings | PASS |
| Parent delete | Real `qe_task_id` worker workspace is cleaned via node API before DB delete | unit test asserts `node-a/node-b` cleanup calls for `qe_actual_task` and DB task delete afterward | PASS |
| Child Loop delete | Child Loop deletion uses loop API and keeps parent task record | unit test asserts `cleanup_loop_workspace(qe_actual_task, Loop3)` and no `DELETE FROM qe_evolution_tasks` | PASS |
| Fail-fast | Node cleanup failure must not delete DB rows or local caches | unit test raises HTTP 502 and local test cache remains | PASS |
| Worker red line | Windows side must not delete worker workspace or use `QE_WORKSPACE_WIN` | static/unit tests verify no worker path use; simulated worker dir remains | PASS |
| Related cleanup regression | Custom-evo cleanup tests still pass | `test_qe_custom_evo_mutation_service.py` | PASS |
| Remaining workspace policy | Existing non-create/retry workspace policy tests still pass | `test_worker_workspace_policy_remaining_paths.py` | PASS |

## Commands

```bash
python -m py_compile backend/routers/quantevolver.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py
python -m pytest backend/tests/unified_engine/test_qe_cleanup_path_policy.py -q
python -m pytest backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
python -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/routers/quantevolver.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py tests/aistock_validation/modules/qe.md
```

## Evidence

- API calls: real destructive API call intentionally not executed; route called directly with mocked DB/node clients
- DB checks: mocked SQL order confirms remote cleanup before `DELETE FROM qe_experiments`; child Loop path does not delete `qe_evolution_tasks`
- Log files: not applicable
- Playwright report/trace: not applicable; no UI change and destructive cleanup is covered by mocked router tests
- Screenshots: not applicable
- Business output summary: py_compile passed; cleanup policy tests `10 passed`; related custom-evo/worker-policy tests `23 passed`; delete now resolves `qe_task_id`/`task_id`, calls task or loop node cleanup, fails before DB/local deletion when node cleanup fails, and reports `worker_cleanup_results`

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Workspace directory remained after history delete | delete endpoint only called node cleanup with `experiment_id`; many history rows store the actual worker directory in `qe_task_id` | collect selected/child/evolution-task metadata, clean real task ids via node API, and use loop-level cleanup for child loops | `test_qe_experiment_delete_uses_qe_task_id_for_worker_workspace` |
| DB could be deleted even if workspace cleanup failed | previous code logged warnings and continued | node cleanup now fail-fast with HTTP 502 before local cache or DB deletion | `test_qe_experiment_delete_fails_before_db_delete_when_node_cleanup_fails` |

## Result

- Final status: PASS
- Remaining risks: production service on port 8001 will keep old behavior until the user restarts it; no real production experiment was deleted during validation
- Need production backend restart: yes, to activate this backend code path, but not performed per production isolation rule
- Need dev service restart: only if a dev backend is already running and should pick up the change
