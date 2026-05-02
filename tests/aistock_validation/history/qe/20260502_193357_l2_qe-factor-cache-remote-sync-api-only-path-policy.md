# QE factor-cache remote sync API-only path policy

- Module: qe
- Level: L2
- Date: 2026-05-02T19:33:57
- Git commit: pending at record creation; final commit is reported after push
- Operator: lc999

## Scope

- Changed files:
- `backend/services/quantevolver/factor_cache_remote_sync_service.py`
- `backend/services/strategy_package/workspace_policy.py`
- `backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py`
- `tests/aistock_validation/modules/qe.md`
- Impacted flows:
- QE factor-cache remote stats/sync planning/execution service.
- Local factor-cache status/job record files under AIstock-owned `rdagent_assets/factor_values`.
- Business goal:
- Factor-cache remote sync must never access worker directories through WSL, SSH, rsync, or `/mnt` conversion.
- Windows-side code may read only AIstock-owned local cache files, then ask the execution-node API to verify/write remote cache data.
- WSL-local API nodes such as `127.0.0.1:9000` are treated as independent execution nodes and are not skipped merely because they are localhost.
- Out of scope:
- QE experiment creation, config composition, scheduling, dispatch, retry, resume, restore, rerun.
- Implementing or restarting the execution-node factor-cache API on WSL/RD-Agent production service.
- Destructive cleanup or real remote factor-cache mutation.
- Protected assets reviewed:
- No QE/RD-Agent worker workspace, remote cache directory, model weight, mlruns, HMM snapshot, StrategyPackage frozen manifest, or live task artifact was modified.
- New tests use temporary local cache directories and fake node API clients only.

## Environment

- Backend port: existing dev backend `127.0.0.1:8011` for read-only QE API smoke; not restarted
- Frontend port: not used; no UI code changed
- TDX port: not used
- Conda/env: repository default Python invocation from `F:\Dev\AIstock`
- Database: no DB writes in tests; read-only dev API smoke used existing backend connection
- Browser/headless: not used; backend-only service policy change

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py --fail-on HIGH ...` returned 0 findings; `git diff --check` returned no errors | PASS |
| Backend tests | Factor-cache policy and QE read/cleanup regressions pass | Focused factor-cache policy: 6 passed; expanded QE matrix: 57 passed | PASS |
| API flow | Existing dev QE API remains readable without 5xx | `GET /quantevolver/evolution/tasks`: status success, 50 tasks, 3 running/processing; selected detail returned status success and 2 loop records | PASS |
| UI E2E | No UI regression expected for backend-only policy slice | No frontend files changed; API smoke verified QE data remains observable | NOT RUN |
| Asset safety | No protected asset modified silently | Only source/tests/matrix/history changed; tests used temp directories and fake node API | PASS |

## Commands

```bash
python -m py_compile backend/services/quantevolver/factor_cache_remote_sync_service.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py

python -m pytest backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py -q

python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py backend/tests/strategy_package/test_model_asset_resolver.py -q

Select-String -Path backend/services/quantevolver/factor_cache_remote_sync_service.py -Pattern 'subprocess','[''wsl''','["wsl"','rsync','ssh','_run_wsl_bash','_run_rsync','_win_to_wsl' -SimpleMatch

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/quantevolver/factor_cache_remote_sync_service.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py tests/aistock_validation/modules/qe.md

git diff --check -- backend/services/quantevolver/factor_cache_remote_sync_service.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py tests/aistock_validation/modules/qe.md

$base='http://127.0.0.1:8011/api/v1'
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks" -Method Get -TimeoutSec 20
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks/{first_task_id}" -Method Get -TimeoutSec 20
```

## Evidence

- API calls:
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks` -> `status=success`, `task_count=50`, `running_or_processing=3`, first task `qe_20260502_193154_17a2`, first status `failed`.
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks/qe_20260502_193154_17a2` -> `status=success`, `detail_loop_count=2`, task status `failed`.
- DB checks:
- No direct DB writes in tests. The modified service no longer writes inferred default remote cache directories from Windows.
- Log files:
- Not collected; tests and read-only API smoke produced no unexpected 5xx.
- Playwright report/trace:
- Not collected; no UI code changed.
- Screenshots:
- Not collected.
- Business output summary:
- `factor_cache_remote_sync_service` no longer imports or runs `subprocess`, WSL, SSH, rsync, `_win_to_wsl`, `_run_wsl_bash`, or `_run_rsync`.
- Remote metadata, file-existence checks, and sync uploads now go through execution-node factor-cache HTTP APIs under `/api/v1/qe_workspace/factor-cache/*`.
- Local factor cache roots are guarded as AIstock-owned artifact paths; worker roots and factor-name traversal are rejected.
- Localhost WSL API nodes are listed as sync-capable API nodes instead of being skipped for being `127.0.0.1`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this run | N/A | N/A | Focused and expanded pytest suites passed |

## Result

- Final status: PASS
- Remaining risks:
- The WSL/RD-Agent production node service must expose compatible factor-cache APIs for live remote sync. This run intentionally did not restart or mutate service `9000`, so endpoint availability was validated with fake node API tests, not against production.
- Existing dev backend `8011` was not restarted, so its running process may still contain the pre-change implementation until the next dev/prod reload.
- QE creation/config composition and task execution paths remain out of scope.
- Need production backend restart: no
- Need dev service restart: no for validation; code deployment requires a normal backend reload chosen by the user
