# QE non-task worker path hardening continuation

- Module: qe
- Level: L2
- Date: 2026-05-02T17:46:44
- Git commit: pending at record creation; final commit is reported after push
- Operator: lc999

## Scope

- Changed files:
- `backend/services/strategy_package/model_asset_resolver.py`
- `backend/services/strategy_package/workspace_policy.py`
- `backend/services/rdagent_asset_service.py`
- `backend/tests/strategy_package/test_model_asset_resolver.py`
- `backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py`
- `tests/aistock_validation/modules/qe.md`
- Impacted flows:
- StrategyPackage execution model asset source/cache path validation.
- RD-Agent local production bundle cache path validation and zip extraction.
- QE validation matrix coverage for non-create/non-retry worker path boundaries.
- Business goal:
- Windows-side backend must not translate or probe WSL/worker model paths.
- RD-Agent bundle cache must stay under AIstock-owned local roots and reject traversal before local read/write/extract.
- Existing QE read/cleanup APIs must remain observable and return accurate task metadata on dev port 8011.
- Out of scope:
- QE experiment creation, scheduling, dispatch, retry, resume, restore, rerun, and worker execution flows.
- Production backend 8001 restart and WSL/RD-Agent production API 9000 restart.
- Destructive cleanup against real QE/RD-Agent tasks or worker assets.
- Protected assets reviewed:
- No QE/RD-Agent worker workspace, model weight, mlruns, StrategyPackage frozen manifest, HMM snapshot, or production task artifact was modified.
- Tests used temporary directories and read-only API probes only.

## Environment

- Backend port: existing dev backend `127.0.0.1:8011` for read-only API smoke; not restarted by this run
- Frontend port: not used; no UI code changed in this slice
- TDX port: not used
- Conda/env: repository default Python invocation from `F:\Dev\AIstock`
- Database: existing dev backend connection only through read-only API smoke
- Browser/headless: not used; UI behavior not changed, and API smoke verified QE data remains visible

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py --fail-on HIGH ...` returned 0 findings; `git diff --check` returned no errors | PASS |
| Backend tests | Worker path policy and QE read/cleanup regressions pass | Focused suite: 23 passed; expanded QE matrix: 51 passed | PASS |
| API flow | Dev QE API still returns real task metadata without 5xx | `GET /quantevolver/evolution/tasks`: status success, 49 tasks, 1 running/processing; `GET /tasks/qe_20260502_162747_0313`: current_loop=4, max_loops=4, loop_count=4 | PASS |
| UI E2E | No UI regression expected for this backend-only policy slice | No frontend files changed; API payloads used by QE UI are still readable on 8011 | NOT RUN |
| Asset safety | No protected asset modified silently | Only source/tests/matrix/history were changed; zip-slip tests used temp dirs | PASS |

## Commands

```bash
python -m py_compile backend/services/strategy_package/model_asset_resolver.py backend/services/strategy_package/workspace_policy.py backend/services/rdagent_asset_service.py backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py

python -m pytest backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q

python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py backend/tests/strategy_package/test_model_asset_resolver.py -q

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/strategy_package/model_asset_resolver.py backend/services/strategy_package/workspace_policy.py backend/services/rdagent_asset_service.py backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py tests/aistock_validation/modules/qe.md

git diff --check -- backend/services/strategy_package/model_asset_resolver.py backend/services/strategy_package/workspace_policy.py backend/services/rdagent_asset_service.py backend/tests/strategy_package/test_model_asset_resolver.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py tests/aistock_validation/modules/qe.md

$base='http://127.0.0.1:8011/api/v1'
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks" -Method Get -TimeoutSec 20
Invoke-RestMethod -Uri "$base/quantevolver/evolution/tasks/qe_20260502_162747_0313" -Method Get -TimeoutSec 20
Invoke-RestMethod -Uri "$base/rdagent/sync/status" -Method Get -TimeoutSec 20
```

## Evidence

- API calls:
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks` -> `status=success`, `task_count=49`, `running_or_processing=1`, first task `qe_20260502_162747_0313` completed.
- `GET http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks/qe_20260502_162747_0313` -> `status=success`, `task_status=completed`, `current_loop=4`, `max_loops=4`, `loop_count=4`.
- `GET http://127.0.0.1:8011/api/v1/rdagent/sync/status` -> HTTP 200 with status fields `state, phase, progress, started_at_ts, finished_at_ts, last_error, last_result`.
- DB checks:
- No direct DB writes or destructive DB checks in this slice.
- Log files:
- Not collected; tests and read-only API smoke produced no unexpected 5xx.
- Playwright report/trace:
- Not collected; no UI code changed.
- Screenshots:
- Not collected.
- Business output summary:
- Model resolver now fails fast on `/mnt/...` worker model paths instead of translating them to Windows paths.
- RD-Agent bundle cache rejects traversal in bundle ids, zip members, manifest relpaths, and workspace ids.
- QE read/cleanup policy regression suite still passes, and dev QE task API still returns real task/loop metadata.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this run | N/A | N/A | Focused and expanded pytest suites passed |

## Result

- Final status: PASS
- Remaining risks:
- Execution-oriented WSL flows remain out of scope by user instruction: QE creation/config composition, scheduling/dispatch, retry/resume/restore/rerun, HMM/manual factor/training WSL executors, and remote factor-cache sync.
- Existing dev backend on 8011 showed one running/processing QE task; this run did not touch it.
- Need production backend restart: no
- Need dev service restart: no for validation; code deployment will require the normal service reload window chosen by the user
