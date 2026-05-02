# QE RD-Agent remaining worker workspace access remediation

- Module: qe
- Level: L2
- Date: 2026-05-02T17:23:01
- Git commit: 370b154
- Operator: lc999

## Scope

- Changed files:
  - `tests/aistock_validation/modules/qe.md`
  - `backend/services/paper_trading/db_selection_service.py`
  - `backend/services/paper_trading/training_service.py`
  - `backend/inference_engine.py`
  - `backend/routers/rdagent.py`
  - `backend/routers/rdagent_sync_admin.py`
  - `backend/services/rdagent_task_sync_service.py`
  - `backend/services/rdagent_factor_catalog_sync.py`
  - `backend/services/rdagent_model_catalog_sync.py`
  - `backend/services/rdagent_catalog_etl_service.py`
  - `backend/services/rdagent_results_api_client.py`
  - `backend/services/rdagent_asset_service.py`
  - `backend/services/selection_center/hmm_runtime.py`
  - `backend/services/strategy_package/workspace_policy.py`
  - `backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py`
- Impacted flows:
  - QE legacy DB selection read path.
  - RD-Agent local task manifest/assets read path.
  - RD-Agent factor/model catalog sync read/write cache path.
  - RD-Agent catalog ETL import from scan payload.
  - RD-Agent sync admin `complete_assets` read proxy.
  - Legacy Paper Trading retraining source-config lookup.
  - Selection Center HMM artifact path resolution.
  - RD-Agent asset bundle cache path policy.
- Business goal:
  - Windows-side FastAPI code must not read, scan, copy, mutate, or delete QE/RD-Agent worker workspaces.
  - WSL and remote Linux nodes are both treated as independent workers; `/mnt/...`, `\\wsl$`, `QE_WORKSPACE_WIN`, `RDAGENT_WORKSPACE_WIN`, and DB `workspace_path` are not authoritative local file paths.
  - UI/API data must come from DB, node APIs, or AIstock-owned materialized caches, with explicit failure when assets are unavailable.
- Out of scope:
  - QE experiment creation, scheduling, retry, resume/restore, fork, append, and task rerun flows.
  - WSL execution refactors for HMM training, manual factor validation, factor-cache remote sync, and QE config generation.
  - Destructive real cleanup against production tasks.
- Protected assets reviewed:
  - No real worker workspace, `mlruns`, model weights, HMM snapshots, frozen StrategyPackage manifests, or active QE/RD-Agent tasks were modified.
  - Tests used mocked DB/API clients and temporary directories for worker-path refusal cases.

## Environment

- Backend port: existing dev backend `127.0.0.1:8011` for smoke only; production `8001` was not restarted.
- Frontend port: not used; no frontend code changed.
- TDX port:
- Conda/env: current AIstock Python environment via `python`.
- Database: local configured DB via dev backend smoke; unit tests mocked DB where destructive or path-policy behavior was validated.
- Browser/headless: not used in this L2 backend/path-policy slice.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | Guardrail scan on all touched files: 0 findings | PASS |
| Backend tests | QE read/cleanup/path-policy suites pass | 41-test QE suite, 26-test cleanup/path suite, 13-test remaining-path suite | PASS |
| API flow | Dev API responds without 5xx and reports real service state | `GET /rdagent/sync/status` 200 idle; `GET /quantevolver/evolution/tasks` 200 with real task rows | PASS |
| UI E2E | Not required because no UI code changed | UI regression is covered by matrix entry for future L3; this patch changes backend data sources only | N/A |
| Asset safety | No protected asset modified silently | `git diff --check` clean; destructive flows mocked only | PASS |

## Commands

```bash
python -m py_compile backend/services/paper_trading/db_selection_service.py backend/services/paper_trading/training_service.py backend/inference_engine.py backend/routers/rdagent.py backend/routers/rdagent_sync_admin.py backend/services/rdagent_task_sync_service.py backend/services/rdagent_factor_catalog_sync.py backend/services/rdagent_model_catalog_sync.py backend/services/rdagent_catalog_etl_service.py backend/services/rdagent_results_api_client.py backend/services/rdagent_asset_service.py backend/services/selection_center/hmm_runtime.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
python -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python -m pytest backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python -m pytest backend/tests/unified_engine/test_qe_evolution_read_paths.py backend/tests/unified_engine/test_qe_experiment_read_paths.py backend/tests/unified_engine/test_qe_log_stream_lifecycle.py backend/tests/unified_engine/test_qe_stop_task.py backend/tests/unified_engine/test_qe_cleanup_path_policy.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python -m pytest backend/tests/selection_center/test_runtime_selection.py -k "hmm" -q
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH tests/aistock_validation/modules/qe.md backend/services/paper_trading/db_selection_service.py backend/services/paper_trading/training_service.py backend/inference_engine.py backend/routers/rdagent.py backend/routers/rdagent_sync_admin.py backend/services/rdagent_task_sync_service.py backend/services/rdagent_factor_catalog_sync.py backend/services/rdagent_model_catalog_sync.py backend/services/rdagent_catalog_etl_service.py backend/services/rdagent_results_api_client.py backend/services/rdagent_asset_service.py backend/services/selection_center/hmm_runtime.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
git diff --check -- tests/aistock_validation/modules/qe.md backend/services/paper_trading/db_selection_service.py backend/services/paper_trading/training_service.py backend/inference_engine.py backend/routers/rdagent.py backend/routers/rdagent_sync_admin.py backend/services/rdagent_task_sync_service.py backend/services/rdagent_factor_catalog_sync.py backend/services/rdagent_model_catalog_sync.py backend/services/rdagent_catalog_etl_service.py backend/services/rdagent_results_api_client.py backend/services/rdagent_asset_service.py backend/services/selection_center/hmm_runtime.py backend/services/strategy_package/workspace_policy.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
```

## Evidence

- API calls:
  - `http://127.0.0.1:8011/api/v1/rdagent/sync/status` returned HTTP 200 with `state=idle`.
  - `http://127.0.0.1:8011/api/v1/quantevolver/evolution/tasks` returned HTTP 200 with real task rows.
- DB checks:
  - DB-touching destructive scenarios were mocked.
  - Live dev API smoke confirmed the backend can query configured data without restarting production.
- Log files:
  - No production log mutation or service restart.
- Playwright report/trace:
  - Not run; no frontend change in this L2 slice.
- Screenshots:
  - None.
- Business output summary:
  - QE DB selection now materializes runtime assets through the node/API resolver.
  - RD-Agent task reads and catalog sync are restricted to guarded AIstock-owned caches.
  - RD-Agent catalog ETL preserves worker `workspace_path` as metadata only.
  - `complete_assets` admin read path now proxies the RD-Agent Results API instead of shelling into WSL.
  - Legacy retraining source-config lookup no longer scans RD-Agent `workspace_path`.
  - HMM runtime refuses `/mnt/...` artifact paths instead of converting them to Windows paths.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial guardrail scan reported a medium hardcoded local path in the new regression test | Test used `/mnt/f/Dev/AIstock/...` as a forbidden sample path | Replaced with a neutral `/mnt/f/worker_hmm/...` sample path | Guardrail rerun completed with 0 findings |
| PowerShell rejected `&&` in a combined compile/test command | Local shell does not support `&&` as a statement separator in this context | Reran compile and pytest as separate commands | Compile and pytest passed |

## Result

- Final status: PASS for this L2 backend/path-policy slice.
- Remaining risks:
  - Creation/scheduling/retry/resume/restore flows still intentionally excluded.
  - WSL execution paths remain in QE config generation, Paper retraining execution, HMM training execution, manual factor validation, and factor-cache remote sync; these are execution/migration items, not this read-path file-access patch.
  - UI L3 should be rerun after the next frontend-affecting QE read flow change.
- Need production backend restart: no
- Need dev service restart: no; used already running dev backend `8011` for smoke.
