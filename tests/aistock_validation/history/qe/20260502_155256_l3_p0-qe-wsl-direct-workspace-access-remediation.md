# P0 QE WSL direct workspace access remediation

- Module: qe / strategy_package / rdagent_catalog
- Level: L3
- Date: 2026-05-02T15:52:56
- Git base commit: a3ce3fd
- Operator: lc999

## Scope

- Changed files:
  - backend/services/strategy_package/workspace_policy.py
  - backend/services/strategy_package/qe_source_resolver.py
  - backend/services/strategy_package/live_inference.py
  - backend/services/strategy_package/selection_artifact.py
  - backend/services/quantevolver/qe_selection_service.py
  - backend/inference_engine.py
  - backend/routers/rdagent_catalog_admin.py
  - backend/tests/strategy_package/test_qe_source_resolver.py
  - backend/tests/selection_center/test_runtime_selection.py
  - backend/tests/test_rdagent_catalog_admin_loop_file.py
- Impacted flows:
  - RD-Agent catalog loop file preview.
  - StrategyPackage QE source asset checks.
  - StrategyPackage live/latest-data QE inference source asset loading.
  - StrategyPackage diagnostic QE pred.pkl artifact generation.
  - Legacy QE experiment selection endpoint.
  - InferenceEngine experiment workspace mode.
- Business goal:
  - Windows-side FastAPI request paths must not directly read WSL/RD-Agent worker workspaces.
  - Runtime assets must be obtained through node APIs and materialized into AIstock-owned cache directories before local reads.
  - Missing diagnostic pred.pkl must fail explicitly instead of scanning worker workspaces.
- Out of scope:
  - Experiment creation, dispatch, retry/rerun/resume/fork/append, delete/cleanup.
  - Running QE task control or worker mutation.
  - Production backend 8001 restart and WSL production API restart.
- Protected assets reviewed:
  - No mlruns/model/HMM/manifest/selection artifact/paper ledger files were modified.
  - Only source code, tests, and this validation record were changed.

## Environment

- Production backend: 8001 remained running with PID 36412; not restarted.
- Dev backend: temporary 8012 only, stopped after validation; 8012 no longer listening.
- Existing dev backend: 8011 was already occupied by PID 71452 and was not touched.
- TDX port: 19080 was read by the temp backend startup path only.
- Conda/env:
  - Unit tests and guardrails used `C:\Users\lc999\miniconda3\python.exe`.
  - Temp backend used `C:/Users/lc999/miniconda3/envs/aistock/python.exe`.
- Browser/headless:
  - Playwright QE read-only UI suite ran against temp backend 8012 and temp frontend 3012.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk direct path/secret/silent fallback finding in touched files | `scan_quality_guardrails.py --fail-on HIGH ...` -> 0 findings | PASS |
| Py compile | All changed backend/test files compile | `python -m py_compile ...` -> exit 0 | PASS |
| Backend tests | StrategyPackage/QE/RDAgent P0 tests pass | targeted pytest -> 7 passed; surrounding selection-center slice -> 31 passed | PASS |
| API flow | Dev backend 8012 serves read-only package/QE endpoints and exposes fail-fast diagnostic error | `/strategy-packages`, `/selection-artifacts`, `/quantevolver/evolution/tasks`, task detail all 200; diagnostic pred.pkl without source_path returns structured 404 DATA_UNAVAILABLE | PASS |
| UI-visible data | Existing UI data sources still contain real package, artifact, task, loop, metric/status data | package `pkg_99142cb1440c40a7824e83902f4e7da9`, source `qe_20260416_082012`, status `SELECTION_ENABLED`; 3 authoritative artifacts; score preview 10; task `qe_20260502_131502_9b54` detail loops=4 | PASS |
| UI E2E | QE UI pages display task/loop/enhanced metrics/log data and terminal-task dashboard does not auto-poll | `nox -s qe_read_ui -- 8012 3012` -> 4 Playwright tests passed | PASS |
| Asset safety | No protected asset modified silently | git diff limited to code/tests/run record; no asset/cache/worker files staged | PASS |

## Commands

```powershell
python -m py_compile backend/services/strategy_package/workspace_policy.py backend/services/strategy_package/qe_source_resolver.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/quantevolver/qe_selection_service.py backend/inference_engine.py backend/routers/rdagent_catalog_admin.py backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/selection_center/test_runtime_selection.py backend/tests/test_rdagent_catalog_admin_loop_file.py

python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/strategy_package/workspace_policy.py backend/services/strategy_package/qe_source_resolver.py backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/quantevolver/qe_selection_service.py backend/inference_engine.py backend/routers/rdagent_catalog_admin.py backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/selection_center/test_runtime_selection.py backend/tests/test_rdagent_catalog_admin_loop_file.py

python -m pytest backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/selection_center/test_runtime_selection.py::test_selection_artifact_service_generates_qe_prediction_as_diagnostic_only backend/tests/selection_center/test_runtime_selection.py::test_live_inference_factor_order_uses_static_dataloader_schema backend/tests/selection_center/test_runtime_selection.py::test_live_inference_load_source_materializes_via_node_api_not_db_workspace backend/tests/selection_center/test_runtime_selection.py::test_selection_artifact_diagnostic_requires_explicit_source_path_without_workspace_scan backend/tests/test_rdagent_catalog_admin_loop_file.py -q

python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/strategy_package/test_qe_source_resolver.py backend/tests/test_rdagent_catalog_admin_loop_file.py -q

$env:DISABLE_INGESTION_SCHEDULER='1'
$env:DISABLE_STRATEGY_SCHEDULER='1'
$env:DISABLE_PAPER_TRADING_SCHEDULER='1'
$env:ENABLE_PAPER_TRADING_V2_SCHEDULER='0'
$env:DISABLE_NODE_HEALTH_SCHEDULER='1'
$env:DISABLE_HMM_SCHEDULER='1'
$env:DISABLE_EVOLUTION_SCANNER='1'
$env:DISABLE_QE_EXPERIMENT_SCANNER='1'
$env:AISTOCK_DISABLE_STARTUP_TASKS='1'
C:/Users/lc999/miniconda3/envs/aistock/python.exe -m uvicorn backend.main:app --host 127.0.0.1 --port 8012

Invoke-RestMethod http://127.0.0.1:8012/api/v1/strategy-packages?limit=1
Invoke-RestMethod http://127.0.0.1:8012/api/v1/strategy-packages/pkg_99142cb1440c40a7824e83902f4e7da9/selection-artifacts?limit=3
Invoke-RestMethod http://127.0.0.1:8012/api/v1/quantevolver/evolution/tasks?limit=1
Invoke-RestMethod http://127.0.0.1:8012/api/v1/quantevolver/evolution/tasks/qe_20260502_131502_9b54

$env:BACKEND_PORT='8012'
$env:FRONTEND_PORT='3012'
$env:QE_API_BASE='http://127.0.0.1:8012/api/v1'
$env:NEXT_PUBLIC_API_BASE='http://127.0.0.1:8012/api/v1'
$env:QE_READ_TASK_ID='qe_20260414_173338_d1c5'
C:/Users/lc999/miniconda3/envs/aistock/python.exe -m nox -s qe_read_ui -- 8012 3012
```

## API Evidence

- `GET /api/v1/strategy-packages?limit=1`
  - 200 OK.
  - package_id=`pkg_99142cb1440c40a7824e83902f4e7da9`
  - source_id=`qe_20260416_082012`
  - package_status=`SELECTION_ENABLED`
- `GET /api/v1/strategy-packages/pkg_99142cb1440c40a7824e83902f4e7da9/selection-artifacts?limit=3`
  - 200 OK.
  - artifact count=3.
  - first artifact status=`SUCCEEDED`.
  - metadata.source_type=`live_qe_model_inference_v1`.
  - metadata.authority_scope=`authoritative_selection`.
  - score_preview count=10.
- `GET /api/v1/quantevolver/evolution/tasks?limit=1`
  - 200 OK.
  - task_id=`qe_20260502_131502_9b54`.
  - status=`running`.
  - current_loop=4, max_loops=4.
- `GET /api/v1/quantevolver/evolution/tasks/qe_20260502_131502_9b54`
  - 200 OK.
  - loops=4.
  - detail_status=`running`.
- `POST /api/v1/strategy-packages/pkg_99142cb1440c40a7824e83902f4e7da9/selection-artifacts/generate-diagnostic-backtest`
  - request omitted `source_path`.
  - 404 DATA_UNAVAILABLE.
  - message=`diagnostic QE pred.pkl generation requires an explicit AIstock-local source_path; automatic worker workspace scanning is disabled`.
- QE read-only UI E2E:
  - `QE evolution terminal task detail is read-only, accurate, and observable` passed.
  - `QE dashboard stops automatic polling when task list has no active task` passed.
  - `QE experiment detail reads accurate enhanced data through experiment API only` passed.
  - `QE terminal log UI shows node log tail without local workspace wording` passed.
  - Run result: 4 passed in 1.4 minutes; no page error, console error, request failure, or unexpected API 4xx/5xx.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Guardrail initially reported 3 HIGH `SILENT_EMPTY_SUCCESS` findings in `qe_selection_service.py` | Existing helper exceptions returned `None` without logging | Added debug logging before returning `None` for optional name/pct helpers | Guardrail rerun -> 0 findings |

## Result

- Final status: PASS.
- Need production backend restart: no restart was performed; code changes require user-controlled production restart only when ready.
- Need dev service restart: temporary 8012 backend was stopped; 8012 no longer listening.
- Remaining risks:
  - Full live inference generation was not executed because it can trigger heavy WSL/Qlib inference; existing authoritative artifact listing was used as a read-only business-data check.
  - Experiment creation/dispatch/retry/delete paths still contain historical `QE_WORKSPACE_WIN` usage and remain intentionally out of this P0 read-path scope.
