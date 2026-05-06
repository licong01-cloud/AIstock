# Selection Center QE evolution loop live inference source fix

- Module: paper_v2_selection_center
- Level: L2 plus direct WSL smoke probe
- Date: 2026-05-06T17:51:17
- Git commit: pending at validation time; final pushed commit is recorded in the handoff response
- Operator: lc999

## Scope

- Changed files: `backend/services/strategy_package/live_inference.py`, `backend/services/strategy_package/selection_artifact.py`, `backend/services/strategy_package/workspace_policy.py`, `backend/tests/selection_center/test_runtime_selection.py`, `backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py`.
- Impacted flows: Selection Center authoritative StrategyPackage live inference artifact generation for `qe_experiment` and `qe_evolution_loop` packages; WSL StrategyPackage inference runtime cache validation.
- Business goal: make Paper v2 Selection Center resolve QE evolution-loop packages to the completed loop experiment, use the correct QE execution node, materialize all runtime assets required by WSL live inference, and fail fast on later data completeness issues instead of returning fake success.
- Out of scope: changing factor data quality, relaxing strict inference missing-value rules, UI E2E, production backend restart, and production asset mutation.

## Environment

- Backend port: not started; production `8001` was not touched.
- Frontend port: not started.
- TDX port: not used.
- Conda/env: local Python from active Codex shell; DB probes loaded `<production-root>\\.env` read-only.
- Database: read-only source/run probes; direct WSL inference smoke did not reach successful signal persistence because it failed fast on feature completeness before scoring.
- Browser/headless: not used.
- Runtime cache: direct WSL smoke regenerated ignored cache files under this worktree's `rdagent_assets/strategy_package_runtime`; no tracked runtime/model asset was committed.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk path/secret/fallback/asset finding | `scan_quality_guardrails.py --fail-on HIGH ...` returned 0 findings; `git diff --check` returned only CRLF warnings | PASS |
| Backend tests | Selection Center/StrategyPackage backend tests and WSL path-policy regression tests pass | `76 passed in 19.12s` after `.env` load | PASS |
| Source resolution | QE evolution-loop package resolves by `(qe_task_id, qe_loop_id)`, not parent task id as direct experiment id | Real package `pkg_1de32357724a4c5b874f2abd90f22da5` resolved to `qe_20260502_231229_0565_L1` | PASS |
| Node/materialization | Runtime assets load from recorded execution node, not default wrong node | Probe used `execution_node_id=rdagent-node1`, downloaded 57 factor files, 1 `params.pkl`, and produced 77-factor prepared order from StaticDataLoader | PASS |
| WSL runtime smoke | Original HTTP 404 path is removed and WSL reaches real inference | WSL smoke prepared `model.py` and reached feature calculation/model path; no HTTP 404 remained | PARTIAL |
| Data completeness oracle | Strict inference must not fill missing features with defaults | WSL smoke failed fast with `input_rows=4636`, `kept_rows=0`, `invalid_cell_count=13047`; this is a remaining data/preprocessing issue, not a 404/source-resolution issue | FAIL-FAST EXPECTED |
| UI E2E | User-visible flow works with no console/page/request errors | Skipped: no frontend changes and no dev services started | SKIPPED |
| Asset safety | No protected tracked asset modified silently | `git status --short -uall` shows only code/tests/validation record; ignored runtime cache used for smoke only | PASS |

## Commands

```bash
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
python -m py_compile backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/strategy_package/workspace_policy.py backend/tests/selection_center/test_runtime_selection.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/selection_center/test_runtime_selection.py -q -k "static_dataloader_schema or materialize_continues or qe_evolution_loop_uses_task_loop or resolves_qe_evolution_loop_source" -p no:cacheprovider
# 4 passed, 29 deselected

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/selection_center/test_runtime_selection.py -q -k "static_dataloader_schema or materialize_continues or materialize_uses_cached_params" -p no:cacheprovider
# 3 passed, 30 deselected

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
python -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py::test_worker_policy_allows_wsl_mounted_aistock_runtime_cache backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py::test_worker_policy_still_refuses_wsl_worker_workspace -q -p no:cacheprovider
# 2 passed

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
# via pytest.main after loading <production-root>\\.env
python -m pytest backend/tests/selection_center backend/tests/strategy_package backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py::test_worker_policy_allows_wsl_mounted_aistock_runtime_cache backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py::test_worker_policy_still_refuses_wsl_worker_workspace -q -p no:cacheprovider
# 76 passed in 19.12s

$env:PYTHONIOENCODING='utf-8'
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/strategy_package/workspace_policy.py backend/tests/selection_center/test_runtime_selection.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py
# Guardrail scan completed with 0 finding(s).

git diff --check -- backend/services/strategy_package/live_inference.py backend/services/strategy_package/selection_artifact.py backend/services/strategy_package/workspace_policy.py backend/tests/selection_center/test_runtime_selection.py backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py tests/aistock_validation/history/paper_v2_selection_center/20260506_175117_l2_selection-center-qe-evolution-loop-live-inference-source-fix.md
# Only CRLF warnings.
```

## Real Probes

### Source materialization probe

- Package: `pkg_1de32357724a4c5b874f2abd90f22da5`
- Resolved experiment: `qe_20260502_231229_0565_L1`
- QE task/loop: `qe_20260502_231229_0565` / `Loop1`
- Execution node: `rdagent-node1`
- Materialized factor files: 57
- Materialized params: 1 `artifacts/params.pkl`
- Prepared factor order: 77
- Dynamic factor source: `qe_static_dataloader`
- Static loader schema available: true
- Warnings: none

### Direct WSL smoke probe

- Prepared workspace: `rdagent_assets\strategy_package_runtime\pkg_1de32357724a4c5b874f2abd90f22da5\ad337eced2f48fe5`
- `model/model.py` copied: true
- Previous blockers removed: no wrong-node `conf.yaml` HTTP 404, no unresolved-Jinja YAML parse failure, no direct `/mnt/...` runtime-cache policy rejection, no `LSTM_10D_hs64_d02` unpickle failure.
- Current remaining blocker: strict feature completeness fails before scoring: `input_rows=4636`, `kept_rows=0`, `dropped_rows=4636`, `invalid_cell_count=13047`, invalid columns include Alpha158 and dynamic factors.
- Interpretation: code now reaches actual live feature computation. A separate data/preprocessing gate is required before claiming end-to-end live selection success; the engine correctly refuses to fill missing features with defaults.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| QE evolution-loop package looked up parent task id as `qe_experiments.experiment_id` | `selection_artifact` called `load_source(record.source_id)` for every package; evolution-loop packages store `source_id` as task id and `loop_id` separately | Added `load_source_for_strategy_package()` and route live artifact generation through `(source_type, source_id, loop_id, run_id)` | New tests plus DB probe resolved the real package to `qe_20260502_231229_0565_L1` |
| Runtime asset download could hit the wrong node and return HTTP 404 | Node id was not read from `result_metrics.execution_trace.node_id` | Node resolution now checks `custom_params.execution_node_id`, `custom_params.node_id`, `result_metrics.execution_node_id`, `result_metrics.execution_trace.node_id`, then default node | Probe selected `rdagent-node1` and materialized assets successfully |
| Node `conf.yaml` could not be parsed due unresolved Jinja placeholders | Historical QE config contains `{{ num_features }}` / `{{ num_timesteps }}` | Added safe YAML loader that sanitizes unresolved Jinja only for config discovery | Tests include unresolved Jinja in StaticDataLoader paths; real materialization succeeds |
| WSL factor entry could miss factor files when cwd changed | Generated factor-entry paths were relative strings | Resolve factor source files to absolute paths before optional Windows-to-WSL conversion | Test asserts absolute `_FACTOR_FILES`; WSL smoke no longer fails on factor source path |
| WSL runtime cache path was rejected as worker workspace | Path policy treated every `/mnt/...` path as forbidden, including AIstock-owned materialized caches | Allow `/mnt/...` only when under AIstock allowed artifact roots; keep `qe_workspace` / `rdagent_workspace` forbidden | New policy tests pass; WSL smoke reaches feature calculation |
| Pickled custom PyTorch model could not be unpickled | Runtime workspace copied `params.pkl` but not QE `model.py`, so pickle could not resolve `LSTM_10D_hs64_d02` | Download optional `model.py` from node and copy it beside `model/params.pkl` in prepared workspace | WSL smoke confirms `model/model.py` exists and progresses beyond unpickle |

## Result

- Final status: PASS for source-resolution, node/materialization, WSL runtime-cache policy, model-code materialization, tests, and guardrails.
- End-to-end live selection status: not yet PASS. The direct WSL smoke now fails later on strict feature completeness (`kept_rows=0`), which should be handled as a separate data/preprocessing task.
- Need production backend restart: yes, to activate code changes in production `8001`; this run did not restart it.
- Need dev service restart: no dev services were started.
- Production port impact: none; production `8001` was not touched.
- Asset-safety status: no tracked protected assets changed; ignored worktree runtime cache was regenerated for smoke validation only.


