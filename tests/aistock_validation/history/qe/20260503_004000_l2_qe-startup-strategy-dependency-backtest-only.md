# QE Startup Dependency and Backtest-Only Validation - 2026-05-03

## Scope

- Fixed QE custom strategy dependency packaging for catalog-backed imports such as `score_weighted_strategy.py`.
- Hardened backtest-only model reuse so retry/cross-node paths package `mlruns` params through the QE node API payload instead of relying on Windows/worker path probes.
- Fixed custom-evo config building from persisted `factor_list` / `factor_names` so historical-loop retry/clone does not produce an empty factor set.
- Added loose `params.pkl` loading in `qrun_limit_minute.py` when MLflow metadata is missing but reusable model params exist.
- Did not restart production FastAPI `8001` or WSL QE API `9000`.
- Did not start any new QE experiment after the already-started Loop3 retry; subsequent checks were passive or mocked.

## Business Risks Covered

- `ModuleNotFoundError: No module named 'score_weighted_strategy'` during custom strategy backtest startup.
- Backtest-only runs failing because only partial `mlruns` params are available and MLflow `meta.yaml` is absent.
- Cross-node backtest-only relying on direct workspace access instead of node API payloads.
- Persisted custom-evo configs losing factor names when `factor_keys` is absent.
- False success: validation requires real Loop3 completion, metrics, enhanced metrics, and dependency files visible through node API.

## Commands

```powershell
python -m py_compile backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/experiment_config_builders.py scripts/qrun_limit_minute.py backend/tests/unified_engine/test_qe_config_truth.py
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -q
python -m pytest backend/tests/unified_engine/test_backtest_executor.py -q
python -m pytest backend/tests/unified_engine/test_factor_cache_remote_sync_policy.py -q
python -m pytest backend/tests/unified_engine/test_worker_workspace_policy_remaining_paths.py -q
python -m pytest backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_qe_custom_evo_mutation_service.py -q
python -m pytest backend/tests/unified_engine/test_qe_cleanup_path_policy.py -q
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py --fail-on HIGH backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/experiment_config_builders.py scripts/qrun_limit_minute.py backend/tests/unified_engine/test_qe_config_truth.py tests/aistock_validation/modules/qe.md
```

## Results

- `test_qe_config_truth.py`: 33 passed.
- `test_backtest_executor.py`: 19 passed.
- `test_factor_cache_remote_sync_policy.py`: 6 passed.
- `test_worker_workspace_policy_remaining_paths.py`: 17 passed.
- `test_custom_evo_mutation_routes.py` + `test_qe_custom_evo_mutation_service.py`: 13 passed.
- `test_qe_cleanup_path_policy.py`: 7 passed.
- Guardrail scan: 0 findings.
- Smoke compose against `qe_20260502_231229_0565` Loop1 persisted config used a fake client and did not submit a real task:
  - `file_count=75`
  - `custom_strategy.py=True`
  - `score_weighted_strategy.py=True`
  - `qe_suspend_filter_score_weighted_strategy.py=True`
  - `qrun_limit_minute.py=True`
  - `filtered_pool_20260502.txt=True`
  - `factor_count=57`
  - `--backtest-only=True`

## Live Passive Evidence

Already-started validation loop: `qe_20260502_231229_0565` / `Loop3`.

Node API evidence from `http://127.0.0.1:9000/api/v1/qe_workspace`:

- `GET /tasks/qe_20260502_231229_0565/loops/Loop3/status`: `{"status":"completed"}`.
- `GET /tasks/qe_20260502_231229_0565/loops/Loop3/metrics`: HTTP 200 with business metrics:
  - `IC=0.06383704319210147`
  - `ICIR=0.5366754305889379`
  - `Rank IC=0.10015650606752109`
  - `Rank ICIR=0.701788517239195`
  - `cagr=0.927677`
  - `final_nav=3.161847`
  - `n_trading_days=442`
  - `1day.excess_return_with_cost.information_ratio=2.2911786397784164`
  - `1day.excess_return_with_cost.max_drawdown=-0.15491815277088394`
- `GET /tasks/qe_20260502_231229_0565/loops/Loop3/enhanced-metrics`: HTTP 200, 674391 bytes, includes `summary` and IC diagnostics.
- `GET /tasks/qe_20260502_231229_0565/loops/Loop3/files/qlib_results_enhanced.json`: HTTP 200, 1092305 bytes.
- `GET /tasks/qe_20260502_231229_0565/loops/Loop3/files/score_weighted_strategy.py`: HTTP 200, 25152 bytes.
- Current run segment of `run.log` has `current_run_has_module_error=False` and ends with `[DONE] loop=Loop3 status=completed`.

Database evidence:

- `qe_evolution_loops`: Loop3 status is `completed`, metrics_json contains the same metrics and enhanced summary.
- `qe_evolution_tasks`: task status is `failed` because Loop1/Loop2/Loop4 remain failed from earlier attempts. No new retry was started for them.

## Bugs Found And Fixed During Validation

- Existing validation guardrail flagged a silent JSON parse fallback in `experiment_config_builders.py`; changed it to debug-log before returning unavailable.
- Smoke compose found persisted custom-evo loop configs may carry `factor_list` without `factor_keys`; fixed builder fallback and added regression coverage.
- Loop1 was observed to fail with the old missing `score_weighted_strategy` packaging before the current fix was applied to that loop. It was not retried again.

## Production Impact

- Production backend `8001` was not restarted.
- WSL QE API `9000` was not restarted.
- The source changes are not guaranteed to be active inside the already-running production backend process until the user performs a controlled backend restart or starts a dev backend from the updated source.
- Existing running QE loop was only observed; no additional QE start/retry/create was issued after the user's clarification.

## Residual Risks

- UI L3 was not rerun in this pass because no frontend files changed and the user prohibited new QE starts; backend/node API and DB evidence verified the business result for the active loop.
- Existing failed loops remain failed and should only be retried if the user explicitly authorizes another QE retry/start.
