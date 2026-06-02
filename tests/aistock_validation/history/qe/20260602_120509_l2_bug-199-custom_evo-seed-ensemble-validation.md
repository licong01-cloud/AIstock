# BUG-199 custom_evo seed ensemble validation

- Module: qe
- Level: L2
- Date: 2026-06-02T13:45:00+08:00
- Git commit before evidence update: e8a4bb3c
- Operator: Codex

## Scope

- Changed files: `backend/services/quantevolver/config_composer.py`, `scripts/qrun_limit_minute.py`, `backend/tests/unified_engine/test_qe_config_truth.py`, plus existing BUG-199 schema/executor/config changes in branch commits.
- Impacted flows: custom_evo config normalization, qe_runtime.ensemble YAML materialization, RDAgent payload, qrun score-level seed bagging, qrun portfolio-level equal-weight holdings merge, read-path/QE contract regression tests.
- Business goal: Fixed explicit seeds let `custom_evo` emit deterministic score-level or portfolio-level seed ensemble results; absent/disabled ensemble keeps existing single-`random_seed` behavior.
- Out of scope: Real 6-seed R8A production backtest, production DB/DDL, and production backend/frontend/TDX restarts.
- Protected assets reviewed: no production DB/DDL, no dependency manifest, no production runtime touched.

## Design Compliance Matrix

| Design item | Implementation refs | Test/evidence | Status | Gap/exception |
|---|---|---|---|---|
| Optional ensemble block, absent/disabled preserves single-seed behavior | `CustomEvoLoopConfig.ensemble`, `normalize_qe_seed_ensemble_config`, `ExperimentConfig.build_runtime_flags`, `ensure_loop_fixed_seed` | targeted pytest: config builder + route seedless ensemble acceptance | PASS | None |
| Score-level seed bagging trains each seed, aggregates `pred.pkl`, then one pred-backtest | `scripts/qrun_limit_minute.py::_run_seed_score_ensemble`, `_aggregate_seed_predictions` | targeted pytest `test_qrun_seed_prediction_aggregation_is_deterministic`; compileall | PASS | Real long QE run not executed in this local validation |
| Portfolio-level ensemble aligns with offline equal-weight holdings proxy | `scripts/qrun_limit_minute.py::_run_seed_portfolio_ensemble`, `_aggregate_seed_positions`, `_aggregate_seed_reports` | targeted pytest `test_qrun_portfolio_seed_aggregation_matches_offline_equal_weight_proxy` | PASS | User-provided R8A offline data not yet supplied in this turn |
| Runtime metadata persists but stays out of Qlib strategy/model kwargs | `BacktestExecutor`, `ConfigComposer._compose_conf_yaml`, `qe_evolution_service` config/action_type | targeted pytest `test_seed_ensemble_runtime_metadata_stays_out_of_strategy_kwargs`, `test_seed_ensemble_reaches_composer_but_not_model_params` | PASS | None |
| Fixed seeds are deterministic and invalid duplicate seeds fail fast | qrun `_get_seed_ensemble_config`, backend normalizer duplicate checks | targeted pytest `test_qrun_seed_ensemble_config_rejects_duplicate_seeds` | PASS | None |
| Runner packaging supports ensemble even when backtest_freq is day/CLOSE_PRICE | `ConfigComposer` qrun_limit_minute packaging and execution_algo preservation | targeted pytest `test_seed_ensemble_day_backtest_packages_minute_runner` | PASS | None |
| No silent fallback / fake success in new portfolio artifact path | `_load_recorder_object`, `_numeric_value`, `_mean_daily_frames` | targeted pytest `test_qrun_optional_recorder_artifact_load_fails_fast_on_corruption`; changed-only guardrail scan | PASS | Optional missing indicators are warning-only by design; corrupt optional artifacts fail fast |

## Commands

```bash
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m compileall backend/routers/quantevolver_evolution.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/experiment_config.py backend/services/quantevolver/experiment_config_builders.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/seed_contract.py scripts/qrun_limit_minute.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_experiment_config.py backend/tests/unified_engine/test_qe_config_truth.py
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/unified_engine/test_experiment_config.py::TestBuildConfigFromCustomEvoLoop::test_custom_loop_seed_ensemble_sets_anchor_seed_and_runtime_metadata backend/tests/unified_engine/test_backtest_executor.py::TestBacktestExecutorBasic::test_seed_ensemble_reaches_composer_but_not_model_params backend/tests/unified_engine/test_custom_evo_mutation_routes.py::test_custom_evo_rerun_accepts_seedless_loop_when_ensemble_seeds_are_explicit backend/tests/unified_engine/test_qe_config_truth.py::test_seed_ensemble_day_backtest_packages_minute_runner backend/tests/unified_engine/test_qe_config_truth.py::test_seed_ensemble_runtime_metadata_stays_out_of_strategy_kwargs backend/tests/unified_engine/test_qe_config_truth.py::test_qrun_seed_prediction_aggregation_is_deterministic backend/tests/unified_engine/test_qe_config_truth.py::test_qrun_seed_ensemble_config_rejects_duplicate_seeds backend/tests/unified_engine/test_qe_config_truth.py::test_qrun_optional_recorder_artifact_load_fails_fast_on_corruption backend/tests/unified_engine/test_qe_config_truth.py::test_qrun_portfolio_seed_aggregation_matches_offline_equal_weight_proxy -q -p no:cacheprovider
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_guardrail_scan.py --changed-only --fail-on-severity P1
ruff check backend/routers/quantevolver_evolution.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/experiment_config.py backend/services/quantevolver/experiment_config_builders.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/seed_contract.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py backend/tests/unified_engine/test_experiment_config.py backend/tests/unified_engine/test_qe_config_truth.py scripts/qrun_limit_minute.py
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_module_registry_l0
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s l0
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_data_contract_backend
rtk cmd /c "set QE_READ_L3_SKIP_UI=1&& C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s qe_read_l3"
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/aistock_issue_workflow.py finish --bug-id BUG-199 --plan-only
rtk git diff --check
rtk C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s validation_center_backend
```

## Evidence

- `compileall`: PASS, all changed Python paths compiled.
- Targeted pytest: PASS, `9 passed in 12.54s`.
- Changed-only guardrail: PASS, `files=5, findings=0, blocking=0` before lint cleanup; rerun after lint cleanup PASS, `files=3, findings=0, blocking=0`.
- Changed-file Ruff lint: PASS, `All checks passed!`; fixed pre-PR lint-only findings in scoped files.
- `validation_module_registry_l0`: PASS, `8 passed`, ownership scan mapped 12/12.
- `l0`: PASS; only non-blocking pre-existing P2/P1 baseline findings, session successful.
- `qe_data_contract_backend`: PASS, `17 passed in 1.40s`.
- `qe_read_l3` with `QE_READ_L3_SKIP_UI=1`: PASS; read backend `14 passed`. Full UI sub-session was attempted earlier and failed only because dev backend `127.0.0.1:8011` was not running; production ports were not started or restarted.
- `finish --plan-only`: PASS, `closure_ready=true`, `workflow_gate=ready_for_pr`, scope_check passed; validation evidence still supplied in later finish/PR step.
- `git diff --check`: PASS.
- `validation_center_backend`: PASS, `321 passed in 117.95s`; coverage `line=79.92`, `branch=62.07`, status `passed`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Portfolio ensemble was initially pending/fail-fast | BUG closure requirements require portfolio-level equal-weight holdings merge | Added `_run_seed_portfolio_ensemble`, daily positions/report aggregation, manifest/final recorder artifacts | targeted pytest portfolio offline proxy PASS |
| Day/CLOSE_PRICE ensemble could select `qrun_limit_minute.py` command without packaging the runner | `seed_ensemble_enabled` changed runner selection but file packaging still keyed only on `backtest_freq != day` | Package minute runner/helpers whenever `_seed_ensemble_config` is present and preserve explicit `execution_algo=CLOSE_PRICE` through runtime contract merge | `test_seed_ensemble_day_backtest_packages_minute_runner` PASS |
| Optional indicators load could hide corruption | optional artifact loader returned None for any exception | Missing optional artifacts remain warning-only; corrupt optional artifacts now raise RuntimeError | `test_qrun_optional_recorder_artifact_load_fails_fast_on_corruption` PASS |
| `qe_read_ui` failed | Dev backend on port 8011 was not running; production restart is disallowed without approval | Re-ran `qe_read_l3` with `QE_READ_L3_SKIP_UI=1` to validate read backend and guardrails without touching production runtime | `qe_read_l3` skip-UI PASS |
| PR automation changed-file Ruff lint failed | Pre-existing scoped lint findings became blocking in PR gate | Removed unused imports/locals, renamed duplicate test method, split one semicolon statement, removed undefined benchmark helper call, and added script-level E402 noqa for the legacy runner import layout | Ruff PASS; compileall PASS; targeted pytest PASS |

## Result

- Final status: PASS for code/schema/runner/backend/validation-center validations; PR-ready pending final workflow PR gate.
- Remaining risks: Real long-running QE ensemble backtest and user-provided R8A offline holdings proxy were not executed in this local turn.
- Need production backend restart: no
- Need dev service restart: no
- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
- Production runtime/DB touched: no
