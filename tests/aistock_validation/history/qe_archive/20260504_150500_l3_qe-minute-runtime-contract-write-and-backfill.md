# QE Minute Runtime Contract Write and Backfill Validation

Date: 2026-05-04
Level: L3 backend/DB/warehouse smoke
Module: QE Archive / StrategyPackage / QuantEvolver

## Scope

- Strengthen QE experiment data writes so minute-backed QE generation and loop completion persist a reproducible runtime contract.
- Backfill historical QE experiment rows only when explicit minute evidence exists in loop/task config.
- Verify StrategyPackage and QE Archive can consume the completed contract without daily fallback.
- No full QE experiment/backtest was run.

## Business Oracles

- No silent conversion of daily or unknown historical experiments into minute runs.
- `backtest_freq` and `bar_freq` are derived audit/compatibility metadata; `execution_algo` and `execution_algo_params` remain the variable execution policy.
- StrategyPackage must stop failing with `QE experiment must declare minute backtest_freq` for minute-evidence QE rows.
- QE Archive payloads must surface the runtime contract in `runtime_flags`, `execution`, `freq`, and `data_context`.

## Commands

```powershell
python -m py_compile backend/services/quantevolver/runtime_contract.py backend/services/quantevolver/config_composer.py backend/services/quantevolver/executors/backtest.py backend/services/quantevolver/qe_evolution_service.py backend/services/quantevolver/analysis/metrics_store.py backend/services/strategy_package/qe_source_resolver.py backend/services/qe_archive/source_assembler.py scripts/backfill_qe_minute_runtime_contract.py
python -m pytest backend/tests/unified_engine/test_qe_runtime_contract.py backend/tests/strategy_package/test_qe_source_resolver.py -q
python -m pytest backend/tests/unified_engine/test_backtest_executor.py backend/tests/test_qe_archive_repository_static.py -q
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py -q
python scripts/backfill_qe_minute_runtime_contract.py --experiment-id qe_20260502_231229_0565_L1 --json
python scripts/backfill_qe_minute_runtime_contract.py --experiment-id qe_20260502_231229_0565_L1 --write --confirm-write QE_MINUTE_RUNTIME_CONTRACT_BACKFILL --json
python scripts/backfill_qe_minute_runtime_contract.py --limit 2000 --json
python scripts/backfill_qe_minute_runtime_contract.py --limit 2000 --write --confirm-write QE_MINUTE_RUNTIME_CONTRACT_BACKFILL --json
python scripts/backfill_qe_minute_runtime_contract.py --limit 2000 --json
```

## Automated Results

- Runtime contract + StrategyPackage tests: `7 passed in 0.46s`.
- BacktestExecutor + QE Archive static tests: `51 passed in 11.59s` before the final combined rerun, then `58 passed in 7.38s` for py_compile + targeted suites.
- QE config truth regression: `34 passed in 7.89s`.
- Final combined targeted result: `58 passed in 7.38s` plus `34 passed in 7.89s`.
- 2026-05-04 final rerun: py_compile plus runtime-contract, StrategyPackage resolver, BacktestExecutor, QE Archive static, and QE config truth suites passed as `92 passed in 13.13s`.
- Broader HMM config-builder probe `python -m pytest backend/tests/unified_engine/test_experiment_config.py backend/tests/unified_engine/test_label_horizon.py -q` still has a pre-existing import-alias failure (`services.quantevolver...` importing `backend/services/hmm_training_service.py` as a top-level package, causing `from ..db.pg_pool import get_conn` to raise `ImportError: attempted relative import beyond top-level package`): `11 failed, 38 passed`. The failing path is outside this runtime-contract write/backfill change.

## DB Backfill Evidence

Targeted dry-run for `qe_20260502_231229_0565_L1`:

```text
mode=dry_run scanned=1 missing_contract=1 updatable=1 updated=0
backtest_freq=1min execution_algo=V25_TWO_STAGE execution_algo_params_keys=device,early_model_path,late_model_path
```

Targeted confirmed write:

```text
mode=write scanned=1 missing_contract=1 updatable=1 updated=1
```

Broad confirmed write after target fix:

```text
mode=write scanned=455 missing_contract=411 updatable=122 updated=122
```

Final dry-run after broad write:

```text
mode=dry_run scanned=455 missing_contract=289 updatable=0 updated=0
```

Interpretation: 122 historical rows had explicit minute evidence and were backfilled. 289 rows still missing the contract were intentionally skipped because no minute runtime evidence was found, so historical daily/unknown experiments were not converted.

## StrategyPackage Evidence

For `qe_20260502_231229_0565_L1` after backfill:

```text
db_contract = backtest_freq=1min, runtime_mode=minute, bar_freq=1m, execution_algo=V25_TWO_STAGE
manifest_freq = 1min
manifest_algo = V25_TWO_STAGE
manifest_algo_config_keys = device, early_model_path, late_model_path
```

## QE Archive Evidence

Experiment payload for `qe_20260502_231229_0565_L1`:

```text
archive_freq = 1min
runtime_flags = runtime_mode=minute, bar_freq=1m, backtest_freq=1min, execution_algo=V25_TWO_STAGE, runtime_contract_version=qe_minute_runtime_contract_v1
execution_context.execution_algo = V25_TWO_STAGE
```

Loop payload for `qe_20260502_231229_0565_Loop1`:

```text
loop_payload_freq = 1min
loop_runtime_flags = runtime_mode=minute, bar_freq=1m, backtest_freq=1min, execution_algo=V25_TWO_STAGE, runtime_contract_version=qe_minute_runtime_contract_v1
loop_execution_context.execution_algo = V25_TWO_STAGE
```

## New Generation Write Smoke

Used `ConfigComposer.compose_experiment_in_memory(skip_db_save=False)` with a synthetic name `qe_runtime_contract_smoke_20260504_145203`, no full QE/backtest execution.

```text
created_row = ('bef68be7', 'qe_runtime_contract_smoke_20260504_145203', 'created', '1min', 'minute', '1m', 'TWAP', {})
wsl_command_contains qrun_limit_minute.py = True
cleanup_deleted = bef68be7
smoke_rows_remaining = 0
```

This proves the generation/save path writes the field set before a formal QE job is run. The synthetic row was deleted after verification to avoid polluting the experiment list.

## Production Impact / Asset Safety

- Production backend port `8001` was not restarted.
- No QE/RD-Agent worker workspace files, model weights, StrategyPackage manifests, HMM snapshots, Paper v2 ledgers, or archive artifact stores were modified.
- Local DB was intentionally updated only in `qe_experiments.custom_params` for rows with explicit minute evidence.

## Residual Risks

- The broad backfill scanned 455 current local rows. If older archived/offline databases exist outside this local DB, run the script there separately.
- Rows without explicit minute evidence remain unconverted by design and still need upstream provenance review if they are ever considered for warehouse promotion.
- A separate HMM test-import alias cleanup is still needed if `backend/tests/unified_engine/test_experiment_config.py` should run from both `backend.services...` and `services...` package paths.
