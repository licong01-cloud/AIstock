---
name: develop-minute-execution-algo
description: Develop or modify AIstock intraday minute execution strategies, including V24/V25/V26-style algorithms, QE Qlib backtest helpers, Paper Trading v2 historical replay or realtime adapters, execution policy config, market-state handling, and fail-fast validation. Use when implementing minute execution algorithms, changing V25/V24 behavior, wiring suspend_d/limit/pre_close/minute-bar requirements, or validating that QE and Paper v2 use the same execution semantics without silent fallback.
---

# Develop Minute Execution Algo

## Core Rule

Treat intraday execution strategy work as high-risk trading infrastructure. Preserve asset/program separation, keep one logical algorithm semantics across QE and Paper v2, and fail fast for data/config/model errors. Never make a daily/TWAP/default-price/default-position fallback to make a run look successful.

## Required Context

Before changing AIstock execution code, read:

- `docs/codex_project_memory.md`
- `docs/architecture/minute_execution_algo_standard_contract.md` if present
- `docs/architecture/qe_v25_minute_execution_regression_audit_20260427.md` if the task touches V25 regressions
- `references/standard.md` in this skill for the reusable contract checklist

Do not modify `AGENTS.md` unless the user explicitly asks.

## Workflow

1. **Classify the change**
   - Framework code: adapters, core logic, ConfigComposer, capabilities, runner, tests.
   - Protected assets: model weights, StrategyPackage manifests, QE workspaces, validated policies, DB asset rows, selection artifacts, HMM snapshots.
   - Do not silently modify protected assets. If an asset change is required, stop and provide impact analysis unless the user explicitly authorized that asset edit.

2. **Split core from adapters**
   - Put reusable algorithm semantics in `backend/execution_algos/<algo>_core.py` when feasible.
   - Keep Paper/QE/QLib/DB/API objects in adapters or helper scripts.
   - QE helper scripts may be copied into workspaces, but they must implement the same logical behavior as the core.

3. **Declare capabilities explicitly**
   - Historical full-day replay and realtime streaming are separate capabilities.
   - If realtime is not genuinely supported, return/raise an explicit unsupported error; do not require impossible warmup bars at market open and do not fall back to another algorithm.
   - Historical mode may require full-day bars. Realtime mode must work incrementally only if the algorithm has a stateful streaming design.

4. **Handle market states as business states**
   - Suspended by `suspend_d`, exchange-suspended, limit-up buy block, limit-down sell block, and live no-new-bar waiting are explicit no-fill/wait states.
   - Missing `pre_close`, missing limit data, missing required minute bars, invalid prices, missing model, and unsupported config are failures.
   - Market states may produce explicit `NO_FILL` events; data/config/model failures must fail the run.

5. **Wire QE and Paper consistently**
   - QE config must place algorithm kwargs in `NestedExecutor.inner_strategy.kwargs`.
   - If signal-time suspend filtering is enabled for V25-like execution, pass the same artifact params to both the outer signal strategy and the inner execution strategy.
   - Paper v2 must use validated execution policy snapshots and explicit data sources; no silent data-source fallback.

6. **Persist traceability**
   - Persist plan metadata, reason codes, order/fill/no-fill events, and runtime config snapshots where the subsystem supports persistence.
   - Include package/policy/hash/date/source context in errors and events.

7. **Test before reporting done**
   - Add or update contract tests for algorithm semantics and market states.
   - Add QE config truth tests for generated YAML.
   - Add Paper v2 tests if runner/market-data/OMS behavior changes.
   - Use small real-data smoke tests when feasible, but do not mutate durable assets without explicit approval.

## Common AIstock Files

- Core/adapter: `backend/execution_algos/`
- Capabilities: `backend/services/trading_core/execution_algo_capabilities.py`
- Execution adapter: `backend/services/trading_core/execution_algo_adapter.py`
- Minute engine: `backend/services/trading_core/minute_execution.py`
- Paper v2 runner/data: `backend/services/paper_trading_v2/`
- QE config: `backend/services/quantevolver/config_composer.py`
- QE helper templates: `scripts/*execution*strategy*.py`, `scripts/tail_twap_v*.py`
- QE tests: `backend/tests/unified_engine/test_qe_config_truth.py`
- Trading Core tests: `backend/tests/trading_core/`

## Verification Commands

Use UTF-8 and no bytecode during validation:

```bash
PYTHONIOENCODING=utf-8 PYTHONDONTWRITEBYTECODE=1 pytest backend/tests/trading_core backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

For Windows PowerShell:

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/trading_core backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
```

If Paper v2 is affected, also run:

```powershell
pytest backend/tests/paper_trading_v2 backend/tests/selection_center -q -p no:cacheprovider
```

## Anti-Patterns To Reject

- Silent fallback from V25/V26 to TWAP, close price, daily mode, default prices, empty orders, default cash, or fake success.
- Treating a market-closed/suspended/limit business state as a data error, or treating a data error as a no-fill business state.
- Mutating model weights, QE workspaces, manifests, DB catalog assets, or validated policies during framework work.
- Making Paper v2-only execution options that were not backtest-validated.
- Updating backend port `8001` during development validation unless the user explicitly asks in that turn.
