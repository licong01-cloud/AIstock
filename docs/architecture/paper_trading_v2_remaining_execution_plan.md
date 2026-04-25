# Paper Trading v2 Remaining Execution Plan

Date: 2026-04-25

This document is the restart-safe handoff for continuing AIstock Trading Core v2
and Paper Trading v2 development. It captures the current implementation state,
remaining execution plan, hard constraints, and verification commands.

## 1. Hard Constraints

- Do not modify `backend/data_service` semantics without a separate impact
  assessment and explicit confirmation.
- Do not modify old QE, RD-Agent, or old `backend/services/paper_trading` flows.
- Do not implement QMT, Shadow, or live trading in the current phase.
- Daily execution is disabled for authoritative Paper v2. Minute bars are
  mandatory.
- Missing data, missing assets, missing algorithms, missing rules, empty
  business results, and incomplete implementations must fail explicitly.
- No silent fallback is allowed:
  - no TDX-to-DB fallback unless the caller explicitly selected that source;
  - no execution algorithm fallback;
  - no empty result pretending success;
  - no catch-and-continue that hides business failure.
- Strategy packages may only originate from:
  - a single QE experiment;
  - a specific QE evolution loop.
- Existing scattered RD-Agent Task/Loop stock-selection entry points are not the
  future authoritative path.

## 2. Current Completed Baseline

Implemented modules:

- `backend/services/trading_core/errors.py`
- `backend/services/trading_core/models.py`
- `backend/services/trading_core/oms.py`
- `backend/services/trading_core/minute_execution.py`
- `backend/services/trading_core/execution_algo_adapter.py`
- `backend/services/trading_core/ledger.py`
- `backend/services/trading_core/limit_price_provider.py`
- `backend/services/trading_core/risk.py`
- `backend/services/strategy_package/models.py`
- `backend/services/strategy_package/manifest.py`
- `backend/services/strategy_package/validators.py`
- `backend/services/strategy_package/qe_source_resolver.py`
- `backend/services/strategy_package/model_asset_resolver.py`
- `backend/services/paper_trading_v2/market_data.py`
- `backend/services/paper_trading_v2/runner.py`
- `backend/routers/strategy_packages.py`
- `backend/execution_algos/v24_plan_algo.py`

Current tests:

```powershell
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 -q -p no:cacheprovider
```

Last known result:

```text
37 passed
```

## 3. V24 / V25 Execution Strategy Status

V24 is no longer the target final execution strategy. V25 is being developed and
may change the runtime protocol significantly.

Current decision:

- Do not spend more effort installing `torch` or finalizing V24 runtime until
  V25 input/output contract is confirmed.
- Keep the execution algorithm boundary and fail-fast behavior.
- Keep `min_observed_bars` / `min_required_bars` support in the strategy package
  runtime path.
- Current TDX realtime market input uses observed minute bars only. The legacy
  `full_day_*` field names are present only for compatibility with old V24
  adapters; in realtime mode their semantic meaning is "observed so far", not
  future full-day data.

When V25 is ready, implement a new adapter instead of forcing V25 into the old
V24 assumptions.

## 4. Model Asset State

V24 model was copied read-only from WSL into the AIstock cache without moving or
mutating the original file:

```text
Original: /home/lc999/data/rl_models/v24/v24_plan_net.pt
Cached:   rdagent_assets/model_cache/execution/V24_PLAN/v24_plan_net_c6965529be96d946.pt
Sidecar:  rdagent_assets/model_cache/execution/V24_PLAN/v24_plan_net_c6965529be96d946.pt.json
```

This is retained only as a prior compatibility artifact. V25 may require new
assets and a different resolver path.

## 5. Remaining Execution Plan

### Phase A: Strategy Package Persistence

Goal: make QE results persist as authoritative AIstock strategy packages.

Tasks:

- Add DB schema/tables for strategy packages, package assets, and status events.
- Persist frozen manifest JSON and `manifest_sha256`.
- Add package status transitions:
  - `DRAFT`
  - `ASSET_VALIDATED`
  - `BACKTEST_APPROVED`
  - `SELECTION_ENABLED`
  - `PAPER_ENABLED`
  - `PAPER_RUNNING`
  - `PAPER_PASSED`
  - `PAPER_FAILED`
  - `RETIRED`
- Add APIs for:
  - create package from QE experiment;
  - create package from QE evolution loop later;
  - list packages;
  - get package detail;
  - validate readiness;
  - enable selection;
  - enable paper trading;
  - retire package.

Acceptance:

- QE source is read-only.
- No reverse write to QE experiment status.
- Manifest hash is stable and re-computable.
- Package versions are immutable after use by a paper portfolio.

### Phase B: Paper v2 Portfolio Persistence

Goal: evolve from in-memory runner to traceable paper portfolios.

Tasks:

- Add schema/tables:
  - `paper_v2.portfolios`
  - `paper_v2.runs`
  - `paper_v2.orders`
  - `paper_v2.fills`
  - `paper_v2.positions`
  - `paper_v2.cash_ledger`
  - `paper_v2.daily_snapshots`
  - `paper_v2.run_events`
  - `paper_v2.errors`
- Add portfolio lifecycle:
  - `DRAFT`
  - `READY`
  - `RUNNING`
  - `PAUSED`
  - `FAILED`
  - `COMPLETED`
  - `RETIRED`
- Freeze portfolio invariants:
  - `package_id`
  - `manifest_sha256`
  - `initial_cash`
  - `start_date`
  - `data_source`
  - `fee_policy`
  - `risk_policy`
  - `execution_policy`

Acceptance:

- Orders, fills, cash changes, positions, and NAV are persisted.
- Any run failure is persisted with explicit error context.
- No partial success is returned unless explicitly modeled and recorded.

### Phase C: Strategy Package Runtime and Rebalance Engine

Goal: convert package output into target positions and `OrderIntent[]`.

Tasks:

- Add `StrategyPackageRuntime`.
- Add selection score loading/production from package artifacts.
- Add `SignalSnapshot`.
- Add `TargetPositionEngine`.
- Add `RebalanceEngine`.
- Convert current positions and target positions into buy/sell `OrderIntent[]`.
- Enforce:
  - 100-share lots;
  - max position weight;
  - cash buffer;
  - turnover constraints.

Acceptance:

- Paper runs no longer require manually supplied `OrderIntent`.
- Missing score/rank/target positions fail explicitly.
- Single-alpha packages are fully supported.
- Multi-alpha packages remain structurally compatible and fail clearly if the
  required multi-alpha runtime artifacts are not available.

### Phase D: Paper Trading Day Runner

Goal: run one full trading day from package to orders to fills to ledger.

Tasks:

- Add `PaperTradingDayRunner`.
- Input: portfolio id and trade date.
- Load frozen package manifest.
- Determine rebalance day.
- Generate selection result.
- Generate target positions.
- Generate order intents.
- Load minute data from explicit source:
  - `TDX_REALTIME`
  - `DB_HISTORICAL`
- Execute minute orders via `MinuteExecutionEngine`.
- Persist orders/fills/positions/cash/daily snapshot/events.

Acceptance:

- A full trading day can run end-to-end.
- Missing minute bars, limit prices, pre-close, strategy output, or algorithm
  runtime fails the day.
- Daily execution path remains unavailable.

### Phase E: Unified Selection Center

Goal: replace scattered old stock-picking features with package-based selection.

Tasks:

- Add package-based selection center service.
- Add selection persistence:
  - `selection_runs`
  - `selection_results`
  - `selection_combinations`
  - `selection_explanations`
- Support:
  - single package selection;
  - package intersection;
  - package union;
  - weighted package fusion later.

Acceptance:

- Every selection result is traceable to package id, manifest hash, trade date,
  data source, and runtime config.
- Empty selection results fail unless explicitly recorded as a valid no-candidate
  result with reasons.

### Phase F: API and Frontend

Goal: expose package center, selection center, and Paper v2 portfolio center.

Tasks:

- Strategy Package APIs.
- Selection Center APIs.
- Paper v2 Portfolio APIs.
- Frontend pages:
  - Strategy Package Center;
  - Unified Selection Center;
  - Paper v2 Portfolio Center;
  - Run logs and failure reasons;
  - Orders/fills/positions/NAV.

Acceptance:

- Frontend displays readiness and failure reasons.
- Frontend never hides backend fail-fast errors behind a generic success state.

### Phase G: V25 Adapter

Goal: integrate the final V25 execution strategy only after its contract is
stable.

Tasks after V25 is finalized:

- Define V25 input/output contract.
- Implement `V25_PLAN` or equivalent execution adapter.
- Resolve/copy V25 assets without mutating original research outputs.
- Install runtime dependencies only after V25 dependency set is confirmed.
- Add tests for:
  - missing dependency;
  - missing model asset;
  - insufficient observed minute bars;
  - invalid model output;
  - no silent fallback.

Acceptance:

- V25 can run one order using observed minute bars.
- V25 can run a full paper trading day.
- Any difference from QE backtest execution behavior is documented.

## 6. Recommended Verification Commands

Unit tests:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/trading_core backend/tests/strategy_package backend/tests/paper_trading_v2 -q -p no:cacheprovider
```

Compile check:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
python -c "from pathlib import Path; files=['backend/services/paper_trading_v2/runner.py','backend/services/paper_trading_v2/market_data.py']; [compile(Path(f).read_text(encoding='utf-8'), f, 'exec') for f in files]; print('compile ok')"
```

Backend start command:

```powershell
$env:PYTHONIOENCODING='utf-8'
$env:PYTHONDONTWRITEBYTECODE='1'
uvicorn backend.main:app --host 127.0.0.1 --port 8001
```

## 7. Fully Autonomous Codex Restart Command

Use this only when full local authority is acceptable. It disables approval
prompts and sandboxing.

```powershell
codex --cd F:\Dev\AIstock --dangerously-bypass-approvals-and-sandbox --search
```

Suggested first prompt after restart:

```text
Read AGENTS.md and docs/codex_project_memory.md first. Then read docs/architecture/paper_trading_v2_remaining_execution_plan.md and continue implementing the remaining Paper Trading v2 plan. Do not install torch yet; V24 is deprecated and V25 is still under development. Preserve fail-fast rules and do not modify backend/data_service semantics without explicit assessment.
```

## 8. Torch / Runtime Dependencies

Do not install `torch` as part of the current continuation unless the user
explicitly confirms the final V25 runtime dependency set.

If V25 later requires Windows-side PyTorch execution, install dependencies as a
separate final environment step and validate with:

```powershell
python -c "import torch; print(torch.__version__)"
```
