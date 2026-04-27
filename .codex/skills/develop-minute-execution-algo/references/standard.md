# Intraday Minute Execution Standard

Use this checklist when implementing or reviewing any AIstock minute execution algorithm.

## Non-Negotiable Contract

- One logical algorithm, multiple adapters: core semantics must not diverge between QE and Paper v2.
- No silent fallback: unsupported config, missing data, missing model, or unknown behavior fails loudly.
- Explicit market states: suspension/limit/no-new-live-bar are business states, not fake success.
- Explicit data sources: `DB_HISTORICAL` and `TDX_REALTIME` must not fall back to each other.
- Protected assets stay separate from framework code.

## Capability Matrix

Every algorithm needs an explicit capability statement:

| Capability | Required answer |
| --- | --- |
| Historical full-day replay | supported / unsupported, minimum bars, required context |
| Realtime streaming | supported / unsupported, state persistence, first valid step |
| Required market data | minute bars, pre_close, limit up/down, suspend_d, volume, amount |
| Required model assets | paths, framework, device, shape, version contract |
| Market-aware states | suspended, limit-up/down, no-fill, waiting |
| Failure states | missing data, invalid price, missing model, unsupported side/config |

If realtime streaming is unsupported, fail with an explicit unsupported error and keep historical mode intact.

## Data And Market-State Taxonomy

Business no-fill/wait states:

- `suspended_by_suspend_d`
- `suspended_by_exchange`
- `limit_up_buy_block`
- `limit_down_sell_block`
- `live_waiting_for_bar`
- zero fill from a valid market constraint

Fail-fast states:

- missing or invalid `pre_close`
- missing required limit price data
- missing required minute bars in historical mode
- malformed minute bars, non-increasing timestamps, cross-date bars
- missing model file, incompatible checkpoint, unavailable device
- unsupported order side or algorithm config
- unsupported data source or implicit fallback request

## V25 Lessons To Preserve

- Signal-time `suspend_d` filtering must remove confirmed suspended candidates and backfill from lower ranks.
- V25 inner execution strategy must also receive `filter_suspended_on_signal`, `suspend_filter_file`, and `suspend_filter_strict` as a second guard.
- Confirmed suspension should skip V25 plan generation; it should not fail on `prev_close=NaN` when suspend evidence exists.
- If `prev_close=NaN` has no suspension evidence, fail as a data-readiness error.
- Historical V25 requires enough full-day bars for the historical plan; realtime V25 needs a separate streaming design and persisted plan/state.
- Do not use zero day features as an authoritative live default unless the task explicitly marks an audited diagnostic mode.

## Implementation Pattern

1. Add or update core semantics in `backend/execution_algos/<algo>_core.py`.
2. Add or update adapter in `backend/execution_algos/<algo>_algo.py`.
3. Update `backend/services/trading_core/execution_algo_capabilities.py`.
4. Update `backend/services/trading_core/execution_algo_adapter.py` only for explicit capability or market-state handling.
5. Update `backend/services/trading_core/minute_execution.py` for event persistence or state behavior.
6. Update QE helper under `scripts/` only when Qlib workspace execution needs the behavior.
7. Update `backend/services/quantevolver/config_composer.py` so generated YAML proves the requested policy is actually used.
8. Add tests in `backend/tests/trading_core/`, `backend/tests/unified_engine/`, and Paper v2 tests as needed.

## QE YAML Requirements

For minute algorithms under Qlib `NestedExecutor`:

- The requested algorithm must appear under `executor.kwargs.inner_strategy`.
- Algorithm kwargs must be under `inner_strategy.kwargs`.
- The outer portfolio strategy owns signal/selection behavior.
- If a runtime filter affects both selection and execution safety, emit it in both places and test both slices of YAML.

V25 suspend example requirements:

```yaml
inner_strategy:
  class: TailTWAPWithV25TwoStageStrategy
  module_path: tail_twap_v25_strategy
  kwargs:
    filter_suspended_on_signal: true
    suspend_filter_file: qe_suspend_filter.json
    suspend_filter_strict: true
strategy:
  class: SuspendFilterTopkDropoutStrategy
  module_path: qe_suspend_filter_strategy
  kwargs:
    filter_suspended_on_signal: true
    suspend_filter_file: qe_suspend_filter.json
    suspend_filter_strict: true
```

## Minimum Test Matrix

- Plan weights and normalization are valid.
- Missing required model/config fails.
- Historical minimum bars enforced.
- Realtime unsupported or streaming behavior is explicit.
- `suspend_d` evidence creates no-fill/skip, not data failure.
- Missing `pre_close` without suspension evidence fails.
- Limit-up buy and limit-down sell produce explicit blocked/no-fill behavior.
- QE generated YAML contains the requested algorithm and all safety kwargs in the correct section.
- Paper v2 persists no-fill/error/run events with reason and context.

## Review Checklist

Before finalizing:

- Search for fallback language or behavior: `fallback`, `default`, `except Exception`, `return []`, `ok=True`.
- Prove that empty results are either invalid or explicitly modeled as `valid_no_candidate` with reason.
- Confirm no protected asset was modified silently.
- Run targeted tests and report exact commands/results.
- If production port `8001` needs a restart, tell the user; do not restart it unless explicitly asked in that turn.
