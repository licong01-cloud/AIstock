# V25.1 STAR Board-Lot Wrapper Fix Validation

Date: 2026-05-05 22:07 Asia/Shanghai

## Scope

- Fixed QE/Qlib V25.1 wrapper child-order sizing so STAR-board 688/689 orders use the real 200-share minimum with 1-share increments above 200.
- Kept legacy V25 default sizing behaviour unchanged via a parent hook in `scripts/tail_twap_v25_strategy.py`.
- Synchronized V25/V25.1 wrapper files across AIstock source, RD-Agent canonical templates, app template variants, and local V25.1 workspace copies.
- Did not modify model weights, Qlib data, StrategyPackage manifests, validated execution policies, DB assets, or HMM snapshots.

## Business Oracles

- `688/689 BUY < 200` must not emit a child order.
- `688/689 BUY 200/201/202` must remain legal and must not be rounded by Qlib global `trade_unit=100`.
- `688/689 SELL < 200` may be emitted only as a full residual flush.
- Main-board and ChiNext orders continue to use 100-share increments.
- P0 and tail-substitute paths must pass through the same final child-order legalization hook.
- Empty V25.1 BUY schedules must fail/no-fill explicitly instead of falling back to the original V25 plan.

## Commands

```powershell
pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/trading_core/test_v25_1_small_cap_contract.py -q -p no:cacheprovider
```

Result: `33 passed in 0.81s`.

```powershell
pytest backend/tests/test_tail_twap_v25_market_state.py backend/tests/trading_core/test_v25_1_small_cap_contract.py backend/tests/unified_engine/test_qe_config_truth.py -k "v25_1 or v25" -q -p no:cacheprovider
```

Result: `39 passed, 39 deselected in 12.49s`.

```powershell
python -m py_compile <all AIstock/RD-Agent tail_twap_v25*.py copies adjacent to V25.1 wrappers>
```

Result: `py_compile_ok=32`.

```powershell
python <hash scan for tail_twap_v25_strategy.py and tail_twap_v25_1_strategy.py>
```

Result:

- AIstock `tail_twap_v25_strategy.py`: 2 copies, 1 unique hash `78db90330dbc`.
- AIstock `tail_twap_v25_1_strategy.py`: 2 copies, 1 unique hash `173baed635a6`.
- RD-Agent `tail_twap_v25_strategy.py`: 14 V25.1-adjacent copies, 1 unique hash `78db90330dbc`.
- RD-Agent `tail_twap_v25_1_strategy.py`: 14 copies, 1 unique hash `173baed635a6`.

## Residual Risks

- Historical QE workspace assets were locally synchronized where V25.1 wrapper copies existed, but GitHub commits focus on source/template files. Existing archived experiments should still be treated as historical assets unless explicitly rerun with the fixed templates.
- This validation did not run a full Qlib backtest; it covered contract/unit semantics, QE config truth, syntax, and copy-hash consistency.
