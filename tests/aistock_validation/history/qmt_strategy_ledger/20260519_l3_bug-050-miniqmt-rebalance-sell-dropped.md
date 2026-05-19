# BUG-050 MiniQMT complete rebalance sell generation

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-19
- Git base: origin/main b1056e68479a956e5c9310faf4038a5705b7d4a1 after PR #82
- Fix branch: bug/BUG-050-miniqmt-rebalance-sell-dropped
- Worktree: F:\Dev\AIstock_worktrees\bug-050-miniqmt-rebalance-sell-dropped
- Operator: codex-app
- Linked bug: BUG-050 / GitHub #53

## Scope

- Changed files: backend/services/qmt_strategy_ledger/selection_order_builder.py; backend/tests/qmt_strategy_ledger/test_selection_order_builder.py; tests/aistock_validation/bugs/20260519_BUG-050-miniqmt-strategy-rebalance-does-not-sell-holdings-dropped-from-current-selection.json; this validation record.
- Impacted flows: StrategyPackage binding order preview, MiniQMT managed order request generation, strategy lot attribution, target-vs-current rebalance deltas, T+1 available-lot sell gating, same-symbol multi-strategy isolation.
- Business goal: a MiniQMT virtual strategy rebalance must compare the current target portfolio against all strategy-attributed holdings, including symbols dropped from the current selection, so stale holdings do not remain forever because the strategy only buys.
- Out of scope: live MiniQMT submit/cancel, production backend restart, production DB writes, broker can_sell runtime POC, T+1 lot rollover, SELL fill lot close/PnL settlement, schema migrations, frontend UI changes.
- Protected assets reviewed: no StrategyPackage manifest/model/factor artifact, production DB data, production backend 8001, frontend 3000, or MiniQMT broker runtime touched.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Dropped holding with available lot | Holding absent from the new selection emits a SELL request with `rebalance_reason=DROPPED_FROM_SELECTION` | `test_selection_order_builder_sells_dropped_holding_with_available_lot` | PASS |
| Dropped holding without T+1 availability | No illegal SELL request is emitted; skipped reason records unavailable lot | `test_selection_order_builder_skips_dropped_holding_without_available_lot` | PASS |
| Dropped fixed-price sell without reference price | For non-latest price types, builder does not fake a price and records `MISSING_REFERENCE_PRICE_FOR_DROPPED_HOLDING` | `test_selection_order_builder_skips_dropped_fixed_price_sell_without_reference_price` | PASS |
| Selected but overweight holding | SELL request is capped to available quantity, with residual blocked quantity recorded | `test_selection_order_builder_caps_overweight_sell_to_available_quantity` | PASS |
| Equal and below target holdings | Equal target is skipped; below-target BUY uses canonical board-lot floor and records residual | `test_selection_order_builder_equal_target_skips_and_below_target_buys_with_board_lot` | PASS |
| Same-symbol multi-strategy isolation | Other strategy's same-symbol lots do not affect current strategy delta or remark | `test_selection_order_builder_ignores_other_strategy_same_symbol_lots` | PASS |
| Existing selected-symbol sell behavior | Selected overweight target still creates partial SELL delta | `test_selection_order_builder_uses_current_lots_to_build_sell_delta` | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | `python -m pytest backend/tests/qmt_strategy_ledger -q` -> 49 passed | PASS |

## Commands

```bash
python -m pytest backend/tests/qmt_strategy_ledger/test_selection_order_builder.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/services/qmt_strategy_ledger/selection_order_builder.py backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/models.py
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Targeted selection builder tests: 12 passed in 0.42s on final rerun.
- Full qmt_strategy_ledger tests: 49 passed in 8.07s on final rerun.
- Compile checks: touched qmt_strategy_ledger modules compiled successfully.
- `git diff --check` and `git diff --cached --check`: passed.
- `validation_module_registry_l0`: 8 passed; ownership scan mapped 12/12 files.
- `guardrail_changed_files`: successful after staging; files=3, findings=1 P2 `ALGO-COMPLEXITY-001` in selection_order_builder.py, blocking=0. The finding is non-blocking and caused by string filtering in `_order_remark`, not a large quant loop or join.
- `l0`: successful; existing baseline/new guardrail findings were non-blocking with blocking=0.
- Business oracle: SELL generation now uses strategy-scoped lot summaries and never reads lots from another strategy_id.
- Business oracle: builder uses canonical `round_to_board_lot(..., side="SELL")` / `side="BUY"`; unavailable sell quantity is recorded in `skipped` instead of being submitted as an illegal request.
- Price oracle: selected-symbol sells use selection reference price; dropped sells use position-lot metadata price when a fixed price is needed, or `price=0` only with MiniQMT latest-price type 5. No fixed-price fake default is introduced.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Dropped holdings were never sold | `build_for_binding()` iterated only current selection candidates; holdings outside the target universe were invisible to delta generation | Added strategy-scoped position summary map and a second pass for positions not in the target symbol set | Targeted selection builder tests 12 passed; full qmt_strategy_ledger tests 49 passed |
| SELL delta could exceed strategy T+1 available quantity | Existing selected-symbol sell path used remaining quantity only and relied on later preflight rejection | SELL request quantity is capped to available lot; unavailable residual is recorded as an explicit skipped reason | `test_selection_order_builder_caps_overweight_sell_to_available_quantity` |
| Dropped holdings lacked candidate reference prices | Dropped symbols have no current `SelectionCandidate`, so fixed-price sell cannot derive a candidate price | Dropped sells derive explicit metadata price when available; otherwise latest-price type 5 uses MiniQMT latest price with price 0, and fixed-price mode skips fail-fast without fake price | dropped-price tests pass |
| Same-symbol holdings from multiple strategies could be a regression risk | MiniQMT account is merged, but AIstock strategy lots must remain isolated | Builder reads `list_position_lots(account.strategy_id)` only and regression test inserts another strategy's same symbol | same-symbol isolation test passes |

## Result

- Current status: PASS for local BUG-050 L3 service-level validation.
- Remaining risks: broker account-level `can_sell`, suspend/limit constraints, actual MiniQMT submit/fill or broker rejection still require runtime SIM POC after merge/deploy; SELL fill lot closing and realized PnL are tracked outside this fix (for example BUG-052); T+1 rollover is tracked outside this fix (BUG-051).
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no schema change in this fix.
- Need MiniQMT broker action: no during local validation; order generation/preflight only.
- Production impact during validation: none; no production 8001/3000, broker order placement, or DB writes used.
