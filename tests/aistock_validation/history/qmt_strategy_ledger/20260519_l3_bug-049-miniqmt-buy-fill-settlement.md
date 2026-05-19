# BUG-049 MiniQMT buy fill settlement regression

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-19
- Git base: origin/main at de4e89e after PR #75 merge
- Fix branch: bug/BUG-049-miniqmt-buy-fill-settlement
- Worktree: F:\Dev\AIstock_worktrees\bug-049-miniqmt-buy-fill-settlement
- Operator: codex-app
- Linked bug: BUG-049 / GitHub #50

## Scope

- Changed files: backend/services/qmt_strategy_ledger/sync_service.py; backend/services/qmt_strategy_ledger/repository.py; backend/tests/qmt_strategy_ledger/test_sync_service.py; this validation record.
- Impacted flows: MiniQMT read-only snapshot sync, trade ledger idempotency, BUY fill cash settlement, terminal BUY cancel/reject residual freeze release, strategy market_value/unrealized_pnl refresh, same-symbol multi-strategy lot valuation.
- Business goal: after MiniQMT BUY fills are synchronized, filled notional no longer stays indefinitely in frozen_cash; each strategy account exposes cash/frozen_cash/market_value/unrealized_pnl from its own cash ledger and lot ledger.
- Out of scope: SELL fill lot close/PnL, live broker order placement, raw MiniQMT connectivity, schema migration, UI changes, production DB backfill.
- Protected assets reviewed: no StrategyPackage manifest/model/factor artifact, HMM snapshot, production DB data, production backend 8001, frontend 3000, or MiniQMT broker runtime touched.

## Environment

- Backend port: not started; no production 8001 access used.
- Frontend port: not started.
- TDX port: not used.
- Database: not used by unit tests; PostgreSQL repository code was compiled only.
- Browser/headless: not used; backend service/repository regression.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Full BUY fill with existing freeze | BUY_FILL entry reduces frozen_cash, records commission cash_delta, creates lot, market_value equals strategy lot cost | `test_sync_service_upserts_attributed_order_trade_and_lot_without_broker_submit` | PASS |
| Repeat sync idempotency | Existing trade and deterministic BUY_FILL cash_id do not append duplicate cash entries or drift account balances | same test reruns sync | PASS |
| Unmanaged/manual BUY fill without freeze | BUY_FILL debits available cash by fill amount plus commission and still creates/values lot | `test_sync_service_settles_unmanaged_buy_fill_against_cash_without_freeze` | PASS |
| Partial cheaper fill then cancel | Filled portion releases reserved frozen_cash, refunds price improvement to cash through BUY_FILL, cancelled remainder releases cash exactly once as UNFREEZE_CANCEL, and intent status becomes CANCELLED | `test_sync_service_settles_cheaper_fill_and_releases_cancelled_residual_once` | PASS |
| Rejected BUY order | Remaining frozen cash releases exactly once as UNFREEZE_REJECT and intent status becomes REJECTED | `test_sync_service_releases_rejected_buy_freeze_once` | PASS |
| Same-symbol multi-strategy positions | Strategy A/B market_value is computed from each strategy lot ledger, not duplicated from account-level merged MiniQMT position | `test_sync_service_values_same_symbol_by_strategy_lots_independently` | PASS |
| Existing unattributed safeguards | Blank strategy, duplicate remark, and unknown trade still route to unattributed tables | existing unattributed sync test | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | `python -m pytest backend/tests/qmt_strategy_ledger -q` -> 43 passed | PASS |

## Commands

```bash
python -m pytest backend/tests/qmt_strategy_ledger/test_sync_service.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/services/qmt_strategy_ledger/sync_service.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/order_service.py
python -m nox -s validation_module_registry_l0
python -m nox -s guardrail_changed_files
python -m nox -s l0
git diff --check
```

## Evidence

- Targeted sync tests: 6 passed in 0.39s after rebasing to origin/main de4e89e.
- Full qmt_strategy_ledger tests: 43 passed in 11.36s after rebasing to origin/main de4e89e.
- validation_module_registry_l0: 8 passed; ownership scan mapped 12/12 files.
- guardrail_changed_files: successful after staging; files=4, findings=3 P2 ALGO-COMPLEXITY in repository.py, blocking=0.
- l0: successful; existing baseline/new guardrail findings were non-blocking with blocking=0.
- git diff --check: passed.
- Compile checks: touched qmt_strategy_ledger service/repository/model/order modules compiled successfully after rebasing to origin/main de4e89e.
- Cash ledger oracle: BUY_FILL is deterministic by account/trade_date/trade_id; terminal filled/cancel/reject residual release is deterministic by account/trade_date/order_id/status reason.
- Account oracle: `cash + frozen_cash + market_value` now changes consistently with fill amount, commission, and residual freeze release.
- Multi-strategy oracle: same broker symbol 300604.SZ with a merged account-level position still yields strategy A market_value 2000 and strategy B market_value 6000 from strategy lots.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Filled BUY notional remained in frozen_cash and market_value stayed zero | sync_service inserted trade/lots only; it did not append BUY_FILL cash movements or recompute account valuation | Added BUY_FILL settlement, deterministic cash events, terminal BUY residual release, and lot-based strategy account revaluation | Targeted sync tests 6 passed; full qmt_strategy_ledger tests 43 passed |
| Repeat snapshot could not safely be used as settlement proof | cash settlement was not represented as an idempotent ledger event | Added repository `append_cash_entry_once` / `apply_cash_entry_once` and deterministic cash_id generation | Idempotency assertions in full, partial/cancel, rejected, and unmanaged-fill tests pass |

## Result

- Current status: PASS for local BUG-049 L3 service-level validation.
- Remaining risks: PR CI still needs to run after commit/push; SELL fill settlement and realized PnL remain outside BUG-049 and should be tracked separately if required.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no schema change in this fix.
- Need MiniQMT broker action: no; this fix only changes AIstock read-only sync settlement and repository behavior.
- Production impact during validation: none; no production 8001/3000, broker order placement, or DB writes used.
