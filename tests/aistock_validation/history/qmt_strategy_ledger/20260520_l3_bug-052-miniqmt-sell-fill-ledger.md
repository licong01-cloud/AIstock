# BUG-052 MiniQMT SELL fill lot/cash/PnL settlement

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main c0cb8f3
- Fix branch: bug/BUG-052-miniqmt-sell-fill-ledger
- Worktree: F:\Dev\AIstock_worktrees\bug-052-miniqmt-sell-fill-ledger
- Operator: codex-app
- Linked bug: BUG-052 / GitHub #55

## Scope

- Changed files: backend/services/qmt_strategy_ledger/sync_service.py; backend/services/qmt_strategy_ledger/repository.py; backend/tests/qmt_strategy_ledger/test_sync_service.py; BUG-052 JSON; this validation record.
- Impacted flows: MiniQMT sync-snapshot SELL trade processing, FIFO strategy lot closure, SELL_FILL cash ledger, realized PnL, account market value refresh, same-symbol multi-strategy isolation.
- Business goal: SELL fills must close the selling strategy's lots, update cash and realized PnL exactly once, and leave per-strategy returns trustworthy after BUG-050/BUG-051 enable legal SELL requests.
- Out of scope: live MiniQMT submit/cancel, production backend restart, production DB writes, binding rollover, raw endpoint guard, batch compensation, broker-side POC.
- Protected assets reviewed: no production 8001/3000, no production DB mutation, no MiniQMT broker submit/cancel, no StrategyPackage artifact changes.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Full SELL fill | FIFO lot closes to zero; SELL_FILL cash entry and realized PnL are recorded once | test_sync_service_full_sell_fill_closes_fifo_lot_and_records_cash_pnl_once | PASS |
| Partial SELL fill | FIFO lot becomes PARTIALLY_CLOSED with remaining/available/cost basis correct | test_sync_service_partial_sell_fill_partially_closes_fifo_lot | PASS |
| Idempotent sync | Re-syncing same SELL trade does not double-count cash or PnL | full SELL idempotent rerun in test_sync_service_full_sell_fill_closes_fifo_lot_and_records_cash_pnl_once | PASS |
| Same-symbol multi-strategy isolation | SELL by strategy A does not close strategy B same-symbol lot | test_sync_service_same_symbol_sell_closes_only_selling_strategy_lots | PASS |
| BUY regression | Existing BUY settlement/freeze release behavior remains green | python -m pytest backend/tests/qmt_strategy_ledger/test_sync_service.py -q -> 12 passed | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | python -m pytest backend/tests/qmt_strategy_ledger -q -> 61 passed | PASS |

## Commands

```bash
python -m pytest backend/tests/qmt_strategy_ledger/test_sync_service.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/services/qmt_strategy_ledger/sync_service.py backend/services/qmt_strategy_ledger/repository.py
# final gate commands:
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Targeted sync tests: 12 passed in 0.43s on final local rerun.
- Full qmt_strategy_ledger suite: 61 passed in 11.04s on final local rerun.
- compileall for changed qmt_strategy_ledger service files: passed.
- git diff --check: passed.
- git diff --cached --check: passed.
- nox validation_module_registry_l0: passed, 8 tests passed.
- nox validation_catalog_integrity: passed, 3 tests passed, state=passed.
- nox guardrail_changed_files: passed, blocking=0, module ownership files=5 mapped=5.
- nox l0: passed, guardrail scan blocking=0; pre-existing baseline/medium findings only.
- Business oracle: SELL fills are matched through the existing order_remark/intent attribution path; external/manual unattributed trades remain unattributed rather than silently assigned.
- Business oracle: lot closure is FIFO by list_position_lots() order and records lot closure trace in SELL_FILL metadata.
- Business oracle: realized PnL = gross proceeds - closed cost basis - proportional commission; account cash increases by net proceeds once.
- Business oracle: idempotency uses trade_ledger uniqueness and cash_ledger.cash_id; repeated sync does not replay lot closure or cash/PnL.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| SELL fills did not affect lots/cash/PnL | sync_service only had BUY branch after trade insertion | Added SELL branch, atomic FIFO lot closure with cash/account updates, SELL_FILL cash entry, realized PnL/account update | targeted sync tests 12 passed; module tests 61 passed |
| Idempotency risk | Repeated sync could otherwise close lots twice | SELL processing only runs after new trade_ledger insert and cash_id lookup guards duplicate settlement | idempotent SELL rerun passes |
| Same-symbol strategy leakage risk | MiniQMT account is merged while strategy lots are isolated | Lot closure reads strategy_id + symbol only from attributed intent/trade | same-symbol isolation test passes |

## Result

- Current status: PASS for BUG-052 local validation; GitHub PR CI still required before merge.
- Remaining risks: actual MiniQMT runtime SELL fill validation requires backend restart and broker SIM runtime after user approval.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no schema change required.
- Need MiniQMT broker action: no during local validation; no submit/cancel used.
- Production impact during validation: none.
