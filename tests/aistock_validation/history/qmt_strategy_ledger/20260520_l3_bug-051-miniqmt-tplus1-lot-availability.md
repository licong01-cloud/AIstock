# BUG-051 MiniQMT T+1 strategy lot sell availability

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main bc33466
- Fix branch: bug/BUG-051-miniqmt-tplus1-lot-availability
- Worktree: F:\Dev\AIstock_worktrees\bug-051-miniqmt-tplus1-lot-availability
- Operator: codex-app
- Linked bug: BUG-051 / GitHub #54

## Scope

- Changed files: backend/services/qmt_strategy_ledger/lot_availability.py; backend/services/qmt_strategy_ledger/sync_service.py; backend/services/qmt_strategy_ledger/order_service.py; backend/services/qmt_strategy_ledger/repository.py; backend/services/qmt_strategy_ledger/selection_order_builder.py; backend/routers/qmt_strategy_ledger.py; backend/tests/qmt_strategy_ledger/test_sync_service.py; backend/tests/qmt_strategy_ledger/test_order_service_preflight.py; backend/tests/qmt_strategy_ledger/test_selection_order_builder.py; backend/tests/qmt_strategy_ledger/test_repository.py; BUG-051 JSON; this validation record.
- Impacted flows: MiniQMT sync-snapshot, strategy lot availability, managed SELL preview/submit preflight, StrategyPackage rebalance order generation, same-symbol multi-strategy isolation.
- Business goal: BUY fill lots remain unavailable on T day, become sellable on the next valid A-share trading day, reserve pending SELL intents, and allow next-day dropped/overweight holdings to be sold without manual DB edits.
- Out of scope: SELL fill lot closure/realized PnL (BUG-052), binding rollover (BUG-053), raw endpoint guard (BUG-054), batch atomicity (BUG-055), production backend restart, production DB writes, and live MiniQMT submit/cancel.
- Protected assets reviewed: no production backend 8001/3000, no production DB mutation, no MiniQMT broker submit/cancel, no StrategyPackage artifact changes.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Same-day BUY lot | T-day lot stays unavailable for SELL | test_preview_keeps_same_day_and_non_trading_day_lot_locked; sync same-day assertion | PASS |
| Next trading day unlock | Prior trading-day lot becomes available on T+1 | test_sync_service_unlocks_prior_trading_day_lot_on_tplus1_idempotently; test_preview_derives_prior_day_lot_sellable_on_next_trading_day | PASS |
| Non-trading day | Weekend/non-trading date does not unlock before next trading day | test_sync_service_does_not_unlock_on_non_trading_day; test_preview_keeps_same_day_and_non_trading_day_lot_locked | PASS |
| Pending SELL reservation | Open SELL intents reduce effective sellable quantity without changing lot remaining_quantity | test_preview_reserves_pending_sell_intents_without_changing_lot_quantity; test_selection_order_builder_reserves_pending_sell_intents_before_emit | PASS |
| Same-symbol multi-strategy | Lots unlock independently by strategy_id and are not merged | test_sync_service_keeps_same_symbol_strategy_lot_unlocks_independent | PASS |
| Rebalance dropped holding | Next-day dropped holding with stored available=0 can emit SELL from derived T+1 availability | test_selection_order_builder_derives_dropped_holding_sellable_on_tplus1 | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | python -m pytest backend/tests/qmt_strategy_ledger -q -> 58 passed | PASS |

## Commands

`ash
python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_sync_service.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_repository.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/services/qmt_strategy_ledger/lot_availability.py backend/services/qmt_strategy_ledger/sync_service.py backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/routers/qmt_strategy_ledger.py
# final gate commands to run before PR/merge:
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
`

## Evidence

- Targeted qmt_strategy_ledger T+1 tests: 40 passed in 0.72s on interim rerun.\n- Full qmt_strategy_ledger suite: 58 passed in 12.18s on final local rerun.\n- Compile checks: touched qmt_strategy_ledger modules and router compiled successfully.\n- `git diff --check` and `git diff --cached --check`: passed.\n- `validation_module_registry_l0`: 8 passed; ownership scan mapped 12/12 files.\n- `validation_catalog_integrity`: passed with 0 findings.\n- `guardrail_changed_files`: successful; files=12, findings=5 P2 non-blocking `ALGO-COMPLEXITY-001`, blocking=0.\n- `l0`: successful; baseline/new guardrail findings were non-blocking with blocking=0.
- Business oracle: lot availability is derived from open_date, 	rade_date, and canonical trading calendar, not from a simplified always-zero/always-available flag.
- Business oracle: sync-snapshot persists T+1 unlock to position_lot.available_quantity; preview/order builder also derive effective availability so tomorrow's SELL preflight does not require manual SQL.
- Business oracle: open SELL intents with submit_status CREATED/SUBMITTED/ACCEPTED reserve availability; CANCELLED/REJECTED/FILLED intents do not reserve.
- Business oracle: same-symbol positions across two strategies remain strategy-scoped; account-level MiniQMT can_sell remains a later broker preflight, not a substitute for strategy attribution.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| BUY lots stayed permanently unsellable | BUY fills were inserted with available_quantity=0 and no T+1 rollover/derivation existed | Added canonical lot_availability helpers, sync-time unlock, and preview/builder effective availability derivation | targeted tests 40 passed; module tests 58 passed |
| Pending SELL risk not represented | Preflight summed lot.available_quantity only and ignored already-open SELL intents | Repository now lists open SELL intents; preflight and builder subtract pending SELL quantities | pending-reservation tests pass |
| Same-symbol multi-strategy risk | MiniQMT account is merged while AIstock strategy lots must remain isolated | Availability reads lots/intents by strategy_id and symbol; same-symbol tests cover two strategies | isolation tests pass |

## Result

- Current status: PASS for local BUG-051 L3 service-level validation.
- Remaining risks: GitHub PR CI and actual MiniQMT runtime sell validation still require merge/deploy, backend restart, and broker SIM runtime after user approval.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no schema change required.
- Need MiniQMT broker action: no during local validation; no submit/cancel used.
- Production impact during validation: none.
