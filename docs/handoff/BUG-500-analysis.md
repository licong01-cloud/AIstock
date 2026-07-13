# BUG-500 Analysis - MiniQMT compiler submit pre-trade risk layer

## Root Cause

QmtManagedOrderService is the compiler/B-side MiniQMT submit boundary used by managed strategy-slot orders. Before this fix it only performed structural checks, board-lot validation, virtual cash/T+1 lot checks, and the BUG-501 account-group aggregate cash gate. There was no explicit, flag-gated pre-trade risk layer for submit-time kill-switch, price collar, fat-finger quantity/notional, or a reason-coded buying-power rejection. MiniQMTSimBackend also remains locked to SIM/account-group runtime semantics; this BUG should not unlock LIVE or change SIM defaults.

## Fix Plan

- Add an inert-by-default miniqmt_pre_trade_risk layer in order_service.py so existing SIM behavior is unchanged unless the compiler/runtime explicitly opts in through account/request risk config.
- Implement loud preflight errors with stable reason codes: PRE_TRADE_KILL_SWITCH_ACTIVE, PRE_TRADE_PRICE_COLLAR_REJECT, PRE_TRADE_FAT_FINGER_QUANTITY, PRE_TRADE_FAT_FINGER_NOTIONAL, PRE_TRADE_BUYING_POWER_REJECT, and PRE_TRADE_RISK_CONFIG_INVALID.
- Keep BUG-501 ACCOUNT_GROUP_CASH_OVERCOMMIT as the always-on account-group aggregate cash hard gate, independent from the BUG-500 flag-gated per-order pre-trade risk layer, and do not convert it into BUG-478 capacity-residual semantics.
- Do not change MiniQMT LIVE locks, service/runtime state, LocalSim, or production DB/DDL.

## Acceptance Assertions

- New unit tests reject collar, fat-finger, buying-power, and kill-switch cases before broker submit.
- A disabled/default risk config stays inert and permits otherwise risk-shaped SIM test orders.
- BUG-501 account-group overcommit tests remain green after rebase, with ACCOUNT_GROUP_CASH_OVERCOMMIT reported as the account-group hard gate rather than the flag-gated miniqmt_pre_trade layer.
- production_ddl_gate: noop
