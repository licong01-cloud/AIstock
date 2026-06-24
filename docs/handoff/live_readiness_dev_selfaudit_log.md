# MiniQMT LIVE Readiness Dev Self-Audit Log

## BUG-501 - 2026-06-24

- Reproduction red-to-green: yes. 	est_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots failed before the fix with roker.place_order_calls == 1, then passed after the account-group preflight gate.
- LocalSim / compiler B inert regression: no LocalSim files touched; BUG-501 only changes MiniQMT qmt_strategy_ledger batch preflight and tests. Compiler B LIVE remains locked; no runtime flags changed.
- Loud failures / reason_code: yes. Offending account-group BUY intents now receive ACCOUNT_GROUP_CASH_OVERCOMMIT with account_group_id, cash limits, batch_required_cash, overcommit_cash, affected orders, and next_action.
- Allowed scope: within BUG-501 allowed_write_scope only.
- Production gates: production_ddl_gate=noop; backend_dependency_gate=noop; frontend_dependency_gate=noop.
