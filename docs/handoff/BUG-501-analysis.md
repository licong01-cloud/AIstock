# BUG-501 Analysis - MiniQMT account-group cash overcommit

## Root Cause

QmtManagedOrderService.submit_batch() treats account-group cash exhaustion as the existing capacity-residual path. In _batch_preflight(), once cumulative BUY freeze exceeds the account-group cash_limit, only the later BUY receives SKIPPED_INSUFFICIENT_CAPITAL; _is_non_compensating_batch_residual() then marks that as non-hard, so the earlier cash-fit BUY is submitted to broker. This violates the LIVE readiness invariant that an account-group batch must be rejected before any broker call when aggregate same-account-group BUY demand exceeds available group buying power.

## Fix Plan

- Add a dedicated ACCOUNT_GROUP_CASH_OVERCOMMIT preflight error for account-group aggregate BUY overcommit.
- When same account-group BUY freeze exceeds the group cash limit and no same-batch sell proceeds can keep the batch in the existing SELL-first/dependent-buy path, annotate every BUY in that account group with the loud overcommit reason.
- Because this error is not a non-compensating capacity residual, submit_batch() will persist a PREFLIGHT_FAILED batch and return before any broker place_order() call.
- Preserve existing strategy-level capacity residual and dependent sell-proceeds behavior; BUG-478 terminal capacity residual remains a post-skip observability concern, while BUG-501 is a submit-time account-group buying-power gate.

## Acceptance Assertions

- Reproduction test becomes green: roker.place_order_calls == 0.
- Overcommit batch returns preflight_passed=False and every BUY in the offending account group has primary 
eason_code=ACCOUNT_GROUP_CASH_OVERCOMMIT.
- A non-overcommit same-account-group batch still submits normally.
- No LocalSim paths, production services, or production DB/DDL are touched.

## Production Gates

- production_ddl_gate: noop
- production_backend_dependency_gate: noop
- production_frontend_dependency_gate: noop
