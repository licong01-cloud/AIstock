# BUG-055 MiniQMT managed batch preflight and compensation

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main 6601808
- Fix branch: bug/BUG-055-miniqmt-batch-preflight
- Worktree: F:\Dev\AIstock_worktrees\bug-055-miniqmt-batch-preflight
- Operator: codex-app
- Linked bug: BUG-055 / GitHub #58

## Scope

- Changed files: backend/services/qmt_strategy_ledger/models.py; backend/services/qmt_strategy_ledger/repository.py; backend/services/qmt_strategy_ledger/order_service.py; backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py; backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py; docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md; BUG-055 JSON; this validation record.
- Impacted flows: managed /api/v1/qmt/virtual-strategies/orders/batch, managed single-order submit path, qmt_strategy.order_batch persistence, router JSON contract for batch preflight/partial/success.
- Business goal: multi-order MiniQMT strategy batches must fail before broker submission when the batch is invalid, and must persist idempotent batch state plus explicit compensation actions when the broker accepts only part of a preflight-passed batch.
- Out of scope: claiming broker-side atomicity, automatic compensation/cancel, raw diagnostic /api/v1/qmt/order/batch behavior, production backend restart, production DB writes, and real MiniQMT submit/cancel.
- Protected assets reviewed: no production backend 8001/3000 touched, no production DB mutation, no real MiniQMT submit/cancel, no StrategyPackage artifact changes.

## Environment

- Backend port: not started for this fix; pytest used service objects and FastAPI TestClient only.
- Frontend port: not used.
- TDX port: not used.
- Conda/env: local Python in AIstock worktree plus final nox gates through C:\Users\lc999\miniconda3\envs\AIstock\python.exe.
- Database: not used in tests; in-memory qmt_strategy_ledger repository only. Repository SQL path compiled and maps to existing qmt_strategy.order_batch DDL from qmt_strategy_ledger_20260518.sql.
- Broker: fake MiniQMT clients only; no live broker action.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Full-batch duplicate remark preflight | Batch returns PREFLIGHT_FAILED and zero broker calls / zero order_intent rows | test_submit_batch_full_preflight_failure_does_not_call_broker | PASS |
| Aggregate buy cash preflight | Batch cash freeze is summed before broker submission and blocks all broker calls when insufficient | test_submit_batch_aggregates_cash_before_broker_call | PASS |
| Aggregate same-symbol sell preflight | Batch same-symbol sell checks strategy available lot and account-level broker can_sell before broker submission | test_submit_batch_aggregates_same_symbol_sell_and_broker_can_sell | PASS |
| Broker partial success | Preflight-passed batch can become PARTIAL, persists batch record, exposes managed cancel compensation actions, and does not auto-cancel | test_submit_batch_reports_partial_success_without_auto_cancel | PASS |
| Idempotent retry | Replaying identical normalized batch returns existing batch_id/result, creates no duplicate intents, and calls no broker | test_submit_batch_retry_is_idempotent_and_does_not_call_broker_again | PASS |
| Router successful batch contract | API exposes batch_id, batch_status=SUCCEEDED, preflight_passed=true, and empty compensation_actions for full success | test_managed_batch_submit_returns_batch_preflight_contract | PASS |
| Router preflight failure contract | API exposes batch_status=PREFLIGHT_FAILED and fake client receives no calls | test_managed_batch_submit_preflight_failure_skips_broker | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | python -m pytest backend/tests/qmt_strategy_ledger -q -> 77 passed | PASS |
| Validation guardrails | Changed files and baseline L0 validation remain non-blocking | nox validation_module_registry_l0, validation_catalog_integrity, guardrail_changed_files, l0 | PASS |

## Commands

```bash
python -m compileall backend/services/qmt_strategy_ledger/models.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/order_service.py
python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py -q
python -m pytest backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py -q
python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Batch service tests: 7 passed; cover full-batch preflight failure, aggregate cash, aggregate sell availability/can_sell, broker partial success, and idempotent retry.
- Router guard/contract tests: 10 passed; cover batch success/preflight-failure JSON contract and unchanged managed/raw route guards.
- Combined targeted tests: 17 passed.
- Full qmt_strategy_ledger suite: 77 passed; managed order, preflight, sync, reconciliation, package binding, selection builder, repository, and migration comment regressions remain green.
- Compile checks: touched qmt_strategy_ledger model/repository/order service compiled successfully.
- `git diff --check` and `git diff --cached --check`: passed.
- `validation_module_registry_l0`: 8 passed; module ownership scan mapped 12/12 files.
- `validation_catalog_integrity`: passed with 0 findings.
- `guardrail_changed_files`: successful; files=8, mapped=8, blocking=0. Four P2 ALGO-COMPLEXITY findings were non-blocking and tied to existing repository functions becoming staged.
- `l0`: successful; baseline/new guardrail findings remained blocking=0.
- Documentation evidence: implementation plan now states batch preflight must validate all items and aggregate constraints before broker calls; partial success returns managed cancel compensation actions and repeat submissions are idempotent by batch_id.
- Business oracle: AIstock prevents avoidable pre-broker partial submissions while still acknowledging that MiniQMT broker-side submission is not atomic.
- Business oracle: partial broker success is explicit state (`PARTIAL`) with operator compensation action, not a vague textual-only warning.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Sequential submit could call broker for early valid items before later invalid items failed | submit_batch delegated to submit_order one item at a time | Added full-batch preflight before broker calls, with batch-level duplicate remark, cash, strategy sell, and broker can_sell aggregation | batch service tests and router tests pass |
| Partial broker success had no persisted batch lifecycle | order_batch model/repository access was not used by service | Added OrderBatchStatus/OrderBatchRecord, repository upsert/get/list-by-batch methods, and batch state transitions | full qmt_strategy_ledger suite passes |
| Retry could duplicate broker orders | no deterministic batch identity or existing-batch replay | Added deterministic qmtbatch_* id from normalized request signatures and existing-batch result reconstruction | retry idempotency test passes |
| Partial compensation was only text | old response used compensation_required plus a generic hint | Added compensation_actions with managed cancel endpoint, intent_id, and qmt_order_id for accepted items | partial success test passes |

## Result

- Current status: PASS for local BUG-055 L3 service-level validation and final local nox gates; GitHub PR CI still needs to run before merge.
- Remaining risks: real MiniQMT behavior is not exercised; production effect requires backend restart after user approval; order_batch table must exist in target DB via qmt_strategy_ledger_20260518.sql.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no new migration in this fix; relies on existing qmt_strategy.order_batch/order_intent.batch_id migration already requested/applied for MiniQMT ledger.
- Need MiniQMT broker action: no during local validation; fake clients only.
- Production impact during validation: none.
