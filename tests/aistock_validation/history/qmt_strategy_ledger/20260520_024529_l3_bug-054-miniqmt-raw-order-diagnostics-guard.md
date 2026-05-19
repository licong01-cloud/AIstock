# BUG-054 MiniQMT raw order diagnostics guard

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main 896259e
- Fix branch: bug/BUG-054-miniqmt-raw-order-guard
- Worktree: F:\Dev\AIstock_worktrees\bug-054-miniqmt-raw-order-guard
- Operator: codex-app
- Linked bug: BUG-054 / GitHub #57

## Scope

- Changed files: backend/routers/qmt.py; backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py; docs/architecture/miniqmt_multi_strategy_virtual_account_poc_design_20260518.md; BUG-054 JSON; this validation record.
- Impacted flows: raw /api/v1/qmt/order, raw /api/v1/qmt/order/batch, managed /api/v1/qmt/virtual-strategies/orders, read-only reconciliation visibility for unattributed activity.
- Business goal: normal multi-strategy execution cannot accidentally bypass the AIstock virtual strategy ledger; raw broker calls remain available only behind an explicit administrator/POC diagnostic switch.
- Out of scope: changing MiniQMT client transport, changing reconciliation semantics, changing batch compensation workflow (BUG-055), simplifying canonical preflight layers (BUG-056), production backend restart, production DB writes, and live MiniQMT submit/cancel.
- Protected assets reviewed: no production backend 8001/3000 touched, no production DB mutation, no real MiniQMT submit/cancel, no StrategyPackage artifact changes.

## Environment

- Backend port: not started for this fix; pytest used FastAPI TestClient only.
- Frontend port: not used.
- TDX port: not used.
- Conda/env: local Python in AIstock worktree; final nox gates to use C:\Users\lc999\miniconda3\envs\AIstock\python.exe.
- Database: not used; in-memory qmt_strategy_ledger repository only.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Raw single order default | /api/v1/qmt/order returns 403 before broker call and points to managed virtual-strategy route | test_raw_order_router_is_disabled_by_default_and_does_not_call_broker | PASS |
| Raw batch default | /api/v1/qmt/order/batch returns 403 before any item broker call and points to managed route | test_raw_batch_order_router_is_disabled_by_default_and_does_not_call_broker | PASS |
| Raw single diagnostic override | AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS=1 preserves diagnostic ability, including blank strategy fields for controlled POC | test_raw_order_router_requires_explicit_diagnostic_switch | PASS |
| Raw batch diagnostic override | explicit diagnostic switch preserves batch diagnostic ability and returns warning | test_raw_batch_order_router_requires_explicit_diagnostic_switch | PASS |
| Managed strategy path | managed order route records order_intent and strategy attribution before fake broker call | test_managed_submit_records_intent_before_broker_call | PASS |
| Reconciliation/unattributed visibility | blank strategy, duplicate remark, and unknown trade still become unattributed records | test_sync_service_routes_blank_strategy_duplicate_remark_and_unknown_trade_to_unattributed; test_reconciliation_reports_position_mismatch_and_unattributed_trade | PASS |
| Module regression | qmt_strategy_ledger full unit suite remains green | python -m pytest backend/tests/qmt_strategy_ledger -q -> 71 passed | PASS |

## Commands

```bash
python -m compileall backend/routers/qmt.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py
python -m pytest backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py -q
python -m pytest backend/tests/qmt_strategy_ledger/test_reconciliation.py backend/tests/qmt_strategy_ledger/test_sync_service.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/routers/qmt.py backend/tests/qmt_strategy_ledger/test_router_managed_orders_guard.py backend/services/qmt_strategy_ledger/reconciliation.py backend/services/qmt_strategy_ledger/sync_service.py
# final gate commands to run before PR/merge:
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Raw route guard tests: 8 passed; fake client call list proves no broker call when the diagnostic switch is absent.
- Reconciliation and sync focused tests: 14 passed; external/manual/unattributed activity still surfaces as unattributed issues instead of silent strategy assignment.
- Full qmt_strategy_ledger suite: 71 passed; managed order, sync, reconciliation, package binding, selection builder, and repository regressions remain green.
- Compile checks: touched router/test plus reconciliation/sync modules compiled successfully.
- `git diff --check` and `git diff --cached --check`: passed.
- `validation_module_registry_l0`: 8 passed; module ownership scan mapped 12/12 files.
- `validation_catalog_integrity`: passed with 0 findings.
- `guardrail_changed_files`: initial rerun exposed pre-existing broad-except/pass patterns in touched `backend/routers/qmt.py`; these were converted to debug logging, then changed-files guardrail passed with files=5, findings=0, blocking=0.
- `l0`: successful; repository baseline/new non-blocking findings remained blocking=0.
- Documentation evidence: design doc now states raw /qmt/order and /qmt/order/batch are default-disabled admin/POC diagnostics and must not be normal strategy execution entrypoints.
- Business oracle: normal multi-strategy order submission path is /api/v1/qmt/virtual-strategies/orders, which creates order_intent, local cash/lot preflight state, broker attribution fields, and order ledger before sync.
- Business oracle: raw diagnostic capability is not silently removed; it remains available only with AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS=1 and response warnings.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Raw order endpoints could bypass virtual ledger | /qmt/order and /qmt/order/batch accepted optional strategy_name/order_remark and called MiniQMT directly | Added explicit AISTOCK_ALLOW_QMT_RAW_ORDER_DIAGNOSTICS gate and 403 guidance to managed route | raw route guard tests pass |
| Router swallowed validation errors as 500 | order endpoints caught generic Exception without re-raising HTTPException | Added except HTTPException: raise around raw order endpoints | raw guard tests assert 403 response |
| Documentation left raw endpoint as POC path without code-level switch | design doc described low-level channel but did not name the runtime gate | Updated design doc with default-disabled diagnostic switch and warning semantics | doc diff reviewed |
| Changed-files guardrail flagged old silent pass blocks in touched router | Staging `backend/routers/qmt.py` made historical broad exception/pass patterns subject to new guardrail | Converted these paths to debug logging without changing response semantics | guardrail_changed_files passed with 0 findings |

## Result

- Current status: PASS for local BUG-054 L3 service-level validation and final local nox gates.
- Remaining risks: GitHub PR CI still needs to pass before merge; actual production effect requires backend restart after user approval.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no.
- Need MiniQMT broker action: no during local validation; fake clients only.
- Production impact during validation: none.
