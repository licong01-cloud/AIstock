# BUG-053 MiniQMT StrategyPackage Binding Rollover

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main 65bea7a
- Fix branch: bug/BUG-053-miniqmt-binding-rollover
- Worktree: F:\Dev\AIstock_worktrees\bug-053-miniqmt-binding-rollover
- Operator: codex-app
- Linked bug: BUG-053 / GitHub #56

## Scope

- Changed files: package binding service/repository/router, selection order builder active-binding guard, migration comments, qmt_strategy_ledger tests, BUG-053 JSON, this validation record.
- Impacted flows: MiniQMT virtual strategy package binding, daily selection_run rollover, active binding lookup, package-bound order preview/build.
- Business goal: next-day MiniQMT operation can bind a fresh selection_run/trade_date without manual SQL while preserving historical binding evidence.
- Out of scope: live MiniQMT submit/cancel, production backend restart, production DB writes, frontend UI changes, broker-side POC.
- Protected assets reviewed: no production 8001/3000, no production DB mutation, no MiniQMT broker submit/cancel.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Same package/selection bind repeated | returns existing ACTIVE binding and does not create duplicate rows | test_package_binding_same_selection_is_idempotent_without_duplicate_active_row | PASS |
| New selection without explicit replace | fails fast with actionable INVALID_STATE_TRANSITION / HTTP 409 | test_package_binding_requires_explicit_rollover_for_different_selection; router test | PASS |
| New selection with replace_active | retires old binding, creates new ACTIVE binding, keeps history | package binding/repository/router tests | PASS |
| Historical binding is not executable | order build rejects RETIRED binding | test_selection_order_builder_rejects_historical_retired_binding | PASS |
| Module regression | qmt_strategy_ledger full suite remains green | python -m pytest backend/tests/qmt_strategy_ledger -q -> 66 passed | PASS |

## Commands

```bash
python -m pytest backend/tests/qmt_strategy_ledger/test_package_binding.py backend/tests/qmt_strategy_ledger/test_repository.py backend/tests/qmt_strategy_ledger/test_router_summary.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
# final gate commands to run before PR/merge:
python -m compileall backend/services/qmt_strategy_ledger/package_binding.py backend/services/qmt_strategy_ledger/repository.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/routers/qmt_strategy_ledger.py
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Targeted binding/router/repository/selection tests: 28 passed in 6.69s on final local rerun.
- Full qmt_strategy_ledger suite: 66 passed in 6.03s on final local rerun.
- compileall for changed qmt_strategy_ledger/router files: passed.
- git diff --check: passed.
- git diff --cached --check: passed.
- nox validation_module_registry_l0: passed, 8 tests passed.
- nox validation_catalog_integrity: passed, 3 tests passed, state=passed.
- nox guardrail_changed_files: passed, blocking=0, module ownership files=11 mapped=11.
- nox l0: passed, guardrail scan blocking=0; pre-existing baseline/medium findings only.
- Business oracle: same strategy/package/selection/trade_date bind is idempotent and returns existing ACTIVE binding.
- Business oracle: a different selection_run/trade_date requires explicit replace_active=true and otherwise returns HTTP 409 with active/requested binding context.
- Business oracle: rollover retires the old binding with binding_lifecycle metadata and creates one new ACTIVE binding.
- Business oracle: package-bound order build rejects RETIRED bindings so historical bindings remain auditable but cannot drive execution.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Next-day binding could fail on unique active binding | bind() was create-only while schema enforces one ACTIVE per strategy | Added bind_with_result lifecycle with idempotent existing and explicit replace_active rollover | targeted tests 28 passed; suite 66 passed |
| Unsafe duplicate active intent had unstructured error | router called bind() directly and let raw errors surface | Router maps TradingCoreError to structured 404/409/422 and exposes action/replaced_binding | router test passes |
| Historical binding could be passed to preview by id | build_for_binding did not verify binding_status | Active-only guard rejects RETIRED binding before reading selection candidates | selection builder test passes |

## Result

- Current status: PASS for BUG-053 local validation; GitHub PR CI still required before merge.
- Remaining risks: actual next-day MiniQMT runtime activation requires backend restart and operator-driven binding/preview workflow after user approval.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no new schema column; migration comments updated only.
- Need MiniQMT broker action: no during local validation; no submit/cancel used.
- Production impact during validation: none.
