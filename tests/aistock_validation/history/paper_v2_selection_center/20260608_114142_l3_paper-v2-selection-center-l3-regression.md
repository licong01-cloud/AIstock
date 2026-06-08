# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-08T11:41:42
- Git commit: d5bc9d5d
- Operator: lc999

## Scope

- Changed files: `backend/services/simulation_runtime/scheduler.py`, `backend/services/qmt_strategy_ledger/order_service.py`, MiniQMT/Paper v2 scheduler and order-service regression tests, `BUG-283` registry.
- Impacted flows: Paper v2 unattended scheduler, MiniQMT managed-order batch submit, qmt_strategy virtual-ledger cash preflight, existing-plan retry.
- Business goal: MiniQMT submit preflight failure must not be marked as simulation success; no-side-effect failed batches must be retryable; rebalance BUY cash checks must account for same-batch SELL proceeds.
- Out of scope: production backend/frontend/TDX/MiniQMT restart, production DB writes, real broker cancel/clear-position operations, frontend UI changes.
- Protected assets reviewed: no StrategyPackage frozen manifest, QE artifact, model weight, HMM snapshot, or production ledger backfill was modified.

## Environment

- Backend port: production `8001` only queried read-only before fix; validation did not restart or mutate it.
- Frontend port: not used; `PAPER_V2_L3_SKIP_UI=1` because this is backend execution logic.
- TDX port: not used by changed code; existing Paper v2 data-quality gate read DB readiness.
- Conda/env: Windows Python environment used by `python -m pytest` and `python -m nox`.
- Database: production DB read by `paper_v2_data_quality`; no DDL or data repair executed.
- Browser/headless: skipped by scope.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking P0/P1 guardrail for this change | `python -m nox -s l0` -> success; guardrail blocking=0, baseline/non-scope findings only | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `python -m nox -s paper_v2_backend` -> 605 passed, 1 skipped, 2 xfailed | PASS |
| MiniQMT preflight retry | PREFLIGHT_FAILED no-broker run stays retryable and can resubmit after cash is available | targeted scheduler regression -> 58-test slice passed | PASS |
| MiniQMT rebalance cash | Same-batch SELL proceeds unblock covered BUY rebalance and submit SELL before BUY | targeted qmt order-service regression -> 58-test slice passed; neighboring MiniQMT/QMT/simulation tests -> 63 passed | PASS |
| UI E2E | No UI change; backend L3 run skips browser by explicit scope | `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` | SKIPPED_BY_SCOPE |
| Asset safety | No protected asset modified silently | `git status --short`, changed files limited to allowed BUG-283 code/tests/registry/evidence | PASS |

## Commands

```bash
python -m ruff check backend/services/qmt_strategy_ledger/order_service.py backend/services/simulation_runtime/scheduler.py backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py
python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q
python -m pytest backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_sync_service.py backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_target_rebalance_shared.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py -q
python -m nox -s l0
python -m nox -s paper_v2_backend
python -m nox -s validation_module_registry_l0
PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3
```

## Evidence

- API calls: pre-fix read-only runtime check on 2026-06-08 showed two `minqmt_sim` runs `SUCCEEDED` while `qmt_batch_status=PREFLIGHT_FAILED`, `broker_called=false`, `submitted_intents=0`, `failed_intents=54/16`; `/api/v1/qmt/orders` and `/api/v1/qmt/trades` were empty.
- DB checks: `paper_v2_data_quality` passed schema, dataset audit, strategy package readiness, selection traceability, and Paper v2 run traceability; legacy ledger consistency warning remained non-blocking without `--strict-history`.
- Log files: no production service logs modified; no restart performed.
- Playwright report/trace: not applicable.
- Screenshots: not applicable.
- Business output summary: failed MiniQMT preflight now remains `FAILED_RETRYABLE`/`BROKER_PRECHECK_FAILED`, skips reconcile-success overwrite, can resubmit persisted plan after the no-side-effect preflight blocker is removed, and covered rebalance batches submit SELL before BUY.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| MiniQMT `PREFLIGHT_FAILED` was reported as `SUCCEEDED` | scheduler reconciled after a no-broker preflight failure and let reconciliation overwrite submit failure | skip reconcile-success path for no-side-effect failed batches; gate reconciliation success on successful MiniQMT submit batch | targeted scheduler regression; `paper_v2_l3` PASS |
| Existing failed batch could not retry | deterministic batch cache returned old `PREFLIGHT_FAILED` | do not reuse cached `PREFLIGHT_FAILED` batches; recompute preflight on retry | targeted qmt order-service regression PASS; neighboring regression 63 passed |
| Full rebalance blocked when virtual cash was low | batch buy preflight ignored same-batch sell proceeds and evaluated buys before the rebalance funding leg | submit SELL before BUY and include same-batch estimated SELL proceeds in aggregate buy cash checks | targeted qmt order-service regression PASS; neighboring regression 63 passed |

## Result

- Final status: PASS for BUG-283 backend/Paper v2 MiniQMT execution logic validation.
- Remaining risks: live production runtime still needs user-performed backend restart before the patched code can affect port `8001`; same-day real MiniQMT broker execution must be rechecked after restart/trading window.
- Need production backend restart: yes, but user-owned only; Codex did not restart services.
- Need dev service restart: no.
