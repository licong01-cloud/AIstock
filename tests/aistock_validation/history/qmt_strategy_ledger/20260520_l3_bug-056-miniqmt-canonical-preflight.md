# BUG-056 MiniQMT canonical readiness and preflight contract

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Git base: origin/main fd3137b
- Fix branch: bug/BUG-056-miniqmt-canonical-preflight
- Worktree: F:\Dev\AIstock_worktrees\bug-056-miniqmt-canonical-preflight
- Operator: codex-app
- Linked bug: BUG-056 / GitHub #59

## Scope

- Changed files: backend/services/qmt_strategy_ledger/order_service.py; backend/tests/qmt_strategy_ledger/test_order_service_preflight.py; backend/tests/qmt_strategy_ledger/test_selection_order_builder.py; backend/tests/selection_center/test_runtime_selection.py; docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md; BUG-056 JSON; this validation record.
- Impacted flows: MiniQMT managed order preflight JSON contract, SelectionOrderBuilder-to-managed-order board-lot compatibility, Selection Center pre-open data readiness audit, MiniQMT strategy execution design documentation.
- Business goal: keep MiniQMT execution safety gates but assign each business rule to one canonical owner so the same order cannot be accepted by the builder and rejected by a downstream simplified rule.
- Out of scope: removing broker authority checks, weakening StrategyPackage asset preflight, raw QMT diagnostic endpoint behavior, production backend restart, production DB writes, real MiniQMT submit/cancel.
- Protected assets reviewed: no production backend 8001/3000 touched, no production DB mutation, no real MiniQMT submit/cancel, no StrategyPackage model/factor artifact changes.

## Canonical Contract Evidence

The design record is in `docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md` section `10.5 MiniQMT canonical readiness / preflight contract`.

Canonical owners:

| Gate | Owner | Operator/API result |
|---|---|---|
| StrategyPackage package / manifest / runnable assets | `SelectionPackageHealthService.require_runnable()` and live inference resolver | `STRATEGY_PACKAGE_VALIDATION_ERROR` or `DATA_UNAVAILABLE` with package/artifact provenance |
| Pre-open market data | `SelectionCenterService._require_data_ready()` and Paper v2 live-session readiness | `DATA_UNAVAILABLE` naming dataset/trade date; `daily_basic` is not a pre-open MiniQMT gate |
| Suspension/tradability | Selection Center `TradabilityFilter` backed by `market.suspend_d` | excluded rows such as `suspended_by_suspend_d`; only all-filtered case blocks selection |
| Board-lot | `backend.execution_algos.board_lot` | `BUY_BOARD_LOT` for manual invalid BUY, builder residual skip reasons for generated orders |
| Virtual cash | `QmtManagedOrderService.preview_order()` and batch aggregate preflight | `INSUFFICIENT_CASH` / `BATCH_INSUFFICIENT_CASH` |
| Strategy T+1 lots | `effective_strategy_available_sell_quantity()` as used by builder and order service | `INSUFFICIENT_STRATEGY_AVAILABLE_LOT` or builder skip reason |
| Broker account can_sell | `QmtManagedOrderService.submit_order()` / `submit_batch()` broker boundary | `INSUFFICIENT_BROKER_CAN_SELL` / `BATCH_INSUFFICIENT_BROKER_CAN_SELL` |
| Idempotency / duplicate remark | `QmtManagedOrderService.preview_order()` / `submit_batch()` | duplicate remark errors or deterministic existing-batch replay |

Managed-order preflight now exposes `primary_error_code` and `primary_error` alongside full `errors`, so UI/API can show one actionable reason while keeping secondary diagnostics.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Builder/service STAR BUY consistency | `SelectionOrderBuilder` emits 688379.SH quantity 2706 and `QmtManagedOrderService.preview_order()` accepts it | `test_selection_order_builder_star_buy_is_accepted_by_managed_order_preflight` | PASS |
| Main/ChiNext invalid manual BUY | 101 shares still rejects through canonical board-lot helper, not a hard-coded 100-share gate | `test_preview_rejects_main_board_and_chinext_non_100_share_buys` | PASS |
| SELL residual legality | STAR residual below 200 can be submitted when strategy lot and broker can_sell permit it | `test_preview_accepts_sell_residuals_allowed_by_canonical_board_lot` | PASS |
| Single actionable preflight reason | preflight response surfaces one primary blocker and keeps full diagnostics | `test_preview_response_exposes_single_primary_error_for_operator_action` | PASS |
| No hard-coded MiniQMT 100-share gate | managed MiniQMT order layers do not contain `quantity % 100` style rules | `test_miniqmt_preflight_does_not_reintroduce_hard_coded_100_share_lot_gate` | PASS |
| Pre-open data gate | Selection Center uses `suspend_d` and explicit `stk_limit`; does not require `daily_basic` for MiniQMT pre-open readiness | `test_selection_center_preopen_readiness_does_not_require_daily_basic_gate` | PASS |
| Module regression | full qmt_strategy_ledger suite remains green | `python -m pytest backend/tests/qmt_strategy_ledger -q` -> 81 passed | PASS |
| Validation guardrails | repository nox gates pass; staged changed-file guard rerun is required after staging | nox validation_module_registry_l0, validation_catalog_integrity, guardrail_changed_files, l0 | PASS |

## Commands

```bash
python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/selection_center/test_runtime_selection.py -q
python -m pytest backend/tests/qmt_strategy_ledger -q
python -m compileall backend/services/qmt_strategy_ledger/order_service.py backend/services/qmt_strategy_ledger/selection_order_builder.py backend/execution_algos/board_lot.py backend/services/selection_center/service.py
rg -n "quantity\s*%\s*100|daily_basic" backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/routers/qmt.py backend/services/selection_center backend/services/strategy_package backend/execution_algos/board_lot.py -S
git diff --check
git diff --cached --check
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_module_registry_l0
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s validation_catalog_integrity
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s guardrail_changed_files
C:\Users\lc999\miniconda3\envs\AIstock\python.exe -m nox -s l0
```

## Evidence

- Targeted regression tests: 68 passed after fixing the test artifact hash to match runtime config.
- Full qmt_strategy_ledger suite: 81 passed.
- Compile checks: touched MiniQMT/Selection Center service modules compiled successfully.
- Static scan: no `quantity % 100` MiniQMT managed-order gate remains; `daily_basic` hits are documentation/asset code snippets, not active pre-open readiness gates.
- `git diff --check`: passed.
- `validation_module_registry_l0`: 8 passed; ownership scan mapped 12/12 files.
- `validation_catalog_integrity`: passed with 0 findings.
- `guardrail_changed_files`: passed after staging with files=7, mapped=7, findings=0, blocking=0.
- `l0`: successful; existing raw JSON / fallback baseline findings remained blocking=0.
- Design evidence: `docs/architecture/miniqmt_multi_strategy_execution_implementation_plan_20260518.md` now lists each gate owner, input, output/API code, blocking semantics, and downstream rule.
- Business oracle: the old BUG-048 class cannot recur in MiniQMT managed order layers without failing the new scan guard.
- Business oracle: `daily_basic` remains available for feature construction where applicable, but it is not treated as a same-day pre-open MiniQMT execution readiness condition.
- Broker evidence: fake clients / service objects only; no live broker action.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Architecture allowed split-brain gate ownership | design did not record canonical gate owner and downstream invariant boundary | Added the canonical readiness/preflight contract table and regression tests for board-lot, pre-open data, primary error, and guard scan | targeted tests 68 passed |
| Operator could receive only a raw list of preflight errors | preflight result had no stable primary blocker field | Added `primary_error` / `primary_error_code` to managed-order preflight JSON | primary-error regression passes |
| Pre-open data gate could regress to daily_basic | no direct Selection Center test asserted daily_basic exclusion from MiniQMT readiness | Added `RecordingRefreshAudit` test proving calls are `suspend_d`, `stk_limit` | targeted tests pass |

## Result

- Current status: PASS for targeted regressions, full qmt_strategy_ledger module suite, compile checks, diff hygiene, staged changed-file guardrail, and repository nox gates.
- Remaining risks: real MiniQMT behavior is not exercised; production effect requires backend restart after user approval; Paper v2 runtime-contract issues are tracked separately by BUG-065/066/069/071/072/074.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no.
- Need MiniQMT broker action: no during validation; fake clients only.
- Production impact during validation: none.
