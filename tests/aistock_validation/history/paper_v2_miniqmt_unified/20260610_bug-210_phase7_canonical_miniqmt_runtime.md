# BUG-210 Phase 7 canonical MiniQMT runtime validation

- issue: BUG-210 / GitHub #567
- branch: `bug/BUG-210-paper-v2-miniqmt-unified-single-and-multi-strate-20260606`
- worktree: `F:\Dev\AIstock_worktrees\BUG-210-paper-v2-miniqmt-unified-single-and-multi-strate-20260606`
- date: 2026-06-10
- phase: Phase 7 - L0-L4 non-live validation and legacy-path retirement gate
- design_doc: `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`
- design_sections: 3 / 4 / 5 / 8 / 10.8 / 10.8.4 / 11 / 14
- production_impact: no production MiniQMT broker submission, no production DB write, no DDL, no backend/frontend/TDX/MiniQMT restart.
- L5_status: `pending-live-window`. This record validates non-live L0-L4 and controlled UI/API paths only; real MiniQMT SIM trading-window validation remains separate.

## 1. Objective

Phase 7 verifies that the Phase 0-6 implementation on the BUG-210 branch converges to a single authoritative MiniQMT execution branch:

- Paper v2 MiniQMT, simulation_runtime MiniQMT, and operator-command entries are clients of `MiniQMTExecutionRuntimeClient`.
- `MiniQMTExecutionRuntimeClient` is the only product-path boundary allowed to touch legacy broker or managed-order submission.
- `MiniQMTLiveAlgoAdapter` is no longer a usable product adapter; it remains only as a fail-fast compatibility gate until a post-L5 deletion chore.
- `UnifiedMiniQMTVnpyExecutionAdapter` remains only as a pure trading_core characterization/core-test object; product paths do not instantiate it.
- V25 remains QE / LocalSim / research-backtest semantics only, and is rejected for MiniQMT broker execution.
- SELL-first deterministic retry for deferred dependent BUY must not regress when runtime-owned vn.py child/algo ids are regenerated on retry.

## 2. Design Trace Matrix

| design_item | design_ref | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|---|
| Single MiniQMT product execution owner | 3.1, 8.1, 10.8.4 | `backend/services/miniqmt_execution_runtime/client.py`, `backend/services/simulation_runtime/bridges.py`, `backend/services/paper_trading_v2/day_runner.py` | `backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py`; static grep guard | PASS | legacy fail-fast file remains until L5 chore |
| Alpha/execution isolation | 3.2, 5.1, 10.8 | `backend/services/simulation_runtime/models.py`, `backend/services/simulation_runtime/bridges.py` | `backend/tests/simulation_runtime/test_miniqmt_signal_contract.py` in targeted 105-test run | PASS | none |
| vn.py-derived behavior owned by runtime | 4.1, 4.2, 10.8 Phase 3/4 | `backend/services/miniqmt_execution_runtime/runtime.py`, `backend/services/miniqmt_execution_runtime/client.py`, `backend/services/simulation_runtime/bridges.py` | `backend/tests/miniqmt_execution_runtime`; `backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py` in targeted run | PASS | upstream attribution preserved in runtime metadata |
| N=1 and N>1 use same path | 7.1, 9, 10.8 Phase 4 | `MiniQMTExecutionRuntimeClient`, `MiniQMTExecutionBridge`, Paper v2 MiniQMT wrapper | `test_paper_v2_n1_and_simulation_runtime_n_many_share_runtime_owner_evidence`; `paper_v2_l3` | PASS | none |
| Funds/capacity determined by preflight, not fixed strategy-count gate | 3.3, 10.8 Phase 5 | `backend/services/qmt_strategy_ledger/order_service.py`, `backend/services/paper_trading_v2/broker/minqmtsim.py` | `simulation_core_l2`, `simulation_dual_backend_l4`, `test_order_service_submit_fake_qmt.py` | PASS | legacy exclusive-account capacity=1 remains for old account mode only; account-group path has no fixed cap |
| SELL-first deferred BUY retry | 10.8 Phase 5, 10.8.4 item 5 | `backend/services/qmt_strategy_ledger/order_service.py` | `test_submit_batch_dependent_buy_retry_matches_logical_batch_when_runtime_ids_change`; scheduler deferred-buy test | PASS | none |
| Operator command runtime ownership | 10.8 Phase 6 | `backend/services/miniqmt_execution_runtime/runtime.py`, `client.py`, `backend/routers/simulation_runtime.py`, `frontend/src/app/paper-v2/simulation-runtime/page.tsx` | `paper_v2_l3` includes `simulation-runtime-ops.spec.ts` operator tests | PASS | none |
| Legacy-path retirement gate | 10.8 Phase 7, 11.1 | `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` fail-fast gate; static grep classification | grep guard results below | PASS | deletion deferred until L5 + user confirmation |
| Production gates | 10.8.1 item 7, PROD-DDL-001 | BUG JSON production gates | `finish --plan-only`; this record | PASS | all gates noop; runtime activation after merge still requires user-managed restart |

## 3. Phase 7 Fix

### 3.1 Failure reproduced

`simulation_core_l2` and `simulation_dual_backend_l4` initially failed at:

- `backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_retries_deferred_miniqmt_dependent_buys_without_duplicate_sells`

Observed behavior:

- First tick correctly submitted SELL first and deferred BUY until sell proceeds were reconciled.
- Second tick, after virtual cash was increased, should retry only the deferred BUY.
- Instead, the second tick generated a different `qmt_batch_id` and failed with `BROKER_PRECHECK_FAILED` because runtime-owned vn.py regenerated `runtime_algo_instance_id` / `runtime_child_order_id`; batch identity was accidentally tied to physical runtime child ids instead of logical execution-plan identity.

### 3.2 Fix implemented

- `backend/services/qmt_strategy_ledger/order_service.py`
  - Added logical batch-id matching that keeps order/account/plan/intent/remark identity while removing physical runtime ids: `runtime_algo_instance_id`, `runtime_child_order_id`, and `runtime_parent_intent_id`.
  - When exact batch id does not find a deferred dependent-buy batch, the service locates the historical batch by order remark and verifies logical batch id equality.
  - The retry then uses the stored historical batch requests/results, so only deferred BUY is submitted and SELL is not duplicated.
- `backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py`
  - Added `test_submit_batch_dependent_buy_retry_matches_logical_batch_when_runtime_ids_change` to cover regenerated runtime child/algo ids with stable logical plan/remarks.

## 4. Positive Validation

| command | result |
|---|---|
| `python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py::test_submit_batch_dependent_buy_retry_matches_logical_batch_when_runtime_ids_change backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_retries_deferred_miniqmt_dependent_buys_without_duplicate_sells -q` | `2 passed in 1.13s` |
| `python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py -q` | `12 passed in 0.70s` |
| `python -m nox -s simulation_core_l2` | `106 passed`; session successful |
| `python -m nox -s simulation_dual_backend_l4` | `135 passed`; session successful |
| `python -m pytest backend/tests/miniqmt_execution_runtime backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_target_rebalance_shared.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_trading_day_defaults.py -q` | `105 passed in 7.14s` |
| `python -m nox -s l0` | success; guardrail scanner reported existing/baseline or P2 non-blocking findings; `blocking=0` |
| `python -m nox -s validation_module_registry_l0` | success; `8 passed`; ownership scan `mapped=12 unmapped=0 ambiguous=0` |
| `python -m nox -s paper_v2_l3` | success; `paper_v2_backend: 612 passed, 1 skipped, 2 xfailed`; `paper_v2_data_quality` success with known legacy ledger warning; `data_quality_deep: 10 passed, 21 skipped`; `paper_v2_ui: 20 passed, 1 skipped` |
| `python -m ruff check ...changed files...` | All checks passed |
| `python -m compileall -q backend\routers\paper_trading_v2.py backend\services\miniqmt_execution_runtime backend\services\simulation_runtime backend\services\paper_trading_v2 backend\services\qmt_strategy_ledger backend\routers` | passed |
| `git diff --check` | passed; line-ending warnings only |

`paper_v2_l3` generated the latest L3 record:

- `tests/aistock_validation/history/paper_v2_selection_center/20260610_162409_l3_paper-v2-selection-center-l3-regression.md`

## 5. Static Guard Scan Classification

### 5.1 Legacy adapter / vn.py adapter

Command:

`rg -n "MiniQMTLiveAlgoAdapter|UnifiedMiniQMTVnpyExecutionAdapter|QmtManagedOrderSubmitter|MiniQMTChildOrderRequest" backend/services/paper_trading_v2 backend/services/simulation_runtime backend/services/miniqmt_execution_runtime backend/routers`

Classification:

- `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` only: allowed fail-fast compatibility gate; constructor raises `ExecutionPathNotCanonicalError` and points to `MiniQMTExecutionRuntimeClient`.
- No product path hit for `UnifiedMiniQMTVnpyExecutionAdapter`, `QmtManagedOrderSubmitter`, or `MiniQMTChildOrderRequest` in Paper v2 / simulation_runtime / runtime product entry directories.
- `UnifiedMiniQMTVnpyExecutionAdapter` still exists under `backend/services/trading_core/miniqmt_vnpy_execution.py` for pure core characterization tests; product paths do not instantiate it.

### 5.2 Raw broker / submit boundary

Command:

`rg -n "broker\.submit_order_intent\(|self\._managed_order_service\.submit_batch\(|managed_order_service\.submit_batch\(|XtQuantQMTClient\(|\.place_order\(" backend/services/paper_trading_v2/day_runner.py backend/services/simulation_runtime/bridges.py backend/services/simulation_runtime/lifecycle.py backend/services/miniqmt_execution_runtime/client.py backend/services/paper_trading_v2/broker/minqmtsim.py backend/routers/qmt.py backend/routers/qmt_strategy_ledger.py`

Classification:

- `backend/services/miniqmt_execution_runtime/client.py`: allowed runtime boundary for `managed_order_service.submit_batch(...)` and Paper v2 gateway `broker.submit_order_intent(...)`.
- `backend/services/simulation_runtime/bridges.py`: `broker.submit_order_intent(...)` is LocalSim-only path, not MiniQMT path.
- `backend/services/paper_trading_v2/broker/minqmtsim.py`: broker adapter boundary to MiniQMT client; account-group metadata now requires runtime ownership attribution.
- `backend/routers/qmt.py`: raw/manual QMT admin endpoint; not used as Paper v2 / simulation_runtime product strategy path.
- No direct product call to `XtQuantQMTClient.place_order` from Paper v2 day runner, simulation runtime bridge, or lifecycle.

### 5.3 SelectionOrderBuilder / V25 / strategy-count gates

Command:

`rg -n "SelectionOrderBuilder|AlphaSignalBook|V25_TWO_STAGE|V25_1_SMALL_CAP|max_concurrent_packages|package_count\s*>|strategy_count\s*>" backend/services/miniqmt_execution_runtime backend/services/simulation_runtime backend/services/paper_trading_v2 backend/routers/qmt_strategy_ledger.py backend/routers/qmt.py`

Classification:

- `SelectionOrderBuilder` appears in `backend/routers/qmt_strategy_ledger.py` preview/admin route, and the builder itself fails fast with `SelectionOrderBuilder direct broker-order generation is disabled`; not a product broker execution path.
- `AlphaSignalBook` appears in simulation_runtime DTO/model and export only; forbidden field tests are included in the targeted 105-test run.
- `V25_*` references in `backend/services/simulation_runtime/bridges.py` are explicit rejection gates for MiniQMT broker execution; references in Paper v2 live/day services are existing QE/LocalSim/realtime feature guards, not MiniQMT broker submit acceptance.
- `max_concurrent_packages=1_000_000_000` in `MiniQMTSimBackend.bind_capacity()` is the account-group sentinel meaning no fixed package cap; legacy exclusive-account remains `1` for compatibility, not the future account-group execution path.
- `backend/services/paper_trading_v2/broker/localsim.py` capacity is LocalSim-only and outside MiniQMT product path.

## 6. Negative Validation

- V25 rejection: `backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py` included in 105-test targeted run.
- Non-canonical MiniQMT path rejection: `backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py` included in 105-test targeted run.
- Alpha signal forbidden fields: `backend/tests/simulation_runtime/test_miniqmt_signal_contract.py` included in 105-test targeted run.
- Legacy `MiniQMTLiveAlgoAdapter` instantiation fails fast: covered by `backend/tests/miniqmt_execution_runtime/test_miniqmt_single_multi_same_runtime.py` included in targeted run.
- Deferred BUY retry with runtime physical ids changed: covered by the new qmt_strategy_ledger regression test and scheduler L2/L4 tests.

## 7. DESIGN-COMPLIANCE-001

| check | status | evidence |
|---|---|---|
| No simplified/POC/placeholder delivery claim | PASS | This record only claims Phase 7 non-live L0-L4 and path convergence; L5 is `pending-live-window` |
| Item-by-item design mapping | PASS | Section 2 maps owner, alpha isolation, vn.py, N=1/N>1, funds/retry, operator, legacy, and production gates |
| Real UI/API/DB behavior verified | PASS | `paper_v2_l3` used temporary backend/frontend ports 8012/3012; backend/API/UI E2E passed; production ports untouched |
| Mock-only not used as business completion | PASS | L2 fake broker, L3 UI/API, and L4 dual backend/restart paths passed; L5 is explicitly not claimed |
| No silent fallback / fake success | PASS | V25 rejection, legacy adapter fail-fast, deferred BUY retry, readiness PIT cutoff, and minute-data default gate have regression coverage |
| Production gates explicit | PASS | `production_ddl_gate=noop`; frontend/backend dependency gates `noop`; runtime activation requires user restart after merge |
| Residual gaps explicit | PASS | L5 real MiniQMT SIM trading-window validation is pending; legacy deletion deferred until L5 + user confirmation |

## 8. Production Gates And Runtime Impact

- `production_ddl_gate`: `noop`. No SQL migration or DB schema change.
- `production_frontend_dependency_gate`: `noop`. No `frontend/package.json` or lockfile change.
- `production_backend_dependency_gate`: `noop`. No Python/Conda dependency change.
- Production services: no backend/frontend/TDX/MiniQMT restart was executed.
- Production DB: no production DB write and no DDL execution. `paper_v2_l3` data-quality smoke is a read/check validation of configured data health.
- Validation services: `paper_v2_l3` started temporary validation backend `8012` and frontend `3012`; nox cleaned the validation frontend port afterward; production ports `8001/3000` were not touched.

## 9. Post-Rebase Validation Refresh

After rebasing the branch onto `origin/main` at `faacf5e7`, Phase 7 non-live validation was rerun from commit `7b273cf2`:

| command | result |
|---|---|
| `python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py::test_submit_batch_dependent_buy_retry_matches_logical_batch_when_runtime_ids_change backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_retries_deferred_miniqmt_dependent_buys_without_duplicate_sells -q` | `2 passed in 1.68s` |
| `python -m pytest backend/tests/miniqmt_execution_runtime backend/tests/simulation_runtime/test_miniqmt_path_uniqueness.py backend/tests/simulation_runtime/test_miniqmt_rejects_v25_broker_execution.py backend/tests/simulation_runtime/test_miniqmt_signal_contract.py backend/tests/simulation_runtime/test_target_rebalance_shared.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/paper_trading_v2/test_trading_day_defaults.py -q` | `105 passed in 10.55s` |
| `python -m ruff check <changed files>` | All checks passed |
| `python -m compileall -q <changed Python paths>` | passed |
| `python -m nox -s l0` | success; guardrail scanner `blocking=0` |
| `python -m nox -s validation_module_registry_l0` | success; `8 passed`; ownership scan `mapped=12 unmapped=0 ambiguous=0` |
| `python -m nox -s simulation_core_l2` | success; `106 passed` |
| `python -m nox -s simulation_dual_backend_l4` | success; `135 passed` |
| `python -m nox -s paper_v2_l3` | success; `paper_v2_backend: 612 passed, 1 skipped, 2 xfailed`; `paper_v2_data_quality` success with known legacy ledger warning; `data_quality_deep: 10 passed, 21 skipped`; `paper_v2_ui: 20 passed, 1 skipped` |

Latest L3 run record:

- `tests/aistock_validation/history/paper_v2_selection_center/20260610_165139_l3_paper-v2-selection-center-l3-regression.md`

This refresh does not change the L5 boundary: real MiniQMT SIM trading-window validation remains `pending-live-window`; production runtime was not restarted and no production DB/DDL was touched.

## 9. Remaining Before Real Trading Validation

- Code-path state: this branch now satisfies the non-live Phase 7 requirement that Paper v2 / simulation_runtime / operator command enter MiniQMT through `MiniQMTExecutionRuntimeClient` as the runtime owner.
- L5: real MiniQMT SIM unattended validation still requires a trading window, MiniQMT SIM login, and TDX current-day minute bars. Do not declare production trading ready before that run.
- Legacy deletion: `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` remains as a fail-fast compatibility gate. Delete it only in a separate chore after L5 passes and the user confirms.
- Runtime activation: after merge, backend/frontend must be restarted by the user-managed production process; this task does not restart services.
