# Simulation Runtime ?????????????2026-05-21?

- ???`feature/sim-remediation-impl-20260521`
- ????`F:\Dev\AIstock_worktrees\sim-remediation-impl-20260521`
- ???`e77b3ce feat(paper-v2): persist execution plans and add broker bridges`?`3dd234a feat(paper-v2): add unified simulation lifecycle orchestration`?`e088c80 test(paper-v2): record simulation runtime validation evidence`??????????`feat(paper-v2): add simulation runtime ops console`
- ?????`docs/architecture/simulation_remediation_project_design_20260521.md`
- ???????? `8001` / `3000`?????? MiniQMT??????/?? broker ???`paper_v2_data_quality` ?????????
- DDL ???????? `paper_v2.execution_plan`?`paper_v2.simulation_daily_run` DDL ? schema bootstrap????? DB ??????? `main` ???? `production_ddl_gate=pending-until-applied` ???

## ?????

1. `ExecutionPlan` ?????? `paper_v2.execution_plan`?DB/in-memory repository?hash idempotency?schema comments?
2. Broker bridge??? `LocalSimExecutionBridge`?`MiniQMTExecutionBridge`?MiniQMT ??? `minqmt_sim` binding ? `SIM` mode??? `strategy_name` / `order_remark_prefix` / `intent_id` / `plan_id` ????
3. ????????? `SimulationDailyRun`?`SimulationLifecycleOrchestrator`?? release/binding/evidence/signal/target/rebalance/execution plan ?? broker-neutral run??? batch build?no-rebalance ?????run/plan idempotency?
4. ????????`RebalanceIntentService` ???????????????? no-trade/no-rebalance ??? intent execution plan?
5. MiniQMT ??????? `/api/v1/qmt/virtual-strategies/execution-plans/{plan_id}/orders/preview`???? `ExecutionPlan` ???????? `/package-bindings/{binding_id}/orders/preview` ?? fail-fast?
6. ??????? `StrategyPerformanceProjectionService`?????????? PnL?overlap symbol ???MiniQMT broker ??????? lot ?????
7. ??????? `SimulationLifecycleScheduler`?? binding/release/backend/approval/?????? `SimulationReleaseBinding`????? StrategyRuntimeRelease -> DailySelectionEvidence -> ExecutionPlan?
8. ?????scheduler ?? `submit=False`??? context provider fail-fast??? LocalSim/MiniQMT broker service ????????????????????????
9. ????????? `SimulationDailyRun` ?? `execution_plan_id`?scheduler ????? plan?????????? submit?
10. ??????? `simulation_core_l2`?`localsim_unattended_l3`?`miniqmt_sim_stub_l3`?`simulation_dual_backend_l4`?`miniqmt_sim_trading_hours_l5` nox/catalog ???L5 ??????????????????
11. ???? API??? `/api/v1/simulation-runtime/scheduler/status`?`/runs`?`/runs/{run_id}`?`/execution-plans/{plan_id}`???? scheduler/run/evidence/plan ?????? tick???? sync?
12. ???? UI??? `/paper-v2/simulation-runtime`??? scheduler ?????LocalSim/MiniQMT run ???release/binding/evidence/plan ????? run ???????????tick?submit ????????
13. ????????? `simulation_runtime_ops_ui` nox/catalog/runner allowlist?mock-first Playwright ?? UI ?????????? `8001`/`3000` ????

## ????

| ?? | ?? | ?? |
|---|---:|---|
| `python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_tail_policy.py backend/tests/simulation_runtime/test_ops_api.py backend/tests/simulation_runtime/test_strategy_performance_projection.py -q -p no:cacheprovider` | `19 passed` | ???? LocalSim ??????MiniQMT ????????tail policy?ops API ?????? |
| `python -m pytest backend/tests/simulation_runtime -q -p no:cacheprovider` | `43 passed` | simulation runtime ????? |
| `python -m nox -s simulation_core_l2` | `43 passed` | L2 ??????? |
| `python -m nox -s localsim_unattended_l3` | `19 passed` | LocalSim ?????? |
| `python -m nox -s miniqmt_sim_stub_l3` | `32 passed` | MiniQMT fake broker / sync / reconcile ??? |
| `python -m nox -s simulation_dual_backend_l4` | `65 passed` | ? backend ?? oracle? |
| `python -m nox -s simulation_runtime_ops_ui` | `1 passed` | ???? UI Playwright? |
| `npm exec tsc -- --noEmit --incremental false`?`frontend/`? | PASS | ????????? |
| `npm run build`?`frontend/`? | PASS | ????????????? React Hook warnings? |
| `python scripts/aistock_guardrail_scan.py --changed-only --baseline-json tests/aistock_validation/guardrails_baseline_20260511.json --fail-new-only --fail-on-severity P1` | PASS | `files=26, findings=0, blocking=0`? |
| `python scripts/aistock_module_ownership_scan.py --staged-only --fail-on-unmapped --fail-on-ambiguous` | PASS | `files=18, mapped=18, unmapped=0, ambiguous=0`? |

## ??????

| ID | ?? | ???? | ???? | ???? |
|---|---|---|---|---|
| A-01 | PASS | `StrategyRuntimeReleaseService`?release/binding forbidden-key validators | `test_strategy_runtime_release.py` | ? |
| A-02 | PASS | `StrategyRuntimeRelease` + `SimulationReleaseBinding` immutable hash | `test_strategy_runtime_release_hash_changes_only_for_policy_changes_not_binding_changes` | ? |
| A-03 | PASS | `DailySelectionEvidence` repository + shared selection service | `test_strategy_package_selection_service.py` | ? |
| A-04 | PASS | readiness/selection/simulation runtime grep ??? `daily_basic` ??????`day_features.py` ?????? | grep + `paper_v2_data_quality` | ? |
| A-05 | PASS | `TargetPositionService` broker-neutral target build | `test_target_and_rebalance_services_are_shared_for_localsim_and_miniqmt` | ? |
| A-06 | PASS | `RebalanceIntentService` ? target/???? SELL | `test_empty_daily_signal_sells_dropped_positions_and_no_trade_is_legal` | ? |
| A-07 | PASS | `ExecutionPlanCompiler` + `paper_v2.execution_plan` + qmt execution-plan preview endpoint | simulation runtime tests + qmt router test | ? |
| A-08 | PASS | ? SelectionOrderBuilder endpoint ?? fail-fast??? official execution-plan endpoint | `test_package_binding_order_preview_fails_fast_until_minqmt_execution_bridge_exists`?`test_execution_plan_order_preview_uses_shared_miniqmt_bridge` | ? |
| A-09 | PASS | `TradingRuleService` ?? `backend.execution_algos.board_lot`?MiniQMT service ????? board-lot source | `test_trading_rule_service_uses_single_a_share_board_lot_source` | ? |
| A-10 | PASS | zero-intent `ExecutionPlan` + lifecycle no-rebalance success | `test_lifecycle_no_rebalance_does_not_call_broker_and_marks_success` | ? |
| A-11 | PASS | `SimulationLifecycleScheduler` ?? LocalSim binding ?? plan/submit?binding-level isolation ?? | `test_scheduler_runs_two_localsim_strategies_with_independent_state_and_restart_idempotency`?`localsim_unattended_l3` | ? |
| A-12 | PARTIAL | `MiniQMTExecutionBridge` + qmt virtual ledger managed orders + scheduler fake broker submit | `test_scheduler_submits_miniqmt_fake_broker_batch_and_reuses_after_restart`?`test_scheduler_miniqmt_two_strategies_same_stock_keep_strategy_lots_and_merged_reconcile`?`miniqmt_sim_stub_l3` | ?????? L5 ????? |
| A-13 | PASS | `StrategyPerformanceProjectionService.overlap_symbols`?per-strategy PnL projection | `test_strategy_performance_projection_keeps_same_stock_strategy_pnl_independent` | ? |
| A-14 | PASS | `reconcile_merged_positions` + qmt reconciliation suite | performance projection tests + qmt reconciliation tests | ? |
| A-15 | PASS | QMT managed-order preflight/batch/cancel existing coverage?lifecycle submit status persists retryable failure | qmt_strategy_ledger suite | ? |
| A-16 | PASS | `tail_policy_version_id/hash` ? release/plan/run ????????? cancel-unfilled-at-close ??? | `test_tail_policy.py` | ? |
| A-17 | PASS | deterministic run id?plan hash?qmt batch idempotency?scheduler ???? `execution_plan_id` ??? plan?????????? submit | lifecycle idempotency test?restart sync/reconcile tests | ? |
| A-18 | PASS | `SimulationLifecycleScheduler` + `SimulationLifecycleBackgroundScheduler` expose unattended lifecycle windows, persisted restart recovery mode, and opt-in background orchestration while keeping default submit disabled | `test_lifecycle_scheduler.py`?`simulation_core_l2`?`localsim_unattended_l3`?`miniqmt_sim_stub_l3`?`simulation_dual_backend_l4`?`simulation_runtime_ops_ui` | background scheduler remains opt-in; full-day soak still a follow-up gate |
| A-19 | PASS | `SimulationRuntimeOpsService`?`backend/routers/simulation_runtime.py`?`frontend/src/app/paper-v2/simulation-runtime/page.tsx` | `backend/tests/simulation_runtime/test_ops_api.py`?`frontend/tests/paper-v2/simulation-runtime-ops.spec.ts`?`simulation_runtime_ops_ui`?`npm run build` | ? |
| A-20 | PARTIAL | ?? `simulation_core_l2`?`localsim_unattended_l3`?`miniqmt_sim_stub_l3`?`simulation_dual_backend_l4`?`simulation_runtime_ops_ui`?`miniqmt_sim_trading_hours_l5` nox/catalog ???L5 ?? skip/manual | ????????catalog integrity tests?ops UI nox | L5 ?? MiniQMT SIM ?????????? |
| A-21 | PASS | `SimulationRuntimeOpsService.build_live_admission_evidence` standardizes Paper v2 / MiniQMT SIM evidence for live approval candidate creation, and `StrategyPackageService.require_live_approval` still fail-fasts on mismatched or unverified evidence | `test_ops_api.py`?`test_live_approval_candidate.py`?`test_live_approval_lifecycle.py` | real MiniQMT L5 evidence still manual when trading hours permit |
| A-22 | PASS | changed-files guardrail blocking=0????? POC/simple/mock-only ???? | guardrail changed-files scan | ? |
| A-23 | PASS | Selection Center delegates shared `StrategyPackageSelectionService`?runtime uses same evidence schema | simulation_runtime + selection_center tests | ? |
| A-24 | PASS | `assert_selection_only_payload_boundary` ?? target/order/broker/cash ?? | selection service negative tests | ? |
| A-25 | PASS | release/binding validators ?? alpha core override | strategy runtime release tests | ? |
| A-26 | PASS | binding hash ??? release hash?broker/account/capital/order remark ?? binding | strategy runtime release tests | ? |

## ??

- ??????? Selection Center?LocalSim?MiniQMT ?????? `DailySelectionEvidence -> ExecutionPlan -> Broker` ???? no-rebalance?tail policy?restart idempotency?shared performance projection??? ops API/UI ????????
- ????????????????????A-12 ??????? L5?A-18 ??? background scheduler/window orchestration?A-20 ??? L5 ???A-21 ? live admission evidence linkage and approval ????????????
- ???????????? `8001` / `3000`????? DB?????? MiniQMT????????? broker ???
