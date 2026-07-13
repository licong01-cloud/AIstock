# BUG-522 Analysis - MiniQMT event_loop shadow runner production SIM wiring

## Root Cause

- `MiniQMTShadowEventLoopAdapter`, `MiniQMTShadowCompilerAdapter`, `MiniQMTShadowParallelRunner`, and `MiniQMTShadowReconciler` were defined and covered by unit tests, but the unattended `simulation_runtime` MiniQMT SIM scheduler never instantiated them.
- The production SIM path went from `SimulationLifecycleScheduler` planning directly to B submit (`MiniQMTExecutionBridge.submit_plan()` / `submit_managed_vnpy_order_requests()`), so no real MiniQMT SIM run could create durable `SHADOW_RECONCILIATION_REPORTED` evidence.
- `MiniQMTGraySwitchController.switch_to_event_loop(require_shadow_evidence=True)` therefore had no evidence source for a scoped canary switch.

## Implemented Wiring

- Added `MiniQMTExecutionBridge.run_shadow_reconciliation()` in `backend/services/simulation_runtime/bridges.py`.
- It reuses the same `_build_vnpy_runtime_submission_kwargs()` input source as B submit: the same `ExecutionPlan`, binding, policy snapshot, managed request factory, and quote/price provider.
- It constructs `MiniQMTShadowInputEvent` objects from parent intents, policy, tick quote/tradability payloads, and runs `MiniQMTShadowParallelRunner` with event-loop and compiler shadow adapters against the same runtime repository.
- It records metadata needed by later canary gates: `portfolio_id`, `strategy_slot_id`, `binding_id`, `run_id`, `trade_date`, `execution_plan_id`, and `account_group_id`.

## Scheduler Hook

- Added `MINIQMT_SHADOW_ENABLED` to `SimulationLifecycleScheduler.diagnostics()` with `default=false`.
- Hooked `_run_miniqmt_shadow_reconciliation_before_submit()` immediately before B submit for fresh plans, rebuilt plans, and persisted-plan restart paths.
- Activation is limited to `MINIQMT_SHADOW_ENABLED=true`, `broker_backend=minqmt_sim`, `mode=SIM`, and non-empty plans.
- When disabled, the hook returns the original run and does not call the shadow bridge.

## Safety and Error Semantics

- Shadow is observation-only: A never calls or mutates broker state; adapters report `broker_called=false` and `broker_mutated=false`.
- B remains broker-authoritative. Shadow success or failure does not change B submit gates, BUG-499 quote behavior, BUG-501/500/502 risk gates, or capacity-residual semantics.
- Shadow failure is loud and durable: run payload gets `miniqmt_shadow_reconciliation.status=FAILED_OBSERVATION_ONLY`, a concrete `reason_code`, context preview, and a `simulation_alert`; B submit continues.
- The implementation intentionally does not create new DDL or production DB writes. Runtime evidence continues to use the configured MiniQMT execution runtime repository.

## Acceptance Evidence Added

- `test_scheduler_miniqmt_shadow_remains_inert_when_disabled_by_default`: default flag off means no shadow payload/event.
- `test_scheduler_miniqmt_shadow_persists_durable_evidence_without_touching_broker`: flag on in SIM produces durable `SHADOW_RECONCILIATION_REPORTED` with required metadata, while B still submits.
- `test_scheduler_miniqmt_shadow_failure_is_loud_and_keeps_b_submit_running`: failure writes loud payload/alert and does not stop B.
- `test_scheduler_miniqmt_shadow_does_not_activate_for_local_sim_bindings`: LocalSim is not touched by this MiniQMT hook.
- `test_phase5_shadow_parallel_runner_replays_design_scenarios_through_a_and_b_adapters`: shadow report metadata now includes the canary scope fields and both sides report no broker calls/mutations.

## Remaining Work Outside BUG-522

- D2: read-only shadow evidence API/CLI for operators.
- D3: strengthen gray canary evidence requirements if the rollout requires multiple days or scenario coverage.
- D4: actual SIM canary switch to event_loop after scoped shadow evidence exists.
