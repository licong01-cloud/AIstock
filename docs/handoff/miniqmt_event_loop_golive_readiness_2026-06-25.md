# MiniQMT event_loop go-live readiness diagnosis (2026-06-25)

## 0. Boundary and verdict

- Diagnosis mode was read-only: no code, service, production DB, or DDL mutation happened during the readiness check.
- This report is kept under `docs/handoff/` and attached to BUG-522 so it does not pollute the project root.
- Target: move A (`durable event_loop`, vn.py-style real execution) toward SIM canary. B (`compiler`) remains the current SIM authority and is not used for return validation.
- Verdict before BUG-522: **shadow was not wired into the production unattended SIM scheduler**, so `require_shadow_evidence=True` had no evidence source and the first event_loop canary was NO-GO.

## 1. Main-chain state before BUG-522

- Unattended SIM chain: `SimulationRuntimeScheduler.run_once()` -> `SimulationLifecycleScheduler.run_once()` -> `_run_binding()` -> `orchestrator.submit_execution_plan()` -> `MiniQMTExecutionBridge.submit_plan()` -> `MiniQMTExecutionRuntimeClient.submit_managed_vnpy_order_requests()`.
- The B submit path builds vn.py-style managed requests from `ExecutionPlan` and `SimulationRunContext`; it remains the only broker-authoritative submit path before canary.
- `MiniQMTShadowEventLoopAdapter`, `MiniQMTShadowCompilerAdapter`, `MiniQMTShadowParallelRunner`, and `MiniQMTShadowReconciler` existed, but production scheduler/lifecycle/bridge did not instantiate them.
- Existing runtime-store scans before this task showed zero `SHADOW_RECONCILIATION_REPORTED` records.

## 2. Canary hard gate observed before BUG-522

- `MiniQMTGraySwitchController.switch_to_event_loop(... require_shadow_evidence=True)` accepts a SIM switch only when the target runtime metadata contains `last_shadow_reconciliation` for the same `portfolio_id` and `strategy_slot_id`.
- It rejects when evidence is missing, scope mismatched, fatal, non-SIM, or when active child orders/algo instances remain in-flight.
- Quantified gate in current code: one latest same-scope no-fatal shadow report is sufficient; it does not yet require N trading days or a full scenario matrix.
- Design risk: that gate is weaker than the broader rollout design; D3 should later strengthen it before broad canary expansion.

## 3. Event-loop authenticity and safety observations

- A/event_loop is not a simplified shell: runtime/gateway expose callback-style order, trade, tick, account, disconnect, reconcile, and restart-recovery paths.
- A must not be made equivalent to B's submit-time synthetic tick/timer compiler path. That would violate the event_loop rollout objective.
- Shadow mode is safe only when it uses dry-run gateways and reports `broker_called=false` and `broker_mutated=false`.
- JSON runtime store is acceptable for shadow evidence/restart debug at this stage, but it must not become the authoritative OMS for event_loop live execution.

## 4. Route to first SIM canary

1. **D1 development**: wire a default-inert shadow runner into the MiniQMT SIM scheduler at the same-source input point as B submit. Hard gate: `MINIQMT_SHADOW_ENABLED=false` has zero behavior change; `true` produces durable no-broker-mutation evidence.
2. **D2 development**: add read-only shadow/gray evidence API or CLI so operators do not have to parse the JSON store manually.
3. **D3 development/configuration**: strengthen or explicitly approve the canary gate (scope match, no fatal drift, no in-flight, SIM mode, minimum days/scenarios if required).
4. **D4 development**: switch exactly one SIM strategy slot to event_loop using the gray controller; keep B rollback ready.
5. **Observation**: monitor order/trade/tick/reconcile/EOD/open-like states and rollback immediately on fatal drift or broker/OMS ambiguity.

## 5. Forbidden shortcuts

- Do not globally set `MINIQMT_EXECUTION_RUNTIME=event_loop` and assume the unattended scheduler becomes A; current B managed lifecycle would loud-reject instead of becoming real event_loop execution.
- Do not bypass `require_shadow_evidence` by directly writing gray overrides.
- Do not modify A into a submit-time one-shot compiler path.
- Do not let shadow touch the broker.
- Do not introduce TDX into MiniQMT/event_loop quote flow.
