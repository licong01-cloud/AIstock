# Simulation Runtime Shared Decision Core Validation - 2026-05-21

## Scope

- Branch: `feature/sim-remediation-impl-20260521`
- Slice: Phase 1 shared decision core after shared selection evidence
- Files under validation:
  - `backend/services/simulation_runtime/decision.py`
  - `backend/services/simulation_runtime/models.py`
  - `backend/services/simulation_runtime/__init__.py`
  - `backend/tests/simulation_runtime/test_target_rebalance_shared.py`

## Design Mapping

| Design item | Implementation | Evidence | Status |
|---|---|---|---|
| A-05 same signal generates consistent target positions | `TargetPositionService` wraps the existing strict `TargetPositionEngine` and binds it to `DailySelectionEvidence` plus `StrategyRuntimeRelease` | `test_target_and_rebalance_services_are_shared_for_localsim_and_miniqmt` | PASS |
| A-06 dropped stocks generate SELL intent | `RebalanceIntentService` diffs current positions and targets; symbols missing from targets emit `DROPPED_FROM_SELECTION` SELL intents | `test_target_and_rebalance_services_are_shared_for_localsim_and_miniqmt` checks `000003.SZ` SELL 77 | PASS |
| A-07 order intent comes from shared execution plan | `ExecutionPlanCompiler` requires shared `OrderIntent` objects and links release, binding, evidence and trading-rule decisions | `test_execution_plan_compiler_links_release_binding_evidence_and_rule_decisions` | PASS |
| A-09 unified trading-rule service | `TradingRuleService` is the authoritative service for board-lot decisions in this shared path and uses `backend.execution_algos.board_lot` | `test_trading_rule_service_uses_single_a_share_board_lot_source` | PASS |
| No simplified/POC delivery claim | This slice only claims Phase 1 shared target/rebalance/rule/plan foundation; LocalSim unattended and MiniQMT bridge remain later phases | This record and final report list residual gaps | PASS |

## Commands

```powershell
python -m pytest backend/tests/simulation_runtime/test_target_rebalance_shared.py -q -p no:cacheprovider
```

Result: `4 passed`

```powershell
python -m pytest backend/tests/simulation_runtime -q -p no:cacheprovider
```

Result: `19 passed`

```powershell
python -m compileall backend/services/simulation_runtime backend/services/selection_center backend/services/strategy_package -q
python -m pytest backend/tests/simulation_runtime backend/tests/selection_center/test_runtime_selection.py backend/tests/selection_center/test_live_inference_preflight_wiring.py backend/tests/strategy_package/test_manifest_alpha_core_boundary.py backend/tests/trading_core/test_v25_1_small_cap_contract.py -q -p no:cacheprovider
```

Result: `94 passed`

```powershell
python -m pytest backend/tests/simulation_runtime backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
```

Result: `574 passed, 1 skipped, 2 xfailed`

```powershell
python -m nox -s l0
```

Result: PASS. Guardrail scan reported existing/baseline and non-blocking findings only; blocking count was `0`.

## Business Outcomes

- Same `DailySelectionEvidence` and `SignalSnapshot` can feed LocalSim and MiniQMT target/rebalance services without broker-specific target generation.
- Dropped current holding `000003.SZ` produces a SELL intent, covering the previous "only buy, no sell" failure class in the shared decision layer.
- STAR market BUY quantity `201` is accepted by the unified trading rule service; main-board BUY `99` is rejected as below board lot; SELL residual `77` is accepted.
- Execution plan references runtime release, simulation binding, selection evidence, order intents, and trading-rule decisions with deterministic hashes.

## Residual Gaps

- The shared decision services are not yet wired into LocalSim unattended lifecycle.
- The shared execution plan is not yet wired into `MiniQMTExecutionBridge` or qmt strategy ledger managed-order submission.
- Existing broker-side legacy safeguards still exist and must be simplified or delegated in later phases after bridge integration.
- L5 real MiniQMT SIM validation was not run in this slice.

## Production Impact

- Production backend `8001`, frontend `3000`, production DB, and real MiniQMT were not touched.
- This slice adds no new DDL beyond the already committed shared selection evidence table from the prior slice.
