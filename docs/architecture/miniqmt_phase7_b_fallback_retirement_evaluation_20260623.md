# MiniQMT A Phase 7 B Fallback / Retirement Evaluation

> Date: 2026-06-23
> Scope: Phase 7 evaluation for ADR 0002 and `miniqmt_durable_execution_runtime_design_20260623.md`.
> Conclusion: keep B as an explicit fallback; do not delete B in this phase.

## 1. Authority And Non-Goals

- Authority: ADR 0002, durable execution runtime design section 7 / section 9 / section 10, and the Phase 0 seam contract.
- This document is an evaluation and operational decision record for Phase 7, not a merge approval for LIVE traffic.
- No production DB or DDL is introduced.
- No runtime service should be started or restarted by this phase.
- B/compiler behavior remains default and unchanged unless a portfolio/strategy slot is explicitly canary-switched.

## 2. Phase 7 Decision

Phase 7 should keep B as the explicit fallback path.

- Global `MINIQMT_EXECUTION_RUNTIME` remains `compiler` by default.
- Per `portfolio_id` / `strategy_slot_id` event-loop canary overrides may exist only after same-scope durable no-fatal shadow evidence.
- One-click rollback removes the per-scope override and resolves the slot back to compiler.
- Unswitched portfolios and strategy slots continue to resolve to compiler.
- B is not deleted in this phase because LIVE admission gates and broader multi-slot canary evidence are not yet complete.

## 3. Fallback Semantics

The fallback is explicit, audited, and non-silent.

| Situation | Required Behavior | Reason Code / Evidence |
| --- | --- | --- |
| No per-scope override | Resolve `compiler` | Default inert flag evidence |
| Event-loop canary requested without same-scope shadow evidence | Reject loudly | `MINIQMT_GRAY_SHADOW_EVIDENCE_MISSING` |
| Shadow evidence belongs to another portfolio/slot | Reject loudly | `MINIQMT_GRAY_SHADOW_SCOPE_MISMATCH` |
| Shadow evidence contains fatal drift | Reject loudly | `MINIQMT_GRAY_SHADOW_EVIDENCE_FATAL` |
| LIVE or LIVE-pending mode requested | Reject loudly | `MINIQMT_GRAY_LIVE_FORBIDDEN` |
| Active child orders or algo instances exist in the slot | Reject switch/rollback loudly until operator reset/cancel | `MINIQMT_GRAY_IN_FLIGHT_AMBIGUOUS` |
| Operator rollback drill passes with no in-flight ambiguity | Remove override and resolve compiler | `GRAY_ROLLBACK_APPLIED` durable event |

## 4. Retirement Criteria For B

B can be considered for retirement only after all criteria below are true and documented in a later PR:

1. Multiple SIM portfolios and strategy slots have durable no-fatal A/B shadow reports for the design scenario matrix.
2. Phase 6 canary switch and rollback drills have passed per portfolio/slot, including in-flight reset/cancel drills.
3. LIVE admission gates outside this phase are closed: pre-trade risk, cash overcommit prevention, disconnect handling, and production monitoring.
4. Event-loop runtime has run through at least one complete observation window with no fatal drift and with all differences explained.
5. Operator runbooks and on-call rollback paths are documented and tested.
6. CI grep guards remain green: no synthetic timer loop, no event-loop gateway `return []`, no TDX in MiniQMT/event_loop, no algorithm-core fork, and default compiler inert.

Until all criteria are satisfied, B remains the explicit fallback and the default runtime.

## 5. Implementation State In This PR

- Phase 6 code adds a scoped controller for canary switch and rollback.
- Runtime decisions are persisted as append-only runtime events and runtime metadata.
- The controller does not create an OMS and does not alter `qmt_strategy` authority.
- The controller does not touch `backend/execution_algos/vnpy_style/`.
- The controller rejects LIVE modes and in-flight ambiguity loudly with reason codes.

## 6. Remaining Work

- Expand canary evidence from the current targeted SIM slot to additional portfolios and strategy slots.
- Add operator-facing runbook text for who may request canary switch / rollback and how to verify post-rollback state.
- Keep B fallback until LIVE admission gates and multi-slot canary evidence are both complete.
- Later retirement PR, if any, must be separate and must not silently delete compiler behavior before Tier2 approval.
