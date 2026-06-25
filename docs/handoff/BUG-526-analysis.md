# BUG-526 Analysis - MiniQMT canary gray-switch shadow evidence gate too weak

## Root cause
`MiniQMTGraySwitchController.switch_to_event_loop(require_shadow_evidence=True)` previously accepted the latest same-scope shadow reconciliation metadata when it had no FATAL differences, no in-flight work, and SIM mode. That was weaker than ADR 0002 section 8, because it did not require durable coverage across trading days or the minimum scenario set.

## Fix direction
- Keep the same-scope gate.
- Read durable `SHADOW_RECONCILIATION_REPORTED` events instead of trusting only the latest metadata snapshot.
- Require at least `N=1` distinct trade date by default, configurable in the controller.
- Require coverage for the MiniQMT scenario set: FULL_FILL, PARTIAL_55_STREAM, REJECT, CANCEL, DISCONNECT, RESTART_RECOVERY.
- Keep the existing no-FATAL, no-in-flight, and SIM-only rejection paths loud.
- Record accepted event ids and gate coverage details in the decision metadata.

## Scope
- `backend/services/miniqmt_execution_runtime/gray.py`
- `backend/services/miniqmt_execution_runtime/shadow.py`
- `backend/tests/miniqmt_execution_runtime/test_miniqmt_phase6_gray_switch.py`

## Safety
- No service start/restart.
- No production DB/DDL.
- No LIVE attempt.
- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`

## Self-audit 2026-06-25
- Design alignment: rechecked ADR 0002 / durable runtime design section 8 and the 2026-06-25 go-live readiness D3 note. The gate now requires durable shadow events, scenario coverage, no fatal drift, no in-flight work, and SIM-only mode.
- True event evidence: gray switch consumes `SHADOW_RECONCILIATION_REPORTED` events from the runtime repository; it no longer treats the latest metadata snapshot as sufficient.
- No second OMS: no new OMS or JSON authority was introduced; this change only reads existing durable shadow reconciliation events.
- B behavior: default `MINIQMT_EXECUTION_RUNTIME` remains compiler; tests assert unswitched scopes remain compiler.
- LIVE safety: no service start/restart, no production DB/DDL, and no `MINIQMT_MODE=LIVE` attempt.
