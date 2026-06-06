# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-04T02:50:43+08:00
- Git commit: dfde2541
- Operator: lc999
- Related issue: BUG-242 / GitHub #676 / PR #677

## Scope

- Changed files: `backend/tests/simulation_runtime/test_lifecycle_scheduler.py`, BUG-242 registry JSON, validation history.
- Impacted flows: Simulation Runtime scheduler tests, AIstock LocalSim unattended gate, MiniQMT SIM stub gate, dual-backend LocalSim/MiniQMT simulation validation, Paper v2/Selection Center L3 backend/data-quality gates.
- Business goal: validation must not inherit production `SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT=true` for a unit test that asserts default submit remains disabled; tomorrow's unattended AIstock LocalSim multi-strategy simulation and MiniQMT multi-strategy simulation gates must be runnable without false failures.
- Out of scope: restarting production backend `8001`, frontend `3000`, TDX `19080`, changing production `.env`, applying DDL, or submitting real MiniQMT orders.
- Protected assets reviewed: no StrategyPackage frozen manifest, model weights, HMM snapshots, Paper ledgers, broker orders, or production DB rows were modified by this fix.

## Environment

- Backend port: no dev backend started; validation used pytest/nox and read-only DB/data-quality smoke.
- Frontend port: skipped by `PAPER_V2_L3_SKIP_UI=1`; no UI files changed in BUG-242.
- TDX port: not required by the BUG-242 test isolation change.
- Conda/env: local AIstock Python/nox environment.
- Database: read-only data-quality smoke reported `password_configured=true`; no DDL or data mutation.
- Browser/headless: not used.
- Evidence log: `tmp/validation/BUG-242-final/bug242_final_validation_20260604_025043.log`.

## Design Compliance Matrix

| Item | Implementation refs | Test or evidence | Status | Gap or exception |
|---|---|---|---|---|
| Fix observed behavior: validation must not read production default-submit env for disabled-by-default assertion | `backend/tests/simulation_runtime/test_lifecycle_scheduler.py` | `python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_background_scheduler_runs_planning_window_and_keeps_submit_disabled_by_default -q` -> 1 passed | PASS | None |
| Preserve BUG-241 production intent: production `default_submit=true` remains intentional and not overwritten by this test fix | Test isolation only; no scheduler runtime code changed in BUG-242 | `SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT=true` no longer breaks `simulation_dual_backend_l4`; `python -m nox -s simulation_dual_backend_l4` -> 91 passed | PASS | Real production activation still depends on backend restart owned outside this PR |
| AIstock LocalSim unattended gate remains runnable for multi-strategy validation preparation | Existing LocalSim unattended validation catalog and backend tests | `python -m nox -s localsim_unattended_l3` -> 32 passed | PASS | No production service started |
| MiniQMT multi-strategy simulation stub gate remains runnable for unattended validation preparation | Existing MiniQMT SIM stub validation catalog and backend tests | `python -m nox -s miniqmt_sim_stub_l3` -> 45 passed | PASS | Trading-hours real MiniQMT L5 remains manual/window-bound |
| Paper v2 / Selection Center L3 remains green around BUG-241/BUG-242 | Paper v2 L3 nox sessions and data-quality smoke | `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` -> l0 success, paper_v2_backend 596 passed / 1 skipped / 2 xfailed, data-quality gates passed | PASS | UI intentionally skipped because no UI behavior changed |
| Validation registry and Validation Center can ingest the evidence and BUG record | `tests/aistock_validation/bugs/20260604_BUG-242-*.json`, this history file | `python -m nox -s validation_module_registry_l0` -> 8 passed; `python -m nox -s validation_center_backend` -> 356 passed, coverage line 80.09 / branch 62.34 | PASS | None |
| Production gates | BUG JSON and PR production gates | `production_ddl_gate=noop`, `production_frontend_dependency_gate=noop`, `production_backend_dependency_gate=noop` | PASS | No DDL to apply |

## Commands

```bash
python -m ruff check backend/tests/simulation_runtime/test_lifecycle_scheduler.py
python -m pytest backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_background_scheduler_runs_planning_window_and_keeps_submit_disabled_by_default -q
python -m nox -s l0
python -m nox -s localsim_unattended_l3
python -m nox -s miniqmt_sim_stub_l3
python -m nox -s simulation_dual_backend_l4
PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3
python -m nox -s validation_module_registry_l0
python -m nox -s validation_center_backend
git diff --check origin/main...HEAD
```

## Evidence

- API calls: not used; BUG-242 is test isolation and validation-readiness only.
- DB checks: `paper_v2_data_quality` passed required schema/audit/traceability gates and printed `password_configured=true`; no password was logged in this record.
- Data-quality note: `paper_v2_ledger_consistency` reported existing legacy order/fill mismatches as WARN only with `strict_history=false`; this is outside BUG-242 and did not block the gate.
- Logs: local validation log at `tmp/validation/BUG-242-final/bug242_final_validation_20260604_025043.log`.
- Playwright report/trace: not applicable; no UI behavior changed and `PAPER_V2_L3_SKIP_UI=1` was used.
- Business output summary: LocalSim unattended, MiniQMT SIM stub, dual-backend simulation, Paper v2 L3, validation registry, and Validation Center backend gates all passed after isolating the default-submit test from production env.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `simulation_dual_backend_l4` could fail when production `.env` has `SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT=true` | Unit test expected disabled default but imported code after production env had been loaded | Isolated the test env so the disabled-by-default assertion is deterministic and does not weaken production submit behavior | Targeted pytest passed; `simulation_dual_backend_l4` passed with 91 tests; LocalSim/MiniQMT/Paper v2 gates passed |

## Result

- Final status: PASS for BUG-242 and the requested AIstock LocalSim + MiniQMT unattended validation preparation gates.
- Remaining risks: real trading-hours MiniQMT L5 validation still requires MiniQMT client connectivity, active bindings, the trading window, and a production backend process that has loaded the merged BUG-241/BUG-242 code.
- Need production backend restart: yes, operationally, to activate BUG-241 runtime scheduler code in the existing production backend; this PR did not restart it.
- Need dev service restart: no.
