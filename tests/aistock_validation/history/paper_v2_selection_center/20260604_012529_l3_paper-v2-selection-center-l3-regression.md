# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-04T01:25:29
- Git commit: fc77127a
- Operator: lc999

## Scope

- Changed files: `backend/services/simulation_runtime/selection.py`, `backend/services/simulation_runtime/scheduler.py`, simulation runtime regression tests, BUG-241 registry record.
- Impacted flows: Simulation Runtime scheduler -> StrategyPackage selection -> authoritative selection artifact lookup -> LocalSim/MiniQMT daily plan creation.
- Business goal: unattended multi-strategy LocalSim and MiniQMT SIM scheduler runs must reuse the StrategyRuntimeRelease-backed `selection_artifact_config` instead of falling back to an empty runtime artifact hash.
- Out of scope: real MiniQMT trading-hours order submission and UI E2E; this pass used backend/stub gates only.
- Protected assets reviewed: no StrategyPackage manifest/model weight/HMM snapshot/Paper ledger assets were modified.

## Environment

- Backend port: no dev backend started; validation used pytest/nox and read-only data-quality DB smoke.
- Frontend port: skipped by `PAPER_V2_L3_SKIP_UI=1`.
- TDX port: not required by this backend/stub fix.
- Conda/env: local AIstock Python via nox `venv_backend=none`.
- Database: production-like local DB read-only smoke in `paper_v2_data_quality`; no DDL or data mutation for this code fix.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Targeted regression | Release metadata `selection_runtime_config` reaches scheduler selection and authoritative artifact lookup | `python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py::test_strategy_package_selection_service_uses_release_selection_artifact_config backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_passes_release_selection_runtime_config_to_selection_service -q` -> 2 passed | PASS |
| Surrounding simulation tests | Existing scheduler and selection behavior remains compatible | `python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> 35 passed | PASS |
| L0 guardrails | No blocking high-risk path/secret/fallback/asset finding | `python -m nox -s l0` -> successful; existing guardrail findings reported as non-blocking/baseline/P2 | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `python -m nox -s paper_v2_backend` -> 596 passed, 1 skipped, 2 xfailed | PASS |
| Paper v2 L3 | Paper v2/Selection Center L3 backend/data-quality gates pass | `PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3` -> l0, paper_v2_backend, paper_v2_data_quality, data_quality_deep success | PASS |
| LocalSim unattended | LocalSim scheduler/restart slice remains green | `python -m nox -s localsim_unattended_l3` -> 32 passed | PASS |
| MiniQMT SIM stub | Fake MiniQMT order/sync/reconcile gates remain green | `python -m nox -s miniqmt_sim_stub_l3` -> 45 passed | PASS |
| Dual backend L4 | LocalSim + MiniQMT backend oracle coverage remains green | `python -m nox -s simulation_dual_backend_l4` -> 91 passed | PASS |
| Validation registry | BUG/validation registry scope is valid | `python -m nox -s validation_module_registry_l0` -> 8 passed and ownership scan pass | PASS |
| Validation Center backend | Added history evidence path keeps validation backend gate green | `python -m nox -s validation_center_backend` -> 356 passed, coverage line 80.07 / branch 62.34 | PASS |
| Asset safety | Protected assets unchanged | `git diff --name-only` includes only runtime code, tests, BUG JSON, validation history | PASS |

## Commands

```bash
python -m ruff check backend/services/simulation_runtime/scheduler.py backend/services/simulation_runtime/selection.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py backend/tests/simulation_runtime/test_strategy_package_selection_service.py
python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py::test_strategy_package_selection_service_uses_release_selection_artifact_config backend/tests/simulation_runtime/test_lifecycle_scheduler.py::test_scheduler_passes_release_selection_runtime_config_to_selection_service -q
python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q
python -m nox -s validation_module_registry_l0
python -m nox -s l0
python -m nox -s paper_v2_backend
PAPER_V2_L3_SKIP_UI=1 python -m nox -s paper_v2_l3
python -m nox -s localsim_unattended_l3
python -m nox -s miniqmt_sim_stub_l3
python -m nox -s simulation_dual_backend_l4
python -m nox -s validation_center_backend
git diff --check
```

## Evidence

- API calls: not used in this code-level BUG fix; production readiness API checks are scheduled after merge/restart.
- DB checks: `paper_v2_data_quality` passed required schema/audit/traceability gates; warned only about existing legacy ledger consistency rows outside this fix.
- Log files: no runtime service log mutation in this branch.
- Playwright report/trace: not applicable; UI E2E skipped by `PAPER_V2_L3_SKIP_UI=1`.
- Screenshots: not applicable.
- Business output summary: scheduler now passes release selection config to selection service; selection service also extracts release metadata and reuses authoritative artifact hashes when caller provides `runtime_config={}`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| BUG-241 repro: runtime hash used empty config (`44136...`) | scheduler called selection with `{}` and selection service did not merge release selection metadata | added release selection config extraction/merge and scheduler propagation | targeted regression + LocalSim/MiniQMT gates passed |

## Result

- Final status: PASS for BUG-241 code fix and backend/stub readiness gates.
- Remaining risks: real MiniQMT SIM submit still requires production runtime restart, MiniQMT client connectivity, active 2026-06-04 bindings, and trading-window verification.
- Need production backend restart: yes after merge to load the fix and updated scheduler env.
- Need dev service restart: no.
