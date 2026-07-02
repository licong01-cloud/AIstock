# MultiAlpha LocalSim admission relaxation + UI self-audit (2026-07-02)

## Scope

- Worktree: `F:\Dev\AIstock_worktrees\multi-alpha-localsim-admission-ui-20260702`
- Branch: `feature/multi-alpha-localsim-admission-ui-20260702`
- Design: `docs/analysis/multi_alpha_localsim_admission_relaxation_ui_design_20260702.md`
- Scope implemented: backend LocalSim venue-aware admission relaxation, selection summary/full alignment, Paper v2 packages dry-run UI, and portfolios query handoff.

## Dependency note for PR body

This PR only relaxes the `paper_admission` eligibility gate. It does not modify cold-start preflight. MultiAlpha parent packages with two model assets may still hit `live_inference.py::_single_model_asset_for_runtime("requires exactly one model asset")` through `preflight_for_strategy_package -> _source_from_package_assets`; that fix belongs to PR #1810. This PR only claims: LocalSim multi-alpha parent packages can appear in selectable lists, admission eligibility passes with warning, and `create_portfolio(broker_backend=local_sim)` is not blocked by missing dry-run admission.

## Design compliance evidence

| Item | Evidence | Status |
|---|---|---|
| F-001 LocalSim missing dry-run no longer blocks | `asset_eligibility.py::_multi_alpha_runtime_blockers`; pytest asserts WARN/context `multi_alpha_localsim_dry_run_not_required` | verified |
| F-002 MiniQMT remains blocked | pytest asserts `broker_backend=minqmt_sim` keeps hard blocker | verified |
| F-003 Other hard gates remain | unknown blocker tests; existing retired/hash/asset tests in targeted suite | verified |
| F-004 Selection full/summary aligned | full-path test in `test_multi_alpha_paper_admission.py`; summary warning-row test in `test_runtime_selection.py`; repository summary tests | verified |
| F-005 LocalSim create portfolio allowed | `test_local_sim_portfolio_create_succeeds_without_admission_and_minqmt_stays_closed` | verified |
| F-006 UI dry-run button | `strategyPackageApi.paperRuntimeDryRun`; packages page broker/topK/date/cash form and result summary; `npm run build` | verified |
| F-007 UI LocalSim create entry | packages link with query; portfolios page query preselect + explicit `broker_backend=local_sim`; `npm run build` | verified |
| F-008 Single-alpha regression | `test_portfolio_broker_backend.py`, `test_enable_paper_invariants.py`, and broader targeted suite | verified |
| F-009 No-silent | failure branches expose reason_code/context; no新增 `except: pass`/empty catch; `ruff check` | verified |
| F-010 Scope/gates | no migrations/DB init/RA/execution-layer files; no service start/restart; no DDL/DML | verified |

## Validation commands

- `rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_repository_service.py backend/tests/selection_center/test_runtime_selection.py backend/tests/paper_trading_v2/test_portfolio_broker_backend.py backend/tests/strategy_package/test_enable_paper_invariants.py -q` -> 144 passed.
- `rtk python -m ruff check backend/services/strategy_package/asset_eligibility.py backend/services/strategy_package/repository.py backend/services/selection_center/service.py backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py backend/tests/strategy_package/test_repository_service.py backend/tests/selection_center/test_runtime_selection.py` -> passed.
- `rtk python -m compileall backend/services/strategy_package backend/services/selection_center backend/services/paper_trading_v2 backend/routers` -> passed.
- `rtk npm ci` in `frontend/` -> installed local dependencies for validation only; no lockfile change intended.
- `rtk npm run lint` in `frontend/` -> passed with pre-existing warnings outside changed files.
- `rtk npm run build` in `frontend/` -> passed with pre-existing warnings outside changed files.
- `rtk python -m nox -s l0` -> passed.
- `rtk python scripts/aistock_feature_workflow.py validate --design docs/analysis/multi_alpha_localsim_admission_relaxation_ui_design_20260702.md --tier F2 --format json` -> PASS.
- `rtk git diff --check` -> passed.

## Production gates

- `production_ddl_gate=noop` (no migrations, no DB schema/init changes, no DDL executed)
- `production_dml_gate=noop` (no production DB writes; no admission rows inserted)
- `production_frontend_dependency_gate=noop` (no `package.json`/lockfile changes; `npm ci` only for local validation)
- `production_backend_dependency_gate=noop` (no Python dependency changes)
- `service_restart=not_performed` (user-owned runtime activation)
- `research_assistant_scope=not_touched`
