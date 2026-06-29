# StrategyPackage enable-selection/enable-paper lifecycle persistence

- Module: strategy_package
- Level: L2
- Date: 2026-06-29
- Git commit: working tree before final commit; synced to `origin/main` `a4897918cc4a4198dfc753a2002e7f07212377d9`
- Operator: Codex

## Scope

- Changed files:
  - `backend/services/strategy_package/service.py`
  - `backend/services/strategy_package/repository.py`
  - `backend/services/strategy_package/asset_eligibility.py`
  - `backend/tests/strategy_package/test_repository_service.py`
  - `backend/tests/strategy_package/test_enable_paper_invariants.py`
  - `backend/tests/strategy_package/test_enable_paper_router_409.py`
  - `backend/tests/strategy_package/test_multi_alpha_paper_admission.py`
  - `backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py`
  - `docs/architecture/strategy_package_enable_lifecycle_f2_design_20260629.md`
- Impacted flows: StrategyPackage status transition state machine; enable-selection endpoint/service; enable-paper endpoint/service; single-alpha and multi-alpha parent package lifecycle parity; Paper v2 package enable compatibility tests.
- Business goal: make `enable_selection` persist `SELECTION_ENABLED` and `enable_paper` persist `PAPER_ENABLED`, with audit events and explicit invalid-transition failures, while keeping PaperPortfolio single `package_id` contract and multi-alpha parent packages on the same state machine.
- Out of scope: production DDL/DML, service restart, frontend, PaperPortfolio schema/contract, multi-alpha promotion implementation, SourceType enum changes.
- Protected assets reviewed: no frozen manifest payloads, model weights, QE artifacts, selection artifacts, paper ledgers, or production DB rows were modified.

## Environment

- Backend port: not started / not touched
- Frontend port: not started / not touched
- TDX port: not started / not touched
- Conda/env: repository default local Python via `rtk`
- Database: no production DB DDL/DML; dev DB only for scoped `pkg_test_int6_%` lifecycle compatibility tests with teardown
- Browser/headless: not applicable

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| F2 workflow | Design has reconcile, transition graph, rollout/rollback, acceptance matrix | `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_enable_lifecycle_f2_design_20260629.md --tier F2` => PASS, `design_items=12 matrix_rows=12 warnings=0` | PASS |
| L0 compile | StrategyPackage services and routers compile | `rtk python -m compileall -q backend/services/strategy_package backend/routers` => exit 0 | PASS |
| Whitespace diff | No trailing whitespace or conflict markers | `rtk git diff --check` => exit 0 | PASS |
| Targeted lifecycle tests | Single-alpha enable-selection/paper persist status and events; illegal transitions fail with context; router maps re-entry to 409; multi-alpha parent parity passes/fails through shared path | `rtk python -m pytest backend/tests/strategy_package/test_repository_service.py backend/tests/strategy_package/test_enable_paper_invariants.py backend/tests/strategy_package/test_enable_paper_router_409.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py -q` => 50 passed | PASS |
| Dev DB lifecycle compatibility | Real PG repository rejects already `PAPER_ENABLED` re-entry and preserves explicit asset-check failure context without mutating state | `rtk python -m pytest backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py -q` => 2 passed | PASS |
| StrategyPackage module regression | Existing StrategyPackage tests remain green | `rtk python -m pytest backend/tests/strategy_package -q` => 240 passed | PASS |
| Selection Center regression | Selectable package path remains asset-eligibility based and green | `rtk python -m pytest backend/tests/selection_center -q` => 86 passed | PASS |
| Paper Trading v2 regression | Paper v2 tests remain green with expected skip/xfail baseline | `rtk python -m pytest backend/tests/paper_trading_v2 -q` => 397 passed, 1 skipped, 2 xfailed, 1 warning | PASS |
| Safe E2E probe | QE candidate dev DB e2e does not regress if fixture is available | `rtk python -m pytest backend/tests/e2e/test_paper_v2_qe_candidate_platform_devdb.py -q` => 1 skipped due dev DB fixture/data precondition | SKIPPED |
| Historical E2E probe | Paper v2 full lifecycle probe is not a gating signal for this lifecycle change when historical dev rows are drifted | `rtk python -m pytest backend/tests/e2e/test_paper_v2_full_lifecycle.py -q` => 2 failed, 2 passed; both failures are pre-existing dev DB `stored manifest_sha256 does not match stored manifest` drift during governance sampling / transient status mutation | NON-GATING |
| Scope guard | No DDL/frontend/S1-protected files changed | `rtk git diff --name-only` reviewed; no `backend/migrations`, `backend/db/init_*`, `frontend/`, `multi_alpha_promotion.py`, or `models.py` | PASS |

## Commands

```bash
rtk python -m compileall -q backend/services/strategy_package backend/routers
rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_enable_lifecycle_f2_design_20260629.md --tier F2
rtk python -m pytest backend/tests/strategy_package/test_repository_service.py backend/tests/strategy_package/test_enable_paper_invariants.py backend/tests/strategy_package/test_enable_paper_router_409.py backend/tests/strategy_package/test_multi_alpha_paper_admission.py -q
rtk python -m pytest backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py -q
rtk python -m pytest backend/tests/strategy_package -q
rtk python -m pytest backend/tests/selection_center -q
rtk python -m pytest backend/tests/paper_trading_v2 -q
rtk python -m pytest backend/tests/e2e/test_paper_v2_qe_candidate_platform_devdb.py -q
rtk python -m pytest backend/tests/e2e/test_paper_v2_full_lifecycle.py -q
rtk git diff --check
```

## Evidence

- API calls: router unit tests use FastAPI `TestClient` for `/strategy-packages/{package_id}/enable-paper` and verify 200/400/409 mappings.
- DB checks: dev DB compatibility tests use only scoped `pkg_test_int6_%` rows and delete status events, validation runs, runtime variants, package assets, and packages on teardown.
- Log files: no services started; no runtime logs produced.
- Playwright report/trace: not applicable.
- Screenshots: not applicable.
- Business output summary:
  - `STATUS_TRANSITIONS` formally supports `SELECTION_ENABLED`, `PAPER_ENABLED`, `PAPER_RUNNING`, `PAPER_PASSED`, `PAPER_FAILED`, and `RETIRED`.
  - `enable_selection()` persists `SELECTION_ENABLED` via repository compare-and-set and writes `package_status_event.reason=enable_selection`.
  - `enable_paper()` persists `PAPER_ENABLED` via repository compare-and-set and writes `package_status_event.reason=enable_paper`.
  - Re-entering `PAPER_ENABLED` and direct `DRAFT -> PAPER_ENABLED` fail with `InvalidStateTransitionError` context including `package_id`, `from_status`, `to_status`, and `allowed_from`.
  - Multi-alpha parent packages use the same `StrategyPackageService` and repository state machine; dry-run admission success permits the same lifecycle, missing dry-run fails before transition with `multi_alpha_runtime_not_validated_until_dry_run`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Existing no-op tests expected `BACKTEST_APPROVED` after enable calls | Tests encoded the removed no-op behavior | Updated expectations to persisted `SELECTION_ENABLED` / `PAPER_ENABLED`, status events, and re-entry 409 | Targeted lifecycle tests => 50 passed |
| In-memory repository invalid-transition context lacked `allowed_from` | PostgreSQL repository had the context but in-memory repository did not | Added `allowed_from` to in-memory `InvalidStateTransitionError` context | `test_enable_paper_rejects_already_enabled_reentry_with_context` and router 409 tests pass |
| Historical e2e full lifecycle probe failed | Dev DB has historical StrategyPackage manifest hash drift unrelated to this code path | Recorded as non-gating residual risk; no production DML/repair attempted | Main StrategyPackage/Selection/Paper module suites passed |

## Result

- Final status: PASS for F2 Phase implementation and module validation; historical e2e drift recorded as non-gating.
- Remaining risks: production runtime activation requires user-owned backend restart after merge; historical dev DB manifest drift remains outside this task.
- Need production backend restart: yes, after merge for runtime activation; not performed by Codex.
- Need dev service restart: no service was started or restarted.

## Production Gates

- production_ddl_gate=noop
- production_frontend_dependency_gate=noop
- production_backend_dependency_gate=noop
