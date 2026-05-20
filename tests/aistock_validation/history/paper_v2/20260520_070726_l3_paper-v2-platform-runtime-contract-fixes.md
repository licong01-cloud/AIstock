# Paper v2 platform runtime contract fixes

- Module: paper_v2
- Level: L3
- Date: 2026-05-20T07:07:26+08:00
- Git commit: 14d4ce1 pre-commit; code changes pending in branch bug/BUG-065-paper-v2-hmm-platform-runtime; final fix commit will be added after commit
- Operator: lc999

## Scope

- Changed files: backend/services/strategy_package/backtest_contract.py, backend/services/strategy_package/runtime.py, backend/services/strategy_package/runtime_variant.py, backend/services/strategy_package/service.py, backend/services/strategy_package/validators.py, backend/services/selection_center/service.py, backend/services/selection_center/package_health.py, backend/services/paper_trading_v2/service.py, backend/services/paper_trading_v2/day_runner.py, backend/services/paper_trading_v2/readiness.py, backend/services/paper_trading_v2/live_session.py, backend/services/paper_trading_v2/session.py, and regression tests under backend/tests/*.
- Impacted flows: StrategyPackage backtest contract, runtime variants, Selection Center package runtime config, Paper v2 runtime profiles, execution-policy activation, day runner, readiness, live session, and MiniQMTSim route.
- Business goal: StrategyPackage binds strategy semantics only; HMM/ST PIT/event-signal/tradability are platform runtime capabilities, and execution-policy/runtime-variant changes are auditable/versioned without mutating frozen package manifests.
- Out of scope: UI E2E and live MiniQMT submit/cancel; production backend 8001/frontend 3000/production DB were not touched.
- Protected assets reviewed: no StrategyPackage manifest/model/factor/HMM artifact/QE asset/database ledger files were modified; tests use in-memory repositories and temp files only.

## Environment

- Backend port: not started
- Frontend port: not started
- TDX port: not used
- Conda/env: local python/nox in worktree F:\Dev\AIstock_worktrees\bug-065-paper-v2-hmm-platform-runtime
- Database: not used by regression tests
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Runtime contract | HMM/ST PIT/event-signal are not re-injected from QE custom_params | `python -m pytest backend/tests/strategy_package/test_backtest_contract.py ...` | PASS |
| Runtime variant | HMM overlay rejected; paper candidate variants must be validation-passed and consumed by Selection/Paper target generation | `backend/tests/strategy_package/test_runtime_variants.py`, `test_day_runner_consumes_validated_runtime_variant_candidate`, `test_selection_center_consumes_validated_runtime_variant_candidate` | PASS |
| Selection Center | platform ST PIT/risk/HMM profile works through runtime profile and health checks | `backend/tests/selection_center/test_runtime_selection.py` | PASS |
| Paper v2 | runtime profile and execution-policy activation paths are versioned/audited | `backend/tests/paper_trading_v2/test_runtime_profile.py`, `backend/tests/paper_trading_v2/test_day_runner.py` | PASS |
| MiniQMTSim | MiniQMT readiness/day_runner do not inherit forbidden QE runtime contract; versioned policy and platform HMM path works | `backend/tests/paper_trading_v2/test_minqmtsim_backend.py` | PASS |
| L0 guardrails | no blocking new P1/P0 guardrail issue | `nox -s l0` | PASS |
| Module ownership/catalog | validation module registry and catalog are consistent | `nox -s validation_module_registry_l0`; `nox -s validation_catalog_integrity` | PASS |
| Backend L3 slice | Paper v2 + Selection + StrategyPackage backend regression passes | `nox -s paper_v2_backend` | PASS |

## Commands

```bash
python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_center_consumes_validated_runtime_variant_candidate -q
python -m pytest backend/tests/paper_trading_v2/test_runner.py::test_runner_fails_for_v24_model_unavailable backend/tests/strategy_package/test_manifest_v1.py::test_paper_readiness_fails_when_v24_model_is_unavailable -q
python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_minqmt_readiness_preserves_disabled_hmm_and_uses_platform_risk_profile backend/tests/paper_trading_v2/test_minqmtsim_backend.py::test_minqmt_day_runner_uses_platform_hmm_snapshot_and_versioned_execution_policy -q
python -m pytest backend/tests/strategy_package/test_backtest_contract.py backend/tests/strategy_package/test_runtime_variants.py backend/tests/paper_trading_v2/test_runtime_profile.py backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/selection_center/test_runtime_selection.py -q
python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package -q
python -m compileall backend/services/strategy_package/backtest_contract.py backend/services/selection_center/service.py backend/services/selection_center/package_health.py backend/services/strategy_package/runtime.py backend/services/strategy_package/runtime_variant.py backend/services/strategy_package/service.py backend/services/strategy_package/validators.py backend/services/paper_trading_v2/service.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/readiness.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/session.py backend/services/paper_trading_v2/replay.py
git diff --check
nox -s validation_module_registry_l0
nox -s validation_catalog_integrity
nox -s l0
nox -s paper_v2_backend
```

## Evidence

- Targeted fixes: focused runtime-variant regression `1 passed` after fake risk-policy injection; broader targeted group `109 passed`.
- Backend module regression: `443 passed, 1 skipped, 2 xfailed in 15.76s` by direct pytest and `443 passed, 1 skipped, 2 xfailed in 15.55s` by `nox -s paper_v2_backend`.
- Compile: compileall passed for all changed service modules.
- Diff hygiene: `git diff --check` passed.
- Nox guardrails: `validation_module_registry_l0` passed; `validation_catalog_integrity` passed; `l0` passed with only non-blocking baseline/P2 UI raw JSON findings.
- API/DB/log checks: not applicable; regression uses in-memory repositories and temp artifacts, no production services.
- Business output summary: MiniQMTSim route preserves disabled HMM, accepts platform HMM snapshot independent of QE-era custom_params, uses date-activated validated execution policy, consumes validated runtime variants, skips LocalSim minute-market checks, and persists MiniQMT broker-authoritative order/snapshot state without local fills.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| V24/V25 asset validation expected at package readiness time | old tests expected package/portfolio creation to be blocked by execution runtime assets | validator now supports static paper catalog checks without runtime asset instantiation; actual run still fail-fasts in execution engine | focused two-test pytest passed; backend suite passed |
| MiniQMTSim regression fixture used fixed backend portfolio id | test backend helper did not bind to dynamically created portfolio | added factory preserving real portfolio/package/data_source binding | MiniQMT focused tests passed |
| readiness result did not expose full runtime_config | test was asserting a non-model field | assert runtime profile through readiness selection check context | MiniQMT focused tests passed |

## Result

- Final status: PASS for code-level and backend L3 validation; ready for commit/PR/CI/merge if no external conflict appears.
- Remaining risks: UI E2E and live MiniQMT submit/cancel were intentionally not run; production backend restart is required only after merge if operator wants runtime activation.
- Need production backend restart: yes after merge for backend code to take effect, but not performed by this validation.
- Need dev service restart: no dev service was started.
