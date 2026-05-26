# PR #237 Paper v2 Backend CI Drift Validation - 2026-05-27

## Scope

- PR: https://github.com/licong01-cloud/AIstock/pull/237
- Branch: `bug/BUG-117-research-assistant-bug-117-close-sync-20260526-20260526`
- Trigger: PR #237 was blocked by GitHub CI job `Backend tests (paper_v2_backend)` after the Research Assistant close-sync commits.
- Classification: CI drift / runtime-gate-cleanup follow-up. The failing files were outside the Research Assistant BUG-105/109/117 closure scope and were handled as an explicit PR CI unblocker.
- Production ports touched: no. `8001` and `3000` were not restarted or killed.
- DB / DDL changes: none. `production_ddl_gate=noop`.
- Dependency changes: none. `production_frontend_dependency_gate=noop`; `production_backend_dependency_gate=noop`.

## Root Cause

The failed CI job reproduced locally with the same `paper_v2_backend` test command. Failures came from two recent Paper v2 gate-cleanup drifts:

1. Several tests still expected legacy `StrategyPackageValidationError` / `PAPER_ENABLED` gate semantics after Paper v2 simulation admission was decoupled from legacy lifecycle gates.
2. Live-session preparation finalized generated runtime config after the generated binding hash was attached, so `validate_runtime_profile_binding` rejected strict live-session ticks with `generated runtime config binding hash mismatch`.

## Changes Validated

- Refresh generated runtime-profile binding after live-session runtime config finalization and deep-copy session runtime config before mutation.
- Update Paper v2 / StrategyPackage tests to assert current typed runtime errors and compatibility semantics.
- Add test coverage that coldstart sentinel still rejects `RETIRED` packages while allowing asset-eligible `BACKTEST_APPROVED` packages.
- Add a fake authoritative selection artifact repository in live-session tests so strict live cursor tests do not reach a real DB-backed artifact generator.

## Commands And Results

| Level | Command | Result |
|---|---|---|
| L1 targeted CI failures | `python -m pytest -q backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_allows_backtest_approved_package_status backend/tests/paper_trading_v2/test_coldstart_sanity_sentinel_endpoint.py::test_sentinel_endpoint_rejects_retired_package_status_before_writes backend/tests/paper_trading_v2/test_live_approval_candidate.py::test_paper_v2_live_approval_candidate_requires_runtime_and_execution_activations backend/tests/paper_trading_v2/test_live_session.py::test_live_prepare_seeds_order_cursor_after_existing_completed_bars backend/tests/paper_trading_v2/test_live_session.py::test_live_tick_never_backfills_prepared_order_with_existing_bars backend/tests/paper_trading_v2/test_runner.py::test_runner_batch_rejects_empty_order_list backend/tests/paper_trading_v2/test_runner.py::test_runner_fails_for_package_mismatch backend/tests/strategy_package/test_backtest_contract.py::test_backtest_contract_rejects_invalid_runtime_topk_boundaries backend/tests/strategy_package/test_enable_paper_invariants.py::test_enable_paper_raises_on_manifest_sha256_mismatch backend/tests/strategy_package/test_enable_paper_invariants.py::test_enable_paper_treats_legacy_paper_enabled_as_noop backend/tests/strategy_package/test_rebalance_runtime.py::test_rebalance_still_fails_when_targets_are_missing -p no:cacheprovider` | `11 passed in 7.51s` |
| L2 CI parity | `python -m pytest backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package --ignore-glob=backend/tests/paper_trading_v2/*dev_db*.py --ignore=backend/tests/paper_trading_v2/test_runtime_enable_paper_compat.py -k 'not test_model_asset_resolver_uses_aistock_cache_without_wsl_unc_probe' -q -p no:cacheprovider` | `493 passed, 1 skipped, 1 deselected in 13.66s` |
| Ruff changed Python files | `python -m ruff check --force-exclude <changed python files>` | `All checks passed!` |
| Compile smoke | `python -m compileall backend/services/research_assistant backend/mcp/modules/research_assistant.py backend/services/paper_trading_v2/live_session.py backend/tests/paper_trading_v2 backend/tests/strategy_package/test_enable_paper_invariants.py backend/tests/mcp/test_profiles_registry_gateway.py` | `compileall_exit=0` |
| Diff hygiene | `git diff --check` | `exit=0` |

## Residual Risks

- This record only covers the CI-blocking Paper v2 backend drift. It does not claim L5 MiniQMT SIM or production Paper v2 activation.
- PR #237 still requires GitHub CI to rerun after the fix commit is pushed.

