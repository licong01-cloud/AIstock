# BUG-096 Runtime Profile Binding Validation - 2026-05-22

## Scope

- BUG: BUG-096 / GitHub #149
- Branch: bug/BUG-096-runtime-profile-binding
- Worktree: F:\Dev\AIstock_worktrees\bug-096-runtime-profile-binding
- Production impact: no backend/frontend restart; no production DB writes; no QMT/MiniQMT order operation.
- production_ddl_gate: noop, no DB schema or migration changes.

## Fix Summary

- Added `refresh_generated_runtime_profile_binding()` for generated `platform_default` and `ad_hoc_non_trading_preview` bindings.
- StrategyPackage selection now refreshes generated binding hashes after PIT / `selection_artifact_config` finalization and validates again before continuing.
- Per-package runtime configs are also refreshed after StrategyPackage backtest-contract normalization so daily selection evidence hashes match the final package config.
- Added regression coverage for direct runtime profile refresh, StrategyPackage selection evidence, and Selection Center `create_paper_portfolio_from_run()`.

## Design / Closure Matrix

| Requirement | Implementation refs | Evidence | Status |
|---|---|---|---|
| `platform_default` binding hash is computed from final persisted trading runtime_config | `backend/services/selection_center/runtime_profile.py`, `backend/services/simulation_runtime/selection.py` | targeted pytest and 65-test regression | PASS |
| PIT / `selection_artifact_config` cannot mutate hash-included fields after binding without refreshed hash | `refresh_generated_runtime_profile_binding()` plus post-PIT validation | `test_default_runtime_profile_binding_can_refresh_after_system_generated_pit_metadata`, `test_strategy_package_selection_refreshes_default_binding_after_pit_finalization` | PASS |
| Selection run can create Paper v2 portfolio without false hash mismatch | `StrategyPackageSelectionService.run_selection()`, `SelectionCenterService.create_paper_portfolio_from_run()` existing validation | `test_selection_center_creates_paper_portfolio_after_default_pit_binding_finalization` | PASS |
| No silent fallback, stale binding, or historical selection evidence bypass | Binding validation remains fail-fast; only generated binding hashes are refreshed; versioned/runtime-release bindings are not silently changed | targeted tests; code review of allowed_write_scope | PASS |

## Commands

```powershell
pytest backend/tests/paper_trading_v2/test_runtime_profile.py::test_default_runtime_profile_binding_can_refresh_after_system_generated_pit_metadata backend/tests/simulation_runtime/test_strategy_package_selection_service.py::test_strategy_package_selection_refreshes_default_binding_after_pit_finalization backend/tests/selection_center/test_runtime_selection.py::test_selection_center_creates_paper_portfolio_after_default_pit_binding_finalization -q -p no:cacheprovider
```

Result: 3 passed.

```powershell
pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/selection_center/test_runtime_selection.py backend/tests/paper_trading_v2/test_runtime_profile.py -q -p no:cacheprovider
```

Result: 65 passed.

```powershell
python -m nox -s l0
```

Result: successful. Guardrail scan reported existing/baseline or P2 findings with blocking=0.

```powershell
git diff --check
```

Result: passed.

## Residual Risk

- Full `paper_v2_backend` nox was not run in this pass; targeted Paper v2 / Selection Center / simulation runtime tests and L0 passed.
- This fix does not address separate historical manifest hash drift for old portfolios.