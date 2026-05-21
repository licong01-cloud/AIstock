# Shared Selection Evidence Validation - 2026-05-21

## Scope

- Branch: `feature/sim-remediation-impl-20260521`
- Worktree: `F:\Dev\AIstock_worktrees\sim-remediation-impl-20260521`
- Change slice: shared broker-neutral `StrategyPackageSelectionService`, `DailySelectionSignalService`, and persisted `DailySelectionEvidence`.
- Production impact: no backend `8001`, frontend `3000`, production DB, or MiniQMT runtime was touched.

## Design Mapping

| Design item | Implementation refs | Evidence | Status |
|---|---|---|---|
| Selection Center, LocalSim and MiniQMT must share one StrategyPackage selection entry | `backend/services/simulation_runtime/selection.py`, `backend/services/selection_center/service.py` | Selection Center now delegates `run_packages()` to `StrategyPackageSelectionService` | PASS |
| Selection-only path must stop before target/rebalance/execution/broker gates | `assert_selection_only_payload_boundary`, `StrategyPackageSelectionService.run_selection()` | Negative tests reject broker, capital, execution, tail, target and rebalance fields | PASS |
| DailySelectionEvidence must contain package, release/runtime profile, source, counts and artifact hash | `DailySelectionEvidence`, `selection.daily_selection_evidence` DDL | Unit tests assert release-backed evidence and repository persistence | PASS |
| New DB schema fields require comments | `backend/migrations/trading_core_v2_schema.sql`, `backend/db/init_trading_core_v2_schema.py` | `test_schema_comments.py` checks table and columns | PASS |
| Existing Selection Center behavior remains compatible | `SelectionCenterService.run_packages()` | Selection Center regression suite passed | PASS |

## Commands

```powershell
python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_strategy_runtime_release.py backend/tests/simulation_runtime/test_schema_comments.py -q -p no:cacheprovider
python -m pytest backend/tests/selection_center/test_runtime_selection.py backend/tests/selection_center/test_live_inference_preflight_wiring.py backend/tests/selection_center/test_selection_center_api.py backend/tests/strategy_package/test_manifest_alpha_core_boundary.py -q -p no:cacheprovider
python -m pytest backend/tests/simulation_runtime backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
git diff --check
```

## Results

- Targeted simulation-runtime tests: `15 passed`.
- Selection Center / boundary regression: `58 passed`.
- Broad Paper v2 / Selection Center / StrategyPackage / QMT ledger regression: `570 passed, 1 skipped, 2 xfailed`.
- `git diff --check`: PASS with only line-ending warnings.

## Residual Scope

- This slice does not implement LocalSim unattended execution, MiniQMT execution bridge, target/rebalance extraction, trading-rule service, execution-plan compiler, scheduler/UI, or L5 MiniQMT SIM validation.
- The implemented evidence service is release-aware; existing Selection Center calls without a formal `StrategyRuntimeRelease` remain compatible and still persist runtime-profile-backed evidence.
