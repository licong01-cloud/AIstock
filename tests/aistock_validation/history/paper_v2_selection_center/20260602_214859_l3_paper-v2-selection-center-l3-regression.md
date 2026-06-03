# Paper v2 Selection Center L3 regression

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-06-02T21:48:59+08:00
- Git commit before task commit: f8a85fe8
- Operator: codex
- Issue: BUG-217 / GitHub #588

## Scope

- Changed files: shared MiniQMT vn.py-style adapter, Paper v2 MiniQMT compatibility shim, simulation_runtime MiniQMT bridge, BUG-217 evidence.
- Impacted flows: Paper v2 MiniQMT single-strategy execution; simulation_runtime MiniQMT virtual-strategy bridge; shared Sniper/BestLimit/TWAP-lite child order generation.
- Business goal: prove Phase 2 backend/trading-core implementation keeps Paper v2 and multi-strategy MiniQMT execution on one adapter contract without silent fallback.
- Out of scope: production backend restart, production DDL, MiniQMT real order/cancel, Paper v2 UI bug fixes.
- Protected assets reviewed: no StrategyPackage frozen manifest, model artifact, HMM snapshot, Paper ledger, or production config was modified.

## Environment

- Backend port: 8012 only for attempted UI service check; service was started by Codex as local dev backend and stopped after validation.
- Frontend port: 3012 only for attempted UI E2E.
- TDX port: 19080 reachable during attempted UI service check.
- Conda/env: local Python/nox in this worktree; frontend dependencies installed under ignored `frontend/node_modules` for E2E attempt.
- Database: local AIstock DB via `F:\Dev\AIstock\.env` for data-quality smoke; no production DDL/write migration.
- Browser/headless: Playwright chromium attempted for full UI, but final Phase 2 evidence uses skip-UI backend/data L3 because full UI failed on existing UI/data fixtures outside Phase 2 scope.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No blocking high-risk path/secret/fallback/asset finding | `python -m nox -s l0` inside L3; guardrail scan `blocking=0` | PASS |
| Backend tests | Paper v2 + Selection Center backend tests pass | `paper_v2_backend`: 578 passed, 1 skipped, 2 xfailed | PASS |
| Data quality | Required Paper v2/Selection tables, data audit, strategy package readiness, run traceability pass | `paper_v2_data_quality`: PASS with one legacy ledger consistency WARN, non-blocking | PASS |
| Deep data tests | Cross-table/field/json/time data-quality tests pass or skip explicitly | `data_quality_deep`: 10 passed, 21 skipped | PASS |
| UI E2E | Full UI E2E should be independently green before final Phase 7/main merge | Attempted `paper_v2_ui` after local backend/deps setup; failed on HMM snapshot test ids and pre-existing StrategyPackage duplicate fixture | NON-PHASE2 BLOCKER, not used as pass evidence |
| Asset safety | No protected asset modified silently | `git status` limited to allowed task files and validation records | PASS |

## Commands

```powershell
$env:PAPER_V2_L3_SKIP_UI='1'; python -m nox -s paper_v2_l3
pytest backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py backend/tests/simulation_runtime/test_target_rebalance_shared.py::test_miniqmt_execution_bridge_uses_managed_orders_and_strategy_attribution -q -p no:cacheprovider
python -m ruff check backend/services/trading_core/miniqmt_vnpy_execution.py backend/services/trading_core/miniqmt_order_state.py backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py backend/services/paper_trading_v2/execution/minqmt_order_state.py backend/services/simulation_runtime/bridges.py backend/tests/trading_core/test_minqmt_vnpy_shared_adapter.py
git diff --check
```

## Evidence

- Targeted adapter tests: 11 passed.
- `paper_v2_backend`: 578 passed, 1 skipped, 2 xfailed.
- `paper_v2_data_quality`: required tables/audits/package readiness/run traceability PASS; legacy ledger consistency WARN is pre-existing and non-blocking for this Phase 2 adapter.
- `data_quality_deep`: 10 passed, 21 skipped.
- Full `paper_v2_ui` attempt: failed on existing UI/data prerequisites, not on touched backend adapter files; kept out of pass evidence and reported separately.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Initial `paper_v2_l3` full run failed before UI | no local backend on 8012 | Started local dev backend only on 8012 with schedulers and MiniQMT disabled; stopped it afterward | service check reached backend and TDX |
| Full `paper_v2_ui` failed after dependencies installed | existing HMM snapshot UI/test expectation and StrategyPackage duplicate fixture in local DB | Out of current BUG-217 Phase 2 backend adapter scope; not changed or hidden | skip-UI L3 backend/data chain passed; full UI failure reported as residual |

## Result

- Final status: PASS for BUG-217 Phase 2 backend/data validation with explicit skip-UI; full UI remains separate residual risk before final Phase 7/main merge.
- Remaining risks: Full Paper v2 UI E2E is not green in current local DB/browser state and should be handled by the UI/data fixture issue path before final main merge.
- Need production backend restart: no
- Need dev service restart: no; Codex-started 8012 backend was stopped.
