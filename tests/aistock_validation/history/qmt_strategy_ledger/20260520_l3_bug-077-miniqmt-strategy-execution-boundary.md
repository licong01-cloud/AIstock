# BUG-077 MiniQMT StrategyPackage execution boundary

- Module: qmt_strategy_ledger
- Level: L3
- Date: 2026-05-20
- Base branch: origin/docs/strategy-platform-boundary-20260520
- Fix branch: bug/BUG-077-miniqmt-strategy-execution-boundary
- Worktree: F:\Dev\AIstock_worktrees\bug-077-miniqmt-strategy-execution-boundary
- Operator: codex-app
- Linked bug: BUG-077 / GitHub #104

## Scope

- Changed files: `backend/services/qmt_strategy_ledger/selection_order_builder.py`, `backend/routers/qmt_strategy_ledger.py`, `backend/tests/qmt_strategy_ledger/test_selection_order_builder.py`, `backend/tests/qmt_strategy_ledger/test_router_summary.py`, `tests/aistock_validation/bugs/20260520_BUG-077-miniqmt-strategy-execution-bypasses-strategypackage-minute-e.json`, this validation record.
- Impacted flows: MiniQMT StrategyPackage binding order preview, SelectionRun-to-managed-order legacy path, structured fail-fast propagation through the QMT strategy ledger router.
- Business goal: prevent MiniQMT StrategyPackage execution from using `SelectionOrderBuilder` as a direct broker-order generator that bypasses daily rebalance intent and validated minute execution policy.
- Intentional behavior: this fix is a fail-fast boundary, not a simplified MiniQMT execution bridge. Until a validated bridge exists, `/package-bindings/{binding_id}/orders/preview` must reject StrategyPackage broker-order generation with an actionable `UNSUPPORTED_FEATURE` response.
- Out of scope: implementing the future MiniQMT execution bridge, changing StrategyPackage manifests, changing model/factor artifacts, live MiniQMT submit/cancel, production backend restart, production DB writes, frontend UI changes.
- Protected assets reviewed: no StrategyPackage manifest/model/factor artifact, validated execution policy asset, production DB data, production backend `8001`, frontend `3000`, or MiniQMT broker runtime was touched.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Default StrategyPackage binding order generation | Direct `SelectionRun -> SelectionOrderBuilder -> ManagedOrderRequest` path is disabled and names BUG-077 | `test_selection_order_builder_rejects_strategy_package_direct_order_generation_by_default` | PASS |
| Router preview endpoint | Operator receives structured `UNSUPPORTED_FEATURE` with disabled path and required validated execution-policy bridge | `test_package_binding_order_preview_fails_fast_until_minqmt_execution_bridge_exists` | PASS |
| Legacy calculator regression | Existing `SelectionOrderBuilder` unit tests still exercise historical sizing semantics only through explicit test opt-in | `test_selection_order_builder.py` helper passes `allow_legacy_direct_order_generation=True` | PASS |
| Manual managed-order path | `/orders/preview` remains covered separately and does not instantiate `SelectionOrderBuilder` | full `backend/tests/qmt_strategy_ledger` suite | PASS |
| Trading-core execution semantics | Trading Core and QE config truth suites remain green; no TWAP/daily fallback was introduced | `pytest backend/tests/trading_core backend/tests/unified_engine/test_qe_config_truth.py` | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'
pytest backend/tests/qmt_strategy_ledger/test_selection_order_builder.py backend/tests/qmt_strategy_ledger/test_router_summary.py -q -p no:cacheprovider
pytest backend/tests/qmt_strategy_ledger -q -p no:cacheprovider
pytest backend/tests/trading_core backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
python -m compileall backend/services/qmt_strategy_ledger backend/routers/qmt_strategy_ledger.py backend/tests/qmt_strategy_ledger
python -m nox -s validation_module_registry_l0
python -m nox -s guardrail_changed_files
python -m nox -s l0
git diff --check
git diff --cached --check
```

## Evidence

- Targeted BUG-077 tests: 21 passed in 9.43s.
- Full `qmt_strategy_ledger` tests: 86 passed in 9.01s.
- Trading Core plus QE config truth tests: 126 passed in 10.45s.
- Compile checks: `backend/services/qmt_strategy_ledger`, `backend/routers/qmt_strategy_ledger.py`, and `backend/tests/qmt_strategy_ledger` compiled successfully.
- `validation_module_registry_l0`: 8 passed; ownership scan mapped 12/12 files.
- `guardrail_changed_files`: successful after staging; files=6, findings=1 P2 `ALGO-COMPLEXITY-001` in `selection_order_builder.py`, blocking=0. The finding is non-blocking and is on the legacy calculator retained behind explicit test-only opt-in; this BUG-077 fix does not expand the production execution loop.
- `l0`: successful. Existing baseline/new non-blocking guardrail findings remain outside this BUG-077 scope; blocking=0.
- `git diff --check` and `git diff --cached --check`: passed; only line-ending warnings were printed for touched Python files.
- GitHub issue #104 was open and label-synced as `status:in_progress` during validation.

## Design Compliance Review

| Requirement | Implementation / evidence | Result |
|---|---|---|
| No simplified strategy bridge may be claimed complete | The code raises `UnsupportedFeatureError` by default and explicitly says a MiniQMT StrategyPackage execution bridge is required | PASS |
| `/package-bindings/{binding_id}/orders/preview` must not emit broker orders through `SelectionOrderBuilder` | Router catches the domain error and returns structured HTTP error instead of preflight requests | PASS |
| Error must be actionable | Error context includes `issue=BUG-077`, `disabled_path`, `required_path`, binding/package/selection trace | PASS |
| Historical unit semantics may remain only as legacy coverage | Existing builder tests opt in through `allow_legacy_direct_order_generation=True`; production construction does not | PASS |
| Future bridge boundary remains explicit | Error context documents required path: StrategyPackage alpha core -> daily target/rebalance intent -> validated execution policy -> MiniQMT execution bridge -> ManagedOrderRequest | PASS |

## Result

- Current status: PASS for local BUG-077 L3 service-level validation.
- This branch intentionally blocks the invalid MiniQMT StrategyPackage execution path rather than implementing a partial/POC execution bridge.
- Remaining work is tracked separately: BUG-087 daily selection lifecycle, BUG-085 StrategyPackage manifest alpha-core boundary, BUG-086 runtime version activation, BUG-089 validated policy evidence, BUG-088 live approval lifecycle, BUG-090 shared strategy engine single decision path.
- Need production backend restart: yes after merge/deploy for runtime effect, but not performed by Codex.
- Need DB migration: no schema change in this fix.
- Need MiniQMT broker action: no during local validation; no broker order placement was attempted.
- Production impact during validation: none; no production `8001`/`3000`, broker order placement, or DB writes used.
