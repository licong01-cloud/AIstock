# BUG-155 Paper v2 vn.py-style Execution Assets Validation Record

Date: 2026-05-29
Worktree: F:\Dev\AIstock_worktrees\registry-paper-v2-execution-implement-vn-py-style-execution-st-20260529-c6b786
Branch: bug/registry-paper-v2-execution-implement-vn-py-style-execution-st-20260529-c6b786
Issue: BUG-155 / GitHub #357

## Scope

Implemented vn.py-derived execution strategies as AIstock execution policy assets:

- `SNIPER_MINIQMT`
- `BEST_LIMIT_MINIQMT`
- `TWAP_LITE_MINIQMT`

The algorithm core is under `backend/execution_algos/vnpy_style/` and is adapter-independent. Paper v2 MiniQMT integration is under `backend/services/paper_trading_v2/execution/` and `backend/services/paper_trading_v2/day_runner.py`.

## DESIGN-COMPLIANCE-001 Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| Versioned execution policy assets, not ad-hoc Paper v2 code | `backend/execution_algos/vnpy_style/registry.py`; `backend/services/trading_core/execution_algo_capabilities.py`; `backend/execution_algos/vnpy_style/legacy_adapter.py` | `test_vnpy_style_assets_are_registered_and_declared_live_supported` | complete | None |
| No simplified/subset/POC/skeleton completion | Full core DTO/lifecycle/action model plus Sniper, BestLimit, TWAP-lite, MiniQMT adapter, diagnostics, tests | This record plus targeted tests | complete | Scope excludes future QE shadow ranking and V25 live scheduler per BUG non-goals |
| Complete core DTO/lifecycle/action/order/tick/trade/timer boundary | `models.py`; `base.py`; `sniper_core.py`; `best_limit_core.py`; `twap_lite_core.py` | `test_template_update_order_trade_and_finish_match_vnpy_lifecycle`; import-boundary test | complete | None |
| Port Sniper semantics from vnpy_algotrading | `sniper_core.py` | ask/bid trigger, active-order cancel, long/short characterization tests | complete | Runtime engine stripped intentionally |
| Port BestLimit semantics from vnpy_algotrading | `best_limit_core.py` | bid/ask best-limit, random child volume injection, quote-change cancel, invalid volume tests | complete | Deterministic random provider added for replayable tests |
| Port TWAP-lite semantics from vnpy_algotrading | `twap_lite_core.py` | interval timer, order_volume, cancel-before-slice, time-exhaust finish test | complete | Timer is adapter-driven, no vn.py EventEngine |
| Expose selectable Paper v2 assets and fail-fast validation | `execution_algo_capabilities.py`; `validators.py`; `legacy_adapter.py` | validator config test; registry/capability test | complete | No fallback to TWAP/market/default success |
| Add MiniQMT adapter without vn.py runtime engine | `execution/minqmt_live_algo_adapter.py`; `execution/minqmt_order_state.py`; `day_runner.py` | `test_minqmt_vnpy_*` adapter tests | complete | MiniQMT remains broker authority |
| Persist/surface diagnostics | `day_runner._run_minqmt_vnpy_style_intent`; `OrderHandleStatus.raw_status/status_msg/raw`; order metadata and execution state diagnostics | rejected-child raw status/status_msg test; adapter diagnostic tests | complete | Actual broker-specific reject classification remains future enhancement |
| Keep QE-ready boundary explicit | Core imports no Paper v2, MiniQMT, FastAPI, DB, vn.py, xtquant; source attribution and asset version stored | `test_vnpy_style_core_import_boundary_has_no_runtime_coupling` | complete | QE workspace/assets untouched |
| Production safety gates | No `.env`, runtime restart, production DB, DDL, StrategyPackage frozen asset, HMM snapshot, model weights, or QE workspace mutations | Git diff / file scope | complete | `production_ddl_gate=noop`, dependency gates noop |

## Validation Evidence

- `pytest backend/tests/trading_core/test_vnpy_style_execution_assets.py backend/tests/paper_trading_v2/test_minqmt_vnpy_execution_adapter.py -q -p no:cacheprovider` -> `15 passed`
- `python -m pytest backend/tests/trading_core backend/tests/paper_trading_v2 -q -p no:cacheprovider -x -vv` -> `359 passed, 1 skipped, 2 xfailed`
- `python -m json.tool tests\aistock_validation\bugs\20260529_BUG-155-implement-vn-py-style-execution-strategy-assets-for-paper-v2-and-qe-read.json` -> passed
- `git diff --check` -> passed
- `python -m nox -s l0 validation_module_registry_l0` -> `l0` success and `validation_module_registry_l0` success

## Production Gates

- `production_ddl_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- Production runtime touched: no
- Production DB touched: no
- `.env` touched: no
