# QE event risk policy forced exit runtime

- Module: qe
- Level: L3
- Date: 2026-05-05T16:59:45
- Git commit: 8d17789
- Operator: lc999

## Scope

- Changed files: `scripts/qe_event_risk_policy.py`, `scripts/qe_suspend_filter_strategy.py`, `scripts/qe_suspend_filter_score_weighted_strategy.py`, `backend/services/quantevolver/config_composer.py`, `backend/tests/unified_engine/test_qe_config_truth.py`, `docs/codex_project_memory.md`
- Impacted flows: QE/Qlib config generation, TopkDropout/ScoreWeighted outer strategy buy filtering, forced-exit sell generation, Qlib Exchange quote/sell universe generation
- Business goal: QE backtests use the same hard-risk semantics as Paper v2: block buys outside ST PIT eligibility and force sell existing holdings after PIT exit, while keeping market no-fill states explicit
- Out of scope: announcement-risk event ingestion/provider, UI controls for risk policy, production data replacement, live QMT execution
- Protected assets reviewed: no StrategyPackage manifest, QE workspace artifact, model weight, validated execution policy, HMM snapshot, or production Qlib dataset was modified

## Environment

- Backend port: not started; unit/config validation only
- Frontend port: not started; no UI change in this slice
- TDX port: not used
- Conda/env: local Python/pytest in AIstock repo, `PYTHONIOENCODING=utf-8`, `PYTHONDONTWRITEBYTECODE=1`
- Database: not mutated; risk artifact DB build path covered by monkeypatch/config tests
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 py_compile | Changed Python files compile without syntax errors | `python -m py_compile ...` | PASS |
| QE config truth | Risk policy wraps supported strategies, writes runtime kwargs, and prepares auto quote universe | `pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider` -> 41 passed | PASS |
| Runtime helper | Local artifact filters blocked buys and returns forced exits with strict date coverage | `test_qe_event_risk_policy_filters_buys_and_marks_forced_exits` | PASS |
| Selection/Paper regression | Existing Selection Center/Paper v2 unified risk behavior remains valid | combined pytest -> 146 passed | PASS |
| Trading/Strategy regression | Trading Core and StrategyPackage runtime remain valid | pytest -> 91 passed | PASS |
| Asset safety | No protected assets or production datasets changed | Git diff limited to framework/tests/docs plus pre-existing dirty workspace | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; python -m py_compile scripts/qe_event_risk_policy.py scripts/qe_suspend_filter_strategy.py scripts/qe_suspend_filter_score_weighted_strategy.py backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/trading_core backend/tests/strategy_package -q -p no:cacheprovider
git diff --check -- backend/services/quantevolver/config_composer.py backend/tests/unified_engine/test_qe_config_truth.py scripts/qe_event_risk_policy.py scripts/qe_suspend_filter_strategy.py scripts/qe_suspend_filter_score_weighted_strategy.py backend/services/selection_center/risk_policy.py backend/services/selection_center/runtime_profile.py backend/services/selection_center/service.py backend/services/selection_center/models.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/readiness.py backend/services/paper_trading_v2/live_session.py backend/services/paper_trading_v2/service.py backend/services/strategy_package/runtime.py backend/tests/selection_center/test_risk_policy.py backend/tests/paper_trading_v2/test_day_runner.py backend/tests/paper_trading_v2/test_runtime_profile.py docs/codex_project_memory.md
```

## Evidence

- API calls: not applicable in this backend/QE config slice
- DB checks: no DB mutation; `_prepare_risk_policy_runtime` covered with monkeypatched artifact builder
- Log files: not generated
- Playwright report/trace: not applicable
- Screenshots: not applicable
- Business output summary: QE now freezes `qe_event_risk_policy.json`, auto-expands Exchange `codes`, filters buy scores, and appends explicit forced-exit sell orders for held symbols outside ST PIT eligibility

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None | N/A | N/A | All targeted tests passed |

## Result

- Final status: PASS for framework/config/runtime unit validation
- Remaining risks: no full manual QE backtest with an actual forced-exit ST holding scenario was run in this turn; announcement-risk provider remains intentionally fail-fast
- Need production backend restart: no
- Need dev service restart: no
