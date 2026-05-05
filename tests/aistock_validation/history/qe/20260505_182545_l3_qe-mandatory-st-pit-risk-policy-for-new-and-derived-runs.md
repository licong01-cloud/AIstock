# QE mandatory ST PIT risk policy for new and derived runs

- Module: qe
- Level: L3
- Date: 2026-05-05T18:25:45+08:00
- Git commit: recorded in final CLI report; this validation record is part of the feature commit
- Operator: lc999

## Scope

- Changed flows: new single QE config generation/run, existing-experiment rerun through unified executor, standard evolution retry, strategy/custom evolution loop generation, custom-evo create/clone/rerun/append, Multi-Alpha group/root backtest generation, Selection Center, Paper Trading v2, and QE Qlib strategy wrappers.
- Business goal: every newly generated QE runnable config must use the ST PIT event-risk policy with `block_buy` and `force_exit`; completed historical experiments are not migrated.
- Runtime goal: QE, Selection Center, and Paper v2 share the same event-risk contract so future announcement-risk providers can add buy blocks, forced exits, and score overlays without a new strategy architecture.
- Backtest validation goal: reuse an existing trained QE model in backtest-only mode on WSL, avoid production backend `8001`, reproduce and fix the Qlib `$close None!!!` warnings, and verify ST PIT + suspend filtering in a realistic loop.
- Out of scope: no production dataset mutation in this validation record; no completed QE experiment backfill; no production backend `8001` restart.
- Protected assets reviewed: no StrategyPackage manifests, model weights, HMM snapshots, validated execution policies, or production Qlib datasets were edited by this validation pass. The backtest-only QE workspaces are generated validation artifacts only.

## Environment

- Production backend: `8001` was not restarted or modified.
- Test backend: `8012` was restarted for API validation; final successful test service PID was `103548`.
- WSL/RD-Agent workspace API: `9000`, used as the authoritative QE backtest status source.
- Frontend port: not used in this QE validation pass; frontend build was run separately.
- Conda/env: current PowerShell Python/pytest environment with `PYTHONIOENCODING=utf-8`, `PYTHONDONTWRITEBYTECODE=1`; WSL `rdagent-gpu` for QE execution.
- Database: read-only for risk/suspend artifact preparation during validation; no dataset replacement or protected asset mutation.
- Startup issue fixed during validation: one dev-backend run needed `PYTHONIOENCODING=utf-8` to avoid Windows console UTF-8 output failure.

## Test Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 compile | Changed QE/risk scripts and tests compile | `python -m py_compile ...` | Pass |
| QE config defaults | Missing `risk_policy` becomes mandatory ST PIT policy | `test_build_custom_params_empty`, `test_qe_risk_policy_runtime_defaults_and_overwrites_stale_quote_universe` | Pass |
| Disabled policy guard | Explicit `risk_policy.enabled=false` fails fast for new QE configs | `test_build_custom_params_rejects_disabled_risk_policy`, `test_qe_risk_policy_runtime_rejects_disabled_policy` | Pass |
| Derived custom-evo flows | Create/clone/rerun/append prepared loop configs carry ST PIT policy | custom-evo route suite | Pass |
| Suspend side effect | Mandatory risk policy also forces signal-side `suspend_d` filtering | `test_qe_risk_policy_forces_outer_strategy_suspend_filter_kwargs`, generated `conf.yaml` | Pass |
| QE runtime artifact | Backtest config includes ST PIT artifact and suspend artifact | final task `qe_20260505_200632_a357` workspace files | Pass |
| Qlib warning regression | `$close None!!!` and custom invalid-price warnings are eliminated | final `run.log` warning counts | Pass |
| Selection/Paper regression | Unified risk policy remains compatible with Selection Center and Paper v2 | selection/paper pytest suite | Pass |
| Trading-core regression | Trading Core and StrategyPackage tests remain green | trading-core/strategy-package pytest suite | Pass |
| Frontend compile | Local data/UI changes still compile | `npm run build` in `frontend` | Pass |
| Asset safety | No protected assets modified or production service restarted | Git diff/file review; port review | Pass |

## QE Backtest-Only Validation

- Source trained task: `qe_20260505_153534_388f`.
- Source loop: `1`.
- Existing model recorder: `8fdfd04a826a40f89838a9cc26b4a0de`.
- Final validation task: `qe_20260505_200632_a357`.
- Final workspace: `F:/Dev/RD-Agent-main/qe_workspace/qe_20260505_200632_a357/Loop1`.
- WSL status: `completed`.
- Backend task API status after `8012` restart: `completed`.
- Backtest-only log evidence: `Backtest-only mode: skipping model training, loading existing model`, `Loaded trained model from recorder 8fdfd04a826a40f89838a9cc26b4a0de`, and `Backtest-only completed successfully`.

### Reproduced Failures And Fixes

| Task | Observed failure | Root cause | Fix |
|---|---|---|---|
| `qe_20260505_191602_0c65` | Qlib `$close None!!!` warnings for suspended/missing-close symbols such as `300165.SZ` and `002494.SZ` | Mandatory risk policy wrapped the strategy but did not force signal-side suspend filtering when inherited params disabled it | Risk policy now forces suspend filter kwargs and prepares risk runtime before suspend filter runtime |
| `qe_20260505_193821_6344` | Custom `??????` warning for `002816.SZ` | ScoreWeighted V2 wrapper attempted price/order logic before suspend/missing-close guard | Added explicit `QESuspendFilter.is_suspended()` alias and pre-price guards |
| `qe_20260505_195004_23a6` | Custom `??????` warnings for `002816.SZ` and `603656.SH` | Output orders still included symbols that became non-orderable after ranking/forced-exit composition | Final `TradeDecisionWO` order list is filtered through tradability guards; forced exits remain pending/no-fill only when market-state blocked |
| `qe_20260505_200632_a357` | No target warning reproduced | Runtime and strategy wrapper fixes active | Completed successfully |

### Final Warning Counts From `run.log`

| Pattern | Count | Interpretation |
|---|---:|---|
| `WARNING - qlib.online operator` | 0 | No Qlib exchange missing-deal-price warnings |
| `None!!!` | 0 | No missing `$close` order-pricing warning |
| `??????` | 0 | No custom invalid buy-price warning |
| `Traceback` | 0 | No Python traceback |
| `load calendar error` | 2 | Known Qlib future-calendar warning, not an ST PIT or price-data failure |

### Final Config/Artifact Evidence

- `conf.yaml` uses `class: SuspendFilterScoreWeightedTopkStrategyV2` and `module_path: qe_suspend_filter_score_weighted_strategy`.
- `conf.yaml` sets `filter_suspended_on_signal: true`, `risk_policy_enabled: true`, `risk_policy_file: qe_event_risk_policy.json`, and `suspend_filter_file: qe_suspend_filter.json`.
- `qe_event_risk_policy.json`: contract `stock_event_risk_policy_v1`, providers `['st_pit']`, hard actions `['block_buy', 'force_exit']`, universe key `shsz_st_pit_active_v1`, `trade_date_count=442`, `span_count=5082`, and `active_spans_len=5082`.
- `qe_suspend_filter.json`: source `market.suspend_d`, `strict_audit=true`, `trade_date_count=442`, `suspended_row_count=6203`, and `suspended_by_date_len=442`.

### Final Metrics

| Metric | Value |
|---|---:|
| IC | `0.07311295385106273` |
| ICIR | `0.66166418700786` |
| Rank IC | `0.11293968232922287` |
| Rank ICIR | `0.8913690213260986` |
| Excess annualized return with cost | `0.312357297256864` |
| Excess IR | `1.266146373532587` |
| Excess max drawdown | `-0.18198432341137405` |
| Absolute CAGR | `0.597964` |
| Absolute Sharpe | `1.6547` |
| Final NAV/value | `2.275359` / `22,753,585.35` |
| Final stock count | `20` |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; python -m py_compile scripts/qe_suspend_filter.py scripts/qe_suspend_filter_score_weighted_strategy.py
# Pass, no output

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
# 45 passed in 8.68s

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/unified_engine/test_experiment_config.py backend/tests/unified_engine/test_backtest_executor.py backend/tests/unified_engine/test_qe_config_truth.py backend/tests/unified_engine/test_custom_evo_mutation_routes.py -q -p no:cacheprovider
# 108 passed in 17.75s

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/selection_center backend/tests/paper_trading_v2 backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider
# Original validation: 151 passed in 12.73s; clean-worktree rerun on HEAD 39f2a17 with feature patch: 149 passed in 20.09s

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/trading_core backend/tests/strategy_package -q -p no:cacheprovider
# Original validation: 91 passed in 1.26s; clean-worktree rerun on HEAD 39f2a17 with feature patch: 88 passed in 1.38s

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; pytest backend/tests/test_stock_universe_pit_service.py backend/tests/test_stock_universe_pit_spans.py backend/tests/test_authoritative_bin_pit_universe.py backend/tests/selection_center/test_risk_policy.py -q -p no:cacheprovider
# Clean-worktree rerun on HEAD 39f2a17 with feature patch: 10 passed in 1.07s

cd frontend; npm run build
# Pass; local-data, validation-center, QE, and Paper v2 routes compiled

$env:PYTHONIOENCODING='utf-8'; $env:PYTHONDONTWRITEBYTECODE='1'; python -m py_compile scripts/qe_suspend_filter.py scripts/qe_suspend_filter_score_weighted_strategy.py; pytest backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider
# Final pre-commit rerun in clean worktree on HEAD 39f2a17 with DB password: 43 passed in 17.64s
```

## Business Outcome

- New and derived QE runs get mandatory ST PIT risk policy even when cloned or retried from old experiments.
- QE risk policy now forces the signal-side suspend filter so selection/ranking does not produce symbols that Qlib cannot price on the trade day.
- ScoreWeighted V1/V2 wrappers block new buys outside ST PIT eligibility, generate forced-exit sells for existing holdings outside the active PIT universe, and avoid default-price/close-price fallback for suspended or missing-close market states.
- Selection Center and Paper v2 use the same normalized risk-policy contract, so ST PIT buy blocking and forced-exit semantics are compatible with future announcement-risk providers.
- Future announcement-risk integration can add `announcement_risk` as another provider with hard actions and score overlays without redesigning QE/Paper strategy boundaries.

## Residual Risks

- Production backend `8001` still needs a user-approved restart before the running production service loads these backend code changes.
- Completed historical QE experiments are intentionally not migrated; only new/derived/retried configs use the mandatory policy.
- `announcement_risk` remains a reserved fail-fast provider until announcement event tables/providers are implemented.
- Qlib `load calendar error` warnings remain known future-calendar lookups and were not treated as data-price/ST PIT failures.

## Clean Worktree Follow-up

- The feature patch was reapplied to detached clean worktree `F:/Dev/AIstock_stpit_commit` on base `39f2a17` to avoid staging unrelated local files.
- `backend.main` import still reports missing unrelated `backend.services.rl_execution` in this detached worktree; Prometheus admin files were copied because `backend/main.py` already imports that router. `pytest backend/tests/test_prometheus_admin.py -q -p no:cacheprovider` passed 5 tests.
