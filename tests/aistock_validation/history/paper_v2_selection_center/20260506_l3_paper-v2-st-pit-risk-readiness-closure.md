# Paper v2 ST PIT Risk Target And Readiness Closure

Date: 2026-05-06
Module: Paper Trading v2
Level: L3 backend/frontend regression
Worktree: `F:/Dev/AIstock_worktrees/selection-st-pit-health-20260506`
Branch: `codex/selection-st-pit-health-20260506`

## Objective

Continue the ST PIT alignment work without modifying QE shared implementation code. This closure is limited to Paper v2 runtime behavior:

- Risk-policy forced exits must override same-symbol QE-style target-engine sells without creating duplicate target rows.
- Day runner, readiness, and live-session target lists must produce one target per symbol before rebalance intent generation.
- Paper v2 readiness must match the DB historical day-run path by loading current prices for existing positions from DB minute bars before equity calculation.
- No production backend port `8001` was restarted.

## Implemented Scope

- Added `backend/services/paper_trading_v2/risk_targets.py` with `overlay_risk_forced_exit_targets(...)`.
- Updated Paper v2 day-runner, readiness, and live-session paths to overlay risk forced-exit targets instead of appending duplicate same-symbol targets.
- Updated Paper v2 readiness to load DB historical first-observed minute close for existing-position equity when `current_prices` are absent, matching the day-runner behavior.
- Added focused Paper v2 tests for forced-exit target overlay and readiness current-price loading.

## Automated Validation

The branch was merged with `origin/main` commit `2c64f078c3b6168f868f97ac59a653e3c86a5f28` and the key validation matrix was rerun after that merge.

```powershell
python -m py_compile backend/services/paper_trading_v2/risk_targets.py backend/services/paper_trading_v2/day_runner.py backend/services/paper_trading_v2/readiness.py backend/services/paper_trading_v2/live_session.py backend/tests/paper_trading_v2/test_risk_targets.py backend/tests/paper_trading_v2/test_day_runner.py
```

Result: passed.

```powershell
python -m pytest backend/tests/paper_trading_v2/test_risk_targets.py backend/tests/paper_trading_v2/test_day_runner.py::test_day_runner_risk_policy_blocks_buy_and_forces_existing_position_exit backend/tests/paper_trading_v2/test_day_runner.py::test_readiness_loads_db_price_for_existing_position_equity backend/tests/paper_trading_v2/test_day_runner.py::test_readiness_risk_policy_forced_exit_overrides_score_sell_target_once -q -p no:cacheprovider
```

Result: `4 passed in 1.18s`.

```powershell
python -m pytest backend/tests/paper_trading_v2 -q -p no:cacheprovider
```

Result: `69 passed in 1.37s`.

```powershell
python -m pytest backend/tests/selection_center backend/tests/strategy_package backend/tests/paper_trading_v2 -q -p no:cacheprovider
```

Result before merge: `145 passed in 33.12s`.
Post-merge rerun result: `145 passed in 15.53s`.

```powershell
python -m pytest backend/tests/unified_engine/test_qe_config_truth.py backend/tests/strategy_package/test_rebalance_runtime.py -q -p no:cacheprovider
```

Result before merge: `49 passed in 28.01s`.
Post-merge rerun result: `49 passed in 23.59s`.

```powershell
cmd /c frontend\node_modules\.bin\tsc.cmd -p frontend\tsconfig.json --noEmit --incremental false --pretty false
```

Result: passed.

```powershell
npm run build
```

Result: passed before and after merge. Next.js built 64 app routes, including `/paper-v2/selection`.

```powershell
git diff --check
```

Result: passed with only line-ending warnings for the Windows checkout.

Guardrail scans:

```powershell
credential scan over changed Paper v2 files
rg -n -i "(silent|fallback|pad|truncate|default price|fake success|empty.*success|qe_workspace|rdagent_workspace)" <changed Paper v2 runtime files>
```

Result: no findings.

## Business Outcomes Verified

- Risk-policy forced exit is the final operator-facing reason when a same-symbol QE-style ghost sell also exists.
- Target generation now keeps one target per symbol before rebalance intent generation; the day-runner `TARGETS_GENERATED` event reports one `000001.SZ` target in the forced-exit test.
- Paper readiness can calculate existing-position equity for DB historical runs without caller-supplied `current_prices`; it reads one DB minute bar and records a `current_position_prices` readiness check.
- Readiness forced-exit scenario produces exactly two targets and two order intents for the forced sell plus replacement buy.

## Scope And Asset Safety

- Modified code is limited to `backend/services/paper_trading_v2` plus Paper v2 tests and validation docs.
- No QE shared implementation files were modified.
- No StrategyPackage frozen manifests, QE/RD-Agent assets, model weights, HMM snapshots, Qlib datasets, validated policies, Paper ledgers, or production database data were modified.
- Production backend `8001` was not restarted or touched.

## Residual Risks

- This does not repair legacy/broken StrategyPackages such as `pkg_1de...`, `pkg_006...`, `pkg_991...`, or `pkg_b668...`; those still require rebuild/retirement or cache materialization repair.
- This does not add a literal Qlib strategy runtime comparison harness because QE shared/runtime code was intentionally not modified in this task.
- Production testing still requires backend restart/reload after the merged code is deployed.
