# Paper v2 QE Runtime Contract Enforcement

Date: 2026-05-05
Status: implemented
Scope: StrategyPackage, Selection Center runtime profile, Paper Trading v2 day/readiness/live/session paths

## Background

Paper v2 is only meaningful if it replays the same strategy contract that QE backtested. The frozen StrategyPackage can freeze factor/model lineage, but the trading-affecting runtime contract must remain identical to QE unless a new QE/backtest package version validates a different contract.

This change closes the previous gap where Paper v2 could:

- build target positions through Paper-only equal-weight TopK logic while the QE package used `score_weighted_topk_v1/v2`;
- use a Paper execution policy different from `manifest.minute_execution_policy`;
- let runtime profiles change TopK, HMM, blacklist, suspension filtering, or risk switches outside the QE contract.

## Contract Authority

The authoritative contract is the frozen `StrategyPackageManifest`:

- portfolio strategy marker and params from `manifest.strategy_config.strategy_id` and `strategy_config.custom_params`;
- portfolio TopK / n_drop from `manifest.portfolio_policy` when absent from `custom_params`;
- HMM, industry blacklist, suspend filtering, and risk-policy runtime features from `custom_params`;
- minute execution policy from `manifest.minute_execution_policy`.

`backend/services/strategy_package/backtest_contract.py` normalizes this into `qe_paper_runtime_contract_v1` and is the shared enforcement entry point.

## Enforced Behavior

Paper v2 now applies these rules:

- Portfolio creation must validate the manifest minute execution policy for Paper runtime readiness.
- Requested validated execution policies must exactly match `manifest.minute_execution_policy`.
- StrategyPackage execution-policy paper-enable rejects non-matching policy JSON.
- Paper execution-policy listing reports non-matching policies as `can_enter_paper=false` with a structured error.
- Execution-policy activations reject policy JSON that differs from the frozen manifest.
- Runtime-profile creation/versioning/activation rejects TopK, HMM, blacklist, suspend, or risk-policy conflicts.
- Day runner, readiness, live session, and session creation re-check the contract at execution time.
- Runtime config normalization fills missing QE runtime fields into Paper runtime config before execution.

## Paper Target Adapter

`TargetPositionEngine.build_targets(..., manifest=...)` uses the QE backtest contract instead of artifact-provided equal weights.

Supported strategy families in this implementation:

- `score_weighted_topk_v1`
- `score_weighted_topk_v2`
- suspend-filter wrappers for the same families

Implemented semantics include:

- rank/score ordered candidate universe;
- score-weighted target weights for `equal`, `rank`, `linear`, and `softmax` methods;
- min/max weight clipping and `max_position_ratio` scaling;
- dynamic n-drop thresholding;
- V1 refill-without-sell mode;
- V2 ghost holding sell targets;
- hold-threshold sell blocking;
- explicit zero-quantity sell targets for strategy sells;
- current price requirement for retained holdings, avoiding avg-cost/default-price fallback.

Unsupported future QE strategy families must be added to `backtest_contract.py` before Paper can run them. They must fail fast until implemented.

## Runtime Feature Mapping

HMM:

- If QE enabled HMM, Paper must execute HMM with the same snapshot/version, preset, and coefficient artifact.
- If required HMM contract fields are missing, Paper fails fast because it cannot prove execution equivalence.
- If QE did not enable HMM, Paper cannot enable it.

Industry blacklist:

- Paper runtime blacklist must equal the QE contract.
- Missing Paper blacklist is filled from the QE contract.

Tradability / suspension:

- Paper `tradability.exclude_suspended` must match QE signal-side suspension filtering.

Risk policy:

- Paper risk-policy enablement must match QE.
- If QE enabled risk policy, Paper can execute forced exits and buy blocks using the same runtime risk profile.

## Operational Impact

Existing Paper portfolios that carry a mismatched execution policy or runtime profile may now fail fast. This is expected: those portfolios were not QE-contract-faithful and should be recreated from a package whose frozen manifest matches the intended runtime behavior.

Stock-pool freeze is intentionally not included here because the QE stock-pool freeze design is still being finalized. When QE finalizes stock-pool freeze semantics, Paper v2 must treat it as another field in the same backtest contract.

## Validation Record

Detailed validation evidence is stored in:

`tests/aistock_validation/history/paper_v2_selection_center/20260505_l3_paper-v2-qe-runtime-contract-enforcement.md`

Key commands passed:

- `python -m py_compile` on changed Paper/StrategyPackage modules.
- `pytest backend/tests/selection_center/test_risk_policy.py backend/tests/paper_trading_v2 backend/tests/selection_center backend/tests/strategy_package backend/tests/unified_engine/test_qe_config_truth.py -q -p no:cacheprovider` -> 180 passed after rebase onto `origin/main`.
- `conda run -n AIstock python -m nox -s paper_v2_backend` -> 137 passed after rebase onto `origin/main`.
- `conda run -n AIstock python -m nox -s paper_v2_data_quality` -> successful; dataset audit freshness gates passed, with a pre-existing non-strict legacy ledger warning.
- `conda run -n AIstock python -m nox -s l0` -> successful, blocking guardrail count 0 after generating the local guardrail baseline required by the clean push worktree.

Production backend port `8001` was not restarted.
