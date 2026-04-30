# HMM Horizon-Aware v2 Optimization, Training, and Script Backtest Plan

Date: 2026-04-28
Scope: additive HMM research/training workflow only. Existing HMM scripts, model files, coefficient files, QE workspaces, StrategyPackage assets, and Paper Trading assets must not be overwritten.

## 1. Background

The current repaired HMM (`HMM_COVFIX_w5_zscore_candidate__n3_diag_rw5_zscore`) fixed several technical problems, including covariance outliers and unstable transition matrices, but the authoritative QE c5b2 analysis showed that the fixed `preset_A` still degraded portfolio performance. The failure mode is not model training, label, IC, or V25 execution. It is the mismatch between HMM state coefficients and the true holding horizon.

Key evidence from the c5b2 report:

- Loop1 and Loop2 raw predictions and labels are effectively identical.
- Loop2 changed HMM snapshot/preset and lost about 29.14M final NAV versus Loop1.
- The new HMM `trending` state is only slightly positive at 1D, but negative at 3D, 5D, 10D, and 20D.
- The strategy holding median is about 4 trading days, and P90 exceeds 40 trading days.
- Therefore, fixed `trending=1.05` is not valid for the actual trading horizon.

## 2. Design Principles

1. Do not overwrite existing HMM training scripts or model artifacts.
2. Use additive scripts and unique output paths/display names.
3. Train and validate HMM with 5D/10D/20D horizon utility as the primary objective.
4. Avoid `limit_up_ratio` in v2 features unless a PIT-consistent runtime feature source is guaranteed.
5. Calibrate coefficients per snapshot from validation forward returns; never assume `trending=1.05`.
6. Use script-only validation and do not launch QE experiments.
7. Keep full trace artifacts for future QE/Paper runtime comparison.

## 3. New HMM v2 Training Specification

### 3.1 Split

For the first out-of-sample validation snapshot:

- Train: 2022-09-01 to 2025-05-30
- Validation/calibration: 2025-06-02 to 2025-08-29
- Six-month script backtest: 2025-09-01 to 2026-03-03

This split keeps the new v2 training and coefficient calibration before the six-month backtest window.

### 3.2 Observation Features

Use DB/PIT-available L2 sector features only:

1. `daily_return`
2. `excess_return_5d_mean`
3. `excess_return_10d_mean`
4. `excess_return_20d_mean`
5. `volatility_5d`
6. `volatility_10d`
7. `volatility_20d`
8. `volume_share_5d_mean`
9. `net_mf_ratio_5d_mean`
10. `elg_net_mf_ratio_5d_mean`

Excluded from v2: `limit_up_ratio`, because earlier diagnostics identified it as the dominant covariance anomaly source and some precompute paths do not consistently supply it.

### 3.3 Preprocessing

- Winsorize observations by global train-set quantiles, default 1% and 99%.
- Apply global z-score using training data only.
- Persist `feature_names`, `winsor_lower`, `winsor_upper`, `zscore_mean`, and `zscore_std` into `models.json`.
- Use `n_states=3`, `covariance_type=diag`, `n_iter=300`.
- Use transition smoothing with `min_self_trans=0.75` so state persistence better matches 5D/10D/20D usage than the previous 0.30 floor.

### 3.4 State Labeling

Do not label states by same-day or 1D return alone. For each sector, label hidden states using train-window forward excess-return utility:

```text
state_utility = 0.35 * forward_excess_5d +
                0.35 * forward_excess_10d +
                0.30 * forward_excess_20d
```

Highest utility state becomes `trending`, lowest becomes `fading`, middle becomes `neutral`.

### 3.5 Coefficient Calibration

Validation labels are decoded using forward filtering. Aggregate validation utility by state label, then map centered utilities to coefficients:

```text
coefficient[label] = clip(1 + lambda * z(label_utility), 0.97, 1.03)
```

Defaults:

- horizon weights: 5D = 0.35, 10D = 0.35, 20D = 0.30
- lambda = 0.015
- coefficient bounds = 0.97 to 1.03

This is intentionally more conservative than `1.05/0.96`.

## 4. Script Deliverables

### 4.1 Training Script

New file: `scripts/hmm_horizon_v2_train.py`

Responsibilities:

- Load L2 sector data and CSI300/market volume from local DB.
- Build v2 feature matrix.
- Train per-sector HMMs.
- Label states by 5D/10D/20D utility.
- Validate and calibrate coefficients.
- Save `models.json`, coefficient artifact, `training_result.json`, and optional DB snapshot rows.

### 4.2 Backtest Script

New file: `scripts/hmm_horizon_v2_compare.py`

Responsibilities:

- Load all active HMM snapshots from DB plus the newly trained v2 asset.
- Build or read daily sector coefficients for each version.
- Run a script-only six-month Top50 comparison using a shared causal raw score.
- Report Raw/no-HMM, old baseline, covfix same-params, covfix w5/zscore, and horizon v2.
- Output JSON, CSV, and Markdown report artifacts.

## 5. Six-Month Script Backtest Method

This validation intentionally does not start QE experiments. It is a standalone, causal stock-selection proxy:

- Universe: A-share daily rows with valid close and volume.
- Raw score at signal date: weighted trailing returns using only past data.
- Main raw score weights: 5D = 0.35, 10D = 0.35, 20D = 0.30.
- HMM adjustment: `adjusted_score = raw_score * sector_coefficient` for version-compatible first pass.
- Main portfolio simulation: Top50 equal-weight, 5-trading-day rebalance, next-period realized returns from close-to-close prices.
- Diagnostics: also compute Top50 overlap, HMM-only/raw-only forward labels, monthly returns, state distribution, and final holdings.

Limitations:

- This is not a QE model backtest and does not include V25 minute execution or unfilled-order simulation.
- It is meant to compare relative HMM overlay behavior, not to replace full QE validation.
- Existing snapshots whose train/validation dates overlap the six-month backtest will be flagged as diagnostic-only if applicable.

## 6. Acceptance Criteria

The new horizon-aware v2 HMM is considered initially promising only if:

1. Validation `trending` utility is positive for the 5D/10D/20D weighted target.
2. The calibrated coefficient for a negative validation utility state is not greater than 1.0.
3. Six-month script Top50 results are not materially worse than Raw/no-HMM.
4. Six-month script results beat or are competitive with at least one prior HMM version.
5. HMM-only selections have equal or better forward 5D/10D/20D labels than raw-only selections.
6. All artifacts are new and old assets remain unchanged.

## 7. Expected Artifacts

- `docs/analysis/hmm_horizon_optimization_training_plan_20260428.md`
- `scripts/hmm_horizon_v2_train.py`
- `scripts/hmm_horizon_v2_compare.py`
- `backend/data/hmm_models/<new_config_id>/<snapshot_date>/models.json`
- `backend/data/hmm_models/<new_config_id>/<snapshot_date>/coefficients_preset_horizon_v2_<start>_<end>.json`
- `.codex_tmp/hmm_horizon_v2_backtest_*.json`
- `.codex_tmp/hmm_horizon_v2_backtest_*.md`

## 8. Operational Notes

- Run scripts in WSL `rdagent-gpu`.
- Use local DB credentials from environment where possible; scripts accept `--db-password` for explicit local runs.
- Do not restart AIstock backend services.
- Do not modify production port 8001.
