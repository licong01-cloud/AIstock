# HMM Sector-Factor Retraining Diagnostic - 2026-05-04

## Scope

- Module: HMM / QE pre-validation
- Level: L2 script-level diagnostic
- Goal: verify that sector-factor combinations are appended to HMM observations before `GaussianHMM.fit`, then evaluate sector-rotation signals before any QE task is launched.
- Safety boundary: no HMM registry writes, no snapshot registration, no QE experiment creation.

## Commands

```bash
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_retrain_diagnostic_20260504.py --output-dir .codex_tmp/hmm_sector_factor_retrain_20260504"
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/hmm_sector_factor_retrain_diagnostic_20260504.py
rg -n "INSERT|UPDATE|DELETE|requests\.|8000|8001|quantevolver|model_train_|register|submit_qe|POST|http://" scripts/hmm_sector_factor_retrain_diagnostic_20260504.py
```

## Evidence

- Summary: `.codex_tmp/hmm_sector_factor_retrain_20260504/summary.csv`
- Report: `.codex_tmp/hmm_sector_factor_retrain_20260504/report.md`
- Daily RankIC: `.codex_tmp/hmm_sector_factor_retrain_20260504/daily_rankic.csv`
- State metrics: `.codex_tmp/hmm_sector_factor_retrain_20260504/state_metrics.csv`
- Per-candidate model diagnostics: `.codex_tmp/hmm_sector_factor_retrain_20260504/models/*/model_diagnostics.json`

## Results

- Data loaded: 131 L2 sectors, 181,638 legacy HMM observation rows, 182,529 sector-factor panel rows.
- Candidates trained: 8.
- Per candidate trained sectors: 131.
- Non-baseline proof: `fit_proof == sector_features_appended_before_GaussianHMM.fit` and `fit_call_count == trained_sector_count`.
- Best holdout weighted RankIC: `flow_core` at 0.013238.
- Best baseline: `baseline_legacy7_winsor_zscore` at 0.010451.
- Weak/rejected in this pass: `turnover_flow_core`, `turnover_core`, `all_core`, and `baseline_legacy7_raw` had negative holdout weighted RankIC.

## Residual Risks

- This is not a QE backtest and does not validate stock-level PnL, turnover, trade costs, or minute execution interaction.
- Validation labels are calibrated on 2024-07-01 to 2025-03-31; conclusions should be treated as candidate screening only.
- Several candidates have holdout state monotonicity failures despite positive RankIC; QE should only be launched after user confirmation.
