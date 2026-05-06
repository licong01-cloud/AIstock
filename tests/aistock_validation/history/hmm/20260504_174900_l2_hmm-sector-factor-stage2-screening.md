# HMM Sector-Factor Stage2 Screening - 2026-05-04

## Scope

- Module: HMM / QE pre-validation
- Level: L2 script-level diagnostic
- Goal: continue HMM-focused R&D by retraining sector HMMs with additional sector-factor observation sets, then screen useful variants before any QE experiment.
- Safety boundary: no HMM snapshot registration, no model registry writes, no QE task submission.

## Changes Tested

- Extended `scripts/hmm_sector_factor_retrain_diagnostic_20260504.py` with:
  - flow ablations: `flow_std_only`, `small_net_only`
  - flow alternatives: `flow_stability_alt`, `flow_tier_core`
  - blended candidates: `flow_plus_vol_defensive`, `flow_plus_breadth`
  - score methods: `trend_fade`, `label_ordinal`, `utility_z`, `utility_raw`
- Continued to require that sector factors enter the HMM observation matrix before `GaussianHMM.fit`.

## Commands

```bash
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_retrain_diagnostic_20260504.py --output-dir .codex_tmp/hmm_sector_factor_stage2_diag3_20260504 --candidates baseline_legacy7_winsor_zscore flow_core flow_std_only small_net_only flow_stability_alt flow_tier_core flow_plus_vol_defensive flow_plus_breadth vol_compress"
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_retrain_diagnostic_20260504.py --output-dir .codex_tmp/hmm_sector_factor_stage2_diag2_20260504 --n-states 2 --candidates baseline_legacy7_winsor_zscore flow_plus_breadth flow_std_only flow_core vol_compress"
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_sector_factor_retrain_diagnostic_20260504.py --output-dir .codex_tmp/hmm_sector_factor_stage2_diag4_20260504 --n-states 4 --candidates baseline_legacy7_winsor_zscore flow_plus_breadth flow_std_only flow_core vol_compress"
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/hmm_sector_factor_retrain_diagnostic_20260504.py
rg -n "INSERT|UPDATE|DELETE|requests\.|8000|8001|quantevolver|model_train_|register|submit_qe|POST|http://" scripts/hmm_sector_factor_retrain_diagnostic_20260504.py
```

## Evidence

- Aggregate report: `.codex_tmp/hmm_sector_factor_stage2_aggregate_20260504/report.md`
- Aggregate summary: `.codex_tmp/hmm_sector_factor_stage2_aggregate_20260504/summary_all_runs.csv`
- Aggregate score-method summary: `.codex_tmp/hmm_sector_factor_stage2_aggregate_20260504/score_method_all_runs.csv`
- Stage2 diag3 run: `.codex_tmp/hmm_sector_factor_stage2_diag3_20260504/summary.csv`
- Stage2 diag2 run: `.codex_tmp/hmm_sector_factor_stage2_diag2_20260504/summary.csv`
- Stage2 diag4 run: `.codex_tmp/hmm_sector_factor_stage2_diag4_20260504/summary.csv`

## Results

- Best candidate: `flow_plus_breadth`, `n_states=3`, `covariance_type=diag`, `score_method=utility_raw`, holdout weighted RankIC 0.026085.
- Close simpler candidate: `flow_core`, `n_states=2`, `diag`, `utility_raw`, holdout weighted RankIC 0.025519.
- Baseline improved by utility scoring but still below top flow candidates: `baseline_legacy7_winsor_zscore`, `n_states=2`, `diag`, `utility_raw`, holdout weighted RankIC 0.024296.
- Defensive/risk candidate: `vol_compress`, best holdout weighted RankIC 0.022138 and better 10D hit/spread profile, but not top overall.
- Rejected in this pass: 4-state flow/breadth variants for primary selection, pure small-net variants, and earlier turnover-heavy variants.

## Residual Risks

- This remains script-level sector-rotation screening, not QE stock-level PnL validation.
- `utility_raw` materially improves RankIC, so QE registration must support posterior/utility-derived coefficients; using old fixed trend/fade coefficients would not reproduce these results.
- Tied covariance was started but stopped due runtime cost and removed from aggregate outputs; diag covariance remains the screened model family.
