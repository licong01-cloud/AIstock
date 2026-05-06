# L2 HMM Coefficient Mapping Stage3 Validation - 2026-05-04

## Scope

- Module: HMM / QE research diagnostics.
- Goal: evaluate practical score-to-sector-coefficient mappings for retrained HMM sector-factor candidates before any QE registration.
- Boundary: file-only script diagnostics; no DB write, no HMM snapshot registration, no QE task submission, no production backend `8001` restart.

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/hmm_coefficient_mapping_diagnostic_20260504.py
rg -n "INSERT|UPDATE|DELETE|requests\.|8000|8001|quantevolver|model_train_|register|submit_qe|POST|http://|psycopg|sqlalchemy|create_engine" scripts/hmm_coefficient_mapping_diagnostic_20260504.py
rg -n "spearmanr|scipy|hmmlearn|GaussianHMM|INSERT|UPDATE|DELETE|requests\." scripts/hmm_coefficient_mapping_diagnostic_20260504.py
wsl bash -lc "cd /mnt/f/Dev/AIstock && source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python -u scripts/hmm_coefficient_mapping_diagnostic_20260504.py --output-dir .codex_tmp/hmm_coeffmap_stage3_20260504"
```

## Execution Notes

- The first unoptimized execution was stopped because per-date `scipy.stats.spearmanr` loops were too slow for 360 mapping combinations.
- The script was optimized to compute daily RankIC and coefficient diagnostics with grouped vectorized pandas operations, then recompiled and rerun successfully.
- Safety scans returned no DB/API/QE submission strings in the script.

## Evidence

- Report: `.codex_tmp/hmm_coeffmap_stage3_20260504/report.md`
- Ranked mappings: `.codex_tmp/hmm_coeffmap_stage3_20260504/mapping_summary_ranked.csv`
- Horizon summary: `.codex_tmp/hmm_coeffmap_stage3_20260504/mapping_summary_by_horizon.csv`
- Daily metrics: `.codex_tmp/hmm_coeffmap_stage3_20260504/daily_mapping_metrics.csv`
- Loaded panels: `.codex_tmp/hmm_coeffmap_stage3_20260504/loaded_score_panels.csv`
- Run config: `.codex_tmp/hmm_coeffmap_stage3_20260504/run_config.json`

## Data Coverage

- Loaded score panels: 15.
- Candidate families: `flow_plus_breadth`, `flow_core`, `flow_std_only`, `vol_compress`, `baseline_legacy7_winsor_zscore`.
- Mapping rows: 360 ranked holdout summaries.
- Horizon summary rows: 2,160.
- Daily metric rows: 468,000.

## Key Results

| Candidate | States | Score | Mapping | Range | Weighted RankIC | Spread | Hit Rate | Avg Abs Dev | Change Rate | Interpretation |
| --- | --- | --- | --- | --- | ---: | ---: | ---: | ---: | ---: | --- |
| flow_plus_breadth | 3 | utility_raw | cs_zscore_clip2 | aggressive_0p95_1p08 | 0.026153 | 0.000526 | 0.511191 | 0.024033 | 0.755824 | Best ordering, but very high coefficient churn. |
| flow_plus_breadth | 3 | utility_raw | val_zscore_clip2 | conservative_0p98_1p03 | 0.026101 | 0.000526 | 0.511191 | 0.009618 | 0.358837 | Best balanced 3-state mapping; churn still non-trivial. |
| flow_core | 2 | utility_raw | val_zscore_clip2 | conservative_0p98_1p03 | 0.025636 | 0.000268 | 0.509267 | 0.009282 | 0.209034 | Simpler low-change candidate. |
| flow_core | 2 | utility_raw | val_softsign | conservative_0p98_1p03 | 0.025519 | 0.000268 | 0.509267 | 0.009163 | 0.199086 | Lowest-churn practical flow candidate. |
| flow_std_only | 2 | utility_raw | val_zscore_clip2 | conservative_0p98_1p03 | 0.024298 | 0.000573 | 0.510634 | 0.009321 | 0.202143 | Slightly weaker RankIC, better spread than flow_core. |
| vol_compress | 4 | utility_raw | val_zscore_clip2 | conservative_0p98_1p03 | 0.022310 | 0.001899 | 0.516186 | 0.009221 | 0.322567 | Defensive/risk candidate; weaker ordering but better hit/spread. |

## Business Outcome

- `utility_raw_score` remains the primary HMM score for QE-candidate discussion; the older `hmm_score`/trend-fade style had lower best weighted RankIC around 0.02313.
- Best pure ordering candidate: `flow_plus_breadth`, `n_states=3`, `diag`, `utility_raw`, with z-score coefficient mapping.
- Practical shortlist before QE:
  - Primary: `flow_plus_breadth_n3_diag_utility_raw` with `val_zscore_clip2` + conservative or neutral range.
  - Low-change alternative: `flow_core_n2_diag_utility_raw` with `val_softsign` or `val_zscore_clip2` + conservative range.
  - Optional defensive: `vol_compress_n4_diag_utility_raw` with conservative range, only if QE wants a risk/defensive sector tilt test.
- QE registration should not use the old fixed coefficients alone; these candidates need utility-derived daily sector coefficients or QE runtime support for utility score mapping.

## Residual Risks

- This is still sector-level forward excess-return validation, not stock-level QE PnL.
- Turnover/cost/minute-execution effects are only proxied by coefficient change rates; they must be validated by QE after user approval.
- No snapshot artifact was registered and no UI-selectable HMM model was changed in this run.
