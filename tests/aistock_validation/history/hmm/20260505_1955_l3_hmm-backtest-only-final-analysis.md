# HMM backtest-only final analysis - 2026-05-05 19:55

## Task

- Backtest-only task: `qe_20260505_170914_a010`
- Source trained-model task: `qe_20260505_123035_bf80`
- Node: `rdagent-node1`
- Execution mode: `parallel_4`
- Final status: `completed`, 7/7 loops completed.

## Validation Result

The backtest-only run reproduced the source task metrics to floating-point precision.

- Max absolute metric diff vs source across annualized return, Sharpe, max drawdown, IC, ICIR and RankIC: `1.33e-15`.
- All 7 loops have `mlruns` as symlinks to the corresponding source loop.
- All 7 run logs include `qrun_limit_minute.py conf.yaml --backtest-only && python read_exp_res.py`.
- All 7 run logs include `Backtest-only mode: skipping model training, loading existing model`.

Evidence:

- Loop comparison CSV: `.codex_tmp/hmm_backtest_only_qe_20260505/final_analysis/loop_comparison.csv`
- Loop comparison JSON: `.codex_tmp/hmm_backtest_only_qe_20260505/final_analysis/loop_comparison.json`
- Remote backtest-only proof: `.codex_tmp/hmm_backtest_only_qe_20260505/final_remote_backtest_only_proof.json`
- Attribution report: `.codex_tmp/hmm_backtest_only_qe_20260505/attribution/qe_20260505_170914_a010/hmm_qe_candidate_attribution.md`

## Loop Ranking

| Loop | Candidate | Annualized | Sharpe | MaxDD | Delta annualized vs no-HMM | Snapshot |
| --- | --- | ---: | ---: | ---: | ---: | --- |
| 3 | Loop10 penalty-only f096 | 0.480163 | 2.076938 | -0.157395 | +0.018046 | `6ea64754-003d-48d8-ad9e-d0e7857716c8` |
| 2 | Loop2 old-covfix w3 raw | 0.475617 | 2.064530 | -0.155894 | +0.013500 | `bbec3863-fb67-445f-938e-66f092d18696` |
| 1 | no-HMM control | 0.462117 | 1.994239 | -0.165808 | 0.000000 | none |
| 4 | Stage3 FBT robust penalty | 0.449764 | 1.953671 | -0.164541 | -0.012353 | `1d7ca1b9-fa65-4a97-9726-a6896f168121` |
| 6 | Stage3 FBT aggressive | 0.445865 | 1.910707 | -0.166404 | -0.016252 | `76267343-d182-4f12-974e-b8bfacfa56ee` |
| 7 | Stage3 turnover-light penalty | 0.444493 | 1.924343 | -0.162916 | -0.017624 | `ef6e044a-095c-46a4-8c91-fcecf107764a` |
| 5 | Stage3 FBT symmetric | 0.441807 | 1.913826 | -0.163839 | -0.020309 | `858b2ed9-4089-467f-98f0-54e6ce6b06b6` |

## Diagnosis

1. The fixed backtest-only path is valid for QE HMM validation when base model factors, Alpha158 flag and label horizon are unchanged.
2. The experiment does not change the ranking from the source task: Loop10 remains best by annualized return and Sharpe; Loop2 has the best max drawdown among HMM candidates.
3. Stage3 retrained candidates remain invalid for promotion because all four underperform no-HMM and both retained baselines.
4. HMM uplift is post-ranking/portfolio uplift only: IC, ICIR and RankIC are identical across loops because the base prediction model is reused and HMM only adjusts selection/ranking after prediction.
5. Turnover is not the reason Loop10 wins or loses: Loop10 average turnover is 0.082781, lower than no-HMM 0.083869 and Loop2 0.084421; Stage3 turnover-light reduces turnover further but sacrifices too much return.
6. Current cost diagnostics report `cost_drag_annualized=0.0`, so this run is not sufficient for real transaction-cost sensitivity; a cost-stress rerun is still needed before final promotion into any live-like workflow.

## Next R&D Direction

### Immediate Cleanup / Promotion

- Keep only no-HMM baseline, Loop2 old-covfix and Loop10 penalty-only in the active QE selector.
- Keep Loop10 as current best candidate and Loop2 as drawdown/control candidate.
- Keep Stage3 underperformers hidden/disabled; do not spend more full QE time on these exact snapshots.

### Backtest-only As The New HMM Validation Mode

Use backtest-only for HMM snapshot/coefficients experiments that do not change the base QE model feature list. This is now proven safe and reproducible.

Applicable:

- New HMM snapshot already trained and registered.
- Coefficient mapping/remapping variants.
- Sector-factor gate/confirmation overlays that only affect HMM coefficients/ranking.
- Transaction cost / turnover stress with same base model.

Not applicable:

- Changing the base QE factor list.
- Changing Alpha158 inclusion.
- Changing label horizon.
- Evaluating a new base stock-prediction model.

### Candidate R&D Tracks

1. Loop10-centered mapping search: local/script screen first, then backtest-only QE for 3-5 finalists.
2. Regime-conditional HMM: keep Loop10 base coefficients but vary penalty strength by market/sector breadth and volatility regime.
3. Sector-factor-gated HMM: revisit sector-factor candidates as HMM gates, not as stock alpha replacements, and only after retraining the HMM/coefficient snapshot when inputs change.
4. Cost-aware HMM objective: add turnover/cost penalties to HMM candidate screening because current Stage3 turnover reductions did not translate into higher net returns.
5. Robustness suite: run best candidates across alternate backtest periods, higher cost assumptions, TopK sensitivity, and seed/model-source sensitivity.

## Recommended Next Experiment

Run a small backtest-only QE batch, not full retraining:

- Loop1: no-HMM baseline.
- Loop2: Loop10 current best.
- Loop3: Loop2 drawdown control.
- Loop4-6: top 3 Loop10-centered mapping variants from script-level screen.
- Optional Loop7-8: Loop10 cost-aware/turnover-penalized variants.

Only after a variant beats Loop10 on annualized return or materially improves drawdown/cost without losing too much return should it be promoted to a larger multi-period validation.
