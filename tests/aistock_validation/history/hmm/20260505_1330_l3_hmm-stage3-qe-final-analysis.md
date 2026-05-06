# HMM Stage-3 QE Final Analysis - L3

## Task

- Task: `qe_20260505_123035_bf80`
- Status: `completed`
- Loops: 7 / 7 completed
- Remote node: `rdagent-node1`
- Execution mode: `parallel_4`
- Evidence root: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/`

## Main Result

Loop10 remains the best HMM version. None of the Stage-3 retrained HMM candidates beat Loop10 or the original Loop2 baseline; all Stage-3 candidates also underperformed the no-HMM control on annualized excess return.

| loop | label | annualized_return | delta_vs_loop10 | sharpe | max_drawdown | avg_turnover |
| ---: | --- | ---: | ---: | ---: | ---: | ---: |
| 1 | `NO_HMM__stage3_control_qe_20260502_131502_9b54_Loop1` | 0.462117 | -0.018046 | 1.994239 | -0.165808 | 0.083869 |
| 2 | `LOOP2_BASE__old_covfix_w3_raw__retained_control` | 0.475617 | -0.004546 | 2.064530 | -0.155894 | 0.084421 |
| 3 | `LOOP10_BASE__penalty_only_f096__retained_best` | 0.480163 | 0.000000 | 2.076938 | -0.157395 | 0.082781 |
| 4 | `STAGE3_FBT_ROBUST_N2_TF_PEN_0p96` | 0.449764 | -0.030399 | 1.953671 | -0.164541 | 0.080664 |
| 5 | `STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04` | 0.441807 | -0.038355 | 1.913826 | -0.163839 | 0.079171 |
| 6 | `STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08` | 0.445865 | -0.034298 | 1.910707 | -0.166404 | 0.079150 |
| 7 | `STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96` | 0.444493 | -0.035670 | 1.924343 | -0.162916 | 0.078305 |

## Interpretation

- Stage-3 retraining did not translate into QE trading improvement. The best Stage-3 candidate, Loop4, trails Loop10 by 3.04 percentage points annualized return and also trails no-HMM by 1.24 percentage points.
- The return loss is not explained by higher turnover. Stage-3 versions have lower turnover than Loop10, so the degradation is signal/ranking quality rather than cost pressure.
- Cost metrics are not discriminative in this task: annualized return with cost equals without cost and `cost_drag_annualized=0` for all loops.
- IC and Rank IC are identical across loops because the same LGB prediction model and factors are used; HMM only changes the post-prediction sector-adjusted portfolio ranking.
- Continuous Stage-3 mappings are too intrusive for this QE strategy. Symmetric/aggressive mappings make almost every sector/date non-neutral and underperform most.

## TopK Attribution

Holdout TopK attribution confirms why Loop10 still wins for this 10-day/TopK QE path:

| label | changed_days | avg_entered_per_day | net_db_ret_5d | net_db_ret_10d | net_db_ret_20d | QE annualized_return |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `LOOP2_BASE__old_covfix_w3_raw__retained_control` | 224 | 2.171548 | 0.002218 | 0.006756 | 0.011428 | 0.475617 |
| `LOOP10_BASE__penalty_only_f096__retained_best` | 212 | 1.799163 | 0.005135 | 0.008780 | 0.010319 | 0.480163 |
| `STAGE3_FBT_ROBUST_N2_TF_PEN_0p96` | 229 | 2.163180 | 0.001891 | 0.008208 | 0.016132 | 0.449764 |
| `STAGE3_FBT_ROBUST_N2_TF_SYM_0p96_1p04` | 239 | 4.146444 | -0.002874 | 0.002327 | 0.011577 | 0.441807 |
| `STAGE3_FBT_ROBUST_N2_TF_AGG_0p95_1p08` | 239 | 6.556485 | -0.001768 | 0.002963 | 0.006798 | 0.445865 |
| `STAGE3_TURNOVER_LIGHT_N3_UTIL_PEN_0p96` | 158 | 0.878661 | -0.009024 | -0.009320 | -0.012233 | 0.444493 |

Loop4 has better 20-day replacement quality than Loop10, but the QE strategy is closer to a 10-day TopK execution path. Loop10 still has the best 5-day and 10-day replacement quality, and this matches the realized QE ranking.

## Coefficient Behavior

| version | min | max | mean | active_rate | unique_coefficients |
| --- | ---: | ---: | ---: | ---: | ---: |
| Loop2 | 0.96 | 1.05 | 0.985630 | 0.454544 | 3 |
| Loop10 | 0.96 | 1.00 | 0.983512 | 0.412196 | 2 |
| Stage3 FBT penalty | 0.96 | 1.00 | 0.982957 | 0.448378 | 17,819 |
| Stage3 FBT symmetric | 0.96 | 1.04 | 1.004018 | 0.997444 | 38,369 |
| Stage3 FBT aggressive | 0.95 | 1.08 | 1.020819 | 0.997755 | 39,516 |
| Stage3 turnover-light penalty | 0.96 | 1.00 | 0.992301 | 0.518566 | 24,349 |

The old winners are sparse/discrete. Stage-3 versions use continuous coefficients, causing broad rank perturbation. That is the main actionable failure mode.

## Evidence Files

- Final task detail: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/task_detail_final.json`
- Custom config: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/custom_evo_config_final.json`
- QE diagnostic JSON: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/qe_evolution_diagnostic_final.json`
- Attribution report: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/attribution/qe_20260505_123035_bf80/hmm_qe_candidate_attribution.md`
- Loop summary: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/summary_tables/loop_summary.csv`
- Mapped holdout attribution: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/summary_tables/holdout_attribution_mapped.csv`
- Monthly return deltas: `.codex_tmp/hmm_stage3_qe_20260505/final_analysis/summary_tables/monthly_delta_vs_loop10.csv`

## Recommended Next Step

- Keep Loop10 as the active best HMM reference.
- Do not promote any Stage-3 candidate from this task.
- Stage-3 candidates should be removed or hidden from the QE selector after the user confirms pruning.
- Next research should not broaden factor combinations directly. It should first align HMM training/evaluation to QE TopK replacement quality and use sparse/discrete penalty mappings.
