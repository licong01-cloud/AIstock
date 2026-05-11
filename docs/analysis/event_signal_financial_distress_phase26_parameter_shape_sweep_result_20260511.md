# Financial Distress Phase 26 Parameter Shape Sweep

Research-only sweep of fixed score-down penalty and active-window shape for Phase-25 financial-distress rules. No runtime consumer is changed.

## Scope

```text
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| item           | value                                                                                                                                       |
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| date range     | 2024-07-01 -> 2026-04-27                                                                                                                    |
| rules          | 12                                                                                                                                          |
| direct report  | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\direct\financial_distress_direct_event_20240701_20260511_134419.json  |
| overlay report | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_145003.json |
| runtime impact | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring                                                                         |
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
```

## Sweep Dimensions

```text
+---------------------+----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| family              | rule idea                                                                                    | validation role                                                                      |
+---------------------+----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
| size_split          | split Phase-24 OCF/leverage stress into 10-30bn and 30-100bn buckets                         | identify whether cheap overlay benefit is concentrated in one investable size bucket |
| component_threshold | isolate q_ocf_to_sales<0, OCF yoy<=-50, debt/assets>=80/90, current ratio<0.8                | test whether stricter quality thresholds improve tail without losing all Top50 hits  |
| compound_context    | combine OCF/leverage stress with actual_yoy<=-80, prior losses, or profit/revenue divergence | search for stronger direct downside and cheap overlay interaction before WSL true QE |
+---------------------+----------------------------------------------------------------------------------------------+--------------------------------------------------------------------------------------+
```

## Combined Shortlist

```text
+-----------+-------+-------+-------+---------+--------+------+-----+--------------------------------------+---------------------+---------+------------------------------------------------------------------------------+
| decision  | score | pos   | avg   | ex_best | min    | drop | td  | mode                                 | direct              | t20_med | rule                                                                         |
+-----------+-------+-------+-------+---------+--------+------+-----+--------------------------------------+---------------------+---------+------------------------------------------------------------------------------+
| WATCHLIST | 56.1  | 14/22 | 0.12% | 0.07%   | -0.09% | 12   | 60  | score_down_rank_15pct_top50_previous | supports_downweight | -0.93%  | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
| WATCHLIST | 55.5  | 15/22 | 0.12% | 0.04%   | -0.10% | 10   | 60  | fixed_10                             | supports_downweight | -0.71%  | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        |
| WATCHLIST | 47.1  | 13/22 | 0.11% | 0.03%   | -0.27% | 10   | 120 | score_down_rank_15pct_top50_previous | supports_downweight | -1.59%  | indicator_decline_current_ratio_lt_08_mv_ge_10bn                             |
| WATCHLIST | 42.7  | 14/22 | 0.05% | 0.02%   | -0.10% | 10   | 60  | fixed_5                              | supports_downweight | -1.19%  | indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  |
| WATCHLIST | 41.9  | 14/22 | 0.11% | 0.03%   | -0.00% | 5    | 60  | fixed_10                             | mixed               | -0.43%  | indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn |
| WATCHLIST | 39.8  | 13/22 | 0.11% | 0.03%   | -0.13% | 5    | 60  | score_down_rank_15pct_top50_previous | supports_downweight | -1.61%  | indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn             |
| WATCHLIST | 34.1  | 13/22 | 0.09% | 0.00%   | -0.10% | 5    | 60  | fixed_10                             | supports_downweight | -0.04%  | indicator_decline_ocf_yoy_le_minus50_mv_10_30bn                              |
| WATCHLIST | 30.7  | 12/22 | 0.02% | 0.01%   | -0.18% | 8    | 120 | score_down_rank_15pct_top50_previous | supports_downweight | -3.43%  | indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn        |
| WATCHLIST | 30.3  | 13/22 | 0.09% | 0.00%   | -0.00% | 3    | 60  | fixed_10                             | supports_downweight | -2.70%  | indicator_decline_debt_assets_ge_80_mv_ge_10bn                               |
| WATCHLIST | 27.2  | 13/22 | 0.09% | 0.00%   | -0.00% | 2    | 60  | fixed_10                             | supports_downweight | -1.53%  | indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn        |
| WATCHLIST | 16.4  | 13/22 | 0.00% | 0.00%   | -0.00% | 1    | 20  | fixed_10                             | supports_downweight | -3.12%  | indicator_decline_ocf_negative_or_leverage_mv_30_100bn                       |
| WATCHLIST | 12.9  | 12/22 | 0.00% | 0.00%   | -0.00% | 1    | 120 | fixed_10                             | supports_downweight | -4.48%  | indicator_decline_debt_assets_ge_90_mv_ge_10bn                               |
+-----------+-------+-------+-------+---------+--------+------+-----+--------------------------------------+---------------------+---------+------------------------------------------------------------------------------+
```

## Direct Event Abnormal Returns

```text
+------------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| rule                                                                         | window | valid | abn_mean | abn_median | neg_rate | miss_px |
+------------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        | T+5    | 784   | 0.54%    | 0.16%      | 48.98%   | 0.38%   |
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        | T+20   | 681   | 1.31%    | -0.71%     | 52.86%   | 13.47%  |
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        | T+60   | 657   | 4.18%    | -1.60%     | 54.64%   | 16.52%  |
| indicator_decline_ocf_negative_or_leverage_mv_30_100bn                       | T+5    | 199   | 0.41%    | -0.73%     | 56.28%   | 0.50%   |
| indicator_decline_ocf_negative_or_leverage_mv_30_100bn                       | T+20   | 176   | -0.34%   | -3.12%     | 66.48%   | 12.00%  |
| indicator_decline_ocf_negative_or_leverage_mv_30_100bn                       | T+60   | 164   | 0.63%    | -2.63%     | 57.93%   | 18.00%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             | T+5    | 510   | 0.56%    | -0.12%     | 50.78%   | 0.58%   |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             | T+20   | 439   | 1.07%    | -0.93%     | 53.99%   | 14.42%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             | T+60   | 422   | 3.91%    | -1.61%     | 54.98%   | 17.74%  |
| indicator_decline_ocf_yoy_le_minus50_mv_10_30bn                              | T+5    | 476   | 0.70%    | -0.02%     | 50.21%   | 0.42%   |
| indicator_decline_ocf_yoy_le_minus50_mv_10_30bn                              | T+20   | 411   | 1.75%    | -0.04%     | 50.12%   | 14.02%  |
| indicator_decline_ocf_yoy_le_minus50_mv_10_30bn                              | T+60   | 396   | 5.10%    | -0.60%     | 51.52%   | 17.15%  |
| indicator_decline_debt_assets_ge_80_mv_ge_10bn                               | T+5    | 77    | -0.48%   | -0.66%     | 54.55%   | 2.53%   |
| indicator_decline_debt_assets_ge_80_mv_ge_10bn                               | T+20   | 72    | -1.33%   | -2.70%     | 65.28%   | 8.86%   |
| indicator_decline_debt_assets_ge_80_mv_ge_10bn                               | T+60   | 69    | 2.22%    | -0.53%     | 55.07%   | 12.66%  |
| indicator_decline_debt_assets_ge_90_mv_ge_10bn                               | T+5    | 12    | -2.00%   | -2.00%     | 75.00%   | 0.00%   |
| indicator_decline_debt_assets_ge_90_mv_ge_10bn                               | T+20   | 11    | -3.94%   | -4.48%     | 63.64%   | 8.33%   |
| indicator_decline_debt_assets_ge_90_mv_ge_10bn                               | T+60   | 10    | 6.82%    | -2.88%     | 50.00%   | 16.67%  |
| indicator_decline_current_ratio_lt_08_mv_ge_10bn                             | T+5    | 161   | -0.47%   | -0.66%     | 54.04%   | 0.62%   |
| indicator_decline_current_ratio_lt_08_mv_ge_10bn                             | T+20   | 140   | -0.11%   | -1.59%     | 61.43%   | 13.58%  |
| indicator_decline_current_ratio_lt_08_mv_ge_10bn                             | T+60   | 135   | 1.82%    | -2.52%     | 60.00%   | 16.67%  |
| indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn        | T+5    | 90    | 0.95%    | -0.66%     | 52.22%   | 2.17%   |
| indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn        | T+20   | 82    | 1.35%    | -1.53%     | 60.98%   | 10.87%  |
| indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn        | T+60   | 80    | 0.90%    | -2.58%     | 61.25%   | 13.04%  |
| indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn             | T+5    | 128   | -0.61%   | -1.23%     | 57.81%   | 1.54%   |
| indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn             | T+20   | 113   | -0.33%   | -1.61%     | 60.18%   | 13.08%  |
| indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn             | T+60   | 109   | 2.09%    | -4.29%     | 62.39%   | 16.15%  |
| indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  | T+5    | 641   | 0.76%    | 0.16%      | 48.83%   | 0.62%   |
| indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  | T+20   | 557   | 1.20%    | -1.19%     | 54.58%   | 13.64%  |
| indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  | T+60   | 534   | 4.71%    | -0.91%     | 53.00%   | 17.21%  |
| indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn        | T+5    | 192   | 0.18%    | -0.22%     | 53.12%   | 0.52%   |
| indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn        | T+20   | 161   | -2.02%   | -3.43%     | 68.32%   | 16.58%  |
| indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn        | T+60   | 151   | 0.56%    | -4.03%     | 60.93%   | 21.76%  |
| indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn | T+5    | 265   | 0.55%    | -0.25%     | 51.32%   | 0.38%   |
| indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn | T+20   | 210   | 1.96%    | -0.43%     | 52.38%   | 21.05%  |
| indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn | T+60   | 196   | 7.55%    | 0.41%      | 48.98%   | 26.32%  |
+------------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
```

## Cheap Overlay Top Rows

```text
+-------+------------------+-------+-------+---------+--------+------+-----+---------------------------------------+------------------------------------------------------------------------------+
| score | decision         | pos   | avg   | ex_best | min    | drop | td  | mode                                  | rule                                                                         |
+-------+------------------+-------+-------+---------+--------+------+-----+---------------------------------------+------------------------------------------------------------------------------+
| 56.1  | WATCHLIST        | 14/22 | 0.12% | 0.07%   | -0.09% | 12   | 60  | score_down_rank_15pct_top50_previous  | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
| 55.7  | CALIBRATION_ONLY | 12/22 | 0.16% | 0.07%   | -0.49% | 19   | 120 | score_down_rank_15pct_top50_previous  | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
| 55.5  | WATCHLIST        | 15/22 | 0.12% | 0.04%   | -0.10% | 10   | 60  | fixed_10                              | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        |
| 51.8  | WATCHLIST        | 15/22 | 0.12% | 0.03%   | -0.10% | 8    | 60  | score_down_rank_7p5pct_top50_previous | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        |
| 47.1  | WATCHLIST        | 13/22 | 0.11% | 0.03%   | -0.27% | 10   | 120 | score_down_rank_15pct_top50_previous  | indicator_decline_current_ratio_lt_08_mv_ge_10bn                             |
| 44.4  | CALIBRATION_ONLY | 14/22 | 0.11% | 0.02%   | -0.62% | 13   | 60  | score_down_rank_15pct_top50_previous  | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        |
| 44.0  | WATCHLIST        | 14/22 | 0.12% | 0.04%   | -0.00% | 5    | 60  | score_down_rank_15pct_top50_previous  | indicator_decline_current_ratio_lt_08_mv_ge_10bn                             |
| 42.7  | WATCHLIST        | 14/22 | 0.05% | 0.02%   | -0.10% | 10   | 60  | fixed_5                               | indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  |
| 41.9  | WATCHLIST        | 14/22 | 0.11% | 0.03%   | -0.00% | 5    | 60  | fixed_10                              | indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn |
| 41.9  | WATCHLIST        | 14/22 | 0.11% | 0.03%   | -0.00% | 5    | 60  | score_down_rank_15pct_top50_previous  | indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn |
| 41.7  | WATCHLIST        | 14/22 | 0.04% | 0.02%   | -0.18% | 11   | 60  | score_down_rank_7p5pct_top50_previous | indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  |
| 39.8  | WATCHLIST        | 13/22 | 0.11% | 0.03%   | -0.13% | 5    | 60  | score_down_rank_15pct_top50_previous  | indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn             |
| 39.5  | WATCHLIST        | 14/22 | 0.11% | 0.03%   | -0.00% | 4    | 60  | score_down_rank_7p5pct_top50_previous | indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn |
| 38.9  | WATCHLIST        | 14/22 | 0.11% | 0.02%   | -0.00% | 4    | 60  | fixed_10                              | indicator_decline_current_ratio_lt_08_mv_ge_10bn                             |
| 37.4  | WATCHLIST        | 13/22 | 0.09% | 0.01%   | -0.37% | 6    | 120 | fixed_10                              | indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn |
| 37.2  | WATCHLIST        | 14/22 | 0.03% | 0.01%   | -0.10% | 7    | 60  | fixed_5                               | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        |
| 36.5  | CALIBRATION_ONLY | 11/22 | 0.09% | 0.01%   | -0.54% | 15   | 120 | fixed_10                              | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
| 36.5  | WATCHLIST        | 14/22 | 0.11% | 0.02%   | -0.00% | 3    | 60  | score_down_rank_7p5pct_top50_previous | indicator_decline_current_ratio_lt_08_mv_ge_10bn                             |
| 36.2  | CALIBRATION_ONLY | 11/22 | 0.09% | 0.00%   | -0.54% | 14   | 120 | score_down_rank_7p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
| 35.3  | CALIBRATION_ONLY | 12/22 | 0.04% | 0.02%   | -0.13% | 9    | 60  | fixed_10                              | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             |
+-------+------------------+-------+-------+---------+--------+------+-----+---------------------------------------+------------------------------------------------------------------------------+
```

## Interpretation

- Cheap overlay remains a shortlist gate only; true QE is required before promotion.
- Financial signals are still non-hard: no buy ban, no forced sell, no alpha boost in this phase.
- Phase 26 is a parameter-shape screen, not a runtime-policy approval.
- Rules with direct downside but poor overlay stay research features; rules with overlay benefit but no direct downside stay calibration-only.
- If a row reaches TRUE_QE_CANDIDATE, run one-loop WSL true QE smoke before any signal-table or runtime design.

## Phase 26 Conclusion

```text
+------------------------------------------------------------------------------+-----------+-------+-------+--------+---------+---------+------+-----+--------------------------------------+---------+
| rule                                                                         | decision  | score | pos   | avg    | ex_best | min     | drop | td  | mode                                 | t20_med |
+------------------------------------------------------------------------------+-----------+-------+-------+--------+---------+---------+------+-----+--------------------------------------+---------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn                             | WATCHLIST | 56.1  | 14/22 | 0.120% | 0.071%  | -0.092% | 12   | 60  | score_down_rank_15pct_top50_previous | -0.925% |
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn                        | WATCHLIST | 55.5  | 15/22 | 0.124% | 0.040%  | -0.100% | 10   | 60  | fixed_10                             | -0.710% |
| indicator_decline_current_ratio_lt_08_mv_ge_10bn                             | WATCHLIST | 47.1  | 13/22 | 0.114% | 0.030%  | -0.273% | 10   | 120 | score_down_rank_15pct_top50_previous | -1.593% |
| indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn  | WATCHLIST | 42.7  | 14/22 | 0.049% | 0.023%  | -0.100% | 10   | 60  | fixed_5                              | -1.185% |
| indicator_decline_ocf_negative_or_leverage_profit_revenue_diverge_mv_ge_10bn | WATCHLIST | 41.9  | 14/22 | 0.112% | 0.028%  | -0.000% | 5    | 60  | fixed_10                             | -0.428% |
+------------------------------------------------------------------------------+-----------+-------+-------+--------+---------+---------+------+-----+--------------------------------------+---------+
```

- Best parameter-shape row is `indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn` with score `56.1`, using 60td and 15% rank penalty.
- This is a modest improvement over Phase 25, but still below the strict `60` cheap gate, so it remains a watchlist feature rather than a true-QE candidate.
- The sweep suggests more fine-grained tuning may still be worthwhile, but not enough yet to justify WSL true-QE spend.
- Next step should be either a tighter sweep around the `q_ocf_to_sales < 0` family or a different structured source if another source is available.
