# Financial Distress Phase 31 Non-q_ocf Structured Screen

Research-only screen for non-q_ocf structured risk families and rank-aware TopK filters. No runtime consumer is changed.

## Scope

```text
+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| item                | value                                                                                                                                           |
+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
| date range          | 2024-07-01 -> 2026-04-27                                                                                                                        |
| rules               | 14                                                                                                                                              |
| top_k sweep         | 50                                                                                                                                              |
| active trading days | 60, 90                                                                                                                                          |
| direct report       | reports\event_signal\financial_distress_phase31_non_qocf_structured_screen\direct\financial_distress_direct_event_20240701_20260511_233417.json |
| overlay reports     | 1                                                                                                                                               |
| runtime impact      | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring                                                                             |
+---------------------+-------------------------------------------------------------------------------------------------------------------------------------------------+
```

## Outcome

```text
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| item               | value                                                                   | interpretation                                                                                  |
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------+
| true-QE candidates | 0                                                                       | 0 means do not spend WSL true-rerun budget in this phase                                        |
| best cheap row     | indicator_decline_current_ratio_lt_08_mv_10_30bn / fixed15_top50 / 60td | score 27.6, avg 0.02%, hit/overlay 0.08%                                                        |
| phase decision     | NO_WSL_TRUE_QE_RERUN                                                    | direct downside may exist, but cheap overlay precision/effect remains below the true-rerun gate |
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------------+
```

## Phase 29 Calibration

```text
+-------------------+---------+-------+-------+-----------+----------+--------------------------------------------+
| case              | penalty | top50 | drops | top50/pen | true_ret | role                                       |
+-------------------+---------+-------+-------+-----------+----------+--------------------------------------------+
| Phase28 q_ocf     | 41,673  | 221   | 25    | 0.530%    | +0.168%  | broad low-precision baseline               |
| Phase19 indicator | 311     | 311   | 24    | 100.000%  | +0.273%  | best one-loop true-smoke benchmark         |
| Phase23 loss/mv   | 304     | 302   | 61    | 99.342%   | +0.066%  | calibration only; drops alone insufficient |
+-------------------+---------+-------+-------+-----------+----------+--------------------------------------------+
```

## Combined Shortlist

```text
+-----------------------+-------+-------+--------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+--------------------------------------------------------+
| decision              | score | pos   | avg    | ex_best | min    | eval | drop | hit/overlay | td | mode          | direct              | t20_med | rule                                                   |
+-----------------------+-------+-------+--------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+--------------------------------------------------------+
| WATCHLIST             | 27.6  | 12/22 | 0.02%  | 0.00%   | -0.00% | 114  | 3    | 0.08%       | 60 | fixed15_top50 | supports_downweight | -1.33%  | indicator_decline_current_ratio_lt_08_mv_10_30bn       |
| WATCHLIST             | 27.0  | 13/22 | 0.02%  | 0.00%   | -0.00% | 46   | 2    | 0.07%       | 60 | fixed10_top50 | supports_downweight | -0.42%  | indicator_decline_profit_revenue_diverge_mv_30_100bn   |
| WATCHLIST             | 23.9  | 14/22 | 0.01%  | -0.01%  | -0.88% | 347  | 10   | 0.07%       | 60 | fixed15_top50 | supports_downweight | -1.12%  | indicator_decline_negative_margin_mv_10_30bn           |
| WATCHLIST             | 23.5  | 13/22 | 0.01%  | -0.00%  | -0.09% | 85   | 5    | 0.05%       | 60 | fixed10_top50 | supports_downweight | -3.10%  | indicator_decline_actual_yoy_le_minus80_mv_30_100bn    |
| WATCHLIST             | 22.8  | 13/22 | 0.02%  | 0.00%   | -0.00% | 23   | 1    | 0.04%       | 90 | fixed10_top50 | supports_downweight | -3.07%  | indicator_decline_current_ratio_lt_08_mv_30_100bn      |
| WATCHLIST             | 22.2  | 13/22 | 0.01%  | -0.01%  | -0.13% | 78   | 4    | 0.06%       | 60 | fixed10_top50 | supports_downweight | -2.38%  | indicator_decline_negative_margin_mv_30_100bn          |
| WATCHLIST             | 17.2  | 12/22 | 0.00%  | 0.00%   | -0.00% | 4    | 1    | 0.03%       | 90 | fixed10_top50 | supports_downweight | -3.35%  | indicator_decline_debt_assets_ge_90_mv_10_30bn         |
| WATCHLIST             | 16.5  | 13/22 | 0.00%  | -0.00%  | -0.08% | 69   | 2    | 0.06%       | 60 | fixed10_top50 | supports_downweight | -3.41%  | indicator_decline_profit_revenue_both_down_mv_30_100bn |
| WATCHLIST             | 15.8  | 12/22 | 0.00%  | 0.00%   | -0.00% | 13   | 1    | 0.02%       | 90 | fixed15_top50 | supports_downweight | -3.15%  | expectation_miss_gap_ge_100_mv_10_30bn_prior_loss_ge_2 |
| WATCHLIST             | 15.3  | 12/22 | 0.00%  | 0.00%   | -0.00% | 15   | 1    | 0.02%       | 90 | fixed15_top50 | supports_downweight | -3.15%  | expectation_miss_gap_ge_100_mv_ge_10bn_prior_loss_ge_2 |
| WATCHLIST             | 13.9  | 13/22 | -0.03% | -0.05%  | -0.88% | 500  | 16   | 0.06%       | 60 | fixed15_top50 | supports_downweight | -0.50%  | indicator_decline_actual_yoy_le_minus80_mv_10_30bn     |
| WATCHLIST             | 10.7  | 14/22 | -0.02% | -0.03%  | -0.88% | 309  | 13   | 0.05%       | 90 | fixed15_top50 | supports_downweight | -0.94%  | indicator_decline_profit_revenue_both_down_mv_10_30bn  |
| WATCHLIST             | 8.8   | 12/22 | 0.00%  | 0.00%   | -0.00% | 0    | 0    | 0.00%       | 60 | fixed10_top50 | supports_downweight | -4.48%  | indicator_decline_debt_assets_ge_90_mv_30_100bn        |
| REJECT_OR_CALIBRATION | 27.2  | 12/22 | 0.01%  | 0.00%   | -0.00% | 230  | 3    | 0.08%       | 60 | fixed10_top50 | mixed               | -0.24%  | indicator_decline_profit_revenue_diverge_mv_10_30bn    |
+-----------------------+-------+-------+--------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+--------------------------------------------------------+
```

## Direct Event Abnormal Returns

```text
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| rule                                                   | window | valid | abn_mean | abn_median | neg_rate | miss_px |
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| expectation_miss_gap_ge_100_mv_ge_10bn_prior_loss_ge_2 | T+5    | 50    | -2.41%   | -2.95%     | 68.00%   | 0.00%   |
| expectation_miss_gap_ge_100_mv_ge_10bn_prior_loss_ge_2 | T+20   | 45    | -2.34%   | -3.15%     | 64.44%   | 10.00%  |
| expectation_miss_gap_ge_100_mv_ge_10bn_prior_loss_ge_2 | T+60   | 35    | -5.64%   | -8.39%     | 74.29%   | 30.00%  |
| expectation_miss_gap_ge_100_mv_10_30bn_prior_loss_ge_2 | T+5    | 39    | -2.33%   | -2.84%     | 66.67%   | 0.00%   |
| expectation_miss_gap_ge_100_mv_10_30bn_prior_loss_ge_2 | T+20   | 35    | -2.65%   | -3.15%     | 65.71%   | 10.26%  |
| expectation_miss_gap_ge_100_mv_10_30bn_prior_loss_ge_2 | T+60   | 29    | -5.02%   | -8.05%     | 72.41%   | 25.64%  |
| indicator_decline_actual_yoy_le_minus80_mv_10_30bn     | T+5    | 708   | 0.76%    | 0.25%      | 48.16%   | 0.28%   |
| indicator_decline_actual_yoy_le_minus80_mv_10_30bn     | T+20   | 612   | 1.80%    | -0.50%     | 51.80%   | 13.80%  |
| indicator_decline_actual_yoy_le_minus80_mv_10_30bn     | T+60   | 586   | 6.36%    | -0.48%     | 51.37%   | 17.46%  |
| indicator_decline_actual_yoy_le_minus80_mv_30_100bn    | T+5    | 168   | 1.14%    | -0.19%     | 50.60%   | 0.59%   |
| indicator_decline_actual_yoy_le_minus80_mv_30_100bn    | T+20   | 152   | -0.14%   | -3.10%     | 60.53%   | 10.06%  |
| indicator_decline_actual_yoy_le_minus80_mv_30_100bn    | T+60   | 140   | 0.65%    | -1.44%     | 55.00%   | 17.16%  |
| indicator_decline_profit_revenue_diverge_mv_10_30bn    | T+5    | 294   | 0.42%    | -0.19%     | 51.70%   | 0.34%   |
| indicator_decline_profit_revenue_diverge_mv_10_30bn    | T+20   | 238   | 2.59%    | -0.24%     | 50.84%   | 19.32%  |
| indicator_decline_profit_revenue_diverge_mv_10_30bn    | T+60   | 222   | 8.72%    | 1.05%      | 47.30%   | 24.75%  |
| indicator_decline_profit_revenue_diverge_mv_30_100bn   | T+5    | 70    | 0.41%    | -0.75%     | 57.14%   | 1.43%   |
| indicator_decline_profit_revenue_diverge_mv_30_100bn   | T+20   | 57    | 1.05%    | -0.42%     | 52.63%   | 20.00%  |
| indicator_decline_profit_revenue_diverge_mv_30_100bn   | T+60   | 50    | 4.73%    | -0.30%     | 52.00%   | 30.00%  |
| indicator_decline_profit_revenue_both_down_mv_10_30bn  | T+5    | 345   | 0.79%    | 0.30%      | 46.09%   | 0.00%   |
| indicator_decline_profit_revenue_both_down_mv_10_30bn  | T+20   | 316   | 0.97%    | -0.94%     | 53.48%   | 8.41%   |
| indicator_decline_profit_revenue_both_down_mv_10_30bn  | T+60   | 308   | 2.43%    | -2.48%     | 56.17%   | 10.72%  |
| indicator_decline_profit_revenue_both_down_mv_30_100bn | T+5    | 105   | 0.73%    | -0.11%     | 52.38%   | 0.94%   |
| indicator_decline_profit_revenue_both_down_mv_30_100bn | T+20   | 97    | -1.45%   | -3.41%     | 69.07%   | 8.49%   |
| indicator_decline_profit_revenue_both_down_mv_30_100bn | T+60   | 94    | -2.04%   | -4.74%     | 63.83%   | 11.32%  |
| indicator_decline_negative_margin_mv_10_30bn           | T+5    | 470   | 0.68%    | 0.03%      | 49.57%   | 0.42%   |
| indicator_decline_negative_margin_mv_10_30bn           | T+20   | 406   | 1.18%    | -1.12%     | 53.20%   | 13.98%  |
| indicator_decline_negative_margin_mv_10_30bn           | T+60   | 390   | 5.56%    | -0.99%     | 52.82%   | 17.37%  |
| indicator_decline_negative_margin_mv_30_100bn          | T+5    | 131   | 0.83%    | 0.02%      | 48.85%   | 0.76%   |
| indicator_decline_negative_margin_mv_30_100bn          | T+20   | 122   | 0.45%    | -2.38%     | 60.66%   | 7.63%   |
| indicator_decline_negative_margin_mv_30_100bn          | T+60   | 113   | 1.00%    | -1.07%     | 53.10%   | 14.50%  |
| indicator_decline_debt_assets_ge_90_mv_10_30bn         | T+5    | 10    | -1.65%   | -1.11%     | 70.00%   | 0.00%   |
| indicator_decline_debt_assets_ge_90_mv_10_30bn         | T+20   | 9     | -3.82%   | -3.35%     | 55.56%   | 10.00%  |
| indicator_decline_debt_assets_ge_90_mv_10_30bn         | T+60   | 8     | 7.87%    | -8.43%     | 62.50%   | 20.00%  |
| indicator_decline_debt_assets_ge_90_mv_30_100bn        | T+5    | 2     | -3.75%   | -3.75%     | 100.00%  | 0.00%   |
| indicator_decline_debt_assets_ge_90_mv_30_100bn        | T+20   | 2     | -4.48%   | -4.48%     | 100.00%  | 0.00%   |
| indicator_decline_debt_assets_ge_90_mv_30_100bn        | T+60   | 2     | 2.60%    | 2.60%      | 0.00%    | 0.00%   |
| indicator_decline_current_ratio_lt_08_mv_10_30bn       | T+5    | 125   | -0.52%   | -0.41%     | 52.80%   | 0.79%   |
| indicator_decline_current_ratio_lt_08_mv_10_30bn       | T+20   | 109   | 0.20%    | -1.33%     | 59.63%   | 13.49%  |
| indicator_decline_current_ratio_lt_08_mv_10_30bn       | T+60   | 107   | 2.19%    | -2.95%     | 60.75%   | 15.08%  |
| indicator_decline_current_ratio_lt_08_mv_30_100bn      | T+5    | 36    | -0.33%   | -1.04%     | 58.33%   | 0.00%   |
| indicator_decline_current_ratio_lt_08_mv_30_100bn      | T+20   | 31    | -1.23%   | -3.07%     | 67.74%   | 13.89%  |
| indicator_decline_current_ratio_lt_08_mv_30_100bn      | T+60   | 28    | 0.43%    | -1.50%     | 57.14%   | 22.22%  |
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
```

## Cheap Overlay Top Rows

```text
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+------------------------------------------------------+
| score | decision         | pos   | avg   | ex_best | min    | eval | drop | hit/overlay | td | mode          | rule                                                 |
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+------------------------------------------------------+
| 27.6  | CALIBRATION_ONLY | 12/22 | 0.02% | 0.00%   | -0.00% | 114  | 3    | 0.08%       | 60 | fixed15_top50 | indicator_decline_current_ratio_lt_08_mv_10_30bn     |
| 27.6  | CALIBRATION_ONLY | 12/22 | 0.02% | 0.00%   | -0.00% | 114  | 3    | 0.08%       | 60 | fixed20_top50 | indicator_decline_current_ratio_lt_08_mv_10_30bn     |
| 27.2  | CALIBRATION_ONLY | 12/22 | 0.01% | 0.00%   | -0.00% | 230  | 3    | 0.08%       | 60 | fixed10_top50 | indicator_decline_profit_revenue_diverge_mv_10_30bn  |
| 27.2  | CALIBRATION_ONLY | 12/22 | 0.01% | 0.00%   | -0.00% | 230  | 3    | 0.08%       | 60 | fixed15_top50 | indicator_decline_profit_revenue_diverge_mv_10_30bn  |
| 27.2  | CALIBRATION_ONLY | 12/22 | 0.01% | 0.00%   | -0.00% | 230  | 3    | 0.08%       | 60 | fixed20_top50 | indicator_decline_profit_revenue_diverge_mv_10_30bn  |
| 27.0  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 46   | 2    | 0.07%       | 60 | fixed10_top50 | indicator_decline_profit_revenue_diverge_mv_30_100bn |
| 26.3  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 64   | 2    | 0.06%       | 90 | fixed10_top50 | indicator_decline_profit_revenue_diverge_mv_30_100bn |
| 24.6  | CALIBRATION_ONLY | 12/22 | 0.00% | 0.00%   | -0.00% | 114  | 2    | 0.08%       | 60 | fixed10_top50 | indicator_decline_current_ratio_lt_08_mv_10_30bn     |
| 23.9  | CALIBRATION_ONLY | 14/22 | 0.01% | -0.01%  | -0.88% | 347  | 10   | 0.07%       | 60 | fixed15_top50 | indicator_decline_negative_margin_mv_10_30bn         |
| 23.9  | CALIBRATION_ONLY | 14/22 | 0.01% | -0.01%  | -0.88% | 347  | 10   | 0.07%       | 60 | fixed20_top50 | indicator_decline_negative_margin_mv_10_30bn         |
| 23.5  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.00%  | -0.09% | 85   | 5    | 0.05%       | 60 | fixed10_top50 | indicator_decline_actual_yoy_le_minus80_mv_30_100bn  |
| 23.5  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.00%  | -0.09% | 85   | 5    | 0.05%       | 60 | fixed15_top50 | indicator_decline_actual_yoy_le_minus80_mv_30_100bn  |
| 23.5  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.00%  | -0.09% | 85   | 5    | 0.05%       | 60 | fixed20_top50 | indicator_decline_actual_yoy_le_minus80_mv_30_100bn  |
| 22.8  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 23   | 1    | 0.04%       | 90 | fixed10_top50 | indicator_decline_current_ratio_lt_08_mv_30_100bn    |
| 22.8  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 23   | 1    | 0.04%       | 90 | fixed15_top50 | indicator_decline_current_ratio_lt_08_mv_30_100bn    |
| 22.8  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 23   | 1    | 0.04%       | 90 | fixed20_top50 | indicator_decline_current_ratio_lt_08_mv_30_100bn    |
| 22.2  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 78   | 4    | 0.06%       | 60 | fixed10_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 22.2  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 78   | 4    | 0.06%       | 60 | fixed15_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 22.2  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 78   | 4    | 0.06%       | 60 | fixed20_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 22.0  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 113  | 4    | 0.06%       | 90 | fixed10_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 22.0  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 113  | 4    | 0.06%       | 90 | fixed15_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 22.0  | CALIBRATION_ONLY | 13/22 | 0.01% | -0.01%  | -0.13% | 113  | 4    | 0.06%       | 90 | fixed20_top50 | indicator_decline_negative_margin_mv_30_100bn        |
| 20.5  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 16   | 1    | 0.04%       | 60 | fixed10_top50 | indicator_decline_current_ratio_lt_08_mv_30_100bn    |
| 20.5  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 16   | 1    | 0.04%       | 60 | fixed15_top50 | indicator_decline_current_ratio_lt_08_mv_30_100bn    |
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+------------------------------------------------------+
```

## Interpretation

- Phase 31 is a cheap screen only; true QE rerun is still required before any policy or runtime integration.
- The desired improvement over Phase 28 is not just higher average return, but better TopK concentration and drop precision.
- Phase 31 does not produce a WSL true-QE candidate; keep these rules as watchlist/direct-event research only.
- Financial distress remains non-hard at this stage: no buy ban, forced sell, score boost, DB policy write, or Paper/QE hook.
- A row must pass the precision cheap screen before spending WSL true-QE budget; otherwise continue research or stop this branch.