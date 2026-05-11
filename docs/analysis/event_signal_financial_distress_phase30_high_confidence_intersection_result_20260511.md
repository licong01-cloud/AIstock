# Financial Distress Phase 30 High-Confidence Intersection Screen

Research-only screen for q_ocf intersections and rank-aware TopK filters. No runtime consumer is changed.

## Scope

```text
+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+
| item                | value                                                                                                                                             |
+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+
| date range          | 2024-07-01 -> 2026-04-27                                                                                                                          |
| rules               | 8                                                                                                                                                 |
| top_k sweep         | 20, 50                                                                                                                                            |
| active trading days | 60, 90                                                                                                                                            |
| direct report       | reports\event_signal\financial_distress_phase30_high_confidence_intersection\direct\financial_distress_direct_event_20240701_20260511_194903.json |
| overlay reports     | 1                                                                                                                                                 |
| runtime impact      | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring                                                                               |
+---------------------+---------------------------------------------------------------------------------------------------------------------------------------------------+
```

## Outcome

```text
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
| item               | value                                                                   | interpretation                                                                            |
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
| true-QE candidates | 0                                                                       | 0 means do not spend WSL true-rerun budget in this phase                                  |
| best cheap row     | indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn / fixed15_top50 / 60td | score 9.5, avg 0.11%, hit/overlay 0.05%                                                   |
| phase decision     | NO_WSL_TRUE_QE_RERUN                                                    | direct downside exists, but cheap overlay precision/effect is far below the Phase-27 gate |
+--------------------+-------------------------------------------------------------------------+-------------------------------------------------------------------------------------------+
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
+-----------+-------+-------+-------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+----------------------------------------------------------------------------+
| decision  | score | pos   | avg   | ex_best | min    | eval | drop | hit/overlay | td | mode          | direct              | t20_med | rule                                                                       |
+-----------+-------+-------+-------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+----------------------------------------------------------------------------+
| WATCHLIST | 9.5   | 15/22 | 0.11% | 0.06%   | -0.00% | 247  | 9    | 0.05%       | 60 | fixed15_top50 | supports_downweight | -0.71%  | indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           |
| WATCHLIST | 2.2   | 13/22 | 0.06% | 0.04%   | -0.09% | 227  | 9    | 0.06%       | 60 | fixed15_top50 | supports_downweight | -0.92%  | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| WATCHLIST | 1.9   | 12/22 | 0.03% | 0.01%   | -0.13% | 55   | 4    | 0.05%       | 60 | fixed15_top50 | supports_downweight | -3.50%  | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| WATCHLIST | 0.8   | 12/22 | 0.03% | 0.01%   | -0.13% | 87   | 4    | 0.05%       | 60 | fixed15_top50 | supports_downweight | -1.29%  | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn |
| WATCHLIST | -2.3  | 13/22 | 0.02% | 0.00%   | -0.00% | 98   | 2    | 0.06%       | 60 | fixed10_top50 | supports_downweight | -1.06%  | indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    |
| WATCHLIST | -4.4  | 13/22 | 0.04% | 0.02%   | -0.00% | 172  | 4    | 0.05%       | 60 | fixed15_top50 | supports_downweight | -0.11%  | indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    |
| WATCHLIST | -5.6  | 12/22 | 0.03% | 0.01%   | -0.13% | 213  | 5    | 0.05%       | 60 | fixed15_top50 | supports_downweight | -1.16%  | indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              |
| WATCHLIST | -6.7  | 12/22 | 0.00% | 0.00%   | -0.00% | 44   | 0    | 0.04%       | 60 | fixed10_top20 | supports_downweight | -1.69%  | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_30bn                           |
+-----------+-------+-------+-------+---------+--------+------+------+-------------+----+---------------+---------------------+---------+----------------------------------------------------------------------------+
```

## Direct Event Abnormal Returns

```text
+----------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| rule                                                                       | window | valid | abn_mean | abn_median | neg_rate | miss_px |
+----------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           | T+5    | 415   | 0.67%    | 0.05%      | 49.64%   | 0.72%   |
| indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           | T+20   | 357   | 1.24%    | -0.71%     | 52.10%   | 14.59%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           | T+60   | 346   | 4.40%    | -1.45%     | 54.62%   | 17.22%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_30bn                           | T+5    | 95    | 0.05%    | -0.67%     | 55.79%   | 0.00%   |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_30bn                           | T+20   | 82    | 0.31%    | -1.69%     | 62.20%   | 13.68%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_30bn                           | T+60   | 76    | 1.72%    | -2.64%     | 56.58%   | 20.00%  |
| indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     | T+5    | 350   | 0.72%    | -0.22%     | 51.43%   | 0.57%   |
| indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     | T+20   | 304   | 1.15%    | -0.92%     | 53.29%   | 13.64%  |
| indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     | T+60   | 294   | 5.58%    | -0.54%     | 51.70%   | 16.48%  |
| indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           | T+5    | 97    | 0.13%    | -0.34%     | 55.67%   | 1.02%   |
| indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           | T+20   | 80    | -3.04%   | -3.50%     | 70.00%   | 18.37%  |
| indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           | T+60   | 75    | 2.68%    | -4.11%     | 58.67%   | 23.47%  |
| indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    | T+5    | 155   | 0.73%    | -0.11%     | 51.61%   | 0.64%   |
| indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    | T+20   | 125   | 1.25%    | -1.06%     | 56.80%   | 19.87%  |
| indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    | T+60   | 118   | 8.40%    | 1.15%      | 45.76%   | 24.36%  |
| indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    | T+5    | 315   | 0.74%    | -0.23%     | 51.75%   | 0.63%   |
| indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    | T+20   | 267   | 2.16%    | -0.11%     | 50.94%   | 15.77%  |
| indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    | T+60   | 256   | 5.04%    | -0.97%     | 52.73%   | 19.24%  |
| indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn | T+5    | 132   | 0.33%    | -0.77%     | 53.79%   | 2.22%   |
| indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn | T+20   | 116   | 0.13%    | -1.29%     | 58.62%   | 14.07%  |
| indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn | T+60   | 112   | 2.80%    | -1.75%     | 57.14%   | 17.04%  |
| indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              | T+5    | 358   | 0.62%    | -0.20%     | 51.68%   | 0.83%   |
| indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              | T+20   | 301   | 1.15%    | -1.16%     | 55.48%   | 16.62%  |
| indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              | T+60   | 289   | 5.20%    | -0.86%     | 52.25%   | 19.94%  |
+----------------------------------------------------------------------------+--------+-------+----------+------------+----------+---------+
```

## Cheap Overlay Top Rows

```text
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+----------------------------------------------------------------------------+
| score | decision         | pos   | avg   | ex_best | min    | eval | drop | hit/overlay | td | mode          | rule                                                                       |
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+----------------------------------------------------------------------------+
| 9.5   | WATCHLIST        | 15/22 | 0.11% | 0.06%   | -0.00% | 247  | 9    | 0.05%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           |
| 3.2   | WATCHLIST        | 14/22 | 0.09% | 0.03%   | -0.18% | 406  | 12   | 0.05%       | 90 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           |
| 2.2   | WATCHLIST        | 13/22 | 0.06% | 0.04%   | -0.09% | 227  | 9    | 0.06%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| 2.2   | WATCHLIST        | 13/22 | 0.06% | 0.04%   | -0.09% | 227  | 9    | 0.06%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| 1.9   | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 55   | 4    | 0.05%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| 1.9   | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 55   | 4    | 0.05%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| 0.8   | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 87   | 4    | 0.05%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn |
| 0.8   | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 87   | 4    | 0.05%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn |
| -0.1  | WATCHLIST        | 13/22 | 0.02% | 0.01%   | -0.29% | 73   | 6    | 0.05%       | 90 | fixed20_top20 | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| -2.1  | WATCHLIST        | 13/22 | 0.05% | 0.02%   | -0.09% | 227  | 8    | 0.06%       | 60 | fixed10_top50 | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| -2.3  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 98   | 2    | 0.06%       | 60 | fixed10_top50 | indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    |
| -2.3  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 98   | 2    | 0.06%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    |
| -2.3  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.00% | 98   | 2    | 0.06%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_profit_revenue_diverge_mv_ge_10bn    |
| -2.4  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.07% | 73   | 3    | 0.05%       | 90 | fixed10_top20 | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| -2.4  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.07% | 73   | 3    | 0.05%       | 90 | fixed15_top20 | indicator_decline_q_ocf_to_sales_lt_0_prior_loss_ge_2_mv_ge_10bn           |
| -4.3  | CALIBRATION_ONLY | 12/22 | 0.04% | 0.01%   | -0.18% | 350  | 12   | 0.06%       | 90 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| -4.3  | CALIBRATION_ONLY | 12/22 | 0.04% | 0.01%   | -0.18% | 350  | 12   | 0.06%       | 90 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn     |
| -4.4  | WATCHLIST        | 13/22 | 0.04% | 0.02%   | -0.00% | 172  | 4    | 0.05%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    |
| -4.9  | WATCHLIST        | 13/22 | 0.03% | 0.02%   | -0.00% | 172  | 5    | 0.05%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn    |
| -5.3  | WATCHLIST        | 14/22 | 0.03% | 0.01%   | -0.00% | 247  | 6    | 0.05%       | 60 | fixed10_top50 | indicator_decline_q_ocf_to_sales_lt_0_mv_10_30bn                           |
| -5.6  | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 213  | 5    | 0.05%       | 60 | fixed15_top50 | indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              |
| -5.6  | CALIBRATION_ONLY | 12/22 | 0.03% | 0.01%   | -0.13% | 213  | 5    | 0.05%       | 60 | fixed20_top50 | indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn              |
| -5.9  | CALIBRATION_ONLY | 12/22 | 0.04% | 0.01%   | -0.21% | 100  | 6    | 0.04%       | 90 | fixed15_top20 | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn |
| -6.7  | CALIBRATION_ONLY | 12/22 | 0.00% | 0.00%   | -0.00% | 44   | 0    | 0.04%       | 60 | fixed10_top20 | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_30bn                           |
+-------+------------------+-------+-------+---------+--------+------+------+-------------+----+---------------+----------------------------------------------------------------------------+
```

## Interpretation

- Phase 30 is a cheap screen only; true QE rerun is still required before any policy or runtime integration.
- The desired improvement over Phase 28 is not just higher average return, but better TopK concentration and drop precision.
- Phase 30 does not produce a WSL true-QE candidate; keep these rules as watchlist/direct-event research only.
- Financial distress remains non-hard at this stage: no buy ban, forced sell, score boost, DB policy write, or Paper/QE hook.
- A row must pass the precision cheap screen before spending WSL true-QE budget; otherwise continue research or stop this branch.