# Financial Distress Phase 24 Signal Family Screen

Research-only structured signal-family screen after Phase 23 showed the clean small-cap loss/mv benchmark is too weak in true QE smoke. No runtime consumer is changed.

## Scope

```text
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| item           | value                                                                                                                                      |
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| date range     | 2024-07-01 -> 2026-04-27                                                                                                                   |
| rules          | 12                                                                                                                                         |
| direct report  | reports\event_signal\financial_distress_phase24_signal_family_screen\direct\financial_distress_direct_event_20240701_20260511_001050.json  |
| overlay report | reports\event_signal\financial_distress_phase24_signal_family_screen\overlay\financial_distress_qe_multiloop_20240701_20260511_005309.json |
| runtime impact | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring                                                                        |
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
```

## Candidate Families

```text
+------------------------------+---------------------------------------------------------------------------------------+-------------------------------------------------------------+
| family                       | rule idea                                                                             | validation role                                             |
+------------------------------+---------------------------------------------------------------------------------------+-------------------------------------------------------------+
| expectation_miss             | forecast >=50% growth but actual misses; test gap>=50/100 and actual source           | direct event downside + cheap overlay top50 interaction     |
| profit_revenue_deterioration | indicator decline with sharper yoy, revenue divergence, both-down, or negative margin | screen all market-cap buckets; no small-cap-only assumption |
| cashflow_leverage_liquidity  | OCF decline, debt/assets>=70%, current ratio<1, combined financial quality stress     | research-only score-down; not hard ban/forced sell          |
+------------------------------+---------------------------------------------------------------------------------------+-------------------------------------------------------------+
```

## Combined Shortlist

```text
+-----------+-------+-------+--------+---------+--------+------+-----+----------+---------------------+---------+--------------------------------------------------------+
| decision  | score | pos   | avg    | ex_best | min    | drop | td  | mode     | direct              | t20_med | rule                                                   |
+-----------+-------+-------+--------+---------+--------+------+-----+----------+---------------------+---------+--------------------------------------------------------+
| WATCHLIST | 55.6  | 14/22 | 0.13%  | 0.05%   | -0.18% | 15   | 60  | fixed_10 | supports_downweight | -1.11%  | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn  |
| WATCHLIST | 44.6  | 14/22 | 0.11%  | 0.03%   | -0.00% | 6    | 60  | fixed_10 | mixed               | -0.25%  | indicator_decline_profit_revenue_diverge_mv_ge_10bn    |
| WATCHLIST | 43.4  | 13/22 | 0.12%  | 0.03%   | -0.13% | 6    | 60  | fixed_20 | supports_downweight | -1.58%  | indicator_decline_current_ratio_lt_1_mv_ge_10bn        |
| WATCHLIST | 43.2  | 14/22 | 0.11%  | 0.02%   | -0.10% | 6    | 60  | fixed_10 | supports_downweight | -0.50%  | indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn        |
| WATCHLIST | 39.8  | 13/22 | 0.04%  | 0.02%   | -0.32% | 10   | 60  | fixed_20 | supports_downweight | -0.70%  | indicator_decline_actual_yoy_le_minus100_mv_10_30bn    |
| WATCHLIST | 36.8  | 14/22 | 0.09%  | 0.00%   | -0.08% | 5    | 60  | fixed_10 | supports_downweight | -1.85%  | indicator_decline_debt_assets_ge_70_mv_ge_10bn         |
| WATCHLIST | 25.0  | 13/22 | 0.01%  | 0.00%   | -0.00% | 4    | 120 | fixed_20 | supports_downweight | -3.85%  | expectation_miss_gap_ge_100_mv_ge_10bn                 |
| WATCHLIST | 24.9  | 13/22 | 0.01%  | 0.00%   | -0.00% | 4    | 120 | fixed_20 | supports_downweight | -3.58%  | expectation_miss_gap_ge_100_mv_10_30bn                 |
| WATCHLIST | 20.0  | 14/22 | 0.01%  | -0.00%  | -0.10% | 6    | 20  | fixed_10 | supports_downweight | -1.56%  | indicator_decline_profit_revenue_both_down_mv_ge_10bn  |
| WATCHLIST | 18.3  | 12/22 | -0.01% | -0.01%  | -0.32% | 9    | 20  | fixed_20 | supports_downweight | -1.03%  | indicator_decline_actual_yoy_le_minus80_mv_ge_10bn     |
| WATCHLIST | 18.2  | 12/22 | 0.06%  | -0.01%  | -1.03% | 27   | 120 | fixed_20 | supports_downweight | -1.52%  | indicator_decline_negative_margin_mv_ge_10bn           |
| WATCHLIST | 15.3  | 12/22 | -0.01% | -0.02%  | -0.37% | 8    | 120 | fixed_20 | supports_downweight | -4.56%  | expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn |
+-----------+-------+-------+--------+---------+--------+------+-----+----------+---------------------+---------+--------------------------------------------------------+
```

## Direct Event Abnormal Returns

```text
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| rule                                                   | window | valid | abn_mean | abn_median | neg_rate | miss_px |
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
| expectation_miss_gap_ge_100_mv_ge_10bn                 | T+5    | 188   | -1.03%   | -0.79%     | 58.51%   | 0.53%   |
| expectation_miss_gap_ge_100_mv_ge_10bn                 | T+20   | 164   | -1.59%   | -3.85%     | 67.68%   | 13.23%  |
| expectation_miss_gap_ge_100_mv_ge_10bn                 | T+60   | 120   | 0.49%    | -2.78%     | 60.83%   | 36.51%  |
| expectation_miss_gap_ge_100_mv_10_30bn                 | T+5    | 152   | -0.92%   | -0.59%     | 57.24%   | 0.00%   |
| expectation_miss_gap_ge_100_mv_10_30bn                 | T+20   | 133   | -1.44%   | -3.58%     | 69.17%   | 12.50%  |
| expectation_miss_gap_ge_100_mv_10_30bn                 | T+60   | 104   | 1.98%    | -1.95%     | 58.65%   | 31.58%  |
| expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn | T+5    | 219   | -1.19%   | -0.62%     | 56.62%   | 0.45%   |
| expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn | T+20   | 184   | -3.33%   | -4.56%     | 71.20%   | 16.36%  |
| expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn | T+60   | 160   | 2.15%    | -2.48%     | 57.50%   | 27.27%  |
| indicator_decline_actual_yoy_le_minus100_mv_10_30bn    | T+5    | 513   | 0.71%    | 0.26%      | 48.15%   | 0.19%   |
| indicator_decline_actual_yoy_le_minus100_mv_10_30bn    | T+20   | 447   | 1.36%    | -0.70%     | 52.80%   | 13.04%  |
| indicator_decline_actual_yoy_le_minus100_mv_10_30bn    | T+60   | 425   | 5.55%    | -0.53%     | 51.76%   | 17.32%  |
| indicator_decline_actual_yoy_le_minus80_mv_ge_10bn     | T+5    | 888   | 0.81%    | 0.15%      | 48.76%   | 0.45%   |
| indicator_decline_actual_yoy_le_minus80_mv_ge_10bn     | T+20   | 774   | 1.39%    | -1.03%     | 53.88%   | 13.23%  |
| indicator_decline_actual_yoy_le_minus80_mv_ge_10bn     | T+60   | 736   | 5.23%    | -0.88%     | 52.17%   | 17.49%  |
| indicator_decline_profit_revenue_diverge_mv_ge_10bn    | T+5    | 367   | 0.41%    | -0.27%     | 52.59%   | 0.54%   |
| indicator_decline_profit_revenue_diverge_mv_ge_10bn    | T+20   | 295   | 2.29%    | -0.25%     | 51.19%   | 20.11%  |
| indicator_decline_profit_revenue_diverge_mv_ge_10bn    | T+60   | 272   | 7.99%    | 0.79%      | 48.16%   | 26.36%  |
| indicator_decline_profit_revenue_both_down_mv_ge_10bn  | T+5    | 458   | 0.73%    | 0.18%      | 47.82%   | 0.43%   |
| indicator_decline_profit_revenue_both_down_mv_ge_10bn  | T+20   | 421   | 0.30%    | -1.56%     | 57.72%   | 8.48%   |
| indicator_decline_profit_revenue_both_down_mv_ge_10bn  | T+60   | 410   | 1.05%    | -3.20%     | 58.78%   | 10.87%  |
| indicator_decline_negative_margin_mv_ge_10bn           | T+5    | 609   | 0.69%    | 0.02%      | 49.43%   | 0.65%   |
| indicator_decline_negative_margin_mv_ge_10bn           | T+20   | 533   | 0.97%    | -1.52%     | 55.16%   | 13.07%  |
| indicator_decline_negative_margin_mv_ge_10bn           | T+60   | 508   | 4.39%    | -1.09%     | 53.15%   | 17.16%  |
| indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn        | T+5    | 594   | 0.79%    | -0.08%     | 50.34%   | 0.34%   |
| indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn        | T+20   | 516   | 1.63%    | -0.50%     | 53.29%   | 13.42%  |
| indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn        | T+60   | 496   | 4.41%    | -0.91%     | 52.82%   | 16.78%  |
| indicator_decline_debt_assets_ge_70_mv_ge_10bn         | T+5    | 230   | 0.42%    | -0.32%     | 51.30%   | 2.13%   |
| indicator_decline_debt_assets_ge_70_mv_ge_10bn         | T+20   | 203   | 0.66%    | -1.85%     | 62.07%   | 13.62%  |
| indicator_decline_debt_assets_ge_70_mv_ge_10bn         | T+60   | 195   | 1.82%    | -2.72%     | 58.46%   | 17.02%  |
| indicator_decline_current_ratio_lt_1_mv_ge_10bn        | T+5    | 273   | -0.47%   | -0.76%     | 54.21%   | 0.73%   |
| indicator_decline_current_ratio_lt_1_mv_ge_10bn        | T+20   | 243   | -0.14%   | -1.58%     | 59.26%   | 11.64%  |
| indicator_decline_current_ratio_lt_1_mv_ge_10bn        | T+60   | 233   | 1.06%    | -2.97%     | 62.23%   | 15.27%  |
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn  | T+5    | 995   | 0.49%    | -0.07%     | 50.35%   | 0.50%   |
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn  | T+20   | 866   | 0.93%    | -1.11%     | 55.89%   | 13.40%  |
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn  | T+60   | 830   | 3.30%    | -1.91%     | 55.66%   | 17.00%  |
+--------------------------------------------------------+--------+-------+----------+------------+----------+---------+
```

## Cheap Overlay Top Rows

```text
+-------+------------------+-------+-------+---------+--------+------+-----+----------+-------------------------------------------------------+
| score | decision         | pos   | avg   | ex_best | min    | drop | td  | mode     | rule                                                  |
+-------+------------------+-------+-------+---------+--------+------+-----+----------+-------------------------------------------------------+
| 55.6  | WATCHLIST        | 14/22 | 0.13% | 0.05%   | -0.18% | 15   | 60  | fixed_10 | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn |
| 44.6  | WATCHLIST        | 14/22 | 0.11% | 0.03%   | -0.00% | 6    | 60  | fixed_10 | indicator_decline_profit_revenue_diverge_mv_ge_10bn   |
| 43.4  | WATCHLIST        | 13/22 | 0.12% | 0.03%   | -0.13% | 6    | 60  | fixed_20 | indicator_decline_current_ratio_lt_1_mv_ge_10bn       |
| 43.2  | WATCHLIST        | 14/22 | 0.11% | 0.02%   | -0.10% | 6    | 60  | fixed_10 | indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn       |
| 39.8  | WATCHLIST        | 13/22 | 0.04% | 0.02%   | -0.32% | 10   | 60  | fixed_20 | indicator_decline_actual_yoy_le_minus100_mv_10_30bn   |
| 38.7  | WATCHLIST        | 14/22 | 0.10% | 0.01%   | -0.10% | 6    | 20  | fixed_10 | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn |
| 38.2  | WATCHLIST        | 13/22 | 0.10% | 0.02%   | -0.13% | 5    | 60  | fixed_10 | indicator_decline_current_ratio_lt_1_mv_ge_10bn       |
| 36.8  | WATCHLIST        | 14/22 | 0.09% | 0.00%   | -0.08% | 5    | 60  | fixed_10 | indicator_decline_debt_assets_ge_70_mv_ge_10bn        |
| 36.8  | WATCHLIST        | 14/22 | 0.09% | 0.00%   | -0.08% | 5    | 60  | fixed_20 | indicator_decline_debt_assets_ge_70_mv_ge_10bn        |
| 32.3  | WATCHLIST        | 14/22 | 0.09% | 0.01%   | -0.00% | 3    | 20  | fixed_10 | indicator_decline_debt_assets_ge_70_mv_ge_10bn        |
| 32.3  | WATCHLIST        | 14/22 | 0.09% | 0.01%   | -0.00% | 3    | 20  | fixed_20 | indicator_decline_debt_assets_ge_70_mv_ge_10bn        |
| 31.7  | WATCHLIST        | 13/22 | 0.02% | 0.00%   | -0.32% | 8    | 60  | fixed_10 | indicator_decline_actual_yoy_le_minus100_mv_10_30bn   |
| 31.0  | CALIBRATION_ONLY | 13/22 | 0.08% | -0.01%  | -0.47% | 21   | 120 | fixed_10 | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn |
| 30.5  | WATCHLIST        | 13/22 | 0.09% | 0.00%   | -0.10% | 4    | 20  | fixed_10 | indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn       |
| 30.0  | WATCHLIST        | 13/22 | 0.09% | 0.00%   | -0.00% | 3    | 20  | fixed_10 | indicator_decline_profit_revenue_diverge_mv_ge_10bn   |
| 30.0  | WATCHLIST        | 13/22 | 0.09% | 0.00%   | -0.00% | 3    | 20  | fixed_20 | indicator_decline_profit_revenue_diverge_mv_ge_10bn   |
| 27.7  | WATCHLIST        | 13/22 | 0.09% | 0.00%   | -0.00% | 2    | 20  | fixed_10 | indicator_decline_current_ratio_lt_1_mv_ge_10bn       |
| 27.7  | WATCHLIST        | 13/22 | 0.09% | 0.00%   | -0.00% | 2    | 20  | fixed_20 | indicator_decline_current_ratio_lt_1_mv_ge_10bn       |
| 25.7  | CALIBRATION_ONLY | 11/22 | 0.08% | -0.00%  | -0.53% | 14   | 120 | fixed_20 | indicator_decline_current_ratio_lt_1_mv_ge_10bn       |
| 25.5  | CALIBRATION_ONLY | 13/22 | 0.06% | -0.03%  | -0.44% | 12   | 120 | fixed_10 | indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn       |
+-------+------------------+-------+-------+---------+--------+------+-----+----------+-------------------------------------------------------+
```

## Interpretation

- Cheap overlay remains a shortlist gate only; true QE is required before promotion.
- Financial signals are still non-hard: no buy ban, no forced sell, no alpha boost in this phase.
- Rules with direct downside but poor overlay stay research features; rules with overlay benefit but no direct downside stay calibration-only.
- If no rule reaches TRUE_QE_CANDIDATE, next research should refine thresholds or test another structured source before LLM/PDF.
## Phase 24 Conclusion

```text
+-------------------------------------------------------+----------------------+--------------------------------------------------------------+
| item                                                  | conclusion           | evidence                                                     |
+-------------------------------------------------------+----------------------+--------------------------------------------------------------+
| best watchlist rule                                   | OCF/leverage stress  | avg +0.13%, ex-best +0.05%, worst -0.18%, direct downside   |
| true-QE promotion                                     | not yet              | best cheap score 55.6 is below strict 60 gate               |
| expectation-miss rules                                | direct-only watch    | T+20 median abnormal -3.6% to -4.6%, but overlay weak       |
| next empirical step                                   | refine or smoke      | refine top rule thresholds, then consider one-loop WSL smoke |
+-------------------------------------------------------+----------------------+--------------------------------------------------------------+
```
