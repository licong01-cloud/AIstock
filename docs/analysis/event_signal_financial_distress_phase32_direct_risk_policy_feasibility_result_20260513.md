# Financial Distress Phase 32 Direct-Risk Policy Feasibility

Research-only study of direct event-risk policy feasibility. No runtime consumer is changed.

## Scope

```text
+----------------+---------------------------------------------------------------------+
| item           | value                                                               |
+----------------+---------------------------------------------------------------------+
| date range     | 2024-07-01 -> 2026-04-27                                            |
| rules          | 46                                                                  |
| return windows | [1, 5, 20, 60, 120]                                                 |
| benchmark      | 000300.SH                                                           |
| events         | 13,563                                                              |
| return rows    | 67,815                                                              |
| runtime impact | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring |
+----------------+---------------------------------------------------------------------+
```

## Outcome

```text
+----------------------------+-------+--------------------------------------------------------------+
| item                       | value | interpretation                                               |
+----------------------------+-------+--------------------------------------------------------------+
| risk-downweight candidates | 37    | eligible for later offline overlay research, not live policy |
| watchlist policy research  | 3     | has direct downside but needs policy-shape validation        |
| short warning only         | 0     | short-lived evidence only                                    |
| too sparse                 | 3     | sample too small for policy                                  |
| reject/mixed               | 3     | no direct-risk policy support                                |
| hard ban / forced sell     | 0     | financial rules remain non-hard in this phase                |
+----------------------------+-------+--------------------------------------------------------------+
```

## Direct Policy Shortlist

```text
+---------------------------+--------------------+-------+-------+---------+---------+---------+---------+-----------------------------------------------------------------------------+
| decision                  | shape              | score | valid | t20_med | t20_neg | t60_med | t60_neg | rule                                                                        |
+---------------------------+--------------------+-------+-------+---------+---------+---------+---------+-----------------------------------------------------------------------------+
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 11.0  | 82    | -1.53%  | 60.98%  | -2.58%  | 61.25%  | indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn       |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_20td | 10.5  | 185   | -4.56%  | 71.35%  | -2.48%  | 57.50%  | expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn                      |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 109   | -1.33%  | 59.63%  | -2.95%  | 60.75%  | indicator_decline_current_ratio_lt_08_mv_10_30bn                            |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 243   | -1.58%  | 59.26%  | -2.97%  | 62.23%  | indicator_decline_current_ratio_lt_1_mv_ge_10bn                             |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 203   | -1.85%  | 62.07%  | -2.72%  | 58.46%  | indicator_decline_debt_assets_ge_70_mv_ge_10bn                              |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 113   | -1.61%  | 60.18%  | -4.29%  | 62.39%  | indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn            |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 421   | -1.56%  | 57.72%  | -3.20%  | 58.78%  | indicator_decline_profit_revenue_both_down_mv_ge_10bn                       |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.5  | 116   | -1.29%  | 58.62%  | -1.75%  | 57.14%  | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn  |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_20td | 10.0  | 72    | -2.70%  | 65.28%  | -0.53%  | 55.07%  | indicator_decline_debt_assets_ge_80_mv_ge_10bn                              |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 10.0  | 97    | -3.41%  | 69.07%  | -4.74%  | 63.83%  | indicator_decline_profit_revenue_both_down_mv_30_100bn                      |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 152   | -3.10%  | 60.53%  | -1.44%  | 55.00%  | indicator_decline_actual_yoy_le_minus80_mv_30_100bn                         |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 774   | -1.03%  | 53.88%  | -0.88%  | 52.17%  | indicator_decline_actual_yoy_le_minus80_mv_ge_10bn                          |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 140   | -1.59%  | 61.43%  | -2.52%  | 60.00%  | indicator_decline_current_ratio_lt_08_mv_ge_10bn                            |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 406   | -1.12%  | 53.20%  | -0.99%  | 52.82%  | indicator_decline_negative_margin_mv_10_30bn                                |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 122   | -2.38%  | 60.66%  | -1.07%  | 53.10%  | indicator_decline_negative_margin_mv_30_100bn                               |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 533   | -1.52%  | 55.16%  | -1.09%  | 53.15%  | indicator_decline_negative_margin_mv_ge_10bn                                |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 557   | -1.19%  | 54.58%  | -0.91%  | 53.00%  | indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 176   | -3.12%  | 66.48%  | -2.63%  | 57.93%  | indicator_decline_ocf_negative_or_leverage_mv_30_100bn                      |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 866   | -1.11%  | 55.89%  | -1.91%  | 55.66%  | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn                       |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 161   | -3.43%  | 68.32%  | -4.03%  | 60.93%  | indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn       |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 316   | -0.94%  | 53.48%  | -2.48%  | 56.17%  | indicator_decline_profit_revenue_both_down_mv_10_30bn                       |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.5   | 301   | -1.16%  | 55.48%  | -0.86%  | 52.25%  | indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn               |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.0   | 35    | -3.15%  | 65.71%  | -8.05%  | 72.41%  | expectation_miss_gap_ge_100_mv_10_30bn_prior_loss_ge_2                      |
| RISK_DOWNWEIGHT_CANDIDATE | avoid_new_buy_60td | 9.0   | 45    | -3.15%  | 64.44%  | -8.39%  | 74.29%  | expectation_miss_gap_ge_100_mv_ge_10bn_prior_loss_ge_2                      |
+---------------------------+--------------------+-------+-------+---------+---------+---------+---------+-----------------------------------------------------------------------------+
```

## Strongest Rule/Window Evidence

```text
+-------+-----+-------+--------+---------+---------+--------+--------+-----------------------------------------------------------------------------+
| score | win | valid | mean   | median  | p25     | neg    | loss10 | rule                                                                        |
+-------+-----+-------+--------+---------+---------+--------+--------+-----------------------------------------------------------------------------+
| 10.0  | 120 | 80    | -3.92% | -11.63% | -22.59% | 70.00% | 56.25% | indicator_decline_ocf_yoy_le_minus50_and_debt_assets_ge_70_mv_ge_10bn       |
| 9.5   | 20  | 185   | -3.39% | -4.56%  | -8.71%  | 71.35% | 20.54% | expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn                      |
| 9.5   | 120 | 107   | 0.59%  | -6.16%  | -17.95% | 63.55% | 40.19% | indicator_decline_current_ratio_lt_08_mv_10_30bn                            |
| 9.5   | 120 | 233   | -1.22% | -6.61%  | -21.19% | 63.09% | 42.49% | indicator_decline_current_ratio_lt_1_mv_ge_10bn                             |
| 9.5   | 120 | 195   | -3.65% | -10.23% | -22.84% | 68.21% | 50.77% | indicator_decline_debt_assets_ge_70_mv_ge_10bn                              |
| 9.5   | 60  | 109   | 2.09%  | -4.29%  | -11.72% | 62.39% | 31.19% | indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn            |
| 9.5   | 120 | 109   | 0.94%  | -6.16%  | -22.48% | 63.30% | 43.12% | indicator_decline_ocf_negative_and_current_ratio_lt_1_mv_ge_10bn            |
| 9.5   | 120 | 311   | -3.72% | -8.57%  | -21.50% | 63.99% | 45.98% | indicator_decline_profit_revenue_both_down_mv_10_30bn                       |
| 9.5   | 120 | 413   | -3.70% | -8.94%  | -22.26% | 63.68% | 46.73% | indicator_decline_profit_revenue_both_down_mv_ge_10bn                       |
| 9.5   | 120 | 112   | -0.74% | -8.14%  | -22.13% | 64.29% | 48.21% | indicator_decline_q_ocf_to_sales_lt_0_and_leverage_or_liquidity_mv_ge_10bn  |
| 9.0   | 120 | 69    | -2.34% | -8.44%  | -22.78% | 63.77% | 46.38% | indicator_decline_debt_assets_ge_80_mv_ge_10bn                              |
| 9.0   | 60  | 94    | -2.04% | -4.74%  | -15.26% | 63.83% | 34.04% | indicator_decline_profit_revenue_both_down_mv_30_100bn                      |
| 8.5   | 120 | 426   | 5.49%  | -5.68%  | -18.47% | 58.69% | 42.25% | indicator_decline_actual_yoy_le_minus100_mv_10_30bn                         |
| 8.5   | 120 | 588   | 7.30%  | -4.87%  | -18.36% | 57.99% | 40.82% | indicator_decline_actual_yoy_le_minus80_mv_10_30bn                          |
| 8.5   | 120 | 139   | -0.23% | -9.67%  | -22.00% | 59.71% | 49.64% | indicator_decline_actual_yoy_le_minus80_mv_30_100bn                         |
| 8.5   | 120 | 737   | 5.81%  | -5.29%  | -19.47% | 58.21% | 42.33% | indicator_decline_actual_yoy_le_minus80_mv_ge_10bn                          |
| 8.5   | 120 | 135   | 0.30%  | -4.91%  | -18.67% | 61.48% | 40.74% | indicator_decline_current_ratio_lt_08_mv_ge_10bn                            |
| 8.5   | 120 | 392   | 4.52%  | -6.25%  | -19.25% | 60.97% | 41.58% | indicator_decline_negative_margin_mv_10_30bn                                |
| 8.5   | 120 | 111   | 0.22%  | -10.21% | -22.88% | 58.56% | 50.45% | indicator_decline_negative_margin_mv_30_100bn                               |
| 8.5   | 120 | 508   | 3.43%  | -6.57%  | -20.47% | 60.63% | 43.50% | indicator_decline_negative_margin_mv_ge_10bn                                |
| 8.5   | 120 | 533   | 4.67%  | -5.46%  | -19.14% | 59.29% | 42.40% | indicator_decline_ocf_negative_or_leverage_actual_yoy_le_minus80_mv_ge_10bn |
| 8.5   | 120 | 659   | 4.12%  | -6.53%  | -18.40% | 61.00% | 42.03% | indicator_decline_ocf_negative_or_leverage_mv_10_30bn                       |
| 8.5   | 120 | 163   | -1.79% | -10.21% | -22.88% | 59.51% | 50.31% | indicator_decline_ocf_negative_or_leverage_mv_30_100bn                      |
| 8.5   | 120 | 831   | 2.78%  | -7.23%  | -20.07% | 60.89% | 43.80% | indicator_decline_ocf_negative_or_leverage_mv_ge_10bn                       |
| 8.5   | 120 | 152   | -0.83% | -10.23% | -23.50% | 66.45% | 50.66% | indicator_decline_ocf_negative_or_leverage_prior_loss_ge_2_mv_ge_10bn       |
| 8.5   | 120 | 398   | 5.99%  | -5.78%  | -17.25% | 59.05% | 40.95% | indicator_decline_ocf_yoy_le_minus50_mv_10_30bn                             |
| 8.5   | 120 | 497   | 4.89%  | -6.51%  | -18.89% | 59.56% | 42.86% | indicator_decline_ocf_yoy_le_minus50_mv_ge_10bn                             |
| 8.5   | 120 | 293   | 5.95%  | -5.46%  | -18.38% | 56.66% | 43.69% | indicator_decline_q_ocf_to_sales_lt_0_actual_yoy_le_minus80_mv_ge_10bn      |
| 8.5   | 120 | 256   | 4.98%  | -6.85%  | -18.99% | 57.81% | 45.70% | indicator_decline_q_ocf_to_sales_lt_0_and_ocf_yoy_le_minus50_mv_ge_10bn     |
| 8.5   | 120 | 289   | 4.99%  | -7.30%  | -20.09% | 59.17% | 46.71% | indicator_decline_q_ocf_to_sales_lt_0_multi_stress_mv_ge_10bn               |
+-------+-----+-------+--------+---------+---------+--------+--------+-----------------------------------------------------------------------------+
```

## Event-Type Evidence

```text
+-------+-----------------------------------------+-----+--------+--------+--------+--------+
| score | event_type                              | win | valid  | median | neg    | loss10 |
+-------+-----------------------------------------+-----+--------+--------+--------+--------+
| 8.5   | financial_indicator_large_decline       | 120 | 10,656 | -6.39% | 59.51% | 43.64% |
| 7.5   | financial_positive_but_miss_expectation | 20  | 564    | -3.85% | 68.97% | 22.34% |
| 7.5   | financial_positive_but_miss_expectation | 120 | 449    | -5.46% | 59.47% | 38.31% |
| 6.5   | financial_positive_but_miss_expectation | 60  | 448    | -3.01% | 60.94% | 32.81% |
| 5.5   | financial_indicator_large_decline       | 60  | 10,642 | -1.29% | 54.10% | 26.72% |
| 4.5   | financial_indicator_large_decline       | 20  | 11,134 | -1.08% | 55.22% | 12.86% |
+-------+-----------------------------------------+-----+--------+--------+--------+--------+
```

## Interpretation

- Phase 32 does not promote any financial signal to hard buy-ban or forced-sell policy.
- Direct downside can justify later offline studies of avoid-new-buy windows or score downweighting outside the alpha factor path.
- Rules with strong direct downside but sparse samples should remain watchlist-only until more history or broader cohorts are available.
- Next empirical step should test the top risk-downweight candidates as portfolio overlays, still outside QE/Paper runtime integration.
