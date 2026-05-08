# Event Signal Financial Distress Market-Cap Bucket QE Overlay Result - 2026-05-08

## Scope

```text
Worktree      : F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
Branch        : codex/financial-distress-rerank-20260508
Research type : offline QE overlay only
Date range    : 2024-07-01 -> 2026-04-27
QE loops      : 10
Rule families : first batch + size bucket + loss history
Validations   : 840
Bucket rows   : 504
Report JSON   : reports/event_signal/financial_distress_market_cap_bucket_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_190841.json
Runtime impact: none
```

## What Changed In This Phase

The offline multi-loop report now includes `market_cap_bucket_summary`. Every rule, active lifetime, simulator mode, and market-cap bucket is represented in JSON, including zero-count buckets. This prevents small-cap financial-distress evidence from being generalized to the whole market without proof.

## Top Stability Rows

```text
+-----+----------------------------------------+-------------------+------+------+------+------+---------+---------+---------+
| td  | rule                                   | mode              | pos  | eval | drop | repl | avg_ret | min_ret | max_ret |
+-----+----------------------------------------+-------------------+------+------+------+------+---------+---------+---------+
| 120 | loss_20_50pct_and_loss_reports_ge_4    | rank20_prev       | 5/10 | 326  | 10   | 7    | 0.23%   | -0.29%  | 1.67%   |
| 120 | loss_20_50pct_and_loss_reports_ge_4    | severity_bal_prev | 5/10 | 326  | 10   | 7    | 0.23%   | -0.29%  | 1.67%   |
| 60  | forecast_loss_to_market_cap_ge_50pct   | rank20_prev       | 6/10 | 78   | 6    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct            | rank20_prev       | 6/10 | 78   | 6    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct_mv_lt_10bn | rank20_prev       | 6/10 | 77   | 6    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct_mv_lt_5bn  | rank20_prev       | 6/10 | 73   | 6    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | forecast_loss_to_market_cap_ge_50pct   | severity_bal_prev | 6/10 | 78   | 5    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct            | severity_bal_prev | 6/10 | 78   | 5    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct_mv_lt_10bn | severity_bal_prev | 6/10 | 77   | 5    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 60  | loss_to_market_cap_ge_50pct_mv_lt_5bn  | severity_bal_prev | 6/10 | 73   | 5    | 3    | 0.20%   | -0.00%  | 1.02%   |
| 242 | loss_20_50pct_and_loss_reports_ge_4    | severity_bal_prev | 4/10 | 560  | 13   | 11   | 0.20%   | -0.33%  | 1.67%   |
| 120 | loss_to_market_cap_20_50pct            | rank20_prev       | 4/10 | 774  | 28   | 24   | 0.20%   | -0.59%  | 1.67%   |
| 242 | loss_20_50pct_and_loss_reports_ge_4    | rank20_prev       | 4/10 | 560  | 14   | 11   | 0.19%   | -0.33%  | 1.67%   |
| 120 | forecast_loss_to_market_cap_ge_50pct   | rank20_prev       | 6/10 | 124  | 9    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | loss_to_market_cap_ge_50pct            | rank20_prev       | 6/10 | 137  | 9    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | loss_to_market_cap_ge_50pct_mv_lt_10bn | rank20_prev       | 6/10 | 136  | 9    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | loss_to_market_cap_ge_50pct_mv_lt_5bn  | rank20_prev       | 6/10 | 111  | 9    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | forecast_loss_to_market_cap_ge_50pct   | severity_bal_prev | 6/10 | 124  | 8    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | loss_to_market_cap_ge_50pct            | severity_bal_prev | 6/10 | 137  | 8    | 4    | 0.18%   | -0.00%  | 0.80%   |
| 120 | loss_to_market_cap_ge_50pct_mv_lt_10bn | severity_bal_prev | 6/10 | 136  | 8    | 4    | 0.18%   | -0.00%  | 0.80%   |
+-----+----------------------------------------+-------------------+------+------+------+------+---------+---------+---------+
```

## Market-Cap Bucket Sample

This table shows 120 trading-day lifetime with `rank20_prev` for the current benchmark, a medium/large-cap severe-loss split, broad loss-history, and the incremental loss-history rule. Composite overlay rows may count into more than one bucket when multiple active events share the same stock/date.

```text
+-------------------------------------------+----------+---------+--------+------+------+-----------+-------+
| rule                                      | mv       | overlay | ov_sh  | eval | drop | drop_rate | still |
+-------------------------------------------+----------+---------+--------+------+------+-----------+-------+
| loss_reports_ge_4                         | <5bn     | 863280  | 68.82% | 1641 | 44   | 2.68%     | 1577  |
| loss_reports_ge_4                         | 5-10bn   | 228060  | 18.18% | 206  | 5    | 2.43%     | 200   |
| loss_reports_ge_4                         | 10-30bn  | 116980  | 9.33%  | 71   | 5    | 7.04%     | 66    |
| loss_reports_ge_4                         | 30-100bn | 36860   | 2.94%  | 35   | 1    | 2.86%     | 34    |
| loss_reports_ge_4                         | >=100bn  | 630     | 0.05%  | 0    | 0    | NA        | 0     |
| loss_reports_ge_4                         | unknown  | 25280   | 2.02%  | 5    | 0    | 0.00%     | 5     |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | <5bn     | 845090  | 80.12% | 1619 | 43   | 2.66%     | 1557  |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 5-10bn   | 214750  | 20.36% | 184  | 5    | 2.72%     | 178   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 10-30bn  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 30-100bn | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | >=100bn  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | unknown  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | <5bn     | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | 5-10bn   | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | 10-30bn  | 4760    | 73.23% | 1    | 0    | 0.00%     | 1     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | 30-100bn | 1740    | 26.77% | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | >=100bn  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_ge_10bn    | unknown  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | <5bn     | 65520   | 77.31% | 112  | 9    | 8.04%     | 102   |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 5-10bn   | 19400   | 22.89% | 25   | 0    | 0.00%     | 25    |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 10-30bn  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 30-100bn | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | >=100bn  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | unknown  | 0       | 0.00%  | 0    | 0    | NA        | 0     |
+-------------------------------------------+----------+---------+--------+------+------+-----------+-------+
```

## Aggregate Bucket Exposure

This aggregates all current financial-distress rule families at 120 trading-day lifetime and `rank20_prev`. It is not a decision rule; it shows where current structured financial-distress candidates actually hit the QE Top50 list.

```text
+----------+---------+------+------+-----------+
| mv       | overlay | eval | drop | drop_rate |
+----------+---------+------+------+-----------+
| <5bn     | 4850240 | 9558 | 284  | 2.97%     |
| 5-10bn   | 1247040 | 1178 | 28   | 2.38%     |
| 10-30bn  | 287430  | 198  | 16   | 8.08%     |
| 30-100bn | 83080   | 72   | 2    | 2.78%     |
| >=100bn  | 1260    | 0    | 0    | NA        |
| unknown  | 49640   | 10   | 0    | 0.00%     |
+----------+---------+------+------+-----------+
```

## Decision

```text
+------------------------------+-----------------------------------------------------+--------------------------------------------------------------------------+
| question                     | answer                                              | evidence                                                                 |
+------------------------------+-----------------------------------------------------+--------------------------------------------------------------------------+
| Only small-cap?              | No for generation; yes for current strongest effect | All buckets are now reported; Top50 drops are concentrated <10bn.        |
| Can large-cap use same rule? | No                                                  | >=100bn has no Top50 eval/drop in this family; 10-30bn sample is sparse. |
| Keep small-cap baseline?     | Yes                                                 | loss/mv>=50% + mv<10bn remains stable benchmark.                         |
| Next research priority       | Medium/large event families                         | Impairment, audit opinion, regulatory, debt, expectation miss.           |
+------------------------------+-----------------------------------------------------+--------------------------------------------------------------------------+
```

## Interpretation

- Signal generation must cover all market-cap buckets; the implementation now records every bucket for every rule/mode.
- Current financial-distress evidence is still concentrated below 10bn CNY market cap. This supports the small-cap benchmark but does not justify a small-cap-only architecture.
- The 10-30bn bucket has some Top50 drops and a high drop-rate in aggregate, but the sample is small and mixed across rule families; this needs dedicated medium-cap event research.
- The >=100bn bucket has almost no current financial-distress Top50 interaction, so raw loss/mv and rolling-loss rules are not enough for large-cap risk detection.
- Medium/large-cap research should shift to event families with stronger economic relevance: impairment, non-standard audit opinion, regulatory investigation/penalty, debt stress, and forecast-vs-report expectation miss.

## Next Phase

```text
+-------+--------------------------------------------+------------------------------------------------------------+
| phase | task                                       | required output                                            |
+-------+--------------------------------------------+------------------------------------------------------------+
| 9     | medium/large-cap event-family research     | rules and 10-loop overlay grouped by market-cap bucket     |
| 10    | loss history + industry + size interaction | prove incremental value vs loss/mv>=50% + mv<10bn baseline |
| 11    | forecast/express/report mismatch           | expectation-miss signal independent from raw growth/loss   |
+-------+--------------------------------------------+------------------------------------------------------------+
```

## Boundary

```text
writes_db=false
changes_qe_runtime=false
changes_selection_center=false
changes_paper_trading=false
changes_qmt_or_live_trading=false
financial_signals_hard_block_enabled=false
financial_signals_force_exit_enabled=false
```
