# AIstock Financial Distress Multi-Loop QE Overlay Result

Date: 2026-05-08
Scope: read-only event_signal research script; no QE runtime / Selection / Paper / QMT / simulated / live trading integration.
Runtime: WSL `rdagent-gpu`, using existing QE artifacts only.
Generated report: `reports/event_signal/financial_distress_qe_multiloop/financial_distress_qe_multiloop_20240701_20260508_101734.json`

## 1. Tested Loops

```text
+-------------------------------------------+--------+------------+------------+
| experiment_id                             | loop   | start      | end        |
+-------------------------------------------+--------+------------+------------+
| qe_20260507_132049_d4e7                   | Loop1  | 2024-07-01 | 2026-04-27 |
| qe_20260507_132049_d4e7                   | Loop2  | 2024-07-01 | 2026-04-27 |
| qe_20260506_182113                        | Loop1  | 2024-07-01 | 2026-04-27 |
| qe_20260505_153534_388f                   | Loop1  | 2024-07-01 | 2026-04-27 |
| qe_20260505_153534_388f                   | Loop2  | 2024-07-01 | 2026-04-27 |
| qe_20260505_122348_690d                   | Loop1  | 2024-07-01 | 2026-04-27 |
| qe_20260505_122348_690d                   | Loop2  | 2024-07-01 | 2026-04-27 |
| qe_20260501_011054_c90a                   | Loop19 | 2024-07-01 | 2026-04-27 |
| qe_20260501_011054_c90a                   | Loop24 | 2024-07-01 | 2026-04-27 |
| qe_20260501_011054_c90a                   | Loop26 | 2024-07-01 | 2026-04-27 |
+-------------------------------------------+--------+------------+------------+
```

## 2. Main Conclusion

```text
+--------------------------------------+--------------------------------------------------------------+
| Question                             | Evidence-based conclusion                                    |
+--------------------------------------+--------------------------------------------------------------+
| Cash-only no-buy overlay             | Not validated. Most cash modes reduce return across loops.   |
| Replacement / next-candidate overlay | Promising, but still an offline approximation, not a formal   |
|                                      | QE execution strategy.                                       |
| Best candidate family                | loss_to_market_cap_ge_50pct remains the cleanest candidate.  |
| Broad rolling-loss rule              | Cash mode is consistently harmful; use only as a future       |
|                                      | score-down / replacement research candidate.                 |
| Size exposure                        | Signals concentrate strongly in small market-cap buckets.    |
| Industry exposure                    | Industry strings need source-encoding cleanup before using    |
|                                      | industry-level conclusions.                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## 3. Stability Summary

```text
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
| active_td | rule_key                             | mode           | pos/loops | blocked | avg_ret_d | med_ret_d | min_ret_d | max_ret_d |
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
| 242       | loss_to_market_cap_20_50pct          | next_candidate | 7/10      | 1475    | 25.68%    | 33.49%    | -22.97%   | 82.62%    |
| 242       | forecast_loss_and_loss_reports_ge_4  | next_candidate | 6/10      | 2305    | 24.18%    | 22.19%    | -37.42%   | 98.74%    |
| 120       | forecast_loss_and_loss_reports_ge_4  | next_candidate | 6/10      | 1918    | 23.13%    | 24.85%    | -33.33%   | 108.07%   |
| 242       | loss_20_50pct_and_loss_reports_ge_4  | next_candidate | 7/10      | 561     | 17.48%    | 22.27%    | -10.06%   | 50.73%    |
| 120       | loss_to_market_cap_20_50pct          | next_candidate | 6/10      | 777     | 15.31%    | 21.01%    | -9.58%    | 38.62%    |
| 242       | loss_to_market_cap_ge_50pct          | next_candidate | 9/10      | 250     | 12.46%    | 12.32%    | -3.91%    | 33.59%    |
| 120       | loss_to_market_cap_ge_50pct          | next_candidate | 7/10      | 138     | 4.67%     | 3.47%     | -3.61%    | 22.80%    |
| 60        | loss_to_market_cap_ge_50pct          | next_candidate | 7/10      | 78      | 0.86%     | 1.01%     | -1.85%    | 3.34%     |
| 60        | loss_to_market_cap_ge_50pct          | cash           | 3/10      | 78      | -1.01%    | -1.04%    | -3.06%    | 1.26%     |
| 120       | loss_to_market_cap_ge_50pct          | cash           | 2/10      | 138     | -2.25%    | -2.31%    | -5.17%    | 0.79%     |
| 242       | loss_to_market_cap_ge_50pct          | cash           | 1/10      | 250     | -3.52%    | -3.72%    | -7.83%    | 1.33%     |
| 60        | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 1048    | -7.59%    | -8.11%    | -12.60%   | -3.33%    |
| 120       | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 1918    | -18.92%   | -20.30%   | -30.00%   | -11.46%   |
| 242       | forecast_loss_and_loss_reports_ge_4  | cash           | 0/10      | 2305    | -21.18%   | -21.18%   | -32.91%   | -11.86%   |
+-----------+--------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
```

## 4. Interpretation For Development

1. Do not implement financial distress as hard no-buy / force-sell based on this result.
2. The most useful next direction is a soft overlay: `score_down`, `buy_next_candidate`, or candidate re-ranking.
3. `loss_to_market_cap_ge_50pct` is still the cleanest rule because it affects fewer trades and has the most interpretable financial meaning.
4. Long active windows can look good in `next_candidate` mode, but they are also more different from real QE execution; treat 242td as a research hypothesis, not a policy.
5. Small-cap concentration is material. The next validation should split `loss_to_market_cap_ge_50pct` by market-cap bucket before any formal signal design.

## 5. Boundaries

```text
writes_db=false
changes_qe_runtime=false
changes_selection_center=false
changes_paper_trading=false
changes_qmt_or_live_trading=false
financial_signals_hard_block_enabled=false
financial_signals_force_exit_enabled=false
```
