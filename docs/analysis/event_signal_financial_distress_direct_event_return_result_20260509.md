# Phase 11 Direct Event Return and Sector Attribution Research - 2026-05-09

Research-only direct event study for the current financial-distress candidates. This phase checks whether the signals are unconditional negative events or only useful inside the QE Top50/model-ranking context. Industry exposure remains explanatory only; no industry neutralization is applied.

## Scope

```text
+------------------+--------------------------------------------------------------------------------------------------------------------------+
| item             | value                                                                                                                    |
+------------------+--------------------------------------------------------------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                                                                                 |
| runtime boundary | no QE/Paper/Selection/QMT runtime integration                                                                            |
| date range       | 2024-07-01 -> 2026-04-27                                                                                                 |
| benchmark        | 000300.SH                                                                                                                |
| rules            | 4                                                                                                                        |
| events           | 3713                                                                                                                     |
| return rows      | 22278                                                                                                                    |
| generated report | reports/event_signal/financial_distress_direct_event_returns/financial_distress_direct_event_20240701_20260509_093006.md |
+------------------+--------------------------------------------------------------------------------------------------------------------------+
```

## Rule/Window Return Summary

Returns use the effective trade date as T0. Raw return is measured from T0 close to target close. Abnormal return subtracts `000300.SH` over the same T0-to-target window.

```text
+----------------------------+--------+-------+-------+--------+----------+------------+----------+---------+
| rule                       | window | valid | mean  | median | abn_mean | abn_median | neg_rate | miss_px |
+----------------------------+--------+-------+-------+--------+----------+------------+----------+---------+
| smallcap_loss_mv50         | T0+1   | 71    | 0.39% | 0.57%  | 0.19%    | -0.00%     | 38.03%   | 10.13%  |
| smallcap_loss_mv50         | T0+5   | 72    | 3.59% | 2.84%  | 2.18%    | 2.01%      | 27.78%   | 8.86%   |
| smallcap_loss_mv50         | T0+20  | 69    | 9.64% | 5.88%  | 8.16%    | 4.72%      | 23.19%   | 12.66%  |
| smallcap_loss_mv50         | T0+60  | 47    | 7.75% | 1.65%  | 5.64%    | -0.55%     | 44.68%   | 40.51%  |
| indicator_decline_10_30bn  | T0+1   | 1118  | 0.14% | 0.04%  | 0.07%    | -0.03%     | 46.78%   | 0.27%   |
| indicator_decline_10_30bn  | T0+5   | 1118  | 1.64% | 0.97%  | 0.52%    | 0.04%      | 40.97%   | 0.27%   |
| indicator_decline_10_30bn  | T0+20  | 971   | 3.63% | 0.67%  | 1.41%    | -0.52%     | 46.76%   | 13.38%  |
| indicator_decline_10_30bn  | T0+60  | 931   | 9.76% | 4.18%  | 4.72%    | -1.26%     | 42.43%   | 16.95%  |
| indicator_decline_30_100bn | T0+1   | 264   | 0.06% | 0.07%  | 0.00%    | -0.19%     | 46.97%   | 0.75%   |
| indicator_decline_30_100bn | T0+5   | 264   | 1.37% | 0.44%  | 0.45%    | -0.59%     | 45.08%   | 0.75%   |
| indicator_decline_30_100bn | T0+20  | 232   | 2.43% | -1.45% | -0.36%   | -3.06%     | 55.60%   | 12.83%  |
| indicator_decline_30_100bn | T0+60  | 214   | 6.03% | 2.83%  | 0.68%    | -2.70%     | 44.86%   | 19.62%  |
| structured_risk_10_30bn    | T0+1   | 2241  | 0.11% | 0.00%  | 0.05%    | -0.11%     | 47.39%   | 0.36%   |
| structured_risk_10_30bn    | T0+5   | 2238  | 1.54% | 0.73%  | 0.66%    | -0.14%     | 42.98%   | 0.49%   |
| structured_risk_10_30bn    | T0+20  | 2049  | 4.18% | 1.06%  | 2.64%    | -0.10%     | 44.46%   | 8.90%   |
| structured_risk_10_30bn    | T0+60  | 1797  | 7.72% | 2.59%  | 2.65%    | -2.57%     | 44.35%   | 20.11%  |
+----------------------------+--------+-------+-------+--------+----------+------------+----------+---------+
```

## Interpretation

```text
+----------------------------+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
| candidate                  | phase-11 decision           | reason                                                                                                                   |
+----------------------------+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
| smallcap_loss_mv50         | KEEP_BENCHMARK_NOT_HARD_BAN | QE overlay positive; direct raw/abnormal returns are positive on 5/20d, so runtime use must stay context-aware.          |
| indicator_decline_10_30bn  | CONTEXTUAL_SCORE_DOWN       | QE overlay is positive but direct abnormal median is <=0 after 20/60d; use as model-context score-down only.             |
| indicator_decline_30_100bn | WATCHLIST_ONLY              | Direct abnormal median is negative after 5/20/60d, but QE effect is weak; not enough for runtime action.                 |
| structured_risk_10_30bn    | COVERAGE_BENCHMARK          | Broad rule has many events and positive abnormal mean, but negative abnormal median; keep for comparison not standalone. |
+----------------------------+-----------------------------+--------------------------------------------------------------------------------------------------------------------------+
```

## Sector Attribution - T0 to T+60

Rows are the largest event-count industries for the two indicator-decline candidates. This is attribution/plate-rotation context only and is not a neutralization gate.

```text
+----------------------------+----------+---------+---------+---------+----------+------------+----------+
| rule                       | industry | valid60 | mean    | median  | abn_mean | abn_median | neg_rate |
+----------------------------+----------+---------+---------+---------+----------+------------+----------+
| indicator_decline_10_30bn  | 电气设备     | 80      | 10.25%  | 5.25%   | 4.70%    | -2.57%     | 42.50%   |
| indicator_decline_10_30bn  | 软件服务     | 79      | 15.09%  | 5.72%   | 10.38%   | 0.61%      | 41.77%   |
| indicator_decline_10_30bn  | 半导体      | 68      | 24.24%  | 9.49%   | 18.82%   | 4.58%      | 33.82%   |
| indicator_decline_10_30bn  | 元器件      | 37      | 2.87%   | 3.85%   | -2.02%   | -3.92%     | 45.95%   |
| indicator_decline_10_30bn  | 化工原料     | 34      | 15.67%  | 11.58%  | 10.79%   | 4.94%      | 26.47%   |
| indicator_decline_10_30bn  | 专用机械     | 33      | 15.97%  | 12.49%  | 11.43%   | 10.51%     | 33.33%   |
| indicator_decline_10_30bn  | 小金属      | 32      | 23.36%  | 20.22%  | 18.01%   | 13.61%     | 25.00%   |
| indicator_decline_10_30bn  | 生物制药     | 29      | 3.58%   | 2.59%   | -1.66%   | -4.77%     | 48.28%   |
| indicator_decline_10_30bn  | 医疗保健     | 27      | 6.59%   | 3.39%   | 1.61%    | 0.97%      | 40.74%   |
| indicator_decline_10_30bn  | 通信设备     | 27      | 4.85%   | 4.18%   | 1.05%    | -4.00%     | 48.15%   |
| indicator_decline_30_100bn | 电气设备     | 29      | 11.87%  | 7.30%   | 4.81%    | 4.88%      | 34.48%   |
| indicator_decline_30_100bn | 半导体      | 26      | 11.23%  | 7.73%   | 4.57%    | 0.01%      | 26.92%   |
| indicator_decline_30_100bn | 软件服务     | 15      | 8.13%   | 2.74%   | 3.35%    | -4.80%     | 40.00%   |
| indicator_decline_30_100bn | 小金属      | 14      | 24.77%  | 22.72%  | 19.77%   | 12.83%     | 28.57%   |
| indicator_decline_30_100bn | 汽车整车     | 12      | 7.28%   | 2.71%   | 2.94%    | 1.37%      | 50.00%   |
| indicator_decline_30_100bn | 全国地产     | 11      | -9.05%  | -14.85% | -12.26%  | -15.53%    | 90.91%   |
| indicator_decline_30_100bn | 生物制药     | 10      | -11.95% | -10.83% | -15.90%  | -17.08%    | 100.00%  |
| indicator_decline_30_100bn | 煤炭开采     | 9       | 0.22%   | -1.13%  | -6.22%   | -8.29%     | 55.56%   |
| indicator_decline_30_100bn | 互联网      | 8       | 24.74%  | 13.16%  | 16.81%   | 5.66%      | 12.50%   |
| indicator_decline_30_100bn | 专用机械     | 7       | 10.34%  | 6.53%   | 7.05%    | 1.12%      | 28.57%   |
+----------------------------+----------+---------+---------+---------+----------+------------+----------+
```

## Research Conclusion

- The direct event study does not support treating `indicator_large_decline_mv_10_30bn` as a hard buy ban or forced-sell signal.
- The same rule can still be useful as a QE overlay because the prior overlay result is conditional on model-ranked Top50 candidates, while this direct study measures unconditional event-date returns.
- The abnormal-return median for `indicator_large_decline_mv_10_30bn` is slightly negative at T+20/T+60 even though the mean is positive, which is consistent with a right-tail rebound/sector-rotation effect rather than a pure downside-risk event.
- The 30-100bn indicator-decline split has weaker QE evidence and more negative abnormal medians; keep it as watchlist-only until broader loop validation proves value.
- The next research phase should test context-aware overlay rules that combine QE rank, event severity, and optional sector/regime attribution; do not move these financial distress rules into hard runtime controls yet.

## Implementation Notes

- Added a research-only direct event-return script under `backend/services/event_signal`.
- Added benchmark-adjusted return aggregation against `000300.SH`.
- Added support for selecting the current small-cap benchmark rule so direct event studies remain comparable with prior QE overlay results.
- Generated reports stay under ignored `reports/`; only this curated summary and validation record are committed.
