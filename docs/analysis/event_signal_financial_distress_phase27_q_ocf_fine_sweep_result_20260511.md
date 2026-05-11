# Financial Distress Phase 27 Parameter Shape Sweep

Research-only fine sweep around the Phase-26 best q_ocf_to_sales rule. No runtime consumer is changed.

## Scope

```text
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
| item           | value                                                                                                                                  |
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
| date range     | 2024-07-01 -> 2026-04-27                                                                                                               |
| rules          | 1                                                                                                                                      |
| direct report  | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\direct\financial_distress_direct_event_20240701_20260511_150037.json  |
| overlay report | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_151633.json |
| runtime impact | none: research-only, no DB writes, no QE/Paper/Selection/QMT wiring                                                                    |
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
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
+-------------------+-------+-------+-------+---------+--------+------+----+--------------------------------------+---------------------+---------+--------------------------------------------------+
| decision          | score | pos   | avg   | ex_best | min    | drop | td | mode                                 | direct              | t20_med | rule                                             |
+-------------------+-------+-------+-------+---------+--------+------+----+--------------------------------------+---------------------+---------+--------------------------------------------------+
| TRUE_QE_CANDIDATE | 68.4  | 14/22 | 0.18% | 0.10%   | -0.18% | 16   | 90 | score_down_rank_15pct_top50_previous | supports_downweight | -0.93%  | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
+-------------------+-------+-------+-------+---------+--------+------+----+--------------------------------------+---------------------+---------+--------------------------------------------------+
```

## Direct Event Abnormal Returns

```text
+--------------------------------------------------+--------+-------+----------+------------+----------+---------+
| rule                                             | window | valid | abn_mean | abn_median | neg_rate | miss_px |
+--------------------------------------------------+--------+-------+----------+------------+----------+---------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn | T+5    | 510   | 0.56%    | -0.12%     | 50.78%   | 0.58%   |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn | T+20   | 439   | 1.07%    | -0.93%     | 53.99%   | 14.42%  |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn | T+60   | 422   | 3.91%    | -1.61%     | 54.98%   | 17.74%  |
+--------------------------------------------------+--------+-------+----------+------------+----------+---------+
```

## Cheap Overlay Top Rows

```text
+-------+--------------------------+-------+-------+---------+--------+------+-----+----------------------------------------+--------------------------------------------------+
| score | decision                 | pos   | avg   | ex_best | min    | drop | td  | mode                                   | rule                                             |
+-------+--------------------------+-------+-------+---------+--------+------+-----+----------------------------------------+--------------------------------------------------+
| 68.4  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.18% | 0.10%   | -0.18% | 16   | 90  | score_down_rank_15pct_top50_previous   | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 68.4  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.18% | 0.10%   | -0.18% | 16   | 90  | score_down_rank_17p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 67.6  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.18% | 0.10%   | -0.25% | 17   | 90  | fixed_20                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 56.1  | WATCHLIST                | 14/22 | 0.12% | 0.07%   | -0.09% | 12   | 60  | score_down_rank_15pct_top50_previous   | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 56.1  | WATCHLIST                | 14/22 | 0.12% | 0.07%   | -0.09% | 12   | 60  | score_down_rank_17p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 55.7  | CALIBRATION_ONLY         | 12/22 | 0.16% | 0.07%   | -0.49% | 19   | 120 | score_down_rank_15pct_top50_previous   | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 55.3  | WATCHLIST                | 14/22 | 0.12% | 0.07%   | -0.09% | 13   | 60  | fixed_20                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 43.7  | WATCHLIST                | 13/22 | 0.11% | 0.02%   | -0.54% | 14   | 90  | score_down_rank_12p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 40.9  | CALIBRATION_ONLY         | 12/22 | 0.10% | 0.02%   | -0.54% | 13   | 90  | fixed_10                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 39.0  | WATCHLIST                | 13/22 | 0.05% | 0.02%   | -0.13% | 10   | 60  | score_down_rank_12p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 37.5  | CALIBRATION_ONLY         | 11/22 | 0.10% | 0.01%   | -0.54% | 16   | 120 | score_down_rank_12p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 36.5  | CALIBRATION_ONLY         | 11/22 | 0.09% | 0.01%   | -0.54% | 15   | 120 | fixed_10                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 35.3  | CALIBRATION_ONLY         | 12/22 | 0.04% | 0.02%   | -0.13% | 9    | 60  | fixed_10                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 33.8  | REJECT                   | 11/22 | 0.11% | 0.03%   | -1.03% | 20   | 120 | score_down_rank_17p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 33.0  | REJECT                   | 11/22 | 0.11% | 0.02%   | -1.03% | 21   | 120 | fixed_20                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 29.9  | WATCHLIST                | 14/22 | 0.03% | 0.01%   | -0.13% | 5    | 40  | score_down_rank_12p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 29.9  | WATCHLIST                | 14/22 | 0.03% | 0.01%   | -0.13% | 5    | 40  | score_down_rank_15pct_top50_previous   | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 29.9  | WATCHLIST                | 14/22 | 0.03% | 0.01%   | -0.13% | 5    | 40  | score_down_rank_17p5pct_top50_previous | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 28.3  | WATCHLIST                | 13/22 | 0.03% | 0.01%   | -0.13% | 6    | 40  | fixed_20                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
| 26.1  | WATCHLIST                | 13/22 | 0.03% | 0.01%   | -0.13% | 4    | 40  | fixed_10                               | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn |
+-------+--------------------------+-------+-------+---------+--------+------+-----+----------------------------------------+--------------------------------------------------+
```

## Interpretation

- Cheap overlay remains a shortlist gate only; true QE is required before promotion.
- Financial signals are still non-hard: no buy ban, no forced sell, no alpha boost in this phase.
- Phase 27 is a parameter-shape screen, not a runtime-policy approval.
- Rules with direct downside but poor overlay stay research features; rules with overlay benefit but no direct downside stay calibration-only.
- If a row reaches TRUE_QE_CANDIDATE, run one-loop WSL true QE smoke before any signal-table or runtime design.

## Phase 27 Conclusion

```text
+-------+--------------------------+-------+--------+---------+---------+------+----+----------------------------------------+
| score | decision                 | pos   | avg    | ex_best | min     | drop | td | mode                                   |
+-------+--------------------------+-------+--------+---------+---------+------+----+----------------------------------------+
| 68.4  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.181% | 0.100%  | -0.183% | 16   | 90 | score_down_rank_15pct_top50_previous   |
| 68.4  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.181% | 0.100%  | -0.183% | 16   | 90 | score_down_rank_17p5pct_top50_previous |
| 67.6  | WSL_TRUE_RERUN_CANDIDATE | 14/22 | 0.178% | 0.097%  | -0.252% | 17   | 90 | fixed_20                               |
| 56.1  | WATCHLIST                | 14/22 | 0.120% | 0.071%  | -0.092% | 12   | 60 | score_down_rank_15pct_top50_previous   |
| 56.1  | WATCHLIST                | 14/22 | 0.120% | 0.071%  | -0.092% | 12   | 60 | score_down_rank_17p5pct_top50_previous |
+-------+--------------------------+-------+--------+---------+---------+------+----+----------------------------------------+
```

- `indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn` now reaches `TRUE_QE_CANDIDATE` with score `68.4`.
- Best shape is 90 trading days with 15% or 17.5% rank penalty; 20% is close but has worse tail.
- Direct event evidence remains supportive (`T+20` abnormal median about `-0.93%`, `T+60` abnormal median about `-1.61%`).
- Next step is a one-loop WSL full-universe true-QE smoke before any signal-table, DB policy, or runtime integration.
