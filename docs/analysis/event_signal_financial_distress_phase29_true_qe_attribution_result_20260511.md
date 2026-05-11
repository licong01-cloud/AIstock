# Phase 29 Financial Distress True QE Attribution - 2026-05-11

Research-only attribution for the true-QE smoke gap. It reads copied QE artifacts and materializer traces, and it does not change QE runtime, Selection Center, Paper Trading, QMT, live trading, database schema, or production backend `8001`.

## Scope

```text
version          : financial_distress_phase29_true_qe_attribution_v1_20260511
baseline recorder: 7b57828280ad40b988e6574c9a083da6
experiment root  : /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2/mlruns/301029085745548565
cases            : 3
```

## Rank Hit Density

The q_ocf candidate is broad: many active penalty rows, but only a small fraction reach original Top50 and even fewer drop out of Top50.

```text
+---------------------------------+---------+---------+-------+-------+----------+-----------+----------+----------+
| case                            | penalty | symbols | top50 | drops | drop_sym | top50/pen | drop/pen | avg_rank |
+---------------------------------+---------+---------+-------+-------+----------+-----------+----------+----------+
| phase28_q_ocf_fixed15_90td      | 41673   | 393     | 221   | 25    | 12       | 0.530%    | 0.060%   | 22.69    |
| phase19_indicator_decline_ctx60 | 311     | 37      | 311   | 24    | 13       | 100.000%  | 7.717%   | 24.05    |
| phase23_loss_mv_fixed20_242td   | 304     | 18      | 302   | 61    | 15       | 99.342%   | 20.066%  | 26.10    |
+---------------------------------+---------+---------+-------+-------+----------+-----------+----------+----------+
```

## Actual Holding Hit State

Top50 rank events often do not become actual removed holdings. This is the direct bridge from rank simulation to true QE execution.

```text
+---------------------------------+-----------+-----------------+---------------+----------+----------------+--------------+
| case                            | top50_evt | top50_base_hold | top50_removed | drop_evt | drop_base_hold | drop_removed |
+---------------------------------+-----------+-----------------+---------------+----------+----------------+--------------+
| phase28_q_ocf_fixed15_90td      | 221       | 183             | 23            | 25       | 8              | 3            |
| phase19_indicator_decline_ctx60 | 311       | 238             | 21            | 24       | 8              | 2            |
| phase23_loss_mv_fixed20_242td   | 302       | 270             | 46            | 61       | 40             | 14           |
+---------------------------------+-----------+-----------------+---------------+----------+----------------+--------------+
```

## End-Of-Day Position Difference

End-of-day holding differences are only an approximation because the V25 minute execution path can create intraday PnL even when end-of-day holdings converge.

```text
+---------------------------------+--------------+---------+-------+--------------+--------------+--------------+-------------+
| case                            | changed_days | removed | added | risk_removed | drop_removed | true_ret_sum | changed_avg |
+---------------------------------+--------------+---------+-------+--------------+--------------+--------------+-------------+
| phase28_q_ocf_fixed15_90td      | 70           | 74      | 73    | 23           | 3            | 0.168%       | -0.003%     |
| phase19_indicator_decline_ctx60 | 109          | 110     | 123   | 21           | 2            | 0.273%       | 0.002%      |
| phase23_loss_mv_fixed20_242td   | 112          | 106     | 134   | 46           | 14           | 0.066%       | -0.001%     |
+---------------------------------+--------------+---------+-------+--------------+--------------+--------------+-------------+
```

## Largest Positive True-Return Delta Dates

```text
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
| case                            | date       | removed | added | risk_removed | drop_removed | ret_delta |
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
| phase28_q_ocf_fixed15_90td      | 2025-07-14 | 0       | 0     | 0            | 0            | 0.378%    |
| phase28_q_ocf_fixed15_90td      | 2025-09-01 | 1       | 1     | 0            | 0            | 0.158%    |
| phase28_q_ocf_fixed15_90td      | 2025-09-05 | 1       | 1     | 0            | 0            | 0.117%    |
| phase19_indicator_decline_ctx60 | 2025-07-11 | 0       | 1     | 0            | 0            | 0.173%    |
| phase19_indicator_decline_ctx60 | 2026-01-16 | 1       | 1     | 0            | 0            | 0.155%    |
| phase19_indicator_decline_ctx60 | 2024-08-28 | 1       | 1     | 0            | 0            | 0.149%    |
| phase23_loss_mv_fixed20_242td   | 2025-07-14 | 0       | 0     | 0            | 0            | 0.375%    |
| phase23_loss_mv_fixed20_242td   | 2025-11-03 | 1       | 1     | 0            | 0            | 0.173%    |
| phase23_loss_mv_fixed20_242td   | 2026-03-30 | 1       | 1     | 0            | 0            | 0.125%    |
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
```

## Largest Negative True-Return Delta Dates

```text
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
| case                            | date       | removed | added | risk_removed | drop_removed | ret_delta |
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
| phase28_q_ocf_fixed15_90td      | 2025-08-05 | 1       | 1     | 0            | 0            | -0.153%   |
| phase28_q_ocf_fixed15_90td      | 2024-09-06 | 1       | 1     | 0            | 0            | -0.110%   |
| phase28_q_ocf_fixed15_90td      | 2026-01-06 | 1       | 1     | 0            | 0            | -0.100%   |
| phase19_indicator_decline_ctx60 | 2026-01-06 | 2       | 2     | 0            | 0            | -0.238%   |
| phase19_indicator_decline_ctx60 | 2024-08-27 | 1       | 1     | 0            | 0            | -0.232%   |
| phase19_indicator_decline_ctx60 | 2024-09-11 | 1       | 1     | 0            | 0            | -0.102%   |
| phase23_loss_mv_fixed20_242td   | 2025-12-11 | 0       | 0     | 0            | 0            | -0.353%   |
| phase23_loss_mv_fixed20_242td   | 2025-09-10 | 1       | 2     | 0            | 0            | -0.247%   |
| phase23_loss_mv_fixed20_242td   | 2025-12-10 | 1       | 1     | 1            | 0            | -0.167%   |
+---------------------------------+------------+---------+-------+--------------+--------------+-----------+
```

## Interpretation

```text
+---------------------------------+------------------------------+----------------------------------------------------------------------------------+
| case                            | decision                     | interpretation                                                                   |
+---------------------------------+------------------------------+----------------------------------------------------------------------------------+
| phase28_q_ocf_fixed15_90td      | DIAGNOSE_BROAD_LOW_PRECISION | broad active overlay; low Top50/drop precision explains weak true-QE materiality |
| phase19_indicator_decline_ctx60 | KEEP_BENCHMARK               | more focused Top50 penalties; remains the stronger true-smoke benchmark          |
| phase23_loss_mv_fixed20_242td   | CALIBRATION_ONLY             | clean benchmark but sparse and weaker true-return improvement                    |
+---------------------------------+------------------------------+----------------------------------------------------------------------------------+
```

## Conclusion

- Phase28 q_ocf is directionally positive in true QE, but its broad signal coverage has low Top50/drop precision.
- Phase19 remains the better one-loop true-smoke benchmark because a much larger share of penalties are concentrated on original Top50 candidates.
- Phase23 shows that high Top50/drop density alone is not enough; the removed names and replacement timing must also improve realized PnL.
- The next cheap research should prefer higher-conviction intersections or rank-aware filters instead of simply increasing q_ocf penalty strength.
- Do not promote any financial signal to runtime yet; no buy ban, forced sell, Paper/Selection/QE hook, or DB policy write is justified by this attribution.
