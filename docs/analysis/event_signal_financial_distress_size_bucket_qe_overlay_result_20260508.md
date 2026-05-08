# AIstock Financial Distress Size-Bucket QE Overlay Result

Date: 2026-05-08
Scope: read-only event_signal research script; no QE runtime / Selection / Paper / QMT / simulated / live trading integration.
Runtime: WSL `rdagent-gpu`, using existing QE artifacts only.
Generated report: `reports/event_signal/financial_distress_sizebucket_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_103233.json`

## 1. Research Question

The previous multi-loop result showed `loss_to_market_cap_ge_50pct` is the cleanest financial-distress candidate, but its exposure is concentrated in small-cap names. This run splits the same rule by PIT market-cap bucket and keeps the same 10 QE loops, 3 active windows, and 2 offline modes.

## 2. Main Conclusion

```text
+--------------------------------------+--------------------------------------------------------------+
| Question                             | Evidence-based conclusion                                    |
+--------------------------------------+--------------------------------------------------------------+
| Is the >=50% rule mainly small-cap?  | Yes. Nearly all affected buys are below 10bn CNY market cap. |
| Is cash-only no-buy validated?       | No. Small-cap cash filters are still negative on average.    |
| Is next-candidate still promising?   | Yes, but only as an offline re-ranking hypothesis.           |
| Is >=10bn bucket useful?             | Not enough exposure; only 1-3 blocked buys across 10 loops.  |
| Next research direction              | Study score-down / re-ranking on <10bn bucket only.          |
+--------------------------------------+--------------------------------------------------------------+
```

## 3. Size-Bucket Stability Summary

```text
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
| active_td | rule_key                               | mode           | pos/loops | blocked | avg_ret_d | med_ret_d | min_ret_d | max_ret_d |
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 9/10      | 247     | 12.36%    | 11.84%    | -3.91%    | 33.59%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_5bn  | next_candidate | 8/10      | 189     | 8.32%     | 10.42%    | -5.04%    | 16.72%    |
| 242       | loss_to_market_cap_ge_50pct_mv_5_10bn  | next_candidate | 9/10      | 58      | 5.24%     | 4.27%     | -1.60%    | 18.63%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 7/10      | 137     | 4.66%     | 3.38%     | -3.61%    | 22.80%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_5bn  | next_candidate | 7/10      | 112     | 3.13%     | 3.21%     | -3.56%    | 11.14%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | next_candidate | 7/10      | 77      | 0.84%     | 0.92%     | -1.85%    | 3.34%     |
| 242       | loss_to_market_cap_ge_50pct_mv_ge_10bn | next_candidate | 5/10      | 3       | 0.09%     | 0.00%     | -0.00%    | 0.94%     |
| 242       | loss_to_market_cap_ge_50pct_mv_5_10bn  | cash           | 6/10      | 58      | 0.21%     | 0.10%     | -0.49%    | 0.93%     |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 3/10      | 77      | -1.04%    | -1.04%    | -3.06%    | 1.26%     |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 2/10      | 137     | -2.28%    | -2.31%    | -5.17%    | 0.75%     |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | cash           | 1/10      | 247     | -3.57%    | -3.72%    | -7.83%    | 0.83%     |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_5bn  | cash           | 1/10      | 189     | -3.77%    | -3.63%    | -8.73%    | 1.33%     |
+-----------+----------------------------------------+----------------+-----------+---------+-----------+-----------+-----------+-----------+
```

## 4. Interpretation

1. The signal is not a broad-market financial distress signal in QE portfolios; it is effectively a small-cap distress overlay.
2. The `mv_lt_10bn` bucket captures almost all useful exposure. The `mv_ge_10bn` bucket is too sparse to support a rule.
3. Cash-only no-buy is still rejected. It removes positions without improving portfolio return.
4. The promising branch is re-ranking or replacement, but the current `next_candidate` simulator is still an approximation. It should not be called a production policy.
5. The next code research should simulate score-down rather than hard exclusion: e.g. penalize distressed small-cap candidate scores and recompute whether the name stays in the TopK candidate set.

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
