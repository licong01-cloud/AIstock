# Event Signal Financial Distress Score-Down QE Overlay Result - 2026-05-08

## Scope

This is research-only validation for the next event-signal direction. It stays inside `backend/services/event_signal`, reads existing QE artifacts, and does not modify QE, Selection Center, Paper Trading, QMT, or live-trading runtime.

```text
Target rule : loss_to_market_cap_ge_50pct_mv_lt_10bn
Signal type : structured Tushare financial distress signal, not PDF/LLM
QE loops    : 10 existing WSL QE loops
Date range  : 2024-07-01 -> 2026-04-27
TopK        : 50
Mode        : score-down rerank, not hard block and not force sell
Rank date   : previous trade date for the main run, aligned with QE shift=1 style signal usage
Reports     : reports/event_signal/financial_distress_score_down_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_113409.json
Probe       : reports/event_signal/financial_distress_score_down_qe_overlay_current_date_probe/financial_distress_qe_multiloop_20240701_20260508_113710.json
```

## Method

The previous `next_candidate` mode assumed that every blocked buy is directly replaced by the next available candidate. That was useful as an upper-bound proxy, but too optimistic for a real QE ranking system.

This run adds a score-down/rerank simulator:

```text
1. Detect actual QE new buys from positions: current_symbols - previous_symbols.
2. If a new buy has an active financial-distress signal, find the stock in the QE prediction ranking.
3. Apply a TopK-relative rank demotion: 5%, 10%, 20%, or 50% of TopK=50.
4. Re-rank the candidate list.
5. Only if the stock drops out of Top50, replace it with a newly admitted Top50 candidate with available return data.
6. Hold the replacement while the original baseline holding remains open.
7. Rebuild only an offline account curve; no DB write and no runtime integration.
```

## Main Result - Previous Prediction Date

```text
+-----------+----------------------------------------+--------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| active_td | rule_key                               | mode                                 | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | med_ret_d | min_ret_d | max_ret_d | avg_mdd_d |
+-----------+----------------------------------------+--------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_previous | 6/10      | 77      | 77        | 6       | 3    | 0.20%     | 0.00%     | -0.00%    | 1.02%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_previous | 6/10      | 137     | 136       | 9       | 4    | 0.18%     | 0.00%     | -0.00%    | 0.80%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_previous | 6/10      | 247     | 246       | 11      | 5    | 0.16%     | 0.00%     | -0.00%    | 0.80%     | -0.00%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_previous | 4/10      | 77      | 77        | 22      | 15   | 0.10%     | -0.00%    | -0.55%    | 1.67%     | 0.00%     |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_10pct_top50_previous | 6/10      | 77      | 77        | 2       | 1    | 0.09%     | 0.00%     | -0.00%    | 0.76%     | -0.00%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_5pct_top50_previous  | 6/10      | 77      | 77        | 2       | 1    | 0.09%     | 0.00%     | -0.00%    | 0.76%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_10pct_top50_previous | 5/10      | 247     | 246       | 5       | 2    | 0.07%     | 0.00%     | -0.22%    | 0.76%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_10pct_top50_previous | 5/10      | 137     | 136       | 4       | 2    | 0.07%     | 0.00%     | -0.22%    | 0.76%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_5pct_top50_previous  | 5/10      | 137     | 136       | 3       | 1    | 0.07%     | 0.00%     | -0.22%    | 0.76%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_5pct_top50_previous  | 5/10      | 247     | 246       | 3       | 1    | 0.07%     | 0.00%     | -0.22%    | 0.76%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_previous | 2/10      | 137     | 136       | 36      | 26   | -0.19%    | -0.12%    | -1.45%    | 0.85%     | 0.00%     |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_previous | 1/10      | 247     | 246       | 56      | 43   | -0.27%    | -0.04%    | -0.88%    | 0.00%     | 0.00%     |
+-----------+----------------------------------------+--------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
```

## Current-Date Probe

This probe intentionally uses current-date predictions instead of previous-date predictions. It is mainly an alignment check, not the recommended method.

```text
+-----------+----------------------------------------+-------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| active_td | rule_key                               | mode                                | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | med_ret_d | min_ret_d | max_ret_d | avg_mdd_d |
+-----------+----------------------------------------+-------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_current | 6/10      | 77      | 47        | 12      | 11   | 0.09%     | 0.00%     | -0.65%    | 1.28%     | 0.00%     |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_current | 6/10      | 137     | 102       | 35      | 29   | 0.05%     | 0.05%     | -0.66%    | 0.77%     | -0.00%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_current | 5/10      | 77      | 47        | 2       | 1    | -0.04%    | 0.00%     | -0.89%    | 0.45%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_current | 3/10      | 137     | 102       | 8       | 5    | -0.07%    | -0.00%    | -0.89%    | 0.40%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_50pct_top50_current | 6/10      | 247     | 189       | 60      | 42   | -0.11%    | 0.08%     | -1.22%    | 0.78%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_rank_20pct_top50_current | 2/10      | 247     | 189       | 17      | 6    | -0.15%    | -0.13%    | -1.00%    | 1.02%     | -0.00%    |
+-----------+----------------------------------------+-------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
```

The previous-date mode evaluates almost every actual blocked buy inside Top50 (`246/247` for 242 trading days), while current-date mode only evaluates `189/247`. This supports using previous prediction date for this offline simulator because the existing QE strategy path is closer to shift=1 behavior.

## Interpretation

```text
+--------------------------------------+--------------------------------------------------------------------------------------------+
| Question                             | Current conclusion                                                                         |
+--------------------------------------+--------------------------------------------------------------------------------------------+
| Is hard no-buy validated?            | No. Prior cash/no-buy and this rerank study both show hard filters are not stable enough.  |
| Is score-down direction promising?   | Yes, but only as mild risk overlay. The effect is small and concentrated at Top50 boundary.|
| Best first candidate                 | 20% of TopK rank demotion, previous-date ranking, 60/120 td windows.                       |
| Too aggressive candidate             | 50% demotion. It changes more trades but becomes negative for 120/242 td windows.          |
| Why next_candidate looked stronger   | It assumed every blocked buy can be replaced; rerank only changes stocks dropping Top50.   |
| Runtime readiness                    | Not ready. This is still offline approximation and must not be wired into QE/Paper yet.    |
+--------------------------------------+--------------------------------------------------------------------------------------------+
```

## Relation To Literature And Practice

The result remains consistent with the earlier literature/practice framing:

```text
+------------------------------+------------------------------------------------------------------------------------------------+
| Reference family             | Borrowed idea                                                                                  |
+------------------------------+------------------------------------------------------------------------------------------------+
| Event study                  | Validate event impact with an explicit event date and post-event return/account effect.        |
| Distress models              | Use loss severity, size bucket, and persistence as risk signals rather than positive alpha.     |
| ST prediction research       | Treat financial distress as early-warning evidence, but avoid immediate hard trading actions.   |
| Institutional risk overlays  | Prefer downweight/risk-budget/rerank overlays before exclusions, except for ST/退市 hard risk.  |
| Barra/AQR-style practice     | Control size/style exposure; this rule is effectively a small-cap distress overlay.             |
+------------------------------+------------------------------------------------------------------------------------------------+
```

## Next Research Direction

```text
+------+---------------------------------------------------------------------------------------------------------------+
| Step | Action                                                                                                        |
+------+---------------------------------------------------------------------------------------------------------------+
| 1    | Keep financial distress as research-only score overlay; do not connect to live QE/Paper runtime yet.          |
| 2    | Expand from a fixed 20% rank penalty to a continuous severity score: loss/mv, market-cap bucket, industry.    |
| 3    | Add industry and size neutral diagnostics so the overlay is not just an accidental small-cap/real-estate bet. |
| 4    | Validate combined signals one at a time: forecast/express loss, consecutive losses, revenue/profit decline.  |
| 5    | Only after stable offline evidence, run a real QE overlay experiment instead of account-curve approximation.  |
+------+---------------------------------------------------------------------------------------------------------------+
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
