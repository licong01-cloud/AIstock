# Phase 9 Medium/Large-Cap Structured Financial Event Research - 2026-05-09

Research-only offline QE overlay study. This phase stays inside `backend/services/event_signal`, tests/docs, and ignored `reports/` artifacts. It does not modify QE, Selection Center, Paper Trading, QMT, or live/paper runtime paths.

## Scope

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | value                                                                          |
+----------------------+--------------------------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                                       |
| worktree             | F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508                    |
| date range           | 2024-07-01 -> 2026-04-27                                                       |
| loops                | 10 QE loops from .codex_tmp/event_signal/financial_distress_qe_loops.json      |
| validations          | 360                                                                            |
| modes                | score_down_rank_20pct_top50_previous, score_down_severity_balanced_top50_prev. |
| active lifetimes     | 60, 120, 242 trading days                                                      |
| generated report     | reports/event_signal/financial_distress_mid_large_qe_overlay/...005254.md      |
+----------------------+--------------------------------------------------------------------------------+
```

## Rules Tested

```text
+-------------------------------------------+---------------------+-------------------------------------------------------------+
| rule_key                                  | phase-9 purpose     | definition                                                  |
+-------------------------------------------+---------------------+-------------------------------------------------------------+
| expectation_miss_mv_ge_10bn               | expectation miss    | positive-but-miss relation and PIT market cap >= 10bn       |
| expectation_miss_gap_ge_50_mv_ge_10bn     | refined miss        | expectation miss with miss_gap >= 50pct points, mv >= 10bn  |
| expectation_miss_mv_ge_30bn               | large-cap miss      | expectation miss and PIT market cap >= 30bn                 |
| forecast_express_large_decline_mv_ge_10bn | forecast/express    | forecast or express large decline and PIT market cap >=10bn |
| indicator_large_decline_mv_ge_10bn        | report indicator    | fina_indicator large decline and PIT market cap >=10bn      |
| structured_financial_risk_mv_ge_10bn      | broad coverage      | loss/decline/miss structured financial risk and mv >=10bn   |
+-------------------------------------------+---------------------+-------------------------------------------------------------+
```

## Main 10-Loop Results

Fixed rank-20pct score-down mode:

```text
+-------------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| rule_key                                  | active_td | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | min_ret_d | max_ret_d |
+-------------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| indicator_large_decline_mv_ge_10bn        | 60        | 4/10      | 361     | 359       | 12      | 5    | 0.06%     | -1.03%    | 1.88%     |
| indicator_large_decline_mv_ge_10bn        | 120       | 3/10      | 699     | 697       | 21      | 10   | 0.17%     | -1.03%    | 3.54%     |
| indicator_large_decline_mv_ge_10bn        | 242       | 4/10      | 1067    | 1061      | 38      | 18   | 0.28%     | -1.68%    | 4.34%     |
| structured_financial_risk_mv_ge_10bn      | 60        | 4/10      | 670     | 667       | 25      | 18   | 0.26%     | -1.03%    | 3.68%     |
| structured_financial_risk_mv_ge_10bn      | 120       | 3/10      | 996     | 992       | 36      | 23   | 0.06%     | -1.32%    | 3.44%     |
| structured_financial_risk_mv_ge_10bn      | 242       | 4/10      | 1377    | 1369      | 55      | 35   | 0.21%     | -2.62%    | 4.48%     |
| forecast_express_large_decline_mv_ge_10bn | 60        | 4/10      | 123     | 122       | 6       | 4    | 0.07%     | -1.03%    | 1.65%     |
| forecast_express_large_decline_mv_ge_10bn | 120       | 3/10      | 211     | 210       | 8       | 4    | -0.04%    | -1.03%    | 1.65%     |
| forecast_express_large_decline_mv_ge_10bn | 242       | 4/10      | 491     | 488       | 20      | 9    | -0.16%    | -2.14%    | 1.65%     |
| expectation_miss_mv_ge_10bn               | 60        | 5/10      | 57      | 57        | 1       | 0    | -0.00%    | -0.02%    | 0.00%     |
| expectation_miss_mv_ge_10bn               | 120       | 5/10      | 114     | 114       | 5       | 4    | 0.00%     | -0.02%    | 0.05%     |
| expectation_miss_mv_ge_10bn               | 242       | 6/10      | 177     | 176       | 7       | 5    | 0.02%     | -0.00%    | 0.14%     |
| expectation_miss_gap_ge_50_mv_ge_10bn     | 60        | 5/10      | 49      | 49        | 1       | 0    | -0.00%    | -0.02%    | 0.00%     |
| expectation_miss_gap_ge_50_mv_ge_10bn     | 120       | 5/10      | 90      | 90        | 5       | 4    | 0.00%     | -0.02%    | 0.05%     |
| expectation_miss_gap_ge_50_mv_ge_10bn     | 242       | 6/10      | 163     | 162       | 7       | 5    | 0.02%     | -0.00%    | 0.14%     |
| expectation_miss_mv_ge_30bn               | 60        | 5/10      | 8       | 8         | 0       | 0    | 0.00%     | -0.00%    | 0.00%     |
| expectation_miss_mv_ge_30bn               | 120       | 5/10      | 13      | 13        | 0       | 0    | 0.00%     | -0.00%    | 0.00%     |
| expectation_miss_mv_ge_30bn               | 242       | 5/10      | 21      | 21        | 0       | 0    | 0.00%     | -0.00%    | 0.00%     |
+-------------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
```

Severity-balanced score-down mode for the main candidates:

```text
+--------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+---------+
| rule_key                             | active_td | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | avg_pen |
+--------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+---------+
| indicator_large_decline_mv_ge_10bn   | 60        | 6/10      | 361     | 359       | 8       | 4    | 0.21%     | 10.00%  |
| indicator_large_decline_mv_ge_10bn   | 120       | 4/10      | 699     | 697       | 12      | 5    | 0.14%     | 10.00%  |
| indicator_large_decline_mv_ge_10bn   | 242       | 4/10      | 1067    | 1061      | 16      | 7    | 0.24%     | 10.00%  |
| structured_financial_risk_mv_ge_10bn | 60        | 6/10      | 670     | 667       | 14      | 9    | 0.27%     | 10.55%  |
| structured_financial_risk_mv_ge_10bn | 120       | 4/10      | 996     | 992       | 19      | 11   | 0.14%     | 10.55%  |
| structured_financial_risk_mv_ge_10bn | 242       | 3/10      | 1377    | 1369      | 25      | 14   | 0.20%     | 10.48%  |
+--------------------------------------+-----------+-----------+---------+-----------+---------+------+-----------+---------+
```

## Baseline Comparison

Previous best small-cap benchmark remains `loss_to_market_cap_ge_50pct_mv_lt_10bn`.

```text
+-------------------------------------------+-----------+----------------+----------------+----------------------------------------------------+
| rule_key                                  | active_td | avg_ret_d      | coverage        | interpretation                                     |
+-------------------------------------------+-----------+----------------+----------------+----------------------------------------------------+
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 60        | 0.20%          | 77 eval_topk    | current strongest benchmark                        |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 120       | 0.18%          | 136 eval_topk   | current strongest benchmark                        |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 242       | 0.16%          | 246 eval_topk   | current strongest benchmark                        |
| indicator_large_decline_mv_ge_10bn        | 60        | 0.06% / 0.21%  | 359 eval_topk   | higher coverage; depends on overlay mode           |
| indicator_large_decline_mv_ge_10bn        | 120       | 0.17% / 0.14%  | 697 eval_topk   | similar average to baseline but less stable loops  |
| indicator_large_decline_mv_ge_10bn        | 242       | 0.28% / 0.24%  | 1061 eval_topk  | strongest phase-9 candidate, still research-only   |
| structured_financial_risk_mv_ge_10bn      | 60        | 0.26% / 0.27%  | 667 eval_topk   | broad coverage; likely too blunt standalone        |
| structured_financial_risk_mv_ge_10bn      | 120       | 0.06% / 0.14%  | 992 eval_topk   | unstable as standalone                             |
| structured_financial_risk_mv_ge_10bn      | 242       | 0.21% / 0.20%  | 1369 eval_topk  | broad and worst-loop drawdown contribution exists  |
+-------------------------------------------+-----------+----------------+----------------+----------------------------------------------------+
```

## Market-Cap Exposure

For the 242td fixed rank-20pct mode:

```text
+--------------------------------------+-----------+-----------+---------+-----------------------------------------------------+
| rule_key                             | mv_bucket | eval_topk | dropped | implication                                         |
+--------------------------------------+-----------+-----------+---------+-----------------------------------------------------+
| expectation_miss_mv_ge_10bn          | 10-30bn   | 156       | 7       | most interaction is medium cap                      |
| expectation_miss_mv_ge_10bn          | 30-100bn  | 17        | 0       | large-cap interaction is too sparse                 |
| expectation_miss_mv_ge_10bn          | >=100bn   | 4         | 0       | mega-cap impact is effectively absent               |
| indicator_large_decline_mv_ge_10bn   | 10-30bn   | 975       | 35      | dominant phase-9 interaction bucket                 |
| indicator_large_decline_mv_ge_10bn   | 30-100bn  | 181       | 5       | non-zero but much smaller than 10-30bn              |
| indicator_large_decline_mv_ge_10bn   | >=100bn   | 6         | 0       | mega-cap standalone evidence is insufficient        |
| structured_financial_risk_mv_ge_10bn | 10-30bn   | 1246      | 51      | broad medium-cap coverage drives most changes       |
| structured_financial_risk_mv_ge_10bn | 30-100bn  | 233       | 6       | sparse positive evidence, not enough for hard rules |
| structured_financial_risk_mv_ge_10bn | >=100bn   | 15        | 0       | no practical portfolio effect in these QE loops     |
+--------------------------------------+-----------+-----------+---------+-----------------------------------------------------+
```

## Decisions

```text
+-------------------------------------------+-----------------------+-----------------------------------------------------------+
| candidate                                 | decision              | reason                                                    |
+-------------------------------------------+-----------------------+-----------------------------------------------------------+
| indicator_large_decline_mv_ge_10bn        | KEEP_RESEARCH_FEATURE | positive average in both modes; needs industry/size split |
| structured_financial_risk_mv_ge_10bn      | COVERAGE_BENCHMARK    | broad rule helps coverage but is too blunt standalone     |
| forecast_express_large_decline_mv_ge_10bn | REJECT_STANDALONE     | 120/242td fixed mode is negative; sample is sparse        |
| expectation_miss_mv_ge_10bn               | WATCHLIST_RESEARCH    | valid event concept but weak Top50 interaction            |
| expectation_miss_gap_ge_50_mv_ge_10bn     | WATCHLIST_RESEARCH    | gap>=50 barely changes result vs any miss                 |
| expectation_miss_mv_ge_30bn               | REJECT_RUNTIME        | 30bn+ sample has no dropped Top50 events                  |
+-------------------------------------------+-----------------------+-----------------------------------------------------------+
```

## Implementation Notes

- Added `MID_LARGE_EVENT_RULES` and CLI switches `--include-mid-large-rules` / `--mid-large-only` to the research-only overlay script.
- Added expectation-miss metric bucketing so `miss_gap` can be used by offline rule filters.
- Optimized `build_financial_signal_rows()` for overlay research by avoiding the unused precision-study combo scan; the same load finished in about 32 seconds locally before the 10-loop run.
- The WSL run initially failed because `.env` values passed to WSL retained quotes around `TDX_DB_PORT`; the rerun stripped quotes.
- No runtime consumer references were added; guardrail scan returned no matches in Selection/Paper/QE/QMT runtime paths.

## Next Phase

Phase 10 should not jump to LLM/PDF yet. The next evidence-based step is to split `indicator_large_decline_mv_ge_10bn` and the broad structured-risk benchmark by:

```text
+------------------+-------------------------------------------------------------+
| split            | purpose                                                     |
+------------------+-------------------------------------------------------------+
| industry         | determine whether gains are concentrated in a few sectors   |
| 10-30bn sub-bins | avoid overgeneralizing medium-cap effects to large caps     |
| loss history     | test whether repeated losses improve indicator-decline rule |
| active lifetime  | compare shorter 20/60td decay against 120/242td persistence |
+------------------+-------------------------------------------------------------+
```
