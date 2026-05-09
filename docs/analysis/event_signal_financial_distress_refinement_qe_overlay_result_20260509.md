# Phase 10 Size/Loss-History/Decay Refinement Research - 2026-05-09

Research-only offline QE overlay study. User confirmed current expected capital is about 10 million CNY, market impact is not a current constraint, and industry neutrality is not required. Therefore this phase does not reject industry concentration; sector/industry exposure is retained only as explanatory context for possible sector rotation.

## Scope

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | value                                                                          |
+----------------------+--------------------------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                                       |
| runtime boundary     | no QE/Paper/Selection/QMT runtime integration                                  |
| capital assumption   | about 10m CNY; no market-impact filter                                         |
| industry assumption  | no industry neutralization; do not reject sector concentration by itself        |
| date range           | 2024-07-01 -> 2026-04-27                                                       |
| loops                | 10 QE loops                                                                    |
| validations          | 640                                                                            |
| active lifetimes     | 20, 60, 120, 242 trading days                                                  |
| modes                | score_down_rank_20pct_top50_previous, score_down_severity_balanced_top50_prev. |
| generated report     | reports/event_signal/financial_distress_refinement_qe_overlay/...090112.md     |
+----------------------+--------------------------------------------------------------------------------+
```

## Rules Tested

```text
+------------------------------------------------------+---------------------+---------------------------------------------------------+
| rule_key                                             | purpose             | definition                                              |
+------------------------------------------------------+---------------------+---------------------------------------------------------+
| indicator_large_decline_mv_10_30bn                   | size split          | indicator large decline, PIT market cap 10-30bn         |
| indicator_large_decline_mv_30_100bn                  | size split          | indicator large decline, PIT market cap 30-100bn        |
| indicator_large_decline_mv_ge_100bn                  | sparse mega-cap     | indicator large decline, PIT market cap >=100bn         |
| indicator_large_decline_mv_ge_10bn_prior_loss_ge_2   | loss-history test   | indicator decline + mv>=10bn + prior loss periods >=2   |
| indicator_large_decline_mv_10_30bn_prior_loss_ge_2   | loss-history test   | indicator decline + mv 10-30bn + prior losses >=2       |
| structured_financial_risk_mv_10_30bn                 | broad size split    | any structured financial risk, PIT market cap 10-30bn   |
| structured_financial_risk_mv_ge_30bn                 | broad size split    | any structured financial risk, PIT market cap >=30bn    |
| structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | broad loss-history  | any structured risk + mv>=10bn + prior losses >=2       |
+------------------------------------------------------+---------------------+---------------------------------------------------------+
```

## Main Results - Indicator Decline Size Split

```text
+--------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| rule_key                                   | active_td | mode                                        | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | min_ret_d | max_ret_d |
+--------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| indicator_large_decline_mv_10_30bn         | 20        | score_down_rank_20pct_top50_previous        | 5/10      | 102     | 101       | 6       | 3    | 0.09%     | -1.03%    | 1.88%     |
| indicator_large_decline_mv_10_30bn         | 60        | score_down_rank_20pct_top50_previous        | 4/10      | 276     | 274       | 9       | 4    | 0.05%     | -1.03%    | 1.88%     |
| indicator_large_decline_mv_10_30bn         | 120       | score_down_rank_20pct_top50_previous        | 3/10      | 575     | 573       | 16      | 6    | 0.17%     | -1.03%    | 3.54%     |
| indicator_large_decline_mv_10_30bn         | 242       | score_down_rank_20pct_top50_previous        | 4/10      | 975     | 969       | 36      | 14   | 0.29%     | -1.68%    | 4.34%     |
| indicator_large_decline_mv_10_30bn         | 20        | score_down_severity_balanced_top50_previous | 6/10      | 102     | 101       | 5       | 2    | 0.20%     | -0.00%    | 1.88%     |
| indicator_large_decline_mv_10_30bn         | 60        | score_down_severity_balanced_top50_previous | 6/10      | 276     | 274       | 6       | 3    | 0.20%     | -0.00%    | 1.88%     |
| indicator_large_decline_mv_10_30bn         | 120       | score_down_severity_balanced_top50_previous | 4/10      | 575     | 573       | 10      | 4    | 0.12%     | -0.47%    | 1.88%     |
| indicator_large_decline_mv_10_30bn         | 242       | score_down_severity_balanced_top50_previous | 4/10      | 975     | 969       | 16      | 7    | 0.24%     | -0.47%    | 2.69%     |
| indicator_large_decline_mv_30_100bn        | 242       | score_down_rank_20pct_top50_previous        | 4/10      | 181     | 181       | 7       | 3    | 0.03%     | -0.38%    | 0.49%     |
| indicator_large_decline_mv_ge_100bn        | 242       | score_down_rank_20pct_top50_previous        | 5/10      | 6       | 6         | 0       | 0    | 0.00%     | -0.00%    | 0.00%     |
+--------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
```

## Main Results - Prior Loss History

```text
+------------------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| rule_key                                             | active_td | mode                                        | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | min_ret_d | max_ret_d |
+------------------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
| indicator_large_decline_mv_ge_10bn_prior_loss_ge_2   | 60        | score_down_rank_20pct_top50_previous        | 5/10      | 65      | 65        | 2       | 1    | 0.04%     | -0.00%    | 0.36%     |
| indicator_large_decline_mv_ge_10bn_prior_loss_ge_2   | 120       | score_down_rank_20pct_top50_previous        | 4/10      | 128     | 128       | 7       | 5    | -0.00%    | -0.18%    | 0.13%     |
| indicator_large_decline_mv_ge_10bn_prior_loss_ge_2   | 242       | score_down_rank_20pct_top50_previous        | 5/10      | 191     | 191       | 10      | 5    | 0.05%     | -0.00%    | 0.19%     |
| indicator_large_decline_mv_10_30bn_prior_loss_ge_2   | 60        | score_down_rank_20pct_top50_previous        | 5/10      | 44      | 44        | 2       | 1    | 0.04%     | -0.00%    | 0.36%     |
| indicator_large_decline_mv_10_30bn_prior_loss_ge_2   | 120       | score_down_rank_20pct_top50_previous        | 4/10      | 110     | 110       | 5       | 3    | -0.02%    | -0.18%    | 0.01%     |
| indicator_large_decline_mv_10_30bn_prior_loss_ge_2   | 242       | score_down_rank_20pct_top50_previous        | 5/10      | 165     | 165       | 8       | 3    | 0.04%     | -0.00%    | 0.19%     |
| structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | 60        | score_down_rank_20pct_top50_previous        | 6/10      | 232     | 232       | 12      | 8    | 0.15%     | -0.00%    | 0.69%     |
| structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | 242       | score_down_severity_balanced_top50_previous | 3/10      | 399     | 398       | 8       | 6    | -0.05%    | -0.51%    | 0.37%     |
+------------------------------------------------------+-----------+---------------------------------------------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+
```

## Interpretation

```text
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| candidate                                  | phase-10 decision     | reason                                                     |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn         | KEEP_PRIMARY_CANDIDATE| strongest and most interpretable phase-10 refinement       |
| indicator_large_decline_mv_30_100bn        | WATCHLIST_ONLY        | non-zero but weak portfolio effect                         |
| indicator_large_decline_mv_ge_100bn        | REJECT_RUNTIME        | no dropped Top50 events; too sparse                        |
| indicator + prior_loss_ge_2 variants       | REJECT_REFINEMENT     | prior-loss filter reduces coverage and does not improve    |
| structured_financial_risk_mv_10_30bn       | COVERAGE_BENCHMARK    | useful comparison but broader/worse tail than indicator    |
| structured_financial_risk_mv_ge_30bn       | WATCHLIST_ONLY        | stable but small effect                                    |
| structured + prior_loss_ge_2               | REJECT_REFINEMENT     | mixed results, not better than size-only candidate         |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
```

## Effective-Lifetime Finding

```text
+------------------------------------+-------------------+------------------------------------------------------------+
| lifetime finding                   | preferred use     | reason                                                     |
+------------------------------------+-------------------+------------------------------------------------------------+
| 20td severity on 10-30bn indicator | short-term option | avg_ret_d 0.20%, pos 6/10, no negative worst loop          |
| 60td severity on 10-30bn indicator | balanced default  | avg_ret_d 0.20%, pos 6/10, broader coverage than 20td      |
| 120td severity                     | not preferred     | lower avg_ret_d and fewer positive loops                   |
| 242td fixed rank20                 | aggressive option | highest avg_ret_d 0.29%, but worse negative loop -1.68%    |
+------------------------------------+-------------------+------------------------------------------------------------+
```

## Updated Research Direction

- Do not use industry concentration as a reason to reject the signal; at 10m CNY and no market-impact constraint, sector clustering can be a feature if it aligns with plate rotation.
- Keep industry/sector exposure in reports as explanation and future attribution, not as a neutralization gate.
- Next research should validate `indicator_large_decline_mv_10_30bn` against more loops or a direct event-date return study before any runtime integration.
- Current best candidate for future non-hard overlay is `indicator_large_decline_mv_10_30bn` with 20-60 trading-day decay and severity-style score-down, not hard ban or forced sell.

## Implementation Notes

- Added `REFINEMENT_RULES` and CLI switches `--include-refinement-rules` / `--refinement-only`.
- Added `prior_loss_report_count_730d` fields so non-loss events like `financial_indicator_large_decline` can be tested against prior loss history without changing the old current-loss-only fields.
- All phase-10 logic remains in event-signal research code and tests only.
