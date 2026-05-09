# Phase 20 Selective True QE Rerun Shortlist - 2026-05-10

Research-only screening report generated from ignored offline reports in `reports\event_signal`. This is a budget gate before expensive WSL full-universe true QE reruns; it does not approve runtime integration, DB policy persistence, hard buy bans, or forced sells.

## Screening Gate

```text
+----------------------+--------------------------------------------------------------+
| gate                 | requirement                                                  |
+----------------------+--------------------------------------------------------------+
| cheap first          | use existing overlay/event-study artifacts before WSL rerun  |
| broad proof          | prefer 22-loop evidence; 10-loop rows need cheap expansion   |
| outlier control      | compare avg with ex-best average and worst loop              |
| real impact          | require actual top50 drops/replacements, not only hits       |
| runtime boundary     | no QE/Paper/Selection/QMT code path is changed               |
+----------------------+--------------------------------------------------------------+
```

## Decision Counts

```text
+------------------------------+------+----------------------------------------------+
| decision                     | rows | meaning                                      |
+------------------------------+------+----------------------------------------------+
| ALREADY_WSL_TESTED_WEAK      | 1    | one-loop true rerun exists but weak          |
| BENCHMARK_ONLY               | 64   | comparison rule, not deployment thesis       |
| EXPAND_22_LOOP_OVERLAY_FIRST | 33   | promising 10-loop row; cheap expansion first |
| REJECT_TRUE_RERUN            | 227  | insufficient evidence                        |
| WATCHLIST                    | 18   | not enough for WSL budget                    |
+------------------------------+------+----------------------------------------------+
```

## Shortlist

```text
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
| score | loops | pos   | avg    | ex_best | min     | drop | td  | mode         | rule                                                 | decision                     |
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
| 50.9  | 22    | 14/22 | 0.113% | 0.029%  | -0.321% | 13   | 60  | ctx_balanced | indicator_large_decline_mv_10_30bn                   | ALREADY_WSL_TESTED_WEAK      |
| 66.9  | 10    | 6/10  | 0.267% | 0.088%  | -0.000% | 14   | 60  | severity     | structured_financial_risk_mv_ge_10bn                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.5  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | loss_to_market_cap_ge_50pct                          | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.4  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | forecast_loss_to_market_cap_ge_50pct                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.3  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | loss_to_market_cap_ge_50pct_mv_lt_5bn                | EXPAND_22_LOOP_OVERLAY_FIRST |
| 54.5  | 10    | 6/10  | 0.148% | 0.030%  | -0.088% | 54   | 242 | severity     | loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss            | EXPAND_22_LOOP_OVERLAY_FIRST |
| 53.4  | 10    | 6/10  | 0.131% | 0.034%  | -0.054% | 53   | 242 | severity     | loss_reports_ge_4_mv_lt_10bn                         | EXPAND_22_LOOP_OVERLAY_FIRST |
| 52.8  | 10    | 6/10  | 0.214% | 0.029%  | -0.000% | 11   | 60  | severity     | structured_financial_risk_mv_10_30bn                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 52.5  | 10    | 6/10  | 0.149% | 0.089%  | -0.000% | 12   | 60  | fixed_20     | structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | EXPAND_22_LOOP_OVERLAY_FIRST |
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
```

## Best Row Per Rule

```text
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
| score | loops | pos   | avg    | ex_best | min     | drop | td  | mode         | best_rule                                            | next                         |
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
| 66.9  | 10    | 6/10  | 0.267% | 0.088%  | -0.000% | 14   | 60  | severity     | structured_financial_risk_mv_ge_10bn                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.5  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | loss_to_market_cap_ge_50pct                          | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.5  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | loss_to_market_cap_ge_50pct_mv_lt_10bn               | BENCHMARK_ONLY               |
| 56.4  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | forecast_loss_to_market_cap_ge_50pct                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 56.3  | 10    | 6/10  | 0.182% | 0.113%  | -0.000% | 9    | 120 | fixed_20     | loss_to_market_cap_ge_50pct_mv_lt_5bn                | EXPAND_22_LOOP_OVERLAY_FIRST |
| 55.2  | 10    | 5/10  | 0.231% | 0.072%  | -0.287% | 10   | 120 | fixed_20     | loss_20_50pct_and_loss_reports_ge_4                  | WATCHLIST                    |
| 54.5  | 10    | 6/10  | 0.148% | 0.030%  | -0.088% | 54   | 242 | severity     | loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss            | EXPAND_22_LOOP_OVERLAY_FIRST |
| 53.4  | 10    | 6/10  | 0.131% | 0.034%  | -0.054% | 53   | 242 | severity     | loss_reports_ge_4_mv_lt_10bn                         | EXPAND_22_LOOP_OVERLAY_FIRST |
| 52.8  | 10    | 6/10  | 0.214% | 0.029%  | -0.000% | 11   | 60  | severity     | structured_financial_risk_mv_10_30bn                 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 52.5  | 10    | 6/10  | 0.149% | 0.089%  | -0.000% | 12   | 60  | fixed_20     | structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | EXPAND_22_LOOP_OVERLAY_FIRST |
| 51.8  | 10    | 6/10  | 0.115% | 0.034%  | -0.054% | 52   | 242 | severity     | forecast_loss_reports_ge_4_mv_lt_10bn                | EXPAND_22_LOOP_OVERLAY_FIRST |
| 50.9  | 22    | 14/22 | 0.113% | 0.029%  | -0.321% | 13   | 60  | ctx_balanced | indicator_large_decline_mv_10_30bn                   | ALREADY_WSL_TESTED_WEAK      |
| 48.6  | 10    | 6/10  | 0.215% | 0.030%  | -0.000% | 8    | 60  | severity     | indicator_large_decline_mv_ge_10bn                   | WATCHLIST                    |
| 39.0  | 10    | 4/10  | 0.196% | 0.032%  | -0.595% | 28   | 120 | fixed_20     | loss_to_market_cap_20_50pct                          | WATCHLIST                    |
| 38.5  | 10    | 5/10  | 0.131% | 0.021%  | -0.353% | 62   | 242 | fixed_20     | loss_reports_ge_4                                    | WATCHLIST                    |
| 36.9  | 10    | 5/10  | 0.115% | 0.021%  | -0.353% | 61   | 242 | fixed_20     | forecast_loss_and_loss_reports_ge_4                  | WATCHLIST                    |
+-------+-------+-------+--------+---------+---------+------+-----+--------------+------------------------------------------------------+------------------------------+
```

## Direct Event Sanity Check

```text
+----------------------------------------+--------+----------+------------+----------+-------+-----------------+
| rule                                   | window | mean_abn | median_abn | neg_rate | valid | note            |
+----------------------------------------+--------+----------+------------+----------+-------+-----------------+
| indicator_large_decline_mv_10_30bn     | T+5    | 0.520%   | 0.042%     | 49.642%  | 1118  | positive median |
| indicator_large_decline_mv_10_30bn     | T+20   | 1.406%   | -0.517%    | 52.523%  | 971   | negative median |
| indicator_large_decline_mv_10_30bn     | T+60   | 4.717%   | -1.258%    | 53.598%  | 931   | negative median |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | T+5    | 2.184%   | 2.007%     | 40.278%  | 72    | positive median |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | T+20   | 8.159%   | 4.718%     | 37.681%  | 69    | positive median |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | T+60   | 5.642%   | -0.553%    | 51.064%  | 47    | negative median |
| structured_financial_risk_mv_10_30bn   | T+5    | 0.663%   | -0.135%    | 51.117%  | 2238  | negative median |
| structured_financial_risk_mv_10_30bn   | T+20   | 2.635%   | -0.096%    | 50.171%  | 2049  | negative median |
| structured_financial_risk_mv_10_30bn   | T+60   | 2.652%   | -2.566%    | 56.761%  | 1797  | negative median |
+----------------------------------------+--------+----------+------------+----------+-------+-----------------+
```

## Phase 20 Conclusion

- No new candidate passes `WSL_TRUE_RERUN_NOW`; broad WSL batch reruns are not justified yet.
- The already WSL-tested `indicator_large_decline_mv_10_30bn / 60td / ctx_balanced` remains a calibrated weak-positive baseline, not a deployment candidate.
- Strong 10-loop rows, especially `structured_financial_risk_mv_ge_10bn`, must first be expanded to the same 22-loop cheap overlay set before a WSL true-rerun budget is spent.
- The old `loss_to_market_cap_ge_50pct_mv_lt_10bn` benchmark remains useful, but direct event returns do not support a hard-risk thesis; keep it as a benchmark rather than a runtime rule.
- Next empirical step should be a cheap 22-loop overlay expansion for the top 10-loop candidates, not LLM/PDF and not production wiring.
