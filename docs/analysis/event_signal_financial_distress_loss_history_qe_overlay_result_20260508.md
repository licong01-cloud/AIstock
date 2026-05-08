# Event Signal Financial Distress Loss-History QE Overlay Result - 2026-05-08

## Scope

```text
Worktree      : F:/Dev/AIstock_worktrees/financial-distress-rerank-20260508
Branch        : codex/financial-distress-rerank-20260508
Research type : offline QE overlay only
Date range    : 2024-07-01 -> 2026-04-27
QE loops      : 10
Signal family : rolling financial loss history only
Runtime impact: none; no QE/Paper/Selection/QMT hook
Report JSON   : reports/event_signal/financial_distress_loss_history_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_173351.json
Report MD     : reports/event_signal/financial_distress_loss_history_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_173351.md
```

## Research Question

Validate whether rolling loss-report count >= 4 within 730 days can serve as an earlier and broader financial-distress score-down signal than the narrower loss-to-market-cap >= 50% rule. This remains an offline overlay only, not a hard buy ban or forced-sell rule.

## Main Result

```text
+-------------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
| rule                                      | td  | mode              | pos  | eval | drop | repl | avg_ret | med_ret | min_ret | max_ret | avg_mdd |
+-------------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
| forecast_loss_reports_ge_4_mv_lt_10bn     | 60  | rank20_prev       | 3/10 | 971  | 23   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.42%   | 0.00%   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 60  | severity_bal_prev | 3/10 | 971  | 22   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.50%   | 0.00%   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 120 | rank20_prev       | 5/10 | 1788 | 49   | 41   | -0.13%  | 0.00%   | -1.60%  | 0.11%   | 0.00%   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 120 | severity_bal_prev | 5/10 | 1788 | 46   | 39   | -0.11%  | 0.00%   | -1.60%  | 0.20%   | 0.00%   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | rank20_prev       | 6/10 | 2179 | 56   | 47   | 0.11%   | 0.02%   | -0.05%  | 0.91%   | 0.07%   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | severity_bal_prev | 6/10 | 2179 | 52   | 44   | 0.11%   | 0.02%   | -0.05%  | 0.84%   | 0.07%   |
| loss_reports_ge_4                         | 60  | rank20_prev       | 4/10 | 1056 | 26   | 23   | -0.07%  | -0.00%  | -1.28%  | 0.61%   | 0.00%   |
| loss_reports_ge_4                         | 60  | severity_bal_prev | 4/10 | 1056 | 25   | 23   | -0.06%  | -0.00%  | -1.28%  | 0.69%   | 0.00%   |
| loss_reports_ge_4                         | 120 | rank20_prev       | 5/10 | 1912 | 55   | 45   | -0.11%  | 0.00%   | -1.98%  | 0.41%   | 0.00%   |
| loss_reports_ge_4                         | 120 | severity_bal_prev | 5/10 | 1912 | 51   | 43   | -0.11%  | 0.00%   | -2.13%  | 0.41%   | 0.00%   |
| loss_reports_ge_4                         | 242 | rank20_prev       | 5/10 | 2300 | 62   | 51   | 0.13%   | 0.00%   | -0.35%  | 1.12%   | 0.07%   |
| loss_reports_ge_4                         | 242 | severity_bal_prev | 5/10 | 2300 | 56   | 47   | 0.11%   | 0.00%   | -0.51%  | 1.01%   | 0.07%   |
| loss_reports_ge_4_mv_lt_10bn              | 60  | rank20_prev       | 3/10 | 984  | 23   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.42%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn              | 60  | severity_bal_prev | 3/10 | 984  | 22   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.50%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn              | 120 | rank20_prev       | 5/10 | 1803 | 49   | 41   | -0.13%  | 0.00%   | -1.60%  | 0.11%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn              | 120 | severity_bal_prev | 5/10 | 1803 | 46   | 39   | -0.11%  | 0.00%   | -1.60%  | 0.20%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn              | 242 | rank20_prev       | 6/10 | 2193 | 57   | 48   | 0.13%   | 0.02%   | -0.05%  | 1.07%   | 0.07%   |
| loss_reports_ge_4_mv_lt_10bn              | 242 | severity_bal_prev | 6/10 | 2193 | 53   | 45   | 0.13%   | 0.02%   | -0.05%  | 1.01%   | 0.07%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 60  | rank20_prev       | 3/10 | 969  | 23   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.42%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 60  | severity_bal_prev | 3/10 | 969  | 22   | 19   | -0.20%  | -0.00%  | -1.28%  | 0.50%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 120 | rank20_prev       | 5/10 | 1760 | 48   | 41   | -0.13%  | 0.00%   | -1.60%  | 0.11%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 120 | severity_bal_prev | 5/10 | 1760 | 45   | 39   | -0.12%  | 0.00%   | -1.60%  | 0.20%   | 0.00%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | rank20_prev       | 6/10 | 2149 | 57   | 48   | 0.13%   | 0.02%   | -0.09%  | 1.12%   | 0.07%   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | severity_bal_prev | 6/10 | 2149 | 54   | 46   | 0.15%   | 0.02%   | -0.09%  | 1.21%   | 0.07%   |
+-------------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
```

## Baseline Comparison

The prior relative-loss baseline rule `loss_to_market_cap_ge_50pct_mv_lt_10bn` is much narrower, but it is more stable across the same 10 QE loops. The loss-history family covers far more candidates and only becomes positive on average at the 242 trading-day lifetime.

```text
+----------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
| baseline_rule                          | td  | mode              | pos  | eval | drop | repl | avg_ret | med_ret | min_ret | max_ret | avg_mdd |
+----------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 60  | rank20_prev       | 6/10 | 77   | 6    | 3    | 0.20%   | 0.00%   | -0.00%  | 1.02%   | -0.00%  |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 60  | severity_bal_prev | 6/10 | 77   | 5    | 3    | 0.20%   | 0.00%   | -0.00%  | 1.02%   | -0.00%  |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 120 | rank20_prev       | 6/10 | 136  | 9    | 4    | 0.18%   | 0.00%   | -0.00%  | 0.80%   | -0.00%  |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 120 | severity_bal_prev | 6/10 | 136  | 8    | 4    | 0.18%   | 0.00%   | -0.00%  | 0.80%   | -0.00%  |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 242 | rank20_prev       | 6/10 | 246  | 11   | 5    | 0.16%   | 0.00%   | -0.00%  | 0.80%   | -0.00%  |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 242 | severity_bal_prev | 6/10 | 246  | 10   | 5    | 0.16%   | 0.00%   | -0.00%  | 0.79%   | -0.00%  |
+----------------------------------------+-----+-------------------+------+------+------+------+---------+---------+---------+---------+---------+
```

## Exposure

The loss-history family averages roughly 426-537 active symbols per day. That is too broad for a direct runtime score-down rule. The actual Top50 dropped count is 22-62 and is concentrated in small-cap stocks.

```text
+-------------------------------------------+-----+--------------+-------------+----------------------------------------------------------------------------------------------+
| rule                                      | td  | overlay_rows | avg_symbols | top_mv_buckets                                                                               |
+-------------------------------------------+-----+--------------+-------------+----------------------------------------------------------------------------------------------+
| forecast_loss_reports_ge_4_mv_lt_10bn     | 60  | 617450       | 428.0       | mv_lt_5bn_yuan:482080; mv_5bn_to_10bn_yuan:134770; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:600    |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 120 | 1080560      | 477.0       | mv_lt_5bn_yuan:854520; mv_5bn_to_10bn_yuan:221220; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:4820   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | 1359610      | 478.0       | mv_lt_5bn_yuan:1032600; mv_5bn_to_10bn_yuan:235710; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:91300 |
| loss_reports_ge_4                         | 60  | 732490       | 489.0       | mv_lt_5bn_yuan:485390; mv_5bn_to_10bn_yuan:135160; mv_10bn_to_30bn_yuan:73490                |
| loss_reports_ge_4                         | 120 | 1254390      | 536.0       | mv_lt_5bn_yuan:853300; mv_5bn_to_10bn_yuan:217120; mv_10bn_to_30bn_yuan:110260               |
| loss_reports_ge_4                         | 242 | 1527170      | 537.0       | mv_lt_5bn_yuan:1014130; mv_5bn_to_10bn_yuan:191490; mv_10bn_to_30bn_yuan:95870               |
| loss_reports_ge_4_mv_lt_10bn              | 60  | 623170       | 429.0       | mv_lt_5bn_yuan:485740; mv_5bn_to_10bn_yuan:136050; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:1380   |
| loss_reports_ge_4_mv_lt_10bn              | 120 | 1085740      | 477.0       | mv_lt_5bn_yuan:857680; mv_5bn_to_10bn_yuan:222460; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:5600   |
| loss_reports_ge_4_mv_lt_10bn              | 242 | 1363190      | 478.0       | mv_lt_5bn_yuan:1034310; mv_5bn_to_10bn_yuan:236800; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:92080 |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 60  | 605480       | 426.0       | mv_lt_5bn_yuan:475260; mv_5bn_to_10bn_yuan:128880; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:1340   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 120 | 1054740      | 469.0       | mv_lt_5bn_yuan:839990; mv_5bn_to_10bn_yuan:209650; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:5100   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | 1334140      | 470.0       | mv_lt_5bn_yuan:1021250; mv_5bn_to_10bn_yuan:229350; mv_5bn_to_10bn_yuan+mv_lt_5bn_yuan:83540 |
+-------------------------------------------+-----+--------------+-------------+----------------------------------------------------------------------------------------------+
```

## Dropped Top50 Exposure

```text
+-------------------------------------------+-----+-------------------+------+---------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+
| rule                                      | td  | mode              | drop | drop_mv                                                                                                                               | drop_industries                      |
+-------------------------------------------+-----+-------------------+------+---------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | severity_bal_prev | 54   | mv_5bn_to_10bn_yuan:4; mv_lt_5bn_yuan:50                                                                                              | 化学制药:4; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| loss_reports_ge_4                         | 242 | rank20_prev       | 62   | mv_10bn_to_30bn_yuan:4; mv_10bn_to_30bn_yuan+mv_5bn_to_10bn_yuan:1; mv_30bn_to_100bn_yuan:1; mv_5bn_to_10bn_yuan:6; mv_lt_5bn_yuan:50 | 化学制药:4; 家居用品:3; 普钢:5; 服饰:6; 环境保护:6   |
| loss_reports_ge_4_mv_lt_10bn              | 242 | severity_bal_prev | 53   | mv_5bn_to_10bn_yuan:3; mv_lt_5bn_yuan:50                                                                                              | 化学制药:4; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | rank20_prev       | 57   | mv_5bn_to_10bn_yuan:7; mv_lt_5bn_yuan:50                                                                                              | 化学制药:4; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| loss_reports_ge_4_mv_lt_10bn              | 242 | rank20_prev       | 57   | mv_5bn_to_10bn_yuan:7; mv_lt_5bn_yuan:50                                                                                              | 化学制药:4; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | severity_bal_prev | 52   | mv_5bn_to_10bn_yuan:3; mv_lt_5bn_yuan:49                                                                                              | 化学制药:3; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| loss_reports_ge_4                         | 242 | severity_bal_prev | 56   | mv_10bn_to_30bn_yuan:2; mv_10bn_to_30bn_yuan+mv_5bn_to_10bn_yuan:1; mv_30bn_to_100bn_yuan:1; mv_5bn_to_10bn_yuan:2; mv_lt_5bn_yuan:50 | 化学制药:4; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | rank20_prev       | 56   | mv_5bn_to_10bn_yuan:7; mv_lt_5bn_yuan:49                                                                                              | 化学制药:3; 家居用品:3; 服饰:6; 汽车配件:3; 环境保护:6 |
+-------------------------------------------+-----+-------------------+------+---------------------------------------------------------------------------------------------------------------------------------------+--------------------------------------+
```

## Decision

```text
+-------------------------------------------+------------------+-------------------------------------------------------------------------------------------------+
| rule                                      | decision         | reason                                                                                          |
+-------------------------------------------+------------------+-------------------------------------------------------------------------------------------------+
| loss_reports_ge_4                         | REJECT_RUNTIME   | Too broad: 489-537 avg daily symbols; 60/120td avg return <=0; use as feature only.             |
| loss_reports_ge_4_mv_lt_10bn              | RESEARCH_FEATURE | Small-cap filter removes mid/large caps but still broad; 242td positive, 60/120td negative.     |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | RESEARCH_FEATURE | Best 242td row (+0.15%) and tests incremental non-ge50 loss history, but not stable enough.     |
| forecast_loss_reports_ge_4_mv_lt_10bn     | RESEARCH_FEATURE | Nearly overlaps small-cap loss-history rule; keep for cross-source explanation, not standalone. |
+-------------------------------------------+------------------+-------------------------------------------------------------------------------------------------+
```

## Interpretation

- `loss_to_market_cap_ge_50pct_mv_lt_10bn` remains the stronger first-batch benchmark: fewer hits, better stability, and positive average return deltas at 60/120/242 trading-day lifetimes.
- Loss-history-only is too broad as a standalone signal. It can include both already-priced-in weak firms and rebound candidates, producing negative average contribution at 60/120 trading-day lifetimes.
- `loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss` is the most useful incremental research candidate because it excludes the existing loss-to-market-cap >= 50% severe-loss rule. Its best 242td `severity_bal_prev` row is +0.15%, but the shorter horizons are still negative.
- Dropped Top50 industry exposure is not driven by one industry; it spans apparel, environmental protection, chemical pharma, home goods, auto parts, steel, and similar groups. Do not add hard industry filters yet.
- The next step should combine loss history with market-cap bucket, relative loss severity, industry bucket, forecast/report timing, and possible ST proximity before any runtime integration.

## Next Research Direction

```text
+------+-----------------------------------------------+----------------+------------------------------------------------------+
| step | research item                                 | code impact    | validation rule                                      |
+------+-----------------------------------------------+----------------+------------------------------------------------------+
| 1    | relative loss >=50% + small cap baseline       | already exists | keep as benchmark; do not replace before stronger EV |
| 2    | loss history + market cap + industry buckets   | script only    | require 60/120/242td not worse than baseline         |
| 3    | loss history + consecutive annual losses       | script only    | verify earlier than ST and lower drawdown risk       |
| 4    | forecast/express/report cross-source mismatch  | script only    | test expectation-miss rather than raw growth/loss    |
| 5    | LLM/PDF preprocessing                          | deferred       | start only after structured signals show value       |
+------+-----------------------------------------------+----------------+------------------------------------------------------+
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
