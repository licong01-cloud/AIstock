# Event Signal Financial Distress Exposure Diagnostics - 2026-05-08

## Scope

This is the follow-up exposure diagnosis for the financial-distress score-down overlay. It remains research-only and does not change QE, Selection Center, Paper Trading, QMT, or live-trading runtime.

```text
Target rule : loss_to_market_cap_ge_50pct_mv_lt_10bn
Compared    : fixed 20% score-down, severity balanced, severity conservative
QE loops    : 10 existing WSL QE loops
Date range  : 2024-07-01 -> 2026-04-27
Report      : reports/event_signal/financial_distress_exposure_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_152103.json
```

## Return Stability

```text
+-----------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+
| active_td | mode                                            | pos/loops | blocked | eval_topk | dropped | repl | avg_pen | avg_ret_d | avg_mdd_d |
+-----------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+
| 60        | score_down_rank_20pct_top50_previous            | 6/10      | 77      | 77        | 6       | 3    | 20.00%  | 0.20%     | -0.00%    |
| 60        | score_down_severity_balanced_top50_previous     | 6/10      | 77      | 77        | 5       | 3    | 16.42%  | 0.20%     | -0.00%    |
| 60        | score_down_severity_conservative_top50_previous | 6/10      | 77      | 77        | 5       | 3    | 13.38%  | 0.20%     | -0.00%    |
| 120       | score_down_rank_20pct_top50_previous            | 6/10      | 137     | 136       | 9       | 4    | 20.00%  | 0.18%     | -0.00%    |
| 120       | score_down_severity_balanced_top50_previous     | 6/10      | 137     | 136       | 8       | 4    | 16.93%  | 0.18%     | -0.00%    |
| 120       | score_down_severity_conservative_top50_previous | 6/10      | 137     | 136       | 7       | 4    | 13.78%  | 0.18%     | -0.00%    |
| 242       | score_down_rank_20pct_top50_previous            | 6/10      | 247     | 246       | 11      | 5    | 20.00%  | 0.16%     | -0.00%    |
| 242       | score_down_severity_balanced_top50_previous     | 6/10      | 247     | 246       | 10      | 5    | 16.44%  | 0.16%     | -0.00%    |
| 242       | score_down_severity_conservative_top50_previous | 6/10      | 247     | 246       | 9       | 5    | 13.40%  | 0.16%     | -0.00%    |
+-----------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+
```

## Market-Cap Exposure

```text
+-----------+-------------------------------------------------+------------------------------+-----------------------------+-------------------------------+
| active_td | mode                                            | evaluated_mv                 | dropped_mv                  | diagnosis                     |
+-----------+-------------------------------------------------+------------------------------+-----------------------------+-------------------------------+
| 60        | fixed 20%                                       | <5bn:73, 5-10bn:4            | <5bn:6, 5-10bn:0            | dropped names are all <5bn    |
| 120       | fixed 20%                                       | <5bn:112, 5-10bn:25          | <5bn:9, 5-10bn:0            | dropped names are all <5bn    |
| 242       | fixed 20%                                       | <5bn:189, 5-10bn:58          | <5bn:10, 5-10bn:1           | almost all dropped are <5bn   |
| 60        | severity balanced                              | <5bn:73, 5-10bn:4            | <5bn:5, 5-10bn:0            | dropped names are all <5bn    |
| 120       | severity balanced                              | <5bn:112, 5-10bn:25          | <5bn:8, 5-10bn:0            | dropped names are all <5bn    |
| 242       | severity balanced                              | <5bn:189, 5-10bn:58          | <5bn:9, 5-10bn:1            | almost all dropped are <5bn   |
+-----------+-------------------------------------------------+------------------------------+-----------------------------+-------------------------------+
```

## Industry Exposure Of Dropped Names

```text
+-----------+--------------------+---------------------------------------------------------------+----------------------------------------------+
| active_td | method             | dropped_industries                                            | diagnosis                                    |
+-----------+--------------------+---------------------------------------------------------------+----------------------------------------------+
| 60        | fixed 20%          | ????:3, ????:1, ????:1, ??:1                     | not pure real-estate; logistics dominates    |
| 120       | fixed 20%          | ????:4, ????:3, ????:1, ??:1                     | real-estate + logistics concentration        |
| 242       | fixed 20%          | ????:4, ????:3, ??:2, ????:1, ???:1           | concentrated in several weak cyclical groups |
| 60        | severity balanced  | ????:3, ????:1, ????:1                             | one fewer textile drop than fixed 20%        |
| 120       | severity balanced  | ????:4, ????:3, ????:1                             | same core concentration                       |
| 242       | severity balanced  | ????:4, ????:3, ????:1, ??:1, ???:1           | same core concentration                       |
+-----------+--------------------+---------------------------------------------------------------+----------------------------------------------+
```

## Interpretation

```text
+--------------------------------------+----------------------------------------------------------------------------------------+
| Question                             | Current answer                                                                         |
+--------------------------------------+----------------------------------------------------------------------------------------+
| Is the gain only a broad small-cap bet?| Mostly yes. Almost all actually dropped/replaced names are <5bn market-cap stocks.     |
| Is the gain only real-estate?         | No. Real-estate is important, but logistics, textile, chemical raw materials also show. |
| Is the rule acting on many stocks?    | No. It evaluates many hits, but only 5-11 names across 10 loops actually drop Top50.    |
| Does severity improve exposure mix?   | Slightly fewer drops; no materially better industry/size profile than fixed 20%.       |
| Should we add hard industry rules?     | No. Current evidence is too sparse and could overfit to a few industries.              |
+--------------------------------------+----------------------------------------------------------------------------------------+
```

## Decision

The score-down effect is real but sparse. It is mainly a micro/small-cap distress overlay with additional concentration in weak cyclical/asset-heavy industries. The next research should not add hard industry filters. Instead, each new signal family should be tested with the same exposure diagnostics.

```text
+------+-----------------------------------------------------------------------------------------------------+
| Step | Next action                                                                                         |
+------+-----------------------------------------------------------------------------------------------------+
| 1    | Keep fixed 20% and severity balanced as benchmark overlays.                                          |
| 2    | Add the next structured signal family: consecutive-loss / loss-history-only candidates.              |
| 3    | For the new family, report return, dropped Top50 count, market-cap mix, and industry mix together.   |
| 4    | Reject any signal that only improves one loop or depends on one tiny industry bucket.                |
| 5    | Still do not connect financial signals to QE/Paper runtime until real QE overlay experiments pass.   |
+------+-----------------------------------------------------------------------------------------------------+
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
