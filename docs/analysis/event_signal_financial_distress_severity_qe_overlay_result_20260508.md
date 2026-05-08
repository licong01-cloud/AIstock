# Event Signal Financial Distress Severity Score-Down QE Overlay Result - 2026-05-08

## Scope

This is the next research step after fixed-rank score-down. It remains research-only and is limited to event-signal offline analysis.

```text
Target rule : loss_to_market_cap_ge_50pct_mv_lt_10bn
Signal type : structured Tushare financial distress signal, not PDF/LLM
QE loops    : 10 existing WSL QE loops
Date range  : 2024-07-01 -> 2026-04-27
TopK        : 50
Rank date   : previous trade date, aligned with QE shift=1 style usage
Mode        : dynamic severity-based score-down rerank
Report      : reports/event_signal/financial_distress_severity_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_150123.json
```

## Severity Profiles

```text
+--------------+----------+--------------+--------------+-----------------+---------+----------------------------------------------+
| profile      | base_pct | loss>=100%   | mv<5bn       | loss_reports>=4 | max_pct | interpretation                                |
+--------------+----------+--------------+--------------+-----------------+---------+----------------------------------------------+
| balanced     | 10%      | +5%          | +5%          | +5%             | 25%     | balanced loss severity + size + persistence   |
| conservative | 10%      | +5%          | +2.5%        | +2.5%           | 20%     | softer version close to fixed 20% upper bound |
| loss_heavy   | 5%       | +15%         | +2.5%        | +2.5%           | 30%     | only severe loss gets strong penalty          |
+--------------+----------+--------------+--------------+-----------------+---------+----------------------------------------------+
```

## Result

```text
+-----------+----------------------------------------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+
| active_td | rule_key                               | mode                                            | pos/loops | blocked | eval_topk | dropped | repl | avg_pen | avg_ret_d | med_ret_d | avg_mdd_d |
+-----------+----------------------------------------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_balanced_top50_previous     | 6/10      | 77      | 77        | 5       | 3    | 16.42%  | 0.20%     | 0.00%     | -0.00%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_conservative_top50_previous | 6/10      | 77      | 77        | 5       | 3    | 13.38%  | 0.20%     | 0.00%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_balanced_top50_previous     | 6/10      | 137     | 136       | 8       | 4    | 16.93%  | 0.18%     | 0.00%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_conservative_top50_previous | 6/10      | 137     | 136       | 7       | 4    | 13.78%  | 0.18%     | 0.00%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_balanced_top50_previous     | 6/10      | 247     | 246       | 10      | 5    | 16.44%  | 0.16%     | 0.00%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_conservative_top50_previous | 6/10      | 247     | 246       | 9       | 5    | 13.40%  | 0.16%     | 0.00%     | -0.00%    |
| 60        | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_loss_heavy_top50_previous   | 6/10      | 77      | 77        | 2       | 1    | 9.04%   | 0.09%     | 0.00%     | -0.00%    |
| 120       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_loss_heavy_top50_previous   | 5/10      | 137     | 136       | 4       | 2    | 10.01%  | 0.07%     | 0.00%     | -0.00%    |
| 242       | loss_to_market_cap_ge_50pct_mv_lt_10bn | score_down_severity_loss_heavy_top50_previous   | 5/10      | 247     | 246       | 5       | 2    | 9.13%   | 0.07%     | 0.00%     | -0.00%    |
+-----------+----------------------------------------+-------------------------------------------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+
```

## Comparison With Fixed Penalty

```text
+-------------------------------+----------+----------+----------+------------------------------+
| method                        | 60td avg | 120td avg| 242td avg| conclusion                   |
+-------------------------------+----------+----------+----------+------------------------------+
| fixed 20% rank penalty        | 0.20%    | 0.18%    | 0.16%    | best simple baseline          |
| severity balanced             | 0.20%    | 0.18%    | 0.16%    | matches fixed 20%, explainable|
| severity conservative         | 0.20%    | 0.18%    | 0.16%    | matches fixed 20%, less harsh |
| severity loss_heavy           | 0.09%    | 0.07%    | 0.07%    | too weak for this rule        |
| fixed 50% rank penalty        | 0.10%    | -0.19%   | -0.27%   | too aggressive                |
+-------------------------------+----------+----------+----------+------------------------------+
```

## Interpretation

```text
+--------------------------------------+-----------------------------------------------------------------------------------------+
| Question                             | Current conclusion                                                                      |
+--------------------------------------+-----------------------------------------------------------------------------------------+
| Did severity improve alpha?          | No clear improvement over fixed 20%; result is almost identical for balanced/conservative.|
| Is severity still useful?            | Yes, for explainability and future configuration; it ties penalty to loss/size/history. |
| Is loss-heavy profile useful?        | Not for this first rule. It penalizes too weakly unless loss/mv >= 100%.                |
| Should it become hard block?         | No. The effect is small and mostly Top50-boundary replacement.                          |
| Current best research candidate      | Balanced or conservative severity profile, but fixed 20% remains the benchmark.         |
+--------------------------------------+-----------------------------------------------------------------------------------------+
```

## Research Decision

The dynamic severity profiles confirm the score-down direction but do not beat the simpler fixed 20% baseline. This suggests the next high-value research is not more penalty curves, but exposure diagnosis and signal expansion:

```text
+------+------------------------------------------------------------------------------------------------------+
| Step | Next action                                                                                          |
+------+------------------------------------------------------------------------------------------------------+
| 1    | Add industry/size-neutral diagnostics for affected buys and replacements.                             |
| 2    | Check whether the small positive result is concentrated in real-estate/unknown/small-cap exposure.    |
| 3    | Add one new structured financial signal family at a time: consecutive losses, forecast loss, decline. |
| 4    | Compare each new family against fixed 20% and severity-balanced overlays.                             |
| 5    | Only after stable multi-loop evidence, run a real QE overlay experiment instead of offline accounts.  |
+------+------------------------------------------------------------------------------------------------------+
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
