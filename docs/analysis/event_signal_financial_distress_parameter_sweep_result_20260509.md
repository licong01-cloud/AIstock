# Phase 15 Parameter Sweep for Financial Distress Score-Down Config - 2026-05-09

Research-only offline QE overlay sweep. This phase compares the Phase 13/14 non-hard score-down candidate across a larger 22-loop set and tests both context-aware and fixed-rank demotion variants. It still does not modify QE runtime, Selection Center, Paper Trading, QMT, or live trading.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 15                                                                  |
| loop set         | 22 QE loops from 8 experiments                                      |
| date range       | 2024-07-01 -> 2026-04-27                                            |
| validations      | 330                                                                 |
| candidate        | indicator_large_decline_mv_10_30bn                                  |
| runtime impact   | none                                                                |
+------------------+---------------------------------------------------------------------+
```

## Loop Set

```text
+--------------------------+-------+--------------------------------------------------------------+
| experiment               | loops | note                                                         |
+--------------------------+-------+--------------------------------------------------------------+
| qe_20260507_132049_d4e7  | 2     | main Phase 12 baseline                                       |
| qe_20260506_182113       | 1     | additional validation artifact                               |
| qe_20260505_153534_388f  | 2     | additional validation artifact                               |
| qe_20260505_122348_690d  | 2     | additional validation artifact                               |
| qe_20260501_011054_c90a  | 3     | additional validation artifact                               |
| qe_20260430_010121_d55f  | 4     | additional validation artifact                               |
| qe_20260429_015755_c4ba  | 4     | additional validation artifact                               |
| qe_20260428_001749_c5b2  | 4     | additional validation artifact                               |
+--------------------------+-------+--------------------------------------------------------------+
```

## Mode Map

```text
+----------------------+--------------------------------------------------------------+
| tag                  | simulator mode                                               |
+----------------------+--------------------------------------------------------------+
| ctx_balanced         | score_down_context_rank_decay_balanced_top50_previous        |
| ctx_severity         | score_down_context_rank_decay_severity_top50_previous        |
| fixed_10             | score_down_rank_10pct_top50_previous                         |
| fixed_15             | score_down_rank_15pct_top50_previous                         |
| fixed_20             | score_down_rank_20pct_top50_previous                         |
+----------------------+--------------------------------------------------------------+
```

## Top Results

```text
+-----------+----------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| active_td | mode_tag       | pos/loops | blocked | eval_topk | dropped | repl | avg_pen | avg_ret_d | min_ret_d | max_ret_d | mdd_d     |
+-----------+----------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| 60        | ctx_balanced   | 14/22     | 733     | 681       | 13      | 7    | 12.59%  | 0.11%     | -0.32%    | 1.88%     | 0.00%     |
| 60        | fixed_10       | 15/22     | 733     | 681       | 16      | 9    | 10.00%  | 0.09%     | -0.88%    | 1.88%     | 0.02%     |
| 20        | fixed_15       | 14/22     | 295     | 275       | 10      | 6    | 15.00%  | 0.09%     | -0.32%    | 1.88%     | 0.00%     |
| 20        | ctx_balanced   | 14/22     | 295     | 275       | 9       | 5    | 12.71%  | 0.08%     | -0.32%    | 1.88%     | 0.00%     |
| 20        | fixed_10       | 14/22     | 295     | 275       | 9       | 5    | 10.00%  | 0.08%     | -0.32%    | 1.88%     | 0.00%     |
| 120       | ctx_severity   | 11/22     | 1420    | 1328      | 17      | 8    | 9.27%   | 0.08%     | -0.47%    | 1.88%     | 0.00%     |
+-----------+----------------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
```

## Interpretation

```text
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| question                             | answer                  | implication                                                  |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| which config is best overall?        | 60td context balanced   | keep as primary research candidate                           |
| is the simple fixed-rank baseline ok | yes, but weaker tail    | keep fixed_10 as simplicity baseline only                   |
| do 20td variants still work?        | yes, but not best       | keep 20td as a comparison window, not the preferred default  |
| does 120td add value?               | partially               | 120td ctx_severity remains a secondary diagnostic candidate  |
| is runtime promotion ready?         | no                      | still artifact-level research; no consumer integration yet   |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
```

## Decision

```text
+--------------------------------------+------------------------+--------------------------------------------------------------+
| candidate                            | phase-15 decision      | next action                                                  |
+--------------------------------------+------------------------+--------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn   | KEEP_RESEARCH_PRIMARY  | use 60td context-balanced as the current best candidate      |
| ctx_severity_120td                   | KEEP_SECONDARY         | keep for tail/coverage diagnostics                            |
| fixed_10_60td                        | KEEP_BASELINE          | retain as the simple rank-demotion baseline                   |
| fixed_15_20td                        | KEEP_COMPARISON        | useful comparison, but not preferred over 60td context       |
| fixed_20_*                           | REJECT_CURRENT_PATH    | no better average/tail tradeoff than the top variants        |
+--------------------------------------+------------------------+--------------------------------------------------------------+
```

## Combined Takeaway

- The 22-loop sweep confirms the 10-30bn indicator-decline signal is still concentrated in the same medium-cap bucket and is not a one-loop artifact.
- `60td + rank_decay_balanced` is the strongest overall shape: best average return, 14/22 positive loops, and much better tail than `fixed_10`.
- `fixed_10` remains a clean and easy-to-explain baseline, but it is not the best policy shape.
- `120td + rank_decay_severity` is still viable as a secondary diagnostic branch, but it does not beat the 60td balanced profile.
- No result here justifies hard buy bans, forced exits, or runtime integration yet; the next step should still stay in research-only mode.

## Next Research Step

Phase 16 should stay offline and compare the current primary candidate against a small parameter grid around the 60td profile, rather than moving to LLM/PDF or runtime wiring:

1. keep `indicator_large_decline_mv_10_30bn` as the only candidate family;
2. compare `rank_decay_balanced` with nearby penalty strengths around the current 60td setting;
3. keep `ctx_severity` only as a diagnostic branch for tail-risk comparison;
4. do not add any hard blocking or forced-sell behavior;
5. promote only after a true QE rerun or a much stronger multi-experiment improvement.
