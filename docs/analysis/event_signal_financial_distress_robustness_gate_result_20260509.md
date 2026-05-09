# Phase 16 Robustness Gate for Financial Distress Score-Down - 2026-05-09

Research-only gate assessment for the current best non-hard financial-distress candidate. This phase asks whether the Phase 15 winner is robust enough for persistence or runtime integration. It does not modify QE runtime, Selection Center, Paper Trading, QMT, live trading, or database schema/data.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 16                                                                  |
| candidate        | indicator_large_decline_mv_10_30bn                                  |
| selected profile | 60td + score_down_context_rank_decay_balanced_top50_previous        |
| source reports   | Phase 15 context/fixed parameter-sweep JSON reports                 |
| loop set         | 22 QE loops from 8 experiments                                      |
| runtime impact   | none                                                                |
| DB impact        | none                                                                |
+------------------+---------------------------------------------------------------------+
```

## Candidate Robustness Summary

```text
+----------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| profile        | active_td | pos/loops | blocked | eval_topk | dropped | repl | avg_ret_d | med_ret_d | min_ret_d | max_ret_d | ex_max_avg|
+----------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
| ctx_balanced   | 60        | 14/22     | 733     | 681       | 13      | 7    | 0.1134%   | 0.0000%   | -0.3206%  | 1.8771%   | 0.0294%   |
| fixed_10       | 60        | 15/22     | 733     | 681       | 16      | 9    | 0.0868%   | 0.0000%   | -0.8788%  | 1.8771%   | 0.0015%   |
| ctx_balanced   | 20        | 14/22     | 295     | 275       | 9       | 5    | 0.0825%   | 0.0000%   | -0.3206%  | 1.8771%   | -0.0030%  |
| ctx_severity   | 120       | 11/22     | 1420    | 1328      | 17      | 8    | 0.0796%   | 0.0000%   | -0.4703%  | 1.8771%   | -0.0060%  |
+----------------+-----------+-----------+---------+-----------+---------+------+-----------+-----------+-----------+-----------+-----------+
```

`ex_max_avg` removes the single best loop. This is the key robustness warning: the preferred profile remains positive after removing the best loop, but the average shrinks from `0.1134%` to `0.0294%`.

## Per-Experiment Robustness

```text
+--------------------------+-----------+-----------+-----------+-----------+---------+-----------+---------+------+
| experiment               | pos/loops | avg_ret_d | med_ret_d | min_ret_d | max_ret_d| eval_topk | dropped | repl |
+--------------------------+-----------+-----------+-----------+-----------+---------+-----------+---------+------+
| qe_20260428_001749_c5b2  | 2/4       | 0.0000%   | 0.0000%   | -0.0000%  | 0.0000% | 137       | 0       | 0    |
| qe_20260429_015755_c4ba  | 2/4       | -0.1052%  | -0.0502%  | -0.3206%  | 0.0000% | 152       | 4       | 1    |
| qe_20260430_010121_d55f  | 4/4       | 0.2402%   | 0.2036%   | 0.0000%   | 0.5537% | 118       | 4       | 4    |
| qe_20260501_011054_c90a  | 2/3       | 0.0231%   | 0.0000%   | -0.0000%  | 0.0692% | 85        | 1       | 1    |
| qe_20260505_122348_690d  | 1/2       | 0.0000%   | 0.0000%   | -0.0000%  | 0.0000% | 32        | 0       | 0    |
| qe_20260505_153534_388f  | 0/2       | -0.0000%  | -0.0000%  | -0.0000%  | -0.0000%| 26        | 0       | 0    |
| qe_20260506_182113       | 1/1       | 0.0000%   | 0.0000%   | 0.0000%   | 0.0000% | 54        | 0       | 0    |
| qe_20260507_132049_d4e7  | 2/2       | 0.9427%   | 0.9427%   | 0.0082%   | 1.8771% | 77        | 4       | 1    |
+--------------------------+-----------+-----------+-----------+-----------+---------+-----------+---------+------+
```

## Gate Assessment

```text
+--------------------------------------+------------+----------------------------+------------------------------------------------------+
| gate                                 | threshold  | observed                   | decision                                             |
+--------------------------------------+------------+----------------------------+------------------------------------------------------+
| avg_return_delta                     | > 0        | 0.1134%                    | PASS                                                 |
| positive_loop_ratio                  | >= 60%     | 14/22 = 63.6%              | PASS                                                 |
| worst_loop_tail                      | > -0.50%   | -0.3206%                   | PASS                                                 |
| ex_best_loop_avg                     | > 0        | 0.0294%                    | WEAK_PASS                                            |
| median_loop_delta                    | > 0        | 0.0000%                    | FAIL                                                 |
| experiment_breadth                   | broad      | positive effect concentrated| FAIL                                                 |
| action_density                       | enough     | 13 drops / 681 evaluated   | WEAK_PASS                                            |
| hard-action evidence                 | required   | absent                     | FAIL_FOR_HARD_ACTION                                 |
+--------------------------------------+------------+----------------------------+------------------------------------------------------+
```

## Interpretation

```text
+-----------------------------+---------------------------------------------------------------+
| issue                       | implication                                                   |
+-----------------------------+---------------------------------------------------------------+
| median is zero              | most loops are unchanged; benefit comes from a few replacements|
| best loop is influential    | result is still somewhat outlier-dependent                    |
| experiment breadth is mixed | one experiment is negative and several are effectively neutral |
| context beats fixed tail    | context-balanced remains better policy shape than fixed_10    |
| no hard-action evidence     | no buy ban, block add, or forced sell should be considered    |
+-----------------------------+---------------------------------------------------------------+
```

## Phase 16 Decision

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| item                                 | decision                   | rationale                                                    |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| current primary candidate            | KEEP_RESEARCH_PRIMARY      | best non-hard profile, but effect is modest                  |
| runtime integration                   | REJECT_NOW                 | not robust enough; artifact overlay only                     |
| DB policy draft persistence           | DEFER                      | wait until true QE rerun or stronger evidence                |
| true QE rerun                         | DESIGN_NEXT                | justified as next research validation, not as deployment     |
| LLM/PDF preprocessing                 | DEFER                      | structured signal still needs more validation first          |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Recommended True QE Rerun Design

```text
+------------------+---------------------------------------------------------------------+
| design item      | recommendation                                                       |
+------------------+---------------------------------------------------------------------+
| scope            | one offline experiment family first, not production QE runtime        |
| candidate        | only indicator_large_decline_mv_10_30bn                              |
| profile          | 60td context-balanced score-down                                     |
| baseline         | original unchanged QE loop outputs                                   |
| comparator       | fixed_10_60td as simple baseline                                     |
| required trace   | candidate rank before/after, penalty, active signal ids, replacement |
| success gate     | positive avg and positive median; no worse tail than Phase 16         |
| deployment gate  | still none; true rerun only informs future research                  |
+------------------+---------------------------------------------------------------------+
```

## Next Phase

Phase 17 should create a research-only true-QE-rerun design or dry-run harness plan. Do not write to policy tables and do not connect Selection Center, Paper Trading, QMT, or live trading. If true QE rerun is too costly or requires modifying production QE code, first document the exact hook points and required audit trace instead of implementing them.
