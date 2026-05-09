# Phase 14 Additional QE Validation for Financial Distress Policy Config - 2026-05-09

Research-only additional validation for the Phase 13 non-hard score-down config. This phase tests the same proposed rule on QE artifacts not used as the main Phase 12 10-loop set. It still does not change QE runtime, Selection Center, Paper Trading, QMT, or live trading.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 14                                                                  |
| loop source      | 12 loops from 3 additional QE experiments                            |
| date range       | 2024-07-01 -> 2026-04-27                                            |
| validations      | 48                                                                  |
| mode             | score_down_context_rank_decay_balanced_top50_previous               |
| active_td        | 20 and 60                                                           |
| report json      | reports/event_signal/financial_distress_policy_config_additional_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_165605.json |
| report md        | reports/event_signal/financial_distress_policy_config_additional_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_165605.md   |
+------------------+---------------------------------------------------------------------+
```

## Additional Loop Set

```text
+--------------------------+-------------------------+-------+
| experiment               | loops                   | count |
+--------------------------+-------------------------+-------+
| qe_20260430_010121_d55f  | Loop1, Loop2, Loop3, Loop4 | 4   |
| qe_20260429_015755_c4ba  | Loop2, Loop3, Loop4, Loop5 | 4   |
| qe_20260428_001749_c5b2  | Loop1, Loop2, Loop3, Loop4 | 4   |
+--------------------------+-------------------------+-------+
```

`qe_20260429_015755_c4ba:Loop1` was intentionally excluded because it did not have both `pred.pkl` and `report_normal_1day.pkl` artifacts.

## Result Summary

```text
+----------------------------------------+-----------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| rule                                   | active_td | pos/loops | blocked | eval_topk | dropped | repl | avg_pen | avg_ret_d | med_ret_d | min_ret_d | max_ret_d |
+----------------------------------------+-----------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
| indicator_large_decline_mv_10_30bn     | 20        | 8/12      | 193     | 174       | 4       | 3    | 13.29%  | -0.01%    | 0.00%     | -0.32%    | 0.19%     |
| indicator_large_decline_mv_10_30bn     | 60        | 8/12      | 457     | 407       | 8       | 5    | 13.66%  | 0.04%     | 0.00%     | -0.32%    | 0.55%     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 20        | 7/12      | 17      | 14        | 0       | 0    | 13.69%  | 0.00%     | 0.00%     | -0.00%    | 0.00%     |
| loss_to_market_cap_ge_50pct_mv_lt_10bn | 60        | 6/12      | 46      | 42        | 1       | 1    | 14.60%  | -0.01%    | 0.00%     | -0.17%    | 0.00%     |
+----------------------------------------+-----------+-----------+---------+-----------+---------+------+---------+-----------+-----------+-----------+-----------+
```

## Interpretation

```text
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| question                             | answer                  | implication                                                  |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
| does extra validation confirm 20td?  | no                      | 20td has 8/12 positive but average is slightly negative      |
| does extra validation support 60td?  | weakly yes              | 60td is positive on average but effect size is small         |
| is this a hard-ban signal?           | no                      | direct event study and extra QE validation remain mixed      |
| is smallcap benchmark useful here?   | limited                 | this extra loop set has too few smallcap topK interactions   |
| can runtime promotion start now?     | not yet                 | needs parameter sweep or true QE rerun before integration    |
+--------------------------------------+-------------------------+--------------------------------------------------------------+
```

## Difference From Phase 12

```text
+-------------------------------+------------------------+-----------------------------+------------------------------+
| item                          | Phase 12 main 10 loops | Phase 14 additional 12 loops | conclusion                   |
+-------------------------------+------------------------+-----------------------------+------------------------------+
| indicator 10-30bn 20td avg    | about +0.20%           | -0.01%                      | not stable enough as default |
| indicator 10-30bn 60td avg    | about +0.20%           | +0.04%                      | better than 20td here        |
| positive loop count           | 6/10                   | 8/12                        | positive count remains ok    |
| worst loop                    | about 0.00%            | -0.32%                      | extra sample has worse tail  |
| dropped events                | 5                      | 8                           | few actual portfolio changes |
+-------------------------------+------------------------+-----------------------------+------------------------------+
```

The additional validation weakens the case for immediately adopting `20td` as the default. It does not reject the candidate, but it changes the preferred research direction: keep the signal as a non-hard candidate and test `60td` plus parameter sweeps before any persistence or runtime integration.

## Why The Effect Is Small

```text
+----------------------------+---------------------------------------------------------------+
| reason                     | explanation                                                   |
+----------------------------+---------------------------------------------------------------+
| rank demotion is moderate  | average penalty moves candidates about 6-7 ranks in Top50      |
| many candidates stay Top50 | 399 of 407 evaluated 60td events remain in Top50              |
| few replacements happen    | only 5 replacement events for 60td indicator rule              |
| signal is contextual       | direct returns show mixed raw effect, so only rank context helps|
+----------------------------+---------------------------------------------------------------+
```

## Phase 14 Decision

```text
+--------------------------------------+----------------------+--------------------------------------------------------------+
| candidate                            | decision             | next step                                                    |
+--------------------------------------+----------------------+--------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn   | KEEP_RESEARCH        | test 60td and slightly stronger non-hard profiles            |
| phase-13 20td default                | DOWNGRADE_TO_TEST    | no longer preferred as the single default                    |
| phase-13 60td candidate              | KEEP_AS_PRIMARY_TEST | best row in additional loops, but not runtime-ready          |
| smallcap_loss_mv50 context benchmark | LOOP_SET_LIMITED     | too few hits in this extra set for a benchmark conclusion    |
+--------------------------------------+----------------------+--------------------------------------------------------------+
```

## Next Phase

Phase 15 should stay research-only and run a parameter sweep instead of LLM/PDF or runtime integration:

1. Keep `indicator_large_decline_mv_10_30bn` and active windows `20/60/120`.
2. Compare `rank_decay_balanced`, `rank_decay_severity`, and fixed rank penalties around `10%/15%/20%`.
3. Keep strict non-hard rules: no block buy, no block add, no force sell, no positive score-up.
4. Use both the Phase 12 main 10-loop set and the Phase 14 additional 12-loop set.
5. Promote only if the combined result improves average return without creating an unacceptable negative tail.
