# Phase 22 Loss-History Tail-Control Parameter Sweep - 2026-05-10

Research-only parameter sweep for Phase 21 loss-history small-cap candidates. The goal is to reduce worst-loop loss before any WSL full-universe true QE rerun decision. This phase does not modify QE runtime, Selection Center, Paper Trading, QMT, DB schema, or live-trading code.

## Scope

```text
+----------------+-------------------------------------------------------------------------------------+
| item           | value                                                                               |
+----------------+-------------------------------------------------------------------------------------+
| branch         | codex/financial-distress-rerank-20260508                                            |
| phase          | 22                                                                                  |
| loop set       | 22 QE loops from 8 experiments                                                      |
| date range     | 2024-07-01 -> 2026-04-27                                                            |
| validations    | 704                                                                                 |
| stability rows | 32                                                                                  |
| focus          | loss-history small-cap tail-control + benchmark                                     |
| output json    | reports/event_signal/financial_distress_phase22_tail_control_overlay/...125735.json |
| runtime impact | none                                                                                |
+----------------+-------------------------------------------------------------------------------------+
```

## Tail-Control Ranking

```text
+-------------------------------------------+-----------+-------+--------+--------+---------+---------+------+----------------------------+
| rule                                      | mode      | pos   | avg    | median | ex_best | worst   | drop | decision                   |
+-------------------------------------------+-----------+-------+--------+--------+---------+---------+------+----------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | sev_bal   | 14/22 | 0.178% | 0.021% | 0.056%  | -1.966% | 127  | HIGH_AVG_TAIL_BAD          |
| loss_reports_ge_4_mv_lt_10bn              | sev_bal   | 14/22 | 0.166% | 0.021% | 0.043%  | -1.966% | 128  | HIGH_AVG_TAIL_BAD          |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | fixed_20  | 14/22 | 0.163% | 0.021% | 0.040%  | -2.226% | 134  | HIGH_AVG_TAIL_BAD          |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | fixed_20  | 13/22 | 0.122% | 0.000% | 0.065%  | -0.174% | 16   | CLEAN_BENCHMARK            |
| forecast_loss_reports_ge_4_mv_lt_10bn     | sev_bal   | 14/22 | 0.158% | 0.021% | 0.035%  | -1.966% | 127  | HIGH_AVG_TAIL_BAD          |
| loss_reports_ge_4_mv_lt_10bn              | fixed_20  | 14/22 | 0.158% | 0.021% | 0.034%  | -2.226% | 136  | HIGH_AVG_TAIL_BAD          |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | fixed_5   | 15/22 | 0.120% | 0.000% | 0.046%  | -0.935% | 43   | IMPROVED_BUT_TAIL_NOT_SAFE |
| forecast_loss_reports_ge_4_mv_lt_10bn     | fixed_20  | 14/22 | 0.150% | 0.021% | 0.026%  | -2.226% | 135  | HIGH_AVG_TAIL_BAD          |
| forecast_loss_reports_ge_4_mv_lt_10bn     | fixed_5   | 14/22 | 0.115% | 0.000% | 0.041%  | -0.935% | 45   | IMPROVED_BUT_TAIL_NOT_SAFE |
| loss_reports_ge_4_mv_lt_10bn              | fixed_5   | 14/22 | 0.115% | 0.000% | 0.041%  | -0.935% | 45   | IMPROVED_BUT_TAIL_NOT_SAFE |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | ctx_light | 17/22 | 0.111% | 0.000% | 0.041%  | -0.935% | 35   | IMPROVED_BUT_TAIL_NOT_SAFE |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | fixed_10  | 13/22 | 0.117% | 0.000% | 0.042%  | -1.339% | 71   | REJECT_OR_WATCH            |
| loss_reports_ge_4_mv_lt_10bn              | ctx_light | 16/22 | 0.106% | 0.000% | 0.036%  | -0.935% | 37   | IMPROVED_BUT_TAIL_NOT_SAFE |
| forecast_loss_reports_ge_4_mv_lt_10bn     | fixed_10  | 13/22 | 0.112% | 0.000% | 0.037%  | -1.339% | 73   | REJECT_OR_WATCH            |
| loss_reports_ge_4_mv_lt_10bn              | fixed_10  | 13/22 | 0.112% | 0.000% | 0.037%  | -1.339% | 73   | REJECT_OR_WATCH            |
| forecast_loss_reports_ge_4_mv_lt_10bn     | ctx_light | 16/22 | 0.081% | 0.000% | 0.009%  | -0.935% | 34   | REJECT_OR_WATCH            |
+-------------------------------------------+-----------+-------+--------+--------+---------+---------+------+----------------------------+
```

## Selected Config Comparison

```text
+-------------------------------------------+-----------+-------+--------+---------+---------+------+------+
| rule                                      | mode      | pos   | avg    | ex_best | worst   | drop | repl |
+-------------------------------------------+-----------+-------+--------+---------+---------+------+------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | sev_bal   | 14/22 | 0.178% | 0.056%  | -1.966% | 127  | 111  |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | fixed_5   | 15/22 | 0.120% | 0.046%  | -0.935% | 43   | 35   |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | ctx_light | 17/22 | 0.111% | 0.041%  | -0.935% | 35   | 22   |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | fixed_20  | 13/22 | 0.122% | 0.065%  | -0.174% | 16   | 10   |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | ctx_light | 13/22 | 0.036% | 0.001%  | -0.056% | 4    | 3    |
+-------------------------------------------+-----------+-------+--------+---------+---------+------+------+
```

## Tail Loop Detail

`pos/zero/neg` separates materially positive, unchanged, and negative QE loops with a small epsilon.

```text
+-------------------------------------------+-----------+--------------+---------+-------------------------------+--------+-------------------------------+
| rule                                      | mode      | pos/zero/neg | worst   | worst_loop                    | best   | best_loop                     |
+-------------------------------------------+-----------+--------------+---------+-------------------------------+--------+-------------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | sev_bal   | 12/5/5       | -1.966% | qe_20260428_001749_c5b2:Loop4 | 2.749% | qe_20260430_010121_d55f:Loop2 |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | fixed_5   | 9/9/4        | -0.935% | qe_20260428_001749_c5b2:Loop1 | 1.672% | qe_20260430_010121_d55f:Loop2 |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | ctx_light | 9/12/1       | -0.935% | qe_20260428_001749_c5b2:Loop1 | 1.597% | qe_20260430_010121_d55f:Loop2 |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | fixed_20  | 4/15/3       | -0.174% | qe_20260428_001749_c5b2:Loop4 | 1.318% | qe_20260429_015755_c4ba:Loop4 |
+-------------------------------------------+-----------+--------------+---------+-------------------------------+--------+-------------------------------+
```

## Best Row Per Rule

```text
+-------------------------------------------+----------+-------+--------+---------+---------+------+-------------------+
| rule                                      | mode     | pos   | avg    | ex_best | worst   | drop | decision          |
+-------------------------------------------+----------+-------+--------+---------+---------+------+-------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | sev_bal  | 14/22 | 0.178% | 0.056%  | -1.966% | 127  | HIGH_AVG_TAIL_BAD |
| loss_reports_ge_4_mv_lt_10bn              | sev_bal  | 14/22 | 0.166% | 0.043%  | -1.966% | 128  | HIGH_AVG_TAIL_BAD |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | fixed_20 | 13/22 | 0.122% | 0.065%  | -0.174% | 16   | CLEAN_BENCHMARK   |
| forecast_loss_reports_ge_4_mv_lt_10bn     | sev_bal  | 14/22 | 0.158% | 0.035%  | -1.966% | 127  | HIGH_AVG_TAIL_BAD |
+-------------------------------------------+----------+-------+--------+---------+---------+------+-------------------+
```

## Interpretation

```text
+---------------------------------------+-------------------------------------------------------------------+-----------------------------------------+
| finding                               | evidence                                                          | implication                             |
+---------------------------------------+-------------------------------------------------------------------+-----------------------------------------+
| tail was reduced by softer modes      | loss-history ex>=50 fixed_5 worst -0.935% vs severity_bal -1.966% | tail improved but still not risk-safe   |
| average return fell with tail control | loss-history ex>=50 fixed_5 avg 0.120% vs severity_bal 0.178%     | expected tradeoff; no free lunch        |
| ctx_light is broader positive count   | 17/22 positive, avg 0.111%, worst -0.935%                         | candidate for watchlist only            |
| clean benchmark remains best tail row | loss/mv<10bn fixed_20 avg 0.122%, worst -0.174%                   | benchmark or alpha-watch, not hard risk |
| no WSL gate passed                    | no loss-history row combines avg >=0.15% and worst >=-0.5%        | do not run WSL true QE yet              |
+---------------------------------------+-------------------------------------------------------------------+-----------------------------------------+
```

## Decision

- No Phase 22 loss-history configuration is promoted to WSL true QE rerun, runtime integration, DB policy, hard buy-ban, or forced-sell logic.
- Softer `fixed_5` and `ctx_light` profiles materially reduce the loss-history tail from about `-1.966%` to `-0.935%`, but the tail remains too large for risk-first use.
- `loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20` remains the clean-tail benchmark row with `avg +0.122%`, `ex_best +0.065%`, and `worst -0.174%`, but its direct event evidence from Phase 20 does not prove a risk rule.
- The next research step should shift from more loss-history tuning to a benchmark true-rerun smoke candidate or alternative signal families with cleaner direct-event economics.
