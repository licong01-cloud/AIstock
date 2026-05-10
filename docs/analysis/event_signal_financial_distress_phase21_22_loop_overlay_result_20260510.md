# Phase 21 Shortlist Cheap 22-Loop Overlay Expansion - 2026-05-10

Research-only expansion of Phase 20 shortlisted financial-distress candidates to the same 22-loop cheap overlay set. This phase does not modify QE runtime, Selection Center, Paper Trading, QMT, DB schema, or live-trading code.

## Scope

```text
+----------------+--------------------------------------------------------------------------------+
| item           | value                                                                          |
+----------------+--------------------------------------------------------------------------------+
| branch         | codex/financial-distress-rerank-20260508                                       |
| phase          | 21                                                                             |
| loop set       | 22 QE loops from 8 experiments                                                 |
| date range     | 2024-07-01 -> 2026-04-27                                                       |
| validations    | 1320                                                                           |
| stability rows | 60                                                                             |
| output json    | reports/event_signal/financial_distress_phase21_22_loop_overlay/...024133.json |
| runtime impact | none                                                                           |
+----------------+--------------------------------------------------------------------------------+
```

## Candidate Result Ranking

```text
+-------------------------------------------+-----+--------------+-------+--------+--------+---------+---------+------+------+
| rule                                      | td  | mode         | pos   | avg    | median | ex_best | worst   | drop | repl |
+-------------------------------------------+-----+--------------+-------+--------+--------+---------+---------+------+------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | severity_bal | 14/22 | 0.178% | 0.021% | 0.056%  | -1.966% | 127  | 111  |
| loss_reports_ge_4_mv_lt_10bn              | 242 | severity_bal | 14/22 | 0.166% | 0.021% | 0.043%  | -1.966% | 128  | 112  |
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | fixed_20     | 14/22 | 0.163% | 0.021% | 0.040%  | -2.226% | 134  | 116  |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | severity_bal | 14/22 | 0.158% | 0.021% | 0.035%  | -1.966% | 127  | 111  |
| loss_reports_ge_4_mv_lt_10bn              | 242 | fixed_20     | 14/22 | 0.158% | 0.021% | 0.034%  | -2.226% | 136  | 118  |
| forecast_loss_reports_ge_4_mv_lt_10bn     | 242 | fixed_20     | 14/22 | 0.150% | 0.021% | 0.026%  | -2.226% | 135  | 117  |
| forecast_loss_to_market_cap_ge_50pct      | 242 | fixed_20     | 13/22 | 0.122% | 0.000% | 0.065%  | -0.174% | 16   | 10   |
| loss_to_market_cap_ge_50pct               | 242 | fixed_20     | 13/22 | 0.122% | 0.000% | 0.065%  | -0.174% | 16   | 10   |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 242 | fixed_20     | 13/22 | 0.122% | 0.000% | 0.065%  | -0.174% | 16   | 10   |
| forecast_loss_to_market_cap_ge_50pct      | 120 | fixed_20     | 13/22 | 0.122% | 0.000% | 0.078%  | -0.174% | 11   | 6    |
| loss_to_market_cap_ge_50pct               | 120 | fixed_20     | 13/22 | 0.117% | 0.000% | 0.073%  | -0.174% | 13   | 8    |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 120 | fixed_20     | 13/22 | 0.117% | 0.000% | 0.073%  | -0.174% | 13   | 8    |
| structured_financial_risk_mv_ge_10bn      | 60  | severity_bal | 13/22 | 0.110% | 0.000% | 0.025%  | -0.879% | 35   | 22   |
| forecast_loss_to_market_cap_ge_50pct      | 60  | fixed_20     | 12/22 | 0.083% | 0.000% | 0.039%  | -0.174% | 7    | 4    |
| loss_to_market_cap_ge_50pct               | 60  | fixed_20     | 12/22 | 0.083% | 0.000% | 0.039%  | -0.174% | 7    | 4    |
| loss_to_market_cap_ge_50pct_mv_lt_10bn    | 60  | fixed_20     | 12/22 | 0.083% | 0.000% | 0.039%  | -0.174% | 7    | 4    |
+-------------------------------------------+-----+--------------+-------+--------+--------+---------+---------+------+------+
```

## Best Row Per Rule

```text
+------------------------------------------------------+-----+--------------+-------+--------+---------+---------+------+-----------------------+
| rule                                                 | td  | mode         | pos   | avg    | ex_best | worst   | drop | decision              |
+------------------------------------------------------+-----+--------------+-------+--------+---------+---------+------+-----------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss            | 242 | severity_bal | 14/22 | 0.178% | 0.056%  | -1.966% | 127  | TAIL_CONTROL_NEXT     |
| loss_reports_ge_4_mv_lt_10bn                         | 242 | severity_bal | 14/22 | 0.166% | 0.043%  | -1.966% | 128  | TAIL_CONTROL_NEXT     |
| forecast_loss_reports_ge_4_mv_lt_10bn                | 242 | severity_bal | 14/22 | 0.158% | 0.035%  | -1.966% | 127  | TAIL_CONTROL_NEXT     |
| forecast_loss_to_market_cap_ge_50pct                 | 242 | fixed_20     | 13/22 | 0.122% | 0.065%  | -0.174% | 16   | BENCHMARK_ALPHA_WATCH |
| loss_to_market_cap_ge_50pct                          | 242 | fixed_20     | 13/22 | 0.122% | 0.065%  | -0.174% | 16   | BENCHMARK_ALPHA_WATCH |
| loss_to_market_cap_ge_50pct_mv_lt_10bn               | 242 | fixed_20     | 13/22 | 0.122% | 0.065%  | -0.174% | 16   | BENCHMARK_ONLY        |
| structured_financial_risk_mv_ge_10bn                 | 60  | severity_bal | 13/22 | 0.110% | 0.025%  | -0.879% | 35   | NOT_STRONG_ENOUGH     |
| loss_to_market_cap_ge_50pct_mv_lt_5bn                | 60  | fixed_20     | 12/22 | 0.083% | 0.039%  | -0.174% | 7    | BENCHMARK_ALPHA_WATCH |
| structured_financial_risk_mv_10_30bn                 | 60  | severity_bal | 15/22 | 0.078% | -0.008% | -0.879% | 25   | NOT_STRONG_ENOUGH     |
| structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | 60  | severity_bal | 13/22 | 0.025% | 0.009%  | -0.293% | 14   | NOT_STRONG_ENOUGH     |
+------------------------------------------------------+-----+--------------+-------+--------+---------+---------+------+-----------------------+
```

## Tail And Breadth Check

`pos/zero/neg` separates materially positive, unchanged, and negative QE loops with an absolute epsilon. This matters because many overlay gains are caused by a small number of actual top50 replacement events.

```text
+-------------------------------------------+-----+--------------+--------------+---------+-------------------------------+--------+-------------------------------+
| rule                                      | td  | mode         | pos/zero/neg | worst   | worst_loop                    | best   | best_loop                     |
+-------------------------------------------+-----+--------------+--------------+---------+-------------------------------+--------+-------------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | severity_bal | 12/5/5       | -1.966% | qe_20260428_001749_c5b2:Loop4 | 2.749% | qe_20260430_010121_d55f:Loop2 |
| loss_to_market_cap_ge_50pct               | 242 | fixed_20     | 4/15/3       | -0.174% | qe_20260428_001749_c5b2:Loop4 | 1.318% | qe_20260429_015755_c4ba:Loop4 |
| structured_financial_risk_mv_ge_10bn      | 60  | severity_bal | 7/10/5       | -0.879% | qe_20260428_001749_c5b2:Loop2 | 1.877% | qe_20260507_132049_d4e7:Loop2 |
+-------------------------------------------+-----+--------------+--------------+---------+-------------------------------+--------+-------------------------------+
```

## Market-Cap Exposure Check

```text
+-------------------------------------------+-----+--------------+----------------------------------------------------+----------------------------------+
| rule                                      | td  | mode         | evaluated_buckets                                  | dropped_buckets                  |
+-------------------------------------------+-----+--------------+----------------------------------------------------+----------------------------------+
| loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss | 242 | severity_bal | 5-10bn:494; <5bn:3510; small_mix:177               | 5-10bn:13; <5bn:113; small_mix:1 |
| loss_to_market_cap_ge_50pct               | 242 | fixed_20     | 10-30bn:3; 5-10bn:123; <5bn:367; small_mix:1       | 5-10bn:3; <5bn:13                |
| structured_financial_risk_mv_ge_10bn      | 60  | severity_bal | 10-30bn:1365; 30-100bn:261; >=100bn:10; mid_mix:11 | 10-30bn:25; 30-100bn:10          |
+-------------------------------------------+-----+--------------+----------------------------------------------------+----------------------------------+
```

## Interpretation

```text
+---------------------------------------+----------------------------------------------------------------------------+---------------------------------------------+
| finding                               | interpretation                                                             | next action                                 |
+---------------------------------------+----------------------------------------------------------------------------+---------------------------------------------+
| loss-history small-cap row improved   | Best average and positive median, but tail is too large for risk-first use | Run tail-control sweep before any WSL rerun |
| structured ge10 row weakened          | 10-loop avg 0.267% fell to 22-loop 0.110% and worst -0.879%                | Do not true-rerun now                       |
| loss/mv ge50 benchmark is clean-tail  | Avg 0.122%, ex-best 0.065%, worst -0.174%, but only 16 drops               | Keep benchmark/alpha-watch, not risk rule   |
| current WSL-tested indicator baseline | Phase 19 true rerun was weak positive and immaterial                       | Keep as weak calibration baseline           |
| direct true rerun gate                | No candidate is both strong and tail-safe enough for broad WSL batch       | Continue cheap research first               |
+---------------------------------------+----------------------------------------------------------------------------+---------------------------------------------+
```

## Decision

- No Phase 21 candidate is promoted into runtime, DB policy, hard buy-ban, or forced-sell logic.
- `loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss / 242td / severity_balanced` is the new strongest cheap overlay by average return, but its worst-loop loss is too large for a risk-first signal.
- `structured_financial_risk_mv_ge_10bn` does not survive the 22-loop expansion strongly enough; it should not enter WSL true QE rerun now.
- `loss_to_market_cap_ge_50pct / 242td / fixed_20` is the cleanest-tail benchmark row, but it has sparse real top50 replacement impact and direct event evidence does not prove risk.
- Phase 22 should run a tail-control parameter sweep, focused on the loss-history small-cap family and benchmark comparison, before deciding any one-loop WSL full-universe true rerun.
