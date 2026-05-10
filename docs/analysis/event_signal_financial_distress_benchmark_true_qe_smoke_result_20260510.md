# Phase 23 Benchmark True QE Smoke Result - 2026-05-10

Research-only WSL full-universe true QE smoke for `loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 / 242td`. This validates the clean-tail benchmark candidate through the real `qrun_limit_minute.py --pred-backtest` portfolio path. No QE runtime, Selection Center, Paper Trading, QMT, live trading, database schema, database data, or production backend `8001` path was modified.

## Scope

```text
+-------------------+----------------------------------------------------+
| item              | value                                              |
+-------------------+----------------------------------------------------+
| branch            | codex/financial-distress-rerank-20260508           |
| phase             | 23                                                 |
| source loop       | qe_20260507_132049_d4e7 / Loop2                    |
| candidate         | loss_to_market_cap_ge_50pct_mv_lt_10bn             |
| profile           | fixed_20, 242td, previous prediction date, TopK=50 |
| baseline recorder | 7b57828280ad40b988e6574c9a083da6                   |
| adjusted recorder | 34ecffc282ac4b44869dcd1261a55301                   |
| runtime impact    | none                                               |
| DB impact         | none                                               |
+-------------------+----------------------------------------------------+
```

## Materialization Summary

```text
+------------------------+-----------+
| metric                 | value     |
+------------------------+-----------+
| overlay rows           | 17,138    |
| overlay symbols        | 80        |
| penalized symbols      | 18        |
| rank-date penalty rows | 302       |
| rank dates touched     | 193       |
| prediction rows        | 2,256,997 |
| changed symbols        | 760       |
| top-k drop count       | 61        |
+------------------------+-----------+
```

## Artifacts

```text
+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------+
| item                | path                                                                                                                                           |
+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------+
| overlay/materialize | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/materialized_fixed20/...                       |
| adjusted log        | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/wsl_full_universe_loss_mv_fixed20_adjusted.log |
| metrics snapshot    | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/metrics_snapshot/                              |
+---------------------+------------------------------------------------------------------------------------------------------------------------------------------------+
```

## WSL Command Shape

```bash
cd /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2
python qrun_limit_minute.py conf.yaml --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/materialized_fixed20/loss_to_market_cap_ge_50pct_mv_lt_10bn_fixed20_adjusted_pred.pkl
```

## Full-Universe Metrics

```text
+-------------------------------+---------------+---------------+---------------+--------------------+
| metric                        | baseline      | adjusted      | delta         | interpretation     |
+-------------------------------+---------------+---------------+---------------+--------------------+
| IC                            | 0.0613135033  | 0.0613204022  | +0.0000068989 | immaterial         |
| ICIR                          | 0.6196672847  | 0.6197517937  | +0.0000845090 | immaterial         |
| Rank IC                       | 0.0999630204  | 0.0999633206  | +0.0000003003 | immaterial         |
| Rank ICIR                     | 0.8744420390  | 0.8744452202  | +0.0000031811 | immaterial         |
| annualized excess return cost | 0.4763182376  | 0.4766762918  | +0.0003580542 | very weak positive |
| information ratio cost        | 2.2159536500  | 2.2177053134  | +0.0017516633 | very weak positive |
| max drawdown cost             | -0.1754268423 | -0.1754042650 | +0.0000225773 | effectively zero   |
| daily excess mean cost        | 0.0020013371  | 0.0020028416  | +0.0000015044 | very weak positive |
+-------------------------------+---------------+---------------+---------------+--------------------+
```

## Comparison With Previous True Smoke

```text
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
| candidate                                               | ann excess delta | IR delta      | MDD delta     | decision                                     |
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
| indicator_large_decline_mv_10_30bn / ctx-balanced 60td  | +0.0014692370    | +0.0063244896 | +0.0002757115 | stronger one-loop smoke, still research-only |
| loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 242td | +0.0003580542    | +0.0017516633 | +0.0000225773 | weaker smoke; benchmark only                 |
+---------------------------------------------------------+------------------+---------------+---------------+----------------------------------------------+
```

## Interpretation

```text
+------------------------------------------+---------------------------------------------------------+------------------------------+
| finding                                  | evidence                                                | decision                     |
+------------------------------------------+---------------------------------------------------------+------------------------------+
| WSL true smoke completed                 | 442/442 backtest steps and PortAnaRecord completed      | path usable                  |
| cheap overlay overstated one-loop effect | cheap final_delta +0.80% vs true ann excess +0.036pp    | use true QE for final gate   |
| impact is positive but tiny              | ann excess +0.000358, IR +0.001752, MDD +0.000023       | not material                 |
| candidate remains clean-tail benchmark   | 22-loop cheap worst -0.174%, true smoke non-negative    | keep as benchmark/watch only |
| runtime integration not justified        | one loop, sparse 18 penalized symbols, small true delta | no DB policy/runtime hook    |
+------------------------------------------+---------------------------------------------------------+------------------------------+
```

## Decision

- `loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 / 242td` passes the technical WSL true-QE smoke but does not pass a promotion gate.
- The effect is directionally positive but too small: annualized excess return improves only about `+0.036` percentage points, IR improves `+0.00175`, and max-drawdown relief is effectively zero.
- Keep this row as a clean-tail benchmark and calibration target, not as a runtime risk rule, buy-ban, force-sell rule, DB policy, or paper/live overlay.
- Next research should look for signal families with stronger direct event economics or broader material true-QE impact before spending WSL rerun budget.

## Next Step

```text
+------+---------------------------------------------------------------------------------------+-----------------------------------+
| step | action                                                                                | gate                              |
+------+---------------------------------------------------------------------------------------+-----------------------------------+
| 1    | Do not promote this candidate; keep as benchmark row                                  | completed                         |
| 2    | Screen cleaner signal families or richer financial rules cheaply first                | avg/tail plus direct event sanity |
| 3    | Run WSL true QE only for candidates with materially stronger cheap evidence           | avoid broad expensive reruns      |
| 4    | If another candidate passes, compare against both Phase19 and Phase23 one-loop smokes | relative materiality              |
+------+---------------------------------------------------------------------------------------+-----------------------------------+
```

## Residual Risks

```text
+--------------------------------------------+----------------------------------------------------------------------+
| risk                                       | mitigation                                                           |
+--------------------------------------------+----------------------------------------------------------------------+
| one-loop dependence                        | do not promote; require multi-loop true evidence before runtime work |
| materialization approximates rank demotion | use only for research pred-backtest, not production strategy logic   |
| cheap overlay mismatch                     | treat cheap overlay as shortlist gate only, not final PnL evidence   |
| sparse hit count                           | report penalized symbols and top-k drops before interpreting effect  |
+--------------------------------------------+----------------------------------------------------------------------+
```
