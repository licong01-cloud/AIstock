# Phase 18 Copied-Loop True QE Smoke Rerun Result - 2026-05-09

Research-only result for the first copied-loop true QE smoke reruns using the financial-distress score-down materializer. This phase validates whether `qrun_limit_minute.py --pred-backtest` can execute with a materialized `pred.pkl` in copied workspaces. It does not modify QE runtime, Selection Center, Paper Trading, QMT, live trading, or database schema/data.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 18                                                                  |
| primary loop     | qe_20260507_132049_d4e7 / Loop2                                     |
| extra probe      | qe_20260430_010121_d55f / Loop1                                     |
| candidate        | indicator_large_decline_mv_10_30bn                                  |
| profile          | rank_decay_balanced, 60td, previous prediction date                 |
| runtime impact   | none                                                                |
| DB impact        | none                                                                |
+------------------+---------------------------------------------------------------------+
```

## Smoke Attempts

```text
+----------------------------+-------------------------+-------------------------------+-------------------------------+
| loop                       | mode                    | result                        | interpretation                |
+----------------------------+-------------------------+-------------------------------+-------------------------------+
| qe20260507 Loop2           | full copied universe    | failed with MemoryError       | full true rerun not feasible  |
| qe20260507 Loop2           | narrowed quote universe | completed PortAnaRecord       | technical smoke only          |
| qe20260430 Loop1           | full copied universe    | failed with MemoryError       | memory limit reproduced       |
| qe20260430 Loop1           | narrowed quote universe | completed with many no-price  | invalid for PnL comparison    |
|                            |                         | warnings                      |                               |
+----------------------------+-------------------------+-------------------------------+-------------------------------+
```

## Materializer Trace Summary

```text
+--------------------------+-------------------+-------------------+
| metric                   | qe20260507 Loop2  | qe20260430 Loop1  |
+--------------------------+-------------------+-------------------+
| prediction_rows          | 2256997           | 2045269           |
| prediction_dates         | 443               | 443               |
| rank_dates_touched       | 193               | 129               |
| rank_dates_with_penalties| 193               | 129               |
| penalty_rows             | 311               | 222               |
| penalized_symbol_count   | 37                | 97                |
| changed_symbol_count     | 581               | 695               |
| topk_drop_count          | 24                | 12                |
| tie_score_dates          | 100               | 40                |
+--------------------------+-------------------+-------------------+
```

## Completed Smoke Metrics

These metrics prove that the copied `pred-backtest` path can run. They must not be used as alpha or risk-control evidence because the completed runs used narrowed quote universes to avoid memory blowups. The original baselines were full-universe runs, so baseline-vs-adjusted PnL is not apples-to-apples.

```text
+-----------------------------+-------------------+-------------------+-------------------------------+
| metric                      | qe20260507 Loop2  | qe20260430 Loop1  | reliability                   |
+-----------------------------+-------------------+-------------------+-------------------------------+
| recorder                    | b78e832b...       | ee9ef939...       | copied-workspace smoke        |
| IC                          | 0.0613167250      | 0.0505485323      | useful artifact sanity check  |
| Rank IC                     | 0.0999631593      | 0.0559025628      | useful artifact sanity check  |
| annualized return with cost | 0.2472135929      | -0.1873636613     | not comparable to baseline    |
| information ratio with cost | 1.4339163401      | -1.0471843579     | not comparable to baseline    |
| max drawdown with cost      | -0.1414032303     | -0.4579331994     | not comparable to baseline    |
+-----------------------------+-------------------+-------------------+-------------------------------+
```

## Interpretation

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| item                                 | decision                   | evidence                                                     |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| copied pred-backtest technical path   | PASS                       | SigAnaRecord + PortAnaRecord can complete in copied workspace|
| full-universe true rerun              | FAIL_CURRENT_MACHINE       | MemoryError reproduced on copied full-universe attempts      |
| narrowed-universe PnL comparison      | REJECT_AS_EVIDENCE         | quote universe mismatch causes non-comparable portfolio PnL  |
| runtime or DB policy promotion        | REJECT                     | Phase 16 robustness remains weak and Phase 18 is smoke only  |
| next empirical step                   | FEASIBILITY_REDESIGN       | need native memory-safe research rerun before 22-loop batch  |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Practical Conclusion

The materializer is useful and the `--pred-backtest` entry point is real, but current copied-loop true rerun is not yet a valid promotion gate. A full quote universe exceeds memory on the current Windows Qlib path. A narrowed quote universe lets the run finish but changes execution conditions and can trigger missing-price behavior, so it cannot validate annualized return or drawdown impact.

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                     |
+------------------------------+---------------------------------------------------------------+
| memory pressure              | design a memory-safe copied runner or lazy quote path first   |
| quote universe mismatch      | require baseline parity in the same narrowed/controlled setup |
| score-weighted sizing drift  | keep rank trace and avoid runtime promotion                   |
| one-loop smoke dependence    | do not expand batch until full-universe parity is solved      |
+------------------------------+---------------------------------------------------------------+
```
