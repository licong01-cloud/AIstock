# Phase 19 WSL Full-Universe True QE Rerun Result - 2026-05-10

Research-only full-universe rerun for the financial-distress score-down materializer. This phase tests whether the Windows copied-loop `MemoryError` can be avoided by running the same full-universe QE loop under WSL/Linux, then compares baseline and adjusted predictions in the same environment.

No QE runtime, Selection Center, Paper Trading, QMT, live trading, database schema, or production backend `8001` path was modified.

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | 19                                                                  |
| source loop      | qe_20260507_132049_d4e7 / Loop2                                     |
| environment      | WSL / rdagent-gpu / Linux Qlib provider paths                       |
| candidate        | indicator_large_decline_mv_10_30bn                                  |
| profile          | rank_decay_balanced, 60td, previous prediction date                 |
| adjusted recorder| 59eaf3f33f864ade97b79ce561a13f2a                                    |
| baseline recorder| 7b57828280ad40b988e6574c9a083da6                                    |
| runtime impact   | none                                                                |
| DB impact        | none                                                                |
+------------------+---------------------------------------------------------------------+
```

## Commands

```bash
cd /mnt/f/Dev/RD-Agent-main/qe_workspace/qe_20260507_132049_d4e7/Loop2

python qrun_limit_minute.py conf.yaml \
  --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/mlruns/455819547877124274/b78e832bfd634afbbc770bcafe2e33ca/artifacts/pred.pkl

python qrun_limit_minute.py conf.yaml \
  --pred-backtest /mnt/f/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/mlruns/455819547877124274/26db7c74fd024a82b803866c235ec519/artifacts/pred.pkl
```

Command logs are stored outside the repo:

```text
+------------------+----------------------------------------------------------------------------------------------------------------+
| run              | log path                                                                                                       |
+------------------+----------------------------------------------------------------------------------------------------------------+
| adjusted          | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/wsl_full_universe_adjusted.log  |
| baseline          | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260509_qe20260507_loop2/wsl_full_universe_baseline.log  |
+------------------+----------------------------------------------------------------------------------------------------------------+
```

## Feasibility Result

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| item                                 | result                     | evidence                                                     |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| Windows copied full-universe path     | FAIL_CURRENT_MACHINE       | Phase 18 reproduced MemoryError during exchange creation     |
| WSL full-universe adjusted rerun      | PASS                       | 442 backtest steps completed with PortAnaRecord              |
| WSL full-universe baseline rerun      | PASS                       | 442 backtest steps completed with PortAnaRecord              |
| WSL total runtime                     | heavy but usable           | adjusted about 52m, baseline about 45m including data setup  |
| same-environment comparability        | PASS                       | both runs used the same full-universe conf.yaml              |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Full-Universe Metrics

```text
+--------------------------------+----------------+----------------+------------------+--------------------+
| metric                         | baseline       | adjusted       | delta            | interpretation     |
+--------------------------------+----------------+----------------+------------------+--------------------+
| IC                             | 0.0613135033   | 0.0613167250   | +0.0000032217    | immaterial         |
| ICIR                           | 0.6196672847   | 0.6197075815   | +0.0000402969    | immaterial         |
| Rank IC                        | 0.0999630204   | 0.0999631593   | +0.0000001390    | immaterial         |
| Rank ICIR                      | 0.8744420390   | 0.8744433485   | +0.0000013094    | immaterial         |
| annualized excess return cost  | 0.4763182376   | 0.4777874746   | +0.0014692370    | weak positive      |
| information ratio cost         | 2.2159536500   | 2.2222781396   | +0.0063244896    | weak positive      |
| max drawdown cost              | -0.1754268423  | -0.1751511308  | +0.0002757115    | weak positive      |
+--------------------------------+----------------+----------------+------------------+--------------------+
```

## Narrowed-Control Comparison

The earlier narrowed Windows run is now interpretable only as a controlled same-universe smoke, not as a production/full-universe proxy.

```text
+--------------------------------+----------------+----------------+------------------+--------------------+
| metric                         | baseline       | adjusted       | delta            | interpretation     |
+--------------------------------+----------------+----------------+------------------+--------------------+
| annualized excess return cost  | 0.2472124718   | 0.2472135929   | +0.0000011211    | effectively zero   |
| information ratio cost         | 1.4339060565   | 1.4339163401   | +0.0000102835    | effectively zero   |
| max drawdown cost              | -0.1414019109  | -0.1414032303  | -0.0000013195    | effectively zero   |
+--------------------------------+----------------+----------------+------------------+--------------------+
```

## Interpretation

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| item                                 | decision                   | rationale                                                    |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| memory-safe full-universe rerun       | USE_WSL_FOR_RESEARCH       | WSL completed the same full universe that Windows could not  |
| current candidate signal              | KEEP_RESEARCH_ONLY         | one-loop true rerun is positive but too small to promote     |
| immediate runtime integration         | REJECT                     | no hard rule, no score-down policy, no DB persistence yet    |
| blind 22-loop full true rerun         | DEFER                      | about 1.5h per loop pair is too costly without stronger gate |
| next research direction               | SELECTIVE_TRUE_RERUN       | screen cheaply first, true-rerun only top candidates         |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Practical Conclusion

The WSL/Linux path solves the full-universe feasibility blocker. The current `indicator_large_decline_mv_10_30bn` score-down candidate has a directionally positive but very small one-loop effect: about +0.147 percentage points annualized excess return, +0.0063 IR, and +0.0276 percentage points max-drawdown relief. This is not enough for runtime or DB policy promotion.

Next research should keep the same two-stage gate:

```text
+------+-------------------------------------------+--------------------------------------------------------------+
| step | gate                                      | purpose                                                      |
+------+-------------------------------------------+--------------------------------------------------------------+
| 1    | cheap overlay / event study screen         | find stronger financial-risk candidates before expensive QE  |
| 2    | WSL full-universe true rerun               | validate only shortlisted candidates with real PortAnaRecord |
| 3    | multi-loop expansion                       | run only if one-loop full-universe effect is material        |
| 4    | runtime policy design                      | start only after robust multi-loop evidence exists           |
+------+-------------------------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                     |
+------------------------------+---------------------------------------------------------------+
| one-loop dependence          | do not promote until multiple loops confirm material benefit  |
| high runtime cost            | reserve WSL full reruns for shortlisted candidate rules       |
| copied workspace drift       | record exact source loop, recorder IDs, and external logs     |
| current signal weak effect   | continue researching stronger structured financial signals    |
+------------------------------+---------------------------------------------------------------+
```
