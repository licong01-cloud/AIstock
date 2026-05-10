# L2 Validation - Financial Distress Benchmark True QE Smoke - 2026-05-10

## Scope

```text
+-------------------------+-----------------------------------------------------------+
| item                    | value                                                     |
+-------------------------+-----------------------------------------------------------+
| module                  | event_signal / financial_distress research                |
| branch                  | codex/financial-distress-rerank-20260508                  |
| candidate               | loss_to_market_cap_ge_50pct_mv_lt_10bn / fixed_20 / 242td |
| source loop             | qe_20260507_132049_d4e7 / Loop2                           |
| production 8001 touched | no                                                        |
| runtime code touched    | no                                                        |
+-------------------------+-----------------------------------------------------------+
```

## Commands Executed

```text
+--------------------------------------------------------+---------------------------------------------------------+
| command                                                | result                                                  |
+--------------------------------------------------------+---------------------------------------------------------+
| generate overlay CSV with existing research module     | PASS: 1 validation, overlay CSV written outside repo    |
| materialize adjusted pred.pkl for fixed_20             | PASS: 2,256,997 prediction rows, 193 rank dates touched |
| WSL qrun_limit_minute.py --pred-backtest adjusted pred | PASS: 442/442 backtest loop, PortAnaRecord completed    |
| metrics extraction from MLflow recorder                | PASS: recorder 34ecffc282ac4b44869dcd1261a55301         |
+--------------------------------------------------------+---------------------------------------------------------+
```

## Business Oracle

```text
+----------------------------------------------------------+--------+
| oracle                                                   | result |
+----------------------------------------------------------+--------+
| no production runtime integration                        | PASS   |
| no Paper Trading / Selection Center / QMT change         | PASS   |
| no DB schema or DB data write                            | PASS   |
| same WSL full-universe source loop as baseline           | PASS   |
| baseline and adjusted metrics compared from MLflow files | PASS   |
+----------------------------------------------------------+--------+
```

## Metrics Evidence

```text
+-------------------------------+---------------+---------------+---------------+
| metric                        | baseline      | adjusted      | delta         |
+-------------------------------+---------------+---------------+---------------+
| IC                            | 0.0613135033  | 0.0613204022  | +0.0000068989 |
| ICIR                          | 0.6196672847  | 0.6197517937  | +0.0000845090 |
| Rank IC                       | 0.0999630204  | 0.0999633206  | +0.0000003003 |
| Rank ICIR                     | 0.8744420390  | 0.8744452202  | +0.0000031811 |
| annualized excess return cost | 0.4763182376  | 0.4766762918  | +0.0003580542 |
| information ratio cost        | 2.2159536500  | 2.2177053134  | +0.0017516633 |
| max drawdown cost             | -0.1754268423 | -0.1754042650 | +0.0000225773 |
| daily excess mean cost        | 0.0020013371  | 0.0020028416  | +0.0000015044 |
+-------------------------------+---------------+---------------+---------------+
```

## Artifacts

```text
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| artifact             | path                                                                                                                                                                          |
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
| curated report       | docs\analysis\event_signal_financial_distress_benchmark_true_qe_smoke_result_20260510.md                                                                                      |
| adjusted WSL log     | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/wsl_full_universe_loss_mv_fixed20_adjusted.log                                |
| materialization meta | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/materialized_fixed20/loss_to_market_cap_ge_50pct_mv_lt_10bn_fixed20_meta.json |
| metrics snapshot     | F:/Dev/AIstock_artifacts/event_signal_true_qe_rerun_20260510_loss_mv_benchmark_qe20260507_loop2/metrics_snapshot/                                                             |
+----------------------+-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------+
```

## Decision

- Validation passed technically.
- Candidate remains research-only because the true-QE impact is too small for runtime integration.
- No backend/frontend restart required; production port `8001` was not touched.

## Regression Checks

```text
+------------------------------------------------------------+-------------------------------------+
| command                                                    | result                              |
+------------------------------------------------------------+-------------------------------------+
| py_compile financial_distress_pred_materializer/qe_overlay | PASS                                |
| pytest pred_materializer + qe_overlay tests                | 35 passed                           |
| pytest backend/tests/event_signal                          | 164 passed                          |
| runtime coupling rg scan                                   | PASS: no matches in runtime modules |
| git diff --check                                           | PASS                                |
+------------------------------------------------------------+-------------------------------------+
```
