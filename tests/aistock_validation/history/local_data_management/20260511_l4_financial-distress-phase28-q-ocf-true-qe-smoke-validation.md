# L4 Validation - Financial Distress Phase28 q_ocf True QE Smoke - 2026-05-11

## Scope

```text
+----------------------+----------------------------------------------------------------+
| item                 | value                                                          |
+----------------------+----------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                       |
| module               | backend/services/event_signal                                  |
| phase                | 28                                                             |
| candidate            | indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn               |
| profile              | fixed_15, 90td, previous prediction date, TopK=50              |
| validation level     | L4 research true-QE smoke                                      |
| production backend   | 8001 not touched                                               |
| runtime integration  | none                                                           |
| DB writes            | none                                                           |
+----------------------+----------------------------------------------------------------+
```

## Commands And Results

```text
+------+---------------------------------------------------------------+----------------------------+
| step | command group                                                 | result                     |
+------+---------------------------------------------------------------+----------------------------+
| 1    | py_compile materializer and tests                             | pass                       |
| 2    | pytest materializer                                           | 6 passed                   |
| 3    | build overlay CSV from enriched rows and PHASE25 rule         | pass                       |
| 4    | materialize fixed_15 adjusted pred.pkl                        | pass                       |
| 5    | WSL qrun_limit_minute.py --pred-backtest fixed15_pred.pkl     | pass                       |
| 6    | pytest materializer + qe_overlay research tests               | 43 passed                  |
| 7    | pytest backend/tests/event_signal                             | 172 passed                 |
| 8    | runtime isolation scan across Selection/Paper/QE/QMT paths    | no matches                 |
| 9    | git diff --check                                              | pass, LF/CRLF warnings only|
+------+---------------------------------------------------------------+----------------------------+
```

## Business Evidence

```text
+-------------------------+----------------------------------+
| metric                  | value                            |
+-------------------------+----------------------------------+
| overlay rows            | 41,673                           |
| overlay symbols         | 393                              |
| prediction rows         | 2,256,997                        |
| penalized symbols       | 386                              |
| top-k drop count        | 25                               |
| WSL backtest steps      | 442/442                          |
| baseline recorder       | 7b57828280ad40b988e6574c9a083da6 |
| adjusted recorder       | 8afe567e2bec4dc88a1f3fe15768567b |
| annualized excess delta | +0.0009068854                    |
| information ratio delta | +0.0054904364                    |
| max drawdown delta      | +0.0000115081                    |
+-------------------------+----------------------------------+
```

## Artifact Evidence

```text
+----------------------+------------------------------------------------------------------------------------------------------------------+
| item                 | path                                                                                                             |
+----------------------+------------------------------------------------------------------------------------------------------------------+
| curated report        | docs/analysis/event_signal_financial_distress_phase28_q_ocf_true_qe_smoke_result_20260511.md                    |
| overlay source        | <artifact_root>/overlay_source/                                                                                 |
| materialized pred     | <artifact_root>/materialized_fixed15/                                                                           |
| WSL qrun log          | <artifact_root>/wsl_full_universe_q_ocf_fixed15_adjusted.log                                                    |
| metrics snapshot      | <artifact_root>/metrics_snapshot/                                                                               |
+----------------------+------------------------------------------------------------------------------------------------------------------+
```

## Bugs / Issues Found

```text
+---------------------------+----------------------------------------------+----------------------------------------------+
| issue                     | cause                                        | resolution                                   |
+---------------------------+----------------------------------------------+----------------------------------------------+
| Windows overlay CLI failed| positions pickle required qlib in Windows env | generated overlay directly from event rows   |
| metric parser read zeros  | MLflow metric files are timestamp/value/step | parser corrected to read the second field    |
+---------------------------+----------------------------------------------+----------------------------------------------+
```

## Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| true QE smoke                         | passed                                                       |
| business conclusion                   | positive but modest; research-only                           |
| runtime promotion                     | rejected                                                     |
| DB policy promotion                   | rejected                                                     |
| QE/Paper/Selection/QMT code impact     | none                                                         |
| production backend 8001               | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                    |
+------------------------------+---------------------------------------------------------------+
| one-loop dependence          | require more evidence before runtime or multi-loop WSL batch  |
| cheap-to-true mismatch       | next phase should inspect holding/rank-date hit translation   |
| materialization approximation| research-only pred-backtest artifact; no live strategy wiring |
+------------------------------+---------------------------------------------------------------+
```
