# L2 Validation - Financial Distress Mid/Large-Cap QE Overlay Research - 2026-05-09

## Scope

```text
+--------------------+--------------------------------------------------------------------------+
| item               | value                                                                    |
+--------------------+--------------------------------------------------------------------------+
| branch             | codex/financial-distress-rerank-20260508                                 |
| feature boundary   | backend/services/event_signal + tests/docs only                          |
| runtime boundary   | no QE/Paper/Selection/QMT runtime integration                            |
| production 8001    | not restarted                                                            |
| generated report   | reports/event_signal/financial_distress_mid_large_qe_overlay/...005254   |
+--------------------+--------------------------------------------------------------------------+
```

## Commands

```text
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
| order | command                                                                                                            | result  |
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
| 1     | python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py ...                   | PASS    |
| 2     | python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q                      | PASS 26 |
| 3     | WSL 10-loop mid-large-only overlay, score_down rank20 + severity balanced                                          | PASS    |
| 4     | python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q                   | PASS156 |
| 5     | rg mid-large/expectation rule names in Selection/Paper/QE/QMT runtime paths                                        | PASS    |
| 6     | git diff --check                                                                                                  | PASS    |
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
```

## WSL Overlay Run

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | value                                                                          |
+----------------------+--------------------------------------------------------------------------------+
| date range           | 2024-07-01 -> 2026-04-27                                                       |
| loops                | 10                                                                             |
| validations          | 360                                                                            |
| active trading days  | 60, 120, 242                                                                   |
| modes                | score_down_rank_20pct_top50_previous; score_down_severity_balanced_top50_prev. |
| report json          | financial_distress_qe_multiloop_20240701_20260509_005254.json                 |
| report md            | financial_distress_qe_multiloop_20240701_20260509_005254.md                   |
+----------------------+--------------------------------------------------------------------------------+
```

## Result Summary

```text
+--------------------------------------+-----------------------+------------------------------------------------------------+
| candidate                            | validation decision   | evidence                                                   |
+--------------------------------------+-----------------------+------------------------------------------------------------+
| indicator_large_decline_mv_ge_10bn   | KEEP_RESEARCH_FEATURE | 242td avg_ret_d 0.28% fixed / 0.24% severity               |
| structured_financial_risk_mv_ge_10bn | COVERAGE_BENCHMARK    | broad positive average but worse-loop risk and too blunt    |
| expectation_miss_mv_ge_10bn          | WATCHLIST_RESEARCH    | valid concept but weak Top50 portfolio interaction          |
| expectation_miss_mv_ge_30bn          | REJECT_RUNTIME        | no dropped Top50 events in this 10-loop validation          |
+--------------------------------------+-----------------------+------------------------------------------------------------+
```

## Guardrails

```text
+----------------------------+---------------------------------------------------------------+
| guardrail                  | status                                                        |
+----------------------------+---------------------------------------------------------------+
| no production 8001 restart | PASS                                                          |
| no runtime-path references | PASS: rg returned no matches in runtime directories           |
| reports ignored            | PASS: generated reports are under ignored reports/ directory  |
| no DB writes               | PASS: overlay script reads event_signal/QE artifacts only     |
| no hard trading action     | PASS: all financial distress signals remain research-only     |
+----------------------------+---------------------------------------------------------------+
```

## Errors / Recovery

```text
+-------+---------------------------------------------+--------------------------------------------------------------+
| error | observation                                 | resolution                                                   |
+-------+---------------------------------------------+--------------------------------------------------------------+
| E001  | WSL env port parsed as quoted string        | stripped quotes when passing TDX_DB_* values into WSL        |
| E002  | initial full load was slow with combo scan  | overlay loader now skips unused precision combo computation  |
+-------+---------------------------------------------+--------------------------------------------------------------+
```

## Residual Risk

- This is offline overlay research against existing QE artifacts, not an executed QE rerun.
- Results show portfolio interaction under the tested Top50 loops only; they are not production signal thresholds.
- Medium-cap effects are mostly 10-30bn; 30bn+ and >=100bn samples remain sparse.
