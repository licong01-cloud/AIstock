# L2 Validation - Financial Distress Refinement QE Overlay Research - 2026-05-09

## Scope

```text
+--------------------+--------------------------------------------------------------------------+
| item               | value                                                                    |
+--------------------+--------------------------------------------------------------------------+
| branch             | codex/financial-distress-rerank-20260508                                 |
| phase              | Phase 10 size/loss-history/decay refinement                              |
| runtime boundary   | no QE/Paper/Selection/QMT runtime integration                            |
| production 8001    | not restarted                                                            |
| industry handling  | explanatory only; no neutralization or industry rejection                |
| generated report   | reports/event_signal/financial_distress_refinement_qe_overlay/...090112  |
+--------------------+--------------------------------------------------------------------------+
```

## Commands

```text
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
| order | command                                                                                                            | result  |
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
| 1     | python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py ...                   | PASS    |
| 2     | python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q                      | PASS 29 |
| 3     | local sample count for REFINEMENT_RULES via load_enriched_financial_rows                                           | PASS    |
| 4     | WSL 10-loop refinement-only overlay, active_td 20/60/120/242, score_down rank20 + severity balanced                | PASS640 |
| 5     | python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q                   | PASS159 |
| 6     | runtime isolation rg scan in Selection/Paper/QE/QMT paths                                                         | PASS    |
| 7     | git diff --check                                                                                                  | PASS    |
+-------+--------------------------------------------------------------------------------------------------------------------+---------+
```

## WSL Overlay Run

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | value                                                                          |
+----------------------+--------------------------------------------------------------------------------+
| date range           | 2024-07-01 -> 2026-04-27                                                       |
| loops                | 10                                                                             |
| validations          | 640                                                                            |
| active trading days  | 20, 60, 120, 242                                                               |
| modes                | score_down_rank_20pct_top50_previous; score_down_severity_balanced_top50_prev. |
| report json          | financial_distress_qe_multiloop_20240701_20260509_090112.json                 |
| report md            | financial_distress_qe_multiloop_20240701_20260509_090112.md                   |
+----------------------+--------------------------------------------------------------------------------+
```

## Sample Availability

```text
+------------------------------------------------------+---------+---------+
| rule_key                                             | rows    | symbols |
+------------------------------------------------------+---------+---------+
| indicator_large_decline_mv_10_30bn                   | 2432    | 807     |
| indicator_large_decline_mv_30_100bn                  | 539     | 198     |
| indicator_large_decline_mv_ge_100bn                  | 87      | 35      |
| indicator_large_decline_mv_ge_10bn_prior_loss_ge_2   | 409     | 202     |
| indicator_large_decline_mv_10_30bn_prior_loss_ge_2   | 325     | 167     |
| structured_financial_risk_mv_10_30bn                 | 4510    | 1045    |
| structured_financial_risk_mv_ge_30bn                 | 1206    | 297     |
| structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 | 1319    | 355     |
+------------------------------------------------------+---------+---------+
```

## Result Summary

```text
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| candidate                                  | validation decision   | evidence                                                   |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
| indicator_large_decline_mv_10_30bn         | KEEP_PRIMARY_CANDIDATE| 60td severity avg_ret_d 0.20%, pos 6/10, min ~0            |
| indicator_large_decline_mv_30_100bn        | WATCHLIST_ONLY        | 242td avg_ret_d about 0.03%, weak but non-zero             |
| indicator_large_decline_mv_ge_100bn        | REJECT_RUNTIME        | no dropped Top50 events                                    |
| prior loss refinements                     | REJECT_REFINEMENT     | lower coverage and no improvement over size-only rule      |
| structured_financial_risk_mv_10_30bn       | COVERAGE_BENCHMARK    | positive but broader and worse tail than indicator split   |
+--------------------------------------------+-----------------------+------------------------------------------------------------+
```

## Guardrails

```text
+----------------------------+--------------------------------------------------------------+
| guardrail                  | status                                                       |
+----------------------------+--------------------------------------------------------------+
| no production 8001 restart | PASS                                                         |
| no runtime integration     | PASS: changes limited to event_signal research/tests/docs    |
| no industry neutralization | PASS: industry retained as explanatory only                  |
| reports ignored            | PASS: generated reports are under ignored reports/ directory |
| no DB writes               | PASS: overlay script reads event_signal/QE artifacts only    |
+----------------------------+--------------------------------------------------------------+
```

## Residual Risk

- This is offline overlay research, not a QE rerun and not production signal activation.
- Sector concentration may be useful for plate rotation, but this report does not yet attribute returns by sector regime.
- The current best refinement should still be tested on direct event-date returns or additional QE loops before runtime design.
