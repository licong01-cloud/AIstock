# L2 Validation - Financial Distress Direct Event Return Research - 2026-05-09

## Scope

```text
+------------------+----------------------------------------------------------------------------------------------------------------------------+
| item             | value                                                                                                                      |
+------------------+----------------------------------------------------------------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                                                                                   |
| phase            | Phase 11 direct event return + sector attribution                                                                          |
| runtime boundary | no QE/Paper/Selection/QMT runtime integration                                                                              |
| production 8001  | not restarted                                                                                                              |
| benchmark        | 000300.SH                                                                                                                  |
| generated json   | reports\event_signal\financial_distress_direct_event_returns\financial_distress_direct_event_20240701_20260509_093006.json |
| generated md     | reports/event_signal/financial_distress_direct_event_returns/financial_distress_direct_event_20240701_20260509_093006.md   |
+------------------+----------------------------------------------------------------------------------------------------------------------------+
```

## Commands

```text
+-------+----------------------------------------------------------------------------------------------------------------------------------+---------------------+
| order | command                                                                                                                          | result              |
+-------+----------------------------------------------------------------------------------------------------------------------------------+---------------------+
| 1     | python -m py_compile backend/services/event_signal/financial_distress_direct_event_research.py ...                               | PASS                |
| 2     | python -m pytest backend/tests/event_signal/test_financial_distress_direct_event_research.py -q                                  | PASS 5              |
| 3     | python -m backend.services.event_signal.financial_distress_direct_event_research --date-from 2024-07-01 --date-to 2026-04-27 ... | PASS 3713 events    |
| 4     | python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q                                 | PASS 164            |
| 5     | runtime isolation rg scan in Selection/Paper/QE/QMT paths                                                                        | PASS no matches     |
| 6     | git diff --check                                                                                                                 | PASS                |
+-------+----------------------------------------------------------------------------------------------------------------------------------+---------------------+
```

## Result Summary

```text
+----------------------------+--------+---------------+-----------------+---------------+-----------------+
| rule                       | events | T+20 abn_mean | T+20 abn_median | T+60 abn_mean | T+60 abn_median |
+----------------------------+--------+---------------+-----------------+---------------+-----------------+
| smallcap_loss_mv50         | 79     | 8.16%         | 4.72%           | 5.64%         | -0.55%          |
| indicator_decline_10_30bn  | 1121   | 1.41%         | -0.52%          | 4.72%         | -1.26%          |
| indicator_decline_30_100bn | 265    | -0.36%        | -3.06%          | 0.68%         | -2.70%          |
| structured_risk_10_30bn    | 2248   | 2.64%         | -0.10%          | 2.65%         | -2.57%          |
+----------------------------+--------+---------------+-----------------+---------------+-----------------+
```

## Guardrails

```text
+----------------------------+---------------------------------------------------------------------+
| guardrail                  | status                                                              |
+----------------------------+---------------------------------------------------------------------+
| no production 8001 restart | PASS                                                                |
| no runtime integration     | PASS: changes limited to event_signal research/tests/docs           |
| no DB writes               | PASS: script reads event_signal, price, calendar, index tables only |
| reports ignored            | PASS: generated reports are under ignored reports/ directory        |
| industry neutralization    | NOT APPLIED: sector is explanatory only                             |
+----------------------------+---------------------------------------------------------------------+
```

## Residual Risk

- This is an offline event study, not a QE rerun and not production signal activation.
- Later-window missing-price rates are non-trivial for events close to the current data boundary, especially the T+60 window.
- Direct event returns and QE overlay returns answer different questions; runtime design should use both and remain context-aware.
