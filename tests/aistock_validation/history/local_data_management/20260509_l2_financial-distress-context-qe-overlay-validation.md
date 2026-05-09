# L2 Validation - Financial Distress Context-Aware QE Overlay Research - 2026-05-09

## Scope

```text
+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| item                | value                                                                                                                             |
+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+
| branch              | codex/financial-distress-rerank-20260508                                                                                          |
| phase               | Phase 12 context-aware QE overlay research                                                                                        |
| runtime boundary    | no QE/Paper/Selection/QMT runtime integration                                                                                     |
| production 8001     | not restarted                                                                                                                     |
| light/severity json | reports/event_signal/financial_distress_context_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_124606.json          |
| balanced json       | reports/event_signal/financial_distress_context_balanced_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_130054.json |
+---------------------+-----------------------------------------------------------------------------------------------------------------------------------+
```

## Commands

```text
+-------+--------------------------------------------------------------------------------------------------+---------------------+
| order | command                                                                                          | result              |
+-------+--------------------------------------------------------------------------------------------------+---------------------+
| 1     | python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py ... | PASS                |
| 2     | python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q    | PASS 31             |
| 3     | WSL 10-loop score_down_context light/severity/sector-relief run                                  | PASS 480            |
| 4     | WSL 10-loop score_down_context balanced/balanced-sector-relief run                               | PASS 320            |
| 5     | python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q | PASS 166            |
| 6     | runtime isolation rg scan in Selection/Paper/QE/QMT paths                                        | PASS no matches     |
| 7     | git diff --check                                                                                 | PASS                |
+-------+--------------------------------------------------------------------------------------------------+---------------------+
```

## Result Summary

```text
+----------------------------+---------------------------------------+-----------+-----------+-----------+---------+---------+
| rule                       | best_context                          | pos/loops | avg_ret_d | min_ret_d | dropped | avg_pen |
+----------------------------+---------------------------------------+-----------+-----------+-----------+---------+---------+
| indicator_decline_10_30bn  | ctx_rank_decay_balanced_sector_relief | 6/10      | 0.20%     | -0.00%    | 5       | 12.03%  |
| indicator_decline_30_100bn | ctx_rank_decay_balanced_sector_relief | 5/10      | 0.04%     | -0.00%    | 4       | 14.90%  |
| smallcap_loss_mv50         | ctx_rank_decay_balanced_sector_relief | 6/10      | 0.18%     | -0.00%    | 8       | 16.13%  |
| structured_risk_10_30bn    | ctx_rank_decay_balanced_sector_relief | 4/10      | 0.17%     | -0.37%    | 24      | 13.18%  |
+----------------------------+---------------------------------------+-----------+-----------+-----------+---------+---------+
```

## Guardrails

```text
+-------------------------------+--------------------------------------------------------------+
| guardrail                     | status                                                       |
+-------------------------------+--------------------------------------------------------------+
| no production 8001 restart    | PASS                                                         |
| no runtime integration        | PASS: changes limited to event_signal research/tests/docs    |
| no DB writes                  | PASS: script reads event_signal and QE artifacts only        |
| reports ignored               | PASS: generated reports are under ignored reports/ directory |
| no hard buy ban / forced sell | PASS: context profiles only generate score-down penalties    |
+-------------------------------+--------------------------------------------------------------+
```

## Residual Risk

- The overlay remains an offline approximation over completed QE artifacts, not a full QE rerun.
- Replacement matching is approximate; future production design needs StrategyPackage/Paper-v2-native audit if integrated.
- Average-return improvements are small; the main Phase 12 value is policy shape and tail control rather than raw alpha gain.
