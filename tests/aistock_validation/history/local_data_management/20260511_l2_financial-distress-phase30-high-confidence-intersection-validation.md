# L2 Validation - Financial Distress Phase30 High-Confidence Intersection - 2026-05-11

## Scope

```text
+---------------------+--------------------------------------------------------------+
| item                | value                                                        |
+---------------------+--------------------------------------------------------------+
| branch              | codex/financial-distress-rerank-20260508                     |
| module              | event_signal research scripts + docs                         |
| phase               | 30                                                           |
| validation level    | L2 cheap overlay + direct event research                     |
| runtime integration | none                                                         |
| DB writes           | none                                                         |
| production backend  | 8001 not touched                                             |
+---------------------+--------------------------------------------------------------+
```

## Commands And Results

```text
+------+--------------------------------------------------------------------------------+-----------------------------+
| step | command group                                                                  | result                      |
+------+--------------------------------------------------------------------------------+-----------------------------+
| 1    | AIstock python -m py_compile changed event_signal services and Phase30 script  | pass                        |
| 2    | AIstock python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q | 39 passed                   |
| 3    | AIstock python scripts/financial_distress_phase30_high_confidence_intersection_screen.py | generated direct + overlay; exposed postprocess bug |
| 4    | fixed Phase30 overlay postprocess top_k parsing and reuse arguments             | pass by rerun with cached overlay |
| 5    | AIstock python scripts/... --reuse-direct-json ... --reuse-overlay-json ...     | pass; curated report written |
| 6    | AIstock python -m pytest backend/tests/event_signal -q                          | 174 passed                  |
| 7    | runtime isolation scan across Selection/Paper/QE/QMT paths                      | no matches                  |
| 8    | git diff --check                                                               | pass, LF/CRLF warnings only |
+------+--------------------------------------------------------------------------------+-----------------------------+
```

## Business Evidence

```text
+------------------------------------------+---------+-------+-------+---------+--------+-------------+
| case                                     | score   | pos   | avg   | ex_best | drop   | hit/overlay |
+------------------------------------------+---------+-------+-------+---------+--------+-------------+
| q_ocf 10-30bn fixed15 top50 60td         | 9.5     | 15/22 | 0.11% | 0.06%   | 9      | 0.05%       |
| q_ocf actual_yoy<=-80 fixed15 top50 60td | 2.2     | 13/22 | 0.06% | 0.04%   | 9      | 0.06%       |
| q_ocf prior_loss>=2 fixed15 top50 60td   | 1.9     | 12/22 | 0.03% | 0.01%   | 4      | 0.05%       |
+------------------------------------------+---------+-------+-------+---------+--------+-------------+
```

## Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| true-QE candidates                    | 0                                                            |
| best cheap row                        | q_ocf 10-30bn / 60td / fixed15_top50                         |
| Phase27 gate comparison               | far below score 68.4 and avg +0.181%                         |
| direct downside                       | present, but not enough without stronger QE overlay effect    |
| runtime promotion                     | rejected                                                     |
| next research                         | stop q_ocf intersections; pivot to other structured families  |
+--------------------------------------+--------------------------------------------------------------+
```

## Evidence Paths

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | path                                                                           |
+----------------------+--------------------------------------------------------------------------------+
| script               | scripts/financial_distress_phase30_high_confidence_intersection_screen.py      |
| curated report        | docs/analysis/event_signal_financial_distress_phase30_high_confidence_intersection_result_20260511.md |
| ignored summary json  | reports/event_signal/financial_distress_phase30_high_confidence_intersection/financial_distress_phase30_high_confidence_intersection.json |
| ignored overlay json  | reports/event_signal/financial_distress_phase30_high_confidence_intersection/overlay/rank_aware/financial_distress_qe_multiloop_20240701_20260511_202514.json |
+----------------------+--------------------------------------------------------------------------------+
```

## Bugs Found And Fixed

```text
+------------------------------+--------------------------------------------------------------+
| issue                        | fix                                                          |
+------------------------------+--------------------------------------------------------------+
| default base python lacks qlib | use AIstock conda python for QE artifact unpickle           |
| top_k removed from function call | parse top_k from simulator_mode and add reuse JSON args    |
| repeated top_k runs too slow | support multiple top_k values in one scenario expansion      |
+------------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+-------------------------------+-------------------------------------------------------------+
| risk                          | mitigation                                                  |
+-------------------------------+-------------------------------------------------------------+
| cheap overlay is not true QE   | no WSL rerun or runtime promotion without a stronger gate   |
| direct downside is mixed by mean | use medians/negative rate as research context only        |
| reports are ignored artifacts  | curated report and validation record are committed          |
+-------------------------------+-------------------------------------------------------------+
```
