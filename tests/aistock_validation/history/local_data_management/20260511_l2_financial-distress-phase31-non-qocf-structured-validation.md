# L2 Validation - Financial Distress Phase31 Non-q_ocf Structured Screen - 2026-05-11

## Scope

```text
+---------------------+--------------------------------------------------------------+
| item                | value                                                        |
+---------------------+--------------------------------------------------------------+
| branch              | codex/financial-distress-rerank-20260508                     |
| module              | event_signal research scripts + docs                         |
| phase               | 31                                                           |
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
| 1    | AIstock python scripts/financial_distress_phase31_non_qocf_structured_screen.py | pass; direct + overlay + report generated |
| 2    | AIstock python -m py_compile changed event_signal services and Phase31 script   | pass                        |
| 3    | AIstock python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q | 41 passed                   |
| 4    | AIstock python -m pytest backend/tests/event_signal -q                          | 176 passed                  |
| 5    | runtime isolation scan across Selection/Paper/QE/QMT paths                      | no matches                  |
| 6    | git diff --check                                                               | pass, LF/CRLF warnings only |
+------+--------------------------------------------------------------------------------+-----------------------------+
```

## Business Evidence

```text
+--------------------------------------------------+-------+-------+-------+---------+------+-------------+
| case                                             | score | pos   | avg   | ex_best | drop | hit/overlay |
+--------------------------------------------------+-------+-------+-------+---------+------+-------------+
| current_ratio<0.8 10-30bn fixed15 top50 60td     | 27.6  | 12/22 | 0.02% | 0.00%   | 3    | 0.08%       |
| profit/revenue diverge 30-100bn fixed10 top50    | 27.0  | 13/22 | 0.02% | 0.00%   | 2    | 0.07%       |
| negative margin 10-30bn fixed15 top50 60td       | 23.9  | 14/22 | 0.01% | -0.01%  | 10   | 0.07%       |
+--------------------------------------------------+-------+-------+-------+---------+------+-------------+
```

## Direct-Event Evidence

```text
+--------------------------------------------------------+-------------------------------+-------------------------------+
| rule                                                   | direct downside evidence      | overlay limitation            |
+--------------------------------------------------------+-------------------------------+-------------------------------+
| expectation miss gap>=100 + prior losses               | T+60 median about -8%         | sparse Top50 hits and drops   |
| profit/revenue both down 30-100bn                      | T+20 -3.41%, T+60 -4.74% med  | cheap overlay score below gate|
| current ratio<0.8 30-100bn                             | T+20 median -3.07%            | sample and drops too small    |
| debt/assets>=90                                        | negative direct small samples | insufficient overlay coverage |
+--------------------------------------------------------+-------------------------------+-------------------------------+
```

## Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| true-QE candidates                    | 0                                                            |
| best cheap row                        | current_ratio<0.8 10-30bn / 60td / fixed15_top50             |
| Phase27 gate comparison               | far below score 68.4 and avg +0.181%                         |
| direct downside                       | present for some families, but not enough for QE overlay     |
| runtime promotion                     | rejected                                                     |
| next research                         | direct-risk policy feasibility before more WSL true reruns   |
+--------------------------------------+--------------------------------------------------------------+
```

## Evidence Paths

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | path                                                                           |
+----------------------+--------------------------------------------------------------------------------+
| script               | scripts/financial_distress_phase31_non_qocf_structured_screen.py               |
| curated report        | docs/analysis/event_signal_financial_distress_phase31_non_qocf_structured_screen_result_20260511.md |
| ignored summary json  | reports/event_signal/financial_distress_phase31_non_qocf_structured_screen/financial_distress_phase31_non_qocf_structured_screen.json |
| ignored overlay json  | reports/event_signal/financial_distress_phase31_non_qocf_structured_screen/overlay/rank_aware/financial_distress_qe_multiloop_20240701_20260512_000610.json |
+----------------------+--------------------------------------------------------------------------------+
```

## Bugs Found And Fixed

```text
+------------------------------+--------------------------------------------------------------+
| issue                        | fix                                                          |
+------------------------------+--------------------------------------------------------------+
| no new functional bug found  | not applicable                                               |
| qlib dependency risk         | used AIstock conda python for all validations                |
+------------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+-------------------------------+-------------------------------------------------------------+
| risk                          | mitigation                                                  |
+-------------------------------+-------------------------------------------------------------+
| cheap overlay is not true QE   | no WSL rerun or runtime promotion in Phase31                |
| direct downside has sparse hits| treat as watchlist/direct-event research only               |
| reports are ignored artifacts  | curated report, docs, script, and validation record commit  |
+-------------------------------+-------------------------------------------------------------+
```
