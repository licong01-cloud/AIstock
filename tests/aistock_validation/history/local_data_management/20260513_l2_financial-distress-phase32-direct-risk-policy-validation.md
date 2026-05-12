# L2 Validation - Financial Distress Phase32 Direct-Risk Policy Feasibility - 2026-05-13

## Scope

```text
+---------------------+--------------------------------------------------------------+
| item                | value                                                        |
+---------------------+--------------------------------------------------------------+
| branch              | codex/financial-distress-rerank-20260508                     |
| module              | event_signal research scripts + docs                         |
| phase               | 32                                                           |
| validation level    | L2 direct-event policy feasibility research                  |
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
| 1    | AIstock python scripts/financial_distress_phase32_direct_risk_policy_feasibility.py | pass; report generated      |
| 2    | AIstock python -m py_compile Phase32 script and test                            | pass                        |
| 3    | AIstock python -m pytest backend/tests/event_signal/test_financial_distress_phase32_policy_feasibility.py -q | 3 passed                    |
| 4    | AIstock python -m pytest backend/tests/event_signal -q                          | 179 passed                  |
| 5    | runtime isolation scan across Selection/Paper/QE/QMT paths                      | no matches                  |
| 6    | git diff --check                                                               | pass                        |
+------+--------------------------------------------------------------------------------+-----------------------------+
```

## Business Evidence

```text
+--------------------------------------------------------+-------+-------+---------+---------+---------+---------+
| case                                                   | score | valid | t20_med | t20_neg | t60_med | t60_neg |
+--------------------------------------------------------+-------+-------+---------+---------+---------+---------+
| ocf_yoy<=-50 and debt/assets>=70 >=10bn                | 11.0  | 82    | -1.53%  | 60.98%  | -2.58%  | 61.25%  |
| expectation miss gap>=50 with actual indicator >=10bn  | 10.5  | 185   | -4.56%  | 71.35%  | -2.48%  | 57.50%  |
| current_ratio<0.8 10-30bn                              | 10.5  | 109   | -1.33%  | 59.63%  | -2.95%  | 60.75%  |
| profit/revenue both down 30-100bn                      | 10.0  | 97    | -3.41%  | 69.07%  | -4.74%  | 63.83%  |
+--------------------------------------------------------+-------+-------+---------+---------+---------+---------+
```

## Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| direct events                         | 13,563                                                       |
| return rows                           | 67,815 across T+1/T+5/T+20/T+60/T+120                       |
| risk-downweight candidates            | 37                                                           |
| watchlist-policy research rows        | 3                                                            |
| hard ban / forced sell                | 0; explicitly rejected for financial rules in this phase     |
| runtime promotion                     | rejected                                                     |
| next research                         | offline portfolio overlay for shortlisted avoid-new-buy rules|
+--------------------------------------+--------------------------------------------------------------+
```

## Evidence Paths

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | path                                                                           |
+----------------------+--------------------------------------------------------------------------------+
| script               | scripts/financial_distress_phase32_direct_risk_policy_feasibility.py           |
| curated report        | docs/analysis/event_signal_financial_distress_phase32_direct_risk_policy_feasibility_result_20260513.md |
| ignored summary json  | reports/event_signal/financial_distress_phase32_direct_risk_policy_feasibility/financial_distress_phase32_direct_risk_policy_feasibility.json |
| unit test             | backend/tests/event_signal/test_financial_distress_phase32_policy_feasibility.py |
+----------------------+--------------------------------------------------------------------------------+
```

## Bugs Found And Fixed

```text
+------------------------------+--------------------------------------------------------------+
| issue                        | fix                                                          |
+------------------------------+--------------------------------------------------------------+
| dataclass importlib test load | insert module into sys.modules before exec_module           |
| no runtime bug found         | not applicable                                               |
+------------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+-------------------------------+-------------------------------------------------------------+
| risk                          | mitigation                                                  |
+-------------------------------+-------------------------------------------------------------+
| direct event evidence is not portfolio PnL | require Phase33 offline overlay before policy work |
| nested rules inflate candidate count       | next phase should dedupe by policy family and overlap       |
| T+120 tail may reflect broader market path | keep benchmark-adjusted rows and require overlay validation |
+-------------------------------+-------------------------------------------------------------+
```
