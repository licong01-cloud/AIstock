# L3 Validation - Financial Distress Phase29 True QE Attribution - 2026-05-11

## Scope

```text
+----------------------+--------------------------------------------------------------+
| item                 | value                                                        |
+----------------------+--------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                     |
| module               | scripts + docs/analysis                                      |
| phase                | 29                                                           |
| validation level     | L3 research artifact attribution                             |
| runtime integration  | none                                                         |
| DB writes            | none                                                         |
| production backend   | 8001 not touched                                             |
+----------------------+--------------------------------------------------------------+
```

## Commands And Results

```text
+------+--------------------------------------------------------------+-----------------------------+
| step | command group                                                | result                      |
+------+--------------------------------------------------------------+-----------------------------+
| 1    | python -m py_compile scripts/financial_distress_phase29_true_qe_attribution.py | pass                        |
| 2    | WSL rdagent-gpu run Phase29 attribution script                | pass, 3 cases analyzed      |
| 3    | generated ignored JSON/MD under reports/event_signal          | pass                        |
| 4    | generated curated doc under docs/analysis                     | pass                        |
| 5    | python -m pytest backend/tests/event_signal -q                | 172 passed                  |
| 6    | runtime isolation scan across Selection/Paper/QE/QMT paths    | no matches                  |
| 7    | git diff --check                                              | pass, LF/CRLF warnings only |
+------+--------------------------------------------------------------+-----------------------------+
```

## Business Evidence

```text
+---------------------------------+---------+-------+-------+--------------+--------------+
| case                            | penalty | top50 | drops | top50/pen    | true_ret_sum |
+---------------------------------+---------+-------+-------+--------------+--------------+
| phase28_q_ocf_fixed15_90td      | 41,673  | 221   | 25    | 0.530%       | +0.168%      |
| phase19_indicator_decline_ctx60 | 311     | 311   | 24    | 100.000%     | +0.273%      |
| phase23_loss_mv_fixed20_242td   | 304     | 302   | 61    | 99.342%      | +0.066%      |
+---------------------------------+---------+-------+-------+--------------+--------------+
```

## Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| q_ocf weak true materiality explained | broad low-precision overlay vs Top50/holdings                |
| Phase19 benchmark                     | still best one-loop true return and hit precision            |
| Phase23 calibration                   | high drop count alone is insufficient for true PnL           |
| runtime promotion                     | rejected                                                     |
| next research                         | high-conviction intersection cheap screen                    |
+--------------------------------------+--------------------------------------------------------------+
```

## Evidence Paths

```text
+----------------------+--------------------------------------------------------------------------------+
| item                 | path                                                                           |
+----------------------+--------------------------------------------------------------------------------+
| script               | scripts/financial_distress_phase29_true_qe_attribution.py                      |
| curated report        | docs/analysis/event_signal_financial_distress_phase29_true_qe_attribution_result_20260511.md |
| ignored report json   | reports/event_signal/financial_distress_phase29_true_qe_attribution/financial_distress_phase29_true_qe_attribution.json |
| ignored report md     | reports/event_signal/financial_distress_phase29_true_qe_attribution/financial_distress_phase29_true_qe_attribution.md   |
+----------------------+--------------------------------------------------------------------------------+
```

## Residual Risks

```text
+------------------------------+---------------------------------------------------------------+
| risk                         | mitigation                                                    |
+------------------------------+---------------------------------------------------------------+
| end-of-day holdings are approximate | report explicitly flags V25 intraday execution limitation |
| one-loop attribution          | use as direction-setting only, not runtime evidence           |
| q_ocf still may help in combos | test intersections cheaply before another WSL true rerun      |
+------------------------------+---------------------------------------------------------------+
```
