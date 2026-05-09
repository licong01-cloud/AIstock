# 2026-05-09 L2 Validation - Financial Distress Robustness Gate

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 16                                                            |
| validation level | L2 research gate assessment                                          |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| report           | docs/analysis/event_signal_financial_distress_robustness_gate_result_20260509.md |
+------------------+---------------------------------------------------------------------+
```

## Input Artifacts

```text
+----------+---------------------------------------------------------------------+
| type     | path                                                                |
+----------+---------------------------------------------------------------------+
| context  | reports/event_signal/financial_distress_policy_parameter_sweep_context_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_172134.json |
| fixed    | reports/event_signal/financial_distress_policy_parameter_sweep_fixed_qe_overlay/financial_distress_qe_multiloop_20240701_20260509_173159.json     |
+----------+---------------------------------------------------------------------+
```

## Checks Run

```powershell
python .codex_tmp/event_signal/phase16_robustness.py
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "financial_distress_robustness_gate|indicator_large_decline_mv_10_30bn|rank_decay_balanced" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+---------------------------------------------+
| check                                | result                                      |
+--------------------------------------+---------------------------------------------+
| robustness parser                    | completed; selected profile summarized       |
| targeted financial distress tests    | 31 passed                                   |
| full event_signal module tests       | 166 passed                                  |
| runtime isolation scan               | no matches in QE/Paper/Selection/QMT paths  |
| diff whitespace check                | passed; LF/CRLF warnings only               |
+--------------------------------------+---------------------------------------------+
```

## Gate Result

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| gate                                 | result                     | implication                                                  |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| research candidate                   | KEEP_RESEARCH_PRIMARY      | 60td context-balanced remains best non-hard candidate        |
| runtime integration                   | REJECT_NOW                 | median zero and outlier dependence are too high              |
| DB policy draft persistence           | DEFER                      | wait until true QE rerun / stronger evidence                 |
| next step                             | DESIGN_TRUE_QE_RERUN       | research-only harness or detailed hook design                |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+----------------------------+---------------------------------------------------------------+
| risk                       | mitigation                                                     |
+----------------------------+---------------------------------------------------------------+
| artifact overlay approx    | true QE rerun design is next                                   |
| outlier dependency         | require positive median or stronger breadth before promotion   |
| no consumer audit trace    | future rerun design must define before/after rank trace        |
+----------------------------+---------------------------------------------------------------+
```
