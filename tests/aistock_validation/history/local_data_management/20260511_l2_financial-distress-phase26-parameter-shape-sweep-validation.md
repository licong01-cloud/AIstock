# L2 Financial Distress Phase 26 Parameter Shape Sweep Validation - 2026-05-11

## Scope

```text
+-------------------------+-------------------------------------------------------+
| item                    | value                                                 |
+-------------------------+-------------------------------------------------------+
| branch                  | codex/financial-distress-rerank-20260508              |
| phase                   | 26 parameter shape sweep                              |
| runtime impact          | none: research-only, no QE/Paper/Selection/QMT wiring |
| DB impact               | none: read-only event and price research              |
| production backend 8001 | not touched                                           |
+-------------------------+-------------------------------------------------------+
```

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/financial_distress_phase26_parameter_shape_sweep.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/financial_distress_phase26_parameter_shape_sweep.py
```

## Results

```text
+-------------------+--------------------------------------------------------------------+
| check             | result                                                             |
+-------------------+--------------------------------------------------------------------+
| py_compile        | PASS                                                               |
| phase26 script    | PASS: 12 rules, 36 direct rows, 180 overlay rows, 3960 validations |
| runtime isolation | PASS: still research-only, no runtime consumers touched            |
+-------------------+--------------------------------------------------------------------+
```

## Key Business Outcome

```text
+-------------------------------------------------------+-------------------------------------+
| candidate                                             | outcome                             |
+-------------------------------------------------------+-------------------------------------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn      | best score 56.1, below TRUE_QE gate |
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn | watchlist only, score 55.5          |
| current_ratio <0.8                                    | secondary watchlist, score 47.1     |
| WSL true QE promotion                                 | rejected for this phase             |
+-------------------------------------------------------+-------------------------------------+
```

## Evidence Paths

```text
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| artifact       | path                                                                                                                                        |
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
| curated report | docs/analysis/event_signal_financial_distress_phase26_parameter_shape_sweep_result_20260511.md                                              |
| phase26 json   | reports/event_signal/financial_distress_phase26_parameter_shape_sweep/financial_distress_phase26_parameter_shape_sweep.json                 |
| direct json    | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\direct\financial_distress_direct_event_20240701_20260511_134419.json  |
| overlay json   | reports\event_signal\financial_distress_phase26_parameter_shape_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_145003.json |
+----------------+---------------------------------------------------------------------------------------------------------------------------------------------+
```

## Residual Risks

- The best candidate still does not clear the strict cheap gate, so it remains a shortlist feature.
- Cheap overlay remains a screen only; it is not final evidence for runtime policy or alpha deployment.
- Generated JSON/loop artifacts under `reports/` remain ignored; committed evidence is curated Markdown plus this validation record.
