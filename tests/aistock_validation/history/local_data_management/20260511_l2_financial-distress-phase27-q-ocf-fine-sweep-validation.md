# L2 Financial Distress Phase 27 q_ocf Fine Sweep Validation - 2026-05-11

## Scope

```text
+-------------------------+-------------------------------------------------------+
| item                    | value                                                 |
+-------------------------+-------------------------------------------------------+
| branch                  | codex/financial-distress-rerank-20260508              |
| phase                   | 27 q_ocf fine sweep                                   |
| runtime impact          | none: research-only, no QE/Paper/Selection/QMT wiring |
| DB impact               | none: read-only event and price research              |
| production backend 8001 | not touched                                           |
+-------------------------+-------------------------------------------------------+
```

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/financial_distress_phase27_q_ocf_fine_sweep.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/financial_distress_phase27_q_ocf_fine_sweep.py
```

## Results

```text
+-------------------+---------------------------------------------------------------+
| check             | result                                                        |
+-------------------+---------------------------------------------------------------+
| py_compile        | PASS                                                          |
| phase27 script    | PASS: 1 rule, 3 direct rows, 20 overlay rows, 440 validations |
| cheap gate        | PASS: TRUE_QE_CANDIDATE score 68.4                            |
| runtime isolation | PASS: research-only script/docs; no runtime consumers changed |
+-------------------+---------------------------------------------------------------+
```

## Key Business Outcome

```text
+--------------------------------------------------+--------------------------------------------------------------+
| candidate                                        | outcome                                                      |
+--------------------------------------------------+--------------------------------------------------------------+
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn | TRUE_QE_CANDIDATE                                            |
| best cheap shape                                 | 90td / 15% rank penalty / previous prediction date / TopK=50 |
| direct evidence                                  | T+20 abnormal median -0.93%, T+60 abnormal median -1.61%     |
| next gate                                        | one-loop WSL full-universe true QE smoke                     |
+--------------------------------------------------+--------------------------------------------------------------+
```

## Evidence Paths

```text
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
| artifact       | path                                                                                                                                   |
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
| curated report | docs/analysis/event_signal_financial_distress_phase27_q_ocf_fine_sweep_result_20260511.md                                              |
| phase27 json   | reports/event_signal/financial_distress_phase27_q_ocf_fine_sweep/financial_distress_phase27_q_ocf_fine_sweep.json                      |
| direct json    | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\direct\financial_distress_direct_event_20240701_20260511_150037.json  |
| overlay json   | reports\event_signal\financial_distress_phase27_q_ocf_fine_sweep\overlay\financial_distress_qe_multiloop_20240701_20260511_151633.json |
+----------------+----------------------------------------------------------------------------------------------------------------------------------------+
```

## Residual Risks

- Cheap overlay is still not final PnL evidence.
- WSL true QE smoke is required before any runtime, DB policy, or alpha/risk overlay promotion.
- Generated JSON/loop artifacts under `reports/` remain ignored; committed evidence is curated Markdown plus this validation record.
