# L2 Financial Distress Phase 25 Threshold Refinement Validation - 2026-05-11

## Scope

```text
+-------------------------+-------------------------------------------------------+
| item                    | value                                                 |
+-------------------------+-------------------------------------------------------+
| branch                  | codex/financial-distress-rerank-20260508              |
| phase                   | 25 threshold refinement                               |
| runtime impact          | none: research-only, no QE/Paper/Selection/QMT wiring |
| DB impact               | none: read-only event and price research              |
| production backend 8001 | not touched                                           |
+-------------------------+-------------------------------------------------------+
```

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/services/event_signal/financial_distress_direct_event_research.py scripts/financial_distress_phase25_threshold_refinement_screen.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/financial_distress_phase25_threshold_refinement_screen.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal -q
rg -n "PHASE25|phase25|indicator_decline_q_ocf_to_sales_lt_0|indicator_decline_ocf_negative_or_leverage_mv_10_30bn" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
```

## Results

```text
+--------------------------------------+-------------------------------------------------------------------+
| check                                | result                                                            |
+--------------------------------------+-------------------------------------------------------------------+
| py_compile                           | PASS                                                              |
| phase25 script                       | PASS: 12 rules, 36 direct rows, 72 overlay rows, 1584 validations |
| pytest backend/tests/event_signal -q | PASS: 170 passed in 1.67s                                         |
| runtime isolation rg scan            | PASS: no matches in Selection/Paper/QE/QMT runtime paths          |
| git diff --check                     | PASS after EOF whitespace cleanup                                 |
+--------------------------------------+-------------------------------------------------------------------+
```

## Key Business Outcome

```text
+-------------------------------------------------------+-----------------------------------------------+
| candidate                                             | outcome                                       |
+-------------------------------------------------------+-----------------------------------------------+
| indicator_decline_ocf_negative_or_leverage_mv_10_30bn | watchlist only; score 55.5 below TRUE_QE gate |
| indicator_decline_q_ocf_to_sales_lt_0_mv_ge_10bn      | watchlist only; score 55.3 below TRUE_QE gate |
| 30-100bn/debt>=90 direct downside                     | research-only; overlay weak/sparse            |
| runtime promotion                                     | rejected for this phase                       |
+-------------------------------------------------------+-----------------------------------------------+
```

## Evidence Paths

```text
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| artifact       | path                                                                                                                                       |
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| curated report | docs/analysis/event_signal_financial_distress_phase25_threshold_refinement_result_20260511.md                                              |
| phase25 json   | reports/event_signal/financial_distress_phase25_threshold_refinement/financial_distress_phase25_threshold_refinement.json                  |
| direct json    | reports\event_signal\financial_distress_phase25_threshold_refinement\direct\financial_distress_direct_event_20240701_20260511_011654.json  |
| overlay json   | reports\event_signal\financial_distress_phase25_threshold_refinement\overlay\financial_distress_qe_multiloop_20240701_20260511_014959.json |
+----------------+--------------------------------------------------------------------------------------------------------------------------------------------+
```

## Residual Risks

- Cheap overlay is a shortlist screen only and does not prove final PnL.
- No Phase-25 rule was promoted to WSL true QE in this validation because the strict cheap score gate was not reached.
- Generated JSON/loop artifacts under `reports/` remain ignored; committed evidence is curated Markdown plus this validation record.
