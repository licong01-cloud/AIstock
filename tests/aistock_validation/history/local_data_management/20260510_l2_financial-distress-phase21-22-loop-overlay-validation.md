# L2 Validation - Financial Distress Phase 21 22-Loop Overlay - 2026-05-10

## Scope

```text
+----------------------+--------------------------------------------------------------+
| item                 | value                                                        |
+----------------------+--------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                     |
| phase                | 21                                                           |
| module               | event_signal financial-distress research                     |
| runtime impact       | none                                                         |
| production 8001      | not touched                                                  |
| report               | phase21_22_loop_overlay_result_20260510                      |
+----------------------+--------------------------------------------------------------+
```

Report path: `docs/analysis/event_signal_financial_distress_phase21_22_loop_overlay_result_20260510.md`.

## Business Goal

Expand the Phase 20 10-loop shortlist to the same 22-loop cheap overlay set before spending WSL full-universe true QE rerun time. The phase must remain research-only and must not create hard buy-ban, forced-sell, Paper, Selection, QMT, or live-trading behavior.

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m backend.services.event_signal.financial_distress_qe_overlay_research `
  --loop-spec-json reports/event_signal/financial_distress_phase21_22_loop_overlay/phase21_loop_specs_22.json `
  --output-dir reports/event_signal/financial_distress_phase21_22_loop_overlay `
  --date-from 2024-07-01 --date-to 2026-04-27 `
  --active-trading-days 60 --active-trading-days 120 --active-trading-days 242 `
  --simulator-mode score_down --simulator-mode score_down_severity `
  --score-down-rank-penalty-pct 0.20 `
  --score-down-severity-profile balanced `
  --score-down-ranking-date-mode previous `
  --include-size-bucket-rules --include-loss-history-rules --include-mid-large-rules --include-refinement-rules `
  --rule-key structured_financial_risk_mv_ge_10bn `
  --rule-key structured_financial_risk_mv_10_30bn `
  --rule-key structured_financial_risk_mv_ge_10bn_prior_loss_ge_2 `
  --rule-key loss_to_market_cap_ge_50pct `
  --rule-key forecast_loss_to_market_cap_ge_50pct `
  --rule-key loss_to_market_cap_ge_50pct_mv_lt_5bn `
  --rule-key loss_to_market_cap_ge_50pct_mv_lt_10bn `
  --rule-key loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss `
  --rule-key loss_reports_ge_4_mv_lt_10bn `
  --rule-key forecast_loss_reports_ge_4_mv_lt_10bn `
  --no-overlay-csv

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal -q
rg -n "financial_distress_phase21|TAIL_CONTROL_NEXT|phase21_22_loop_overlay" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| 22-loop overlay command               | pass                                                         |
| validations                           | 1320                                                         |
| stability rows                        | 60                                                           |
| best average row                      | loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss, +0.178%          |
| best average row worst loop           | -1.966%, too large for risk-first promotion                  |
| structured ge10 expansion             | +0.110% avg, -0.879% worst; not strong enough                |
| py_compile                            | pass                                                         |
| focused financial distress pytest      | 35 passed                                                    |
| event_signal pytest suite              | 164 passed                                                   |
| runtime isolation scan                 | no runtime references added                                  |
| git diff --check                       | pass, LF/CRLF warnings only                                  |
| runtime promotion                     | rejected                                                     |
+--------------------------------------+--------------------------------------------------------------+
```

## Business Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| outcome                              | evidence                                                     |
+--------------------------------------+--------------------------------------------------------------+
| no WSL true rerun now                 | no candidate is both strong and tail-safe                    |
| no runtime integration                | research-only docs and ignored reports only                  |
| next research                         | loss-history tail-control parameter sweep                    |
| production 8001                       | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Residual Risks

- The overlay remains a cheap candidate-list simulation; it is not equivalent to full Qlib portfolio rerun.
- The best loss-history row has attractive average return but unacceptable worst-loop loss for risk-first use.
- Ignored raw report JSON remains under `reports/event_signal`; curated evidence is preserved in docs and this validation record.
