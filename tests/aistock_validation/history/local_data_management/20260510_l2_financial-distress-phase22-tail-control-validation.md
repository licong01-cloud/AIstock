# L2 Validation - Financial Distress Phase 22 Tail-Control Sweep - 2026-05-10

## Scope

```text
+----------------------+--------------------------------------------------------------+
| item                 | value                                                        |
+----------------------+--------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                     |
| phase                | 22                                                           |
| module               | event_signal financial-distress research                     |
| runtime impact       | none                                                         |
| production 8001      | not touched                                                  |
| report               | phase22_tail_control_result_20260510                         |
+----------------------+--------------------------------------------------------------+
```

Report path: `docs/analysis/event_signal_financial_distress_phase22_tail_control_result_20260510.md`.

## Business Goal

Run a focused loss-history small-cap tail-control parameter sweep on the 22-loop cheap overlay set. The goal is to determine whether softer/rank-aware profiles reduce the Phase 21 worst-loop loss enough to justify WSL full-universe true QE rerun. This phase must remain research-only.

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m backend.services.event_signal.financial_distress_qe_overlay_research `
  --loop-spec-json reports/event_signal/financial_distress_phase21_22_loop_overlay/phase21_loop_specs_22.json `
  --output-dir reports/event_signal/financial_distress_phase22_tail_control_overlay `
  --date-from 2024-07-01 --date-to 2026-04-27 `
  --active-trading-days 242 `
  --simulator-mode score_down --simulator-mode score_down_severity --simulator-mode score_down_context `
  --score-down-rank-penalty-pct 0.05 --score-down-rank-penalty-pct 0.10 --score-down-rank-penalty-pct 0.15 --score-down-rank-penalty-pct 0.20 `
  --score-down-severity-profile conservative --score-down-severity-profile balanced `
  --score-down-context-profile rank_decay_light --score-down-context-profile rank_decay_balanced `
  --score-down-ranking-date-mode previous `
  --include-size-bucket-rules --include-loss-history-rules `
  --rule-key loss_reports_ge_4_mv_lt_10bn_ex_ge50_loss `
  --rule-key loss_reports_ge_4_mv_lt_10bn `
  --rule-key forecast_loss_reports_ge_4_mv_lt_10bn `
  --rule-key loss_to_market_cap_ge_50pct_mv_lt_10bn `
  --no-overlay-csv

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py scripts/financial_distress_true_qe_shortlist.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal -q
rg -n "financial_distress_phase22|phase22_tail_control|TAIL_CONTROL" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| tail-control overlay command          | pass                                                         |
| validations                           | 704                                                          |
| stability rows                        | 32                                                           |
| best high-average row                 | +0.178% avg but -1.966% worst                                |
| fixed_5 tail-control row              | +0.120% avg and -0.935% worst                                |
| ctx_light tail-control row            | +0.111% avg, 17/22 positive, -0.935% worst                   |
| clean benchmark row                   | +0.122% avg and -0.174% worst                                |
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
| no loss-history WSL true rerun        | tail still too large after softer profiles                   |
| no runtime integration                | research-only docs and ignored reports only                  |
| next research                         | benchmark smoke or cleaner signal-family screen              |
| production 8001                       | not touched                                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Residual Risks

- The overlay remains a candidate-list simulation and is not a full Qlib portfolio rerun.
- The clean benchmark row has sparse replacement count and does not prove a hard-risk rule.
- More loss-history parameter tuning may overfit; next research should pivot unless a new hypothesis is introduced.
