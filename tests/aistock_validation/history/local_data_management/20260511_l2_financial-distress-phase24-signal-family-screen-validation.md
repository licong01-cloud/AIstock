# L2 Validation - Financial Distress Phase 24 Signal Family Screen - 2026-05-11

## Scope

```text
+----------------------+--------------------------------------------------------------+
| item                 | value                                                        |
+----------------------+--------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                     |
| phase                | 24                                                           |
| module               | event_signal financial-distress research                     |
| runtime impact       | none                                                         |
| production 8001      | not touched                                                  |
| report               | phase24_signal_family_screen_result_20260510                 |
+----------------------+--------------------------------------------------------------+
```

Report path: `docs/analysis/event_signal_financial_distress_phase24_signal_family_screen_result_20260510.md`.

## Business Goal

Search for stronger structured financial-risk signal families after Phase 23 showed the clean small-cap loss/mv benchmark is too weak in one-loop true QE smoke. This phase must stay research-only: no DB writes, no hard buy-ban, no forced sell, no alpha boost, and no QE/Paper/Selection/QMT runtime wiring.

## Commands

```powershell
$env:PYTHONPATH=(Get-Location).Path
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/financial_distress_phase24_signal_family_screen.py

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile `
  scripts/financial_distress_phase24_signal_family_screen.py `
  backend/services/event_signal/early_financial_distress_research.py `
  backend/services/event_signal/financial_distress_qe_overlay_research.py `
  backend/services/event_signal/financial_distress_direct_event_research.py

C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal -q

rg -n "phase24|PHASE24|indicator_decline_ocf_negative_or_leverage|expectation_miss_gap_ge_100" `
  backend/services/selection_center backend/services/paper_trading_v2 `
  backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S

git diff --check
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| phase24 screen command                | pass                                                         |
| direct event report                   | 12 rules, 36 rule/window rows                                |
| 22-loop cheap overlay                 | 1584 validations, 72 stability rows                          |
| combined shortlist                    | 12 rows; no TRUE_QE_CANDIDATE                                |
| best cheap row                        | ocf_negative_or_leverage, 60td fixed_10, avg +0.131%         |
| best cheap row tail                   | worst loop -0.183%, ex-best +0.047%                          |
| direct downside                       | many rules have negative T+20 abnormal median                |
| py_compile                            | pass                                                         |
| event_signal pytest suite             | 168 passed                                                   |
| runtime isolation scan                 | no runtime references found                                  |
| git diff --check                       | pass, LF/CRLF warnings only                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Key Evidence

```text
+--------------------------------------------------------+----------------+--------+---------+--------+------+---------------------+
| rule                                                   | best mode      | avg    | ex_best | min    | drop | direct              |
+--------------------------------------------------------+----------------+--------+---------+--------+------+---------------------+
| indicator_decline_ocf_negative_or_leverage_mv_ge_10bn  | 60td fixed_10  | +0.13% | +0.05%  | -0.18% | 15   | supports_downweight |
| indicator_decline_profit_revenue_diverge_mv_ge_10bn    | 60td fixed_10  | +0.11% | +0.03%  | -0.00% | 6    | mixed               |
| indicator_decline_current_ratio_lt_1_mv_ge_10bn        | 60td fixed_20  | +0.12% | +0.03%  | -0.13% | 6    | supports_downweight |
| expectation_miss_gap_ge_50_actual_indicator_mv_ge_10bn | 120td fixed_20 | -0.01% | -0.02%  | -0.37% | 8    | supports_downweight |
+--------------------------------------------------------+----------------+--------+---------+--------+------+---------------------+
```

## Business Outcome

- The best new family is `indicator_decline_ocf_negative_or_leverage_mv_ge_10bn`: it has direct T+20/T+60 downside and the best cheap overlay score, but still does not meet the strict WSL true-QE gate.
- Expectation-miss rules show clear direct downside, but current QE top50 interaction is too weak; keep as research/watchlist rather than runtime policy.
- The phase confirms structure-first research remains useful; LLM/PDF should still wait until structured sources stop improving or need explanation fields.
- No runtime code path was wired; all changes remain in event-signal research modules, scripts, docs, and tests.

## Artifacts

```text
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| artifact             | path                                                                                                                                       |
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
| curated report       | docs/analysis/event_signal_financial_distress_phase24_signal_family_screen_result_20260510.md                                               |
| summary json         | reports/event_signal/financial_distress_phase24_signal_family_screen/financial_distress_phase24_signal_family_screen.json                   |
| direct event json    | reports/event_signal/financial_distress_phase24_signal_family_screen/direct/financial_distress_direct_event_20240701_20260511_001050.json   |
| overlay json         | reports/event_signal/financial_distress_phase24_signal_family_screen/overlay/financial_distress_qe_multiloop_20240701_20260511_005309.json |
+----------------------+--------------------------------------------------------------------------------------------------------------------------------------------+
```

## Residual Risk

- Cheap overlay is still a shortlist gate, not final PnL evidence; no candidate should be promoted without WSL true-QE rerun.
- Direct event results use abnormal returns from available local prices; missing-price rates become material for T+60 and should be treated cautiously.
- The report file is dated 20260510 because the research phase started then; validation completed after midnight on 2026-05-11.
