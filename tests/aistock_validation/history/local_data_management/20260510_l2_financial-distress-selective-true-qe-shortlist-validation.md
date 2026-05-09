# L2 Validation - Financial Distress Selective True QE Shortlist - 2026-05-10

## Scope

```text
+----------------------+--------------------------------------------------------------+
| item                 | value                                                        |
+----------------------+--------------------------------------------------------------+
| branch               | codex/financial-distress-rerank-20260508                     |
| phase                | 20                                                           |
| module               | event_signal financial-distress research                     |
| runtime impact       | none                                                         |
| production 8001      | not touched                                                  |
| report               | selective_true_qe_shortlist_20260510                         |
+----------------------+--------------------------------------------------------------+
```

Report path: `docs/analysis/event_signal_financial_distress_selective_true_qe_shortlist_20260510.md`.

## Business Goal

Screen existing offline overlay and direct-event artifacts before spending WSL full-universe true QE rerun time. This phase must not promote any rule into QE/Paper/Selection/QMT runtime and must not create a hard buy ban or forced-sell policy.

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/financial_distress_true_qe_shortlist.py --output-dir reports/event_signal/financial_distress_selective_true_qe_shortlist --doc-path docs/analysis/event_signal_financial_distress_selective_true_qe_shortlist_20260510.md
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile scripts/financial_distress_true_qe_shortlist.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/event_signal -q
rg -n "financial_distress_true_qe_shortlist|selective_true_qe_shortlist|WSL_TRUE_RERUN_NOW" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+--------------------------------------------------------------+
| check                                | result                                                       |
+--------------------------------------+--------------------------------------------------------------+
| shortlist script generation           | pass                                                         |
| multiloop reports scanned             | 13 latest ignored reports under reports/event_signal         |
| candidate stability rows              | 343                                                          |
| strict WSL_TRUE_RERUN_NOW rows         | 0                                                            |
| py_compile                            | pass                                                         |
| focused financial distress pytest      | 35 passed                                                    |
| event_signal pytest suite              | 164 passed                                                   |
| runtime isolation scan                 | no runtime references added                                  |
| git diff --check                       | pass, LF/CRLF warnings only                                  |
+--------------------------------------+--------------------------------------------------------------+
```

## Business Outcome

```text
+--------------------------------------+--------------------------------------------------------------+
| outcome                              | evidence                                                     |
+--------------------------------------+--------------------------------------------------------------+
| no immediate WSL batch rerun          | no candidate passed strict cheap gate                        |
| current WSL-tested rule remains weak  | indicator_large_decline_mv_10_30bn stays research-only       |
| next empirical step                   | expand top 10-loop candidates to 22-loop cheap overlay first |
| hard-risk policy remains rejected     | benchmark direct T+5/T+20 abnormal medians are positive      |
+--------------------------------------+--------------------------------------------------------------+
```

## Residual Risks

- The shortlist score is a research triage heuristic, not a trading metric.
- Several promising rows only have 10-loop overlay evidence and must not be promoted without 22-loop expansion.
- Ignored raw reports under `reports/event_signal` are not committed; curated conclusions are preserved in docs and this validation record.
