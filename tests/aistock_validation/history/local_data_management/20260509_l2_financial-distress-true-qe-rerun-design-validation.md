# 2026-05-09 L2 Validation - Financial Distress True QE Rerun Design

## Scope

```text
+------------------+---------------------------------------------------------------------+
| item             | value                                                               |
+------------------+---------------------------------------------------------------------+
| branch           | codex/financial-distress-rerank-20260508                            |
| phase            | Phase 17                                                            |
| validation level | L2 research harness design + unit validation                         |
| runtime impact   | no QE/Paper/Selection/QMT/runtime integration                        |
| production 8001  | not touched                                                         |
| report           | docs/analysis/event_signal_financial_distress_true_qe_rerun_design_20260509.md |
+------------------+---------------------------------------------------------------------+
```

## Checks Run

```powershell
python -m py_compile backend/services/event_signal/financial_distress_pred_materializer.py backend/tests/event_signal/test_financial_distress_pred_materializer.py
python -m pytest backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_pred_materializer.py -q
python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
rg -n "financial_distress_pred_materializer|true_qe_rerun|rank_decay_balanced|indicator_large_decline_mv_10_30bn" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
git diff --check
```

## Results

```text
+--------------------------------------+---------------------------------------------+
| check                                | result                                      |
+--------------------------------------+---------------------------------------------+
| py_compile                           | pass                                        |
| pred materializer targeted tests     | 4 passed                                    |
| financial distress focused tests     | 35 passed                                   |
| full event_signal module tests       | 170 passed                                  |
| previous-date mapping                | verified T signal maps to T-1 pred date     |
| score materialization                | verified penalized TopK name drops from TopK|
| file artifact writer                 | verified pkl/csv/json/md files are written  |
| runtime isolation scan               | no matches in QE/Paper/Selection/QMT paths  |
| diff whitespace check                | passed; LF/CRLF warnings only               |
| production 8001                      | not touched                                 |
+--------------------------------------+---------------------------------------------+
```

## Business Outcome

```text
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| outcome                              | status                     | evidence                                                     |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
| safe true-rerun path identified       | PASS                       | qrun_limit_minute.py already supports --pred-backtest        |
| event overlay can affect Qlib ranks   | PASS                       | adjusted pred.pkl changes score order on prediction date     |
| original loop artifacts protected     | PASS_BY_DESIGN             | command shape requires copied loop workspace                 |
| runtime promotion                     | REJECTED                   | Phase 16 robustness gate still blocks integration            |
| DB policy persistence                 | DEFERRED                   | true rerun batch not completed yet                           |
+--------------------------------------+----------------------------+--------------------------------------------------------------+
```

## Residual Risks

```text
+----------------------------+---------------------------------------------------------------+
| risk                       | mitigation                                                     |
+----------------------------+---------------------------------------------------------------+
| score-weighted sizing drift| run copied-loop baseline and compare before/after deltas       |
| environment drift          | rerun original pred.pkl and adjusted pred.pkl in same copy      |
| batch runtime cost         | start with one loop smoke, then expand to 22-loop batch         |
| no runtime audit table     | keep trace.csv/meta.json until DB persistence is justified      |
+----------------------------+---------------------------------------------------------------+
```
