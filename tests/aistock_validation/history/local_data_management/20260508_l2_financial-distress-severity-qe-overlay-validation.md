# L2 Event Signal Financial Distress Severity Score-Down QE Overlay Validation - 2026-05-08

## Scope

Research-only follow-up validation for dynamic severity-based score-down. The change remains limited to `backend/services/event_signal`, tests, and analysis docs. It does not integrate with QE, Selection Center, Paper Trading, QMT, or live-trading runtime.

## Commands

```text
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py backend/tests/event_signal/test_financial_distress_qe_overlay_research.py
# passed

python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 17 passed

python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
# 147 passed

rg -n "score_down_severity|SeverityProfile|financial_distress_severity" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
# no runtime references

git diff --check
# passed; line-ending warnings only
```

## Research Run

```text
WSL env: rdagent-gpu
DB env : TDX_DB_HOST=127.0.0.1, TDX_DB_NAME=aistock, TDX_DB_USER=postgres
Loops  : 10 existing QE loop artifacts
Dates  : 2024-07-01 -> 2026-04-27
Rule   : loss_to_market_cap_ge_50pct_mv_lt_10bn
Mode   : score_down_severity, TopK=50, previous-date predictions
Profiles: balanced, conservative, loss_heavy
Output : reports/event_signal/financial_distress_severity_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_150123.json
Output : reports/event_signal/financial_distress_severity_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_150123.md
```

## Business Result

```text
+----------------------------+----------------------------------------------------------------------------+
| Check                      | Result                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Hard no-buy/force-sell     | Not introduced; financial distress remains research-only.                  |
| Severity implementation    | Added variable per-symbol rank penalty based on loss/size/loss history.    |
| Main finding               | Balanced/conservative match fixed 20%, but do not clearly improve it.      |
| Weak profile               | Loss-heavy is too weak for this rule and gives smaller return delta.        |
| Runtime isolation          | Guardrail scan found no references in QE/Paper/Selection/QMT runtime.      |
+----------------------------+----------------------------------------------------------------------------+
```

## Residual Risks

```text
+----------------------------+----------------------------------------------------------------------------+
| Risk                       | Status                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Offline approximation      | Still not a full QE strategy rerun; do not use as production policy.       |
| Severity curve overfit     | Profiles are initial research hypotheses, not trained parameters.          |
| Generated reports          | Under reports/ and intentionally not committed unless curated separately.  |
| Production services        | Production backend 8001 was not restarted or touched.                      |
+----------------------------+----------------------------------------------------------------------------+
```
