# L2 Event Signal Financial Distress Score-Down QE Overlay Validation - 2026-05-08

## Scope

Research-only event-signal validation. The change is limited to `backend/services/event_signal`, tests, and analysis docs. No QE, Selection Center, Paper Trading, QMT, or live-trading runtime integration was changed.

## Commands

```text
python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 14 passed

python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
# 144 passed

rg -n "score_down_rank|run_score_down_rerank|financial_distress_score_down|score-down" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
# no runtime references

git diff --check
# passed; line-ending warnings only
```

## Research Runs

```text
WSL env: rdagent-gpu
DB env : TDX_DB_HOST=127.0.0.1, TDX_DB_NAME=aistock, TDX_DB_USER=postgres
Loops  : 10 existing QE loop artifacts
Dates  : 2024-07-01 -> 2026-04-27
Rule   : loss_to_market_cap_ge_50pct_mv_lt_10bn
Mode   : score_down, TopK=50, previous-date predictions
Output : reports/event_signal/financial_distress_score_down_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_113409.json
Output : reports/event_signal/financial_distress_score_down_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_113409.md

Probe  : current-date ranking alignment check
Output : reports/event_signal/financial_distress_score_down_qe_overlay_current_date_probe/financial_distress_qe_multiloop_20240701_20260508_113710.json
Output : reports/event_signal/financial_distress_score_down_qe_overlay_current_date_probe/financial_distress_qe_multiloop_20240701_20260508_113710.md
```

## Business Result

```text
+----------------------------+----------------------------------------------------------------------------+
| Check                      | Result                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Hard no-buy/force-sell     | Not introduced; financial distress remains research-only.                  |
| Score-down implementation  | Added offline TopK-relative rank demotion simulator.                       |
| Main finding               | 20% TopK demotion is mildly positive across 6/10 loops but small in size.  |
| Over-aggressive setting    | 50% demotion becomes negative for 120/242 td windows.                      |
| Runtime isolation          | Guardrail scan found no references in QE/Paper/Selection/QMT runtime.      |
+----------------------------+----------------------------------------------------------------------------+
```

## Residual Risks

```text
+----------------------------+----------------------------------------------------------------------------+
| Risk                       | Status                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Offline approximation      | Still not a full QE strategy rerun; do not use as production policy.       |
| Candidate date alignment   | Previous-date mode is more aligned; current-date probe documented.         |
| Generated reports          | Under reports/ and intentionally not committed unless curated separately.  |
| Production services        | Production backend 8001 was not restarted or touched.                      |
+----------------------------+----------------------------------------------------------------------------+
```
