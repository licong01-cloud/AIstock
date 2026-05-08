# L2 Event Signal Financial Distress Exposure Diagnostics Validation - 2026-05-08

## Scope

Research-only exposure diagnostics for financial-distress score-down overlays. The change remains limited to `backend/services/event_signal`, tests, and analysis docs. It does not integrate with QE, Selection Center, Paper Trading, QMT, or live-trading runtime.

## Commands

```text
python -m py_compile backend/services/event_signal/financial_distress_qe_overlay_research.py
# passed

python -m pytest backend/tests/event_signal/test_financial_distress_qe_overlay_research.py -q
# 17 passed

python -m pytest backend/tests/test_unified_event_signal_schema.py backend/tests/event_signal -q
# 147 passed

rg -n "score_down_evaluated_by|Score-Down Hit Exposure|financial_distress_exposure" backend/services/selection_center backend/services/paper_trading_v2 backend/services/quantevolver backend/infra/qmt_client.py backend/routers/qmt.py -S
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
Modes  : fixed 20%, severity balanced, severity conservative
Output : reports/event_signal/financial_distress_exposure_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_152103.json
Output : reports/event_signal/financial_distress_exposure_qe_overlay/financial_distress_qe_multiloop_20240701_20260508_152103.md
```

## Business Result

```text
+----------------------------+----------------------------------------------------------------------------+
| Check                      | Result                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Runtime isolation          | Guardrail scan found no references in QE/Paper/Selection/QMT runtime.      |
| Market-cap exposure        | Almost all dropped Top50 names are <5bn market-cap stocks.                 |
| Industry exposure          | Drops concentrate in logistics, real estate, textile, chemical materials.  |
| Overfit risk               | High; dropped counts are sparse, so no hard industry filters are justified.|
| Next research              | Add next structured signal family and keep exposure diagnostics mandatory. |
+----------------------------+----------------------------------------------------------------------------+
```

## Residual Risks

```text
+----------------------------+----------------------------------------------------------------------------+
| Risk                       | Status                                                                     |
+----------------------------+----------------------------------------------------------------------------+
| Sparse samples             | Only 5-11 drops across 10 loops per window; conclusions remain tentative.  |
| Offline approximation      | Still not a full QE strategy rerun; do not use as production policy.       |
| Generated reports          | Under reports/ and intentionally not committed unless curated separately.  |
| Production services        | Production backend 8001 was not restarted or touched.                      |
+----------------------------+----------------------------------------------------------------------------+
```
