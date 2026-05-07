# Financial Structured Event Signal Study Validation

- Module: local_data_management / event_signal
- Level: L2
- Date: 2026-05-06T23:51:45+08:00
- Worktree: `F:\Dev\AIstock_worktrees\event-signal-st-llm-design-20260506`
- Branch: `codex/event-signal-st-llm-design-20260506`
- Production backend impact: none; port `8001` was not restarted or touched.

## Scope

- Added offline financial event-study script `backend/services/event_signal/financial_event_study.py`.
- Added regression tests `backend/tests/event_signal/test_financial_event_study.py`.
- Validated existing structured Tushare financial raw data and financial event signals in DB.
- Ran a recent full-window aggregate study for 2024-01-01 through 2026-05-06 without writing large detail CSV artifacts.
- Out of scope: QE, Selection Center, Paper v2, QMT, simulated/live trading consumers, frontend UI, LLM/PDF analysis, and schema changes.

## Business Goal

- Verify that Tushare structured financial signals can be studied independently before any trading integration.
- Keep negative/risk events as warnings only, and keep positive growth as `record_only` until event-study evidence is strong enough for a later alpha phase.
- Prove that financial event research can use the same `market.event_signal` source as future backtest/paper/live consumers without changing those consumers now.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Financial study calculation | Point windows and cumulative windows compute raw/benchmark/abnormal returns and carry suspend/down-limit/missing flags | `pytest backend/tests/event_signal/test_financial_event_study.py -q` | PASS |
| Financial signal tests | Existing structured financial adapters/backfill/raw sync/time semantics remain valid | 44-test financial/event suite | PASS |
| DB coverage | Raw Tushare financial tables and unified financial event signals are populated through 2026Q1 / latest available ann_date | DB count query below | PASS |
| Recent financial event study | 2024-01-01 to 2026-05-06 aggregate report generated from `market.event_signal` | report paths below | PASS |
| Artifact control | Full recent study writes JSON/Markdown only, no large detail CSV by default | `output_csv=None` | PASS |
| Isolation | No QE/Selection/Paper/QMT consumer code changed or referenced by the new study | path/status review | PASS |

## Commands

```powershell
$env:PYTHONIOENCODING='utf-8'
pytest backend/tests/event_signal/test_financial_event_study.py -q
pytest backend/tests/event_signal/test_financial_event_adapter.py backend/tests/event_signal/test_financial_event_backfill.py backend/tests/event_signal/test_tushare_event_raw_sync.py backend/tests/event_signal/test_tushare_event_raw_schema.py backend/tests/event_signal/test_time_semantics.py backend/tests/event_signal/test_financial_event_study.py -q
python -m py_compile backend\services\event_signal\financial_event_study.py

# DB count query via backend.db.pg_pool after load_dotenv(F:/Dev/AIstock/.env)
# Recent full-window event study, aggregate only:
from backend.services.event_signal.financial_event_study import run_event_study
summary = run_event_study(start_date=date(2024,1,1), end_date=date(2026,5,6), output_dir=Path('reports/event_signal/financial'), write_details=False)
```

## Automated Test Results

- `test_financial_event_study.py`: `4 passed in 0.39s`.
- Financial/event regression slice: `44 passed in 1.05s`.
- `py_compile`: passed for `backend/services/event_signal/financial_event_study.py`.

## DB Coverage Evidence

Raw table coverage:

| Raw table | Rows | Ann date range | Report period range |
|---|---:|---|---|
| `market.tushare_forecast_raw` | 66,837 | 2018-04-10 to 2026-04-29 | 2018-06-30 to 2026-03-31 |
| `market.tushare_express_raw` | 14,114 | 2018-07-03 to 2026-04-28 | 2018-06-30 to 2026-03-31 |
| `market.tushare_fina_indicator_raw` | 302,327 | 2018-07-10 to 2026-05-07 | 2018-06-30 to 2026-03-31 |

Unified financial event facts:

| Source type | Fact rows | Source event date range | Report period range |
|---|---:|---|---|
| `tushare_forecast` | 53,932 | 2018-04-10 to 2026-04-29 | 2018-06-30 to 2026-03-31 |
| `tushare_express` | 14,114 | 2018-07-03 to 2026-04-28 | 2018-06-30 to 2026-03-31 |
| `tushare_fina_indicator` | 186,663 | 2018-07-10 to 2026-05-01 | 2018-06-30 to 2026-03-31 |

Active financial event signals under `unified_event_signal_rules_v0_20260506`: `145,571`.

| Signal event type | Rows | Treatment |
|---|---:|---|
| `financial_forecast_loss` | 15,107 | P2 `warn_review` |
| `financial_forecast_large_decline` | 9,934 | P2 `warn_review` |
| `financial_forecast_large_growth` | 13,798 | P3 `record_only` |
| `financial_forecast_turnaround` | 5,288 | P3 `record_only` |
| `financial_express_loss` | 1,884 | P2 `warn_review` |
| `financial_express_large_decline` | 597 | P2 `warn_review` |
| `financial_express_large_growth` | 11,589 | P3 `record_only` |
| `financial_indicator_large_decline` | 36,613 | P2 `warn_review` |
| `financial_indicator_large_growth` | 44,141 | P3 `record_only` |
| `financial_positive_but_miss_expectation` | 6,620 | P2 `warn_review` |

## Event Study Evidence

Recent full-window report:

- Report id: `financial_event_study_20240101_20260506_full_20260506_235123`.
- Output JSON: `reports/event_signal/financial/financial_event_study_20240101_20260506_full_20260506_235123.json`.
- Output Markdown: `reports/event_signal/financial/financial_event_study_20240101_20260506_full_20260506_235123.md`.
- Output CSV: none by default.
- Active signal rows in scope: `45,512`.
- Deduped events used: `45,489`.
- Detail rows aggregated in memory: `454,890`.
- Price key count: `895,900`.

Recent scope event counts:

| Event type | Deduped events |
|---|---:|
| `financial_forecast_loss` | 5,573 |
| `financial_forecast_large_decline` | 2,163 |
| `financial_forecast_large_growth` | 3,055 |
| `financial_forecast_turnaround` | 1,619 |
| `financial_express_loss` | 804 |
| `financial_express_large_decline` | 162 |
| `financial_express_large_growth` | 3,268 |
| `financial_indicator_large_decline` | 13,540 |
| `financial_indicator_large_growth` | 13,578 |
| `financial_positive_but_miss_expectation` | 1,727 |

Selected sanity metrics:

| Event type | Window | Mean raw | Median raw | Negative rate | Notes |
|---|---|---:|---:|---:|---|
| `financial_forecast_loss` | T0 | -1.2440% | -1.0292% | 62.29% | Loss forecasts show same-day negative reaction. |
| `financial_forecast_large_decline` | T0 | -0.9669% | -0.7500% | 61.20% | Large-decline forecasts show same-day negative reaction. |
| `financial_express_loss` | T0_T20 | -3.3398% | -6.1272% | 66.99% | Express loss remains negative over 20 trading days in recent sample. |
| `financial_positive_but_miss_expectation` | T0_T20 | +2.3141% | +0.1475% | 49.41% | Not yet strong enough for hard risk; keep `warn_review` and refine. |
| `financial_forecast_large_growth` | T0 | +0.3602% | 0.0000% | 49.13% | Positive, but weak; keep `record_only`. |
| `financial_indicator_large_growth` | T0 | +0.7836% | +0.5004% | 41.62% | Positive candidate requires broader event study before alpha use. |

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| First financial study smoke failed with missing temp table | Temp table was created `ON COMMIT DROP`; the project DB connection autocommits between statements | Changed temp table to preserve rows for the connection lifetime | Re-ran financial study smoke and recent full-window report successfully |
| Initial limited report id did not show that `limit=2000` was applied | Report id used only date scope | Added `limit` to summary payload and report id scope | `financial_event_study_all_all_limit2000_20260506_234924` generated successfully |

## Result

- Final status for financial structured event-study slice: PASS.
- Need production backend restart: no.
- Need dev service restart: no.
- Remaining risks:
  - Recent 2024-2026 study is useful validation but not enough to enable positive alpha boosts.
  - `financial_positive_but_miss_expectation` needs threshold and relation refinement before stronger risk actions.
  - Full 2018-2026 financial event study should be run in a batched/aggregate mode before any consumer integration to avoid huge detail artifacts.
