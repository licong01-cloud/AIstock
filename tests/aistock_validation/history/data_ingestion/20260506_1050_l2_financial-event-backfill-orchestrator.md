# Financial event backfill orchestrator

- Module: data_ingestion / event_signal
- Level: L2
- Date: 2026-05-06T10:50:00+08:00
- Git commit: pending at validation time
- Operator: Codex

## Scope

- Changed files: `backend/services/event_signal/financial_event_backfill.py`, `scripts/backfill_financial_event_signals.py`, and `backend/tests/event_signal/test_financial_event_backfill.py`.
- Impacted flows: source-only Tushare financial raw backfill and derived unified financial event fact/relation/signal generation.
- Business goal: provide one repeatable CLI/service to backfill historical Tushare `forecast`, `express`, and `fina_indicator` raw rows by report period, then generate unified non-daily event signals without touching trading consumers.
- Historical data scope: 2018-08-01 baseline through 2026-05-06, mapped to report periods 20180630 through 20260331.
- Out of scope: QE, RD-Agent, Selection Center, Paper Trading v2, simulated trading, QMT, live trading, frontend UI, and scheduled incremental automation.
- Protected assets reviewed: no StrategyPackage, model, Qlib, HMM, QE workspace, Paper ledger, or validated execution policy files modified.

## Environment

- Backend port: not started; production `8001` not restarted.
- Frontend port: not started.
- TDX port: not started by this validation.
- Database: local PostgreSQL configured from `F:\Dev\AIstock\.env`.
- Network/API: Tushare VIP APIs used for one-period raw smoke only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Period planning | 2018-08-01 baseline includes 2018H1 and advances by quarter | `pytest backend/tests/event_signal/test_financial_event_backfill.py` | PASS |
| Raw orchestration | Selected raw datasets are synced before signal generation and mapped to source types | unit fake service test | PASS |
| Failure semantics | Failed periods are recorded; `stop-on-error` stops after first failure | unit fake failure tests | PASS |
| Existing event rules | Financial adapter/raw/schema announcement regression remains green | targeted event-signal pytest suite | PASS |
| DB smoke, derived only | Existing 20231231 raw rows regenerate unified signals | CLI `--period 20231231 --skip-raw` | PASS |
| DB smoke, raw + derived | Tushare 20230930 raw rows sync and derived signals generate | CLI `--period 20230930` | PASS |
| Historical backfill | 32 report periods sync raw rows and generate derived facts/signals without failed periods | CLI `--start-date 2018-08-01 --end-date 2026-05-06` plus DB aggregate checks | PASS |

## Commands

```powershell
git diff --check
python -m py_compile backend\services\event_signal\financial_event_backfill.py scripts\backfill_financial_event_signals.py backend\tests\event_signal\test_financial_event_backfill.py
pytest backend\tests\event_signal\test_financial_event_backfill.py backend\tests\event_signal\test_financial_event_adapter.py backend\tests\event_signal\test_tushare_event_raw_sync.py backend\tests\event_signal\test_tushare_event_raw_schema.py backend\tests\test_unified_event_signal_schema.py -q -p no:cacheprovider
$script | python -  # loads F:\Dev\AIstock\.env, then runs scripts/backfill_financial_event_signals.py --period 20231231 --skip-raw --run-mode smoke --time-mode backtest
$script | python -  # loads F:\Dev\AIstock\.env, then runs scripts/backfill_financial_event_signals.py --period 20230930 --run-mode smoke --time-mode backtest
$script | python -  # loads F:\Dev\AIstock\.env, then runs scripts/backfill_financial_event_signals.py --start-date 2018-08-01 --end-date 2026-05-06 --run-mode backfill --time-mode backtest
$script | python -  # DB aggregate verification for raw/fact/signal/run counts
```

## Evidence

- `git diff --check`: PASS.
- `py_compile`: PASS.
- Targeted pytest: `33 passed in 3.53s`.
- 20231231 derived-only smoke: `processed_rows=11398`, `fact_rows=11398`, `relation_rows=399`, `signal_rows=7570`, `failed_periods=0`.
- 20230930 raw+derived smoke: forecast `fetched_rows=751`, `written_rows=389`, `skipped_rows=362`; express `fetched_rows=30`, `written_rows=30`; fina_indicator `fetched_rows=7096`, `written_rows=7096`; derived `processed_rows=6407`, `fact_rows=6407`, `relation_rows=26`, `signal_rows=2861`, `failed_periods=0`.
- Full historical backfill: `success_periods=32`, `failed_periods=0`, periods `20180630` through `20260331`.
- Raw table aggregate after full backfill: `forecast=66837 rows/32 periods`, `express=14114 rows/32 periods`, `fina_indicator=302276 rows/32 periods`.
- Derived fact aggregate after latest-row dedupe: `tushare_forecast=53932`, `tushare_express=14114`, `tushare_fina_indicator=186663`, all covering 32 periods.
- Derived signal aggregate: `P2_REVIEW warn_review=70755` including `financial_relation=6620`; `P3_POSITIVE_CANDIDATE record_only=74816`; total financial event signals `145571`.
- Backfill run ledger: 32 `SUCCESS` financial backfill runs, `source_input_rows=254709`, `fact_rows=254709`, `relation_rows=6620`, `signal_rows=145571`.
- API calls: no FastAPI service calls; no backend/frontend service restart.
- Business output summary: historical backfill can now be run per period/range with optional `--skip-raw`, `--skip-signals`, `--dataset`, `--max-periods`, and `--stop-on-error`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this validation slice | Not applicable | Not applicable | All targeted commands passed |

## Result

- Final status: PASS for implemented L2 scope.
- Remaining risks: event-signal consumers are intentionally not wired yet; scheduled incremental automation for these financial sources is not implemented in this slice; future Tushare API changes may require chunked reruns.
- Need production backend restart: no.
- Need dev service restart: no.
