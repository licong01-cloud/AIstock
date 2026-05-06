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
- Out of scope: QE, RD-Agent, Selection Center, Paper Trading v2, simulated trading, QMT, live trading, frontend UI, and full historical production backfill.
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

## Commands

```powershell
git diff --check
python -m py_compile backend\services\event_signal\financial_event_backfill.py scripts\backfill_financial_event_signals.py backend\tests\event_signal\test_financial_event_backfill.py
pytest backend\tests\event_signal\test_financial_event_backfill.py backend\tests\event_signal\test_financial_event_adapter.py backend\tests\event_signal\test_tushare_event_raw_sync.py backend\tests\event_signal\test_tushare_event_raw_schema.py backend\tests\test_unified_event_signal_schema.py -q -p no:cacheprovider
$script | python -  # loads F:\Dev\AIstock\.env, then runs scripts/backfill_financial_event_signals.py --period 20231231 --skip-raw --run-mode smoke --time-mode backtest
$script | python -  # loads F:\Dev\AIstock\.env, then runs scripts/backfill_financial_event_signals.py --period 20230930 --run-mode smoke --time-mode backtest
```

## Evidence

- `git diff --check`: PASS.
- `py_compile`: PASS.
- Targeted pytest: `33 passed in 3.53s`.
- 20231231 derived-only smoke: `processed_rows=11398`, `fact_rows=11398`, `relation_rows=399`, `signal_rows=7570`, `failed_periods=0`.
- 20230930 raw+derived smoke: forecast `fetched_rows=751`, `written_rows=389`, `skipped_rows=362`; express `fetched_rows=30`, `written_rows=30`; fina_indicator `fetched_rows=7096`, `written_rows=7096`; derived `processed_rows=6407`, `fact_rows=6407`, `relation_rows=26`, `signal_rows=2861`, `failed_periods=0`.
- API calls: no FastAPI service calls; no backend/frontend service restart.
- Business output summary: historical backfill can now be run per period/range with optional `--skip-raw`, `--skip-signals`, `--dataset`, `--max-periods`, and `--stop-on-error`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| None in this validation slice | Not applicable | Not applicable | All targeted commands passed |

## Result

- Final status: PASS for implemented L2 scope.
- Remaining risks: full 2018-08-01 through latest historical backfill has not yet been run in this validation record; Tushare rate limits/API availability may require chunked reruns.
- Need production backend restart: no.
- Need dev service restart: no.
