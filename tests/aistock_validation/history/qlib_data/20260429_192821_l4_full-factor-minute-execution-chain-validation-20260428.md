# Full factor minute execution chain validation 20260428

- Module: qlib_data
- Level: L4
- Date: 2026-04-29T19:28:21
- Git commit: 178a0b7
- Operator: lc999

## Scope

- Changed files: `scripts/qlib_full_factor_minute_chain_validate.py`
- Impacted flows: Qlib candidate daily provider, snapshot factor datasets, DB minute export, generated small-stock 1min Qlib bin, Qlib NestedExecutor minute execution.
- Business goal: validate the 2026-04-28 candidate with all available factor datasets over the full daily range and run a full-date small-stock minute execution chain over all DB minute dates.
- Out of scope: full-market minute bin export, V24/V25 model execution, production dataset replacement.
- Protected assets reviewed: only candidate output dirs were created; production Qlib/RD-Agent/QE artifacts were not overwritten.

## Environment

- Backend port: not used
- Frontend port: not used
- TDX port: not used
- Conda/env: Windows Python for DB export; WSL Ubuntu conda `rdagent-gpu` for dump_bin/Qlib backtest
- Database: PostgreSQL `aistock`, read-only queries to `market.kline_minute_raw`, `market.adj_factor`, `market.stk_limit`, `market.trading_calendar`, `market.suspend_d`, `market.kline_daily_raw`
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Script syntax | Validation script compiles | `python -m py_compile scripts/qlib_full_factor_minute_chain_validate.py` | PASS |
| DB minute export | Selected stocks cover all trading dates with 240/241 valid bars and no missing stock-date | `minute_export_summary.json` | PASS |
| Qlib minute provider | Generated 1min bin loads all selected minute bars with no NaN in required execution fields | `report.json` | PASS |
| Full factor coverage | Daily bin + static/aux factor datasets load for full daily range and coverage is summarized by source | `full_daily_factor_source_summary.csv` | PASS |
| Minute execution chain | Qlib `NestedExecutor(day -> 1min)` with inner `TWAPStrategy` completes 560-day full-date run | `minute_chain_portfolio_report.csv` | PASS |
| Daily missing classification | Full-calendar daily missing rows are suspended days, not unexplained data loss | `daily_missing_db_crosscheck_summary.json` | PASS |
| Asset safety | New candidate dirs only; old datasets untouched | output paths | PASS |

## Commands

```bash
python -m py_compile scripts/qlib_full_factor_minute_chain_validate.py

$env:TDX_DB_PASSWORD='lc78080808'
$env:PYTHONUNBUFFERED='1'
$env:PYTHONIOENCODING='utf-8'
python scripts/qlib_full_factor_minute_chain_validate.py --stage all --overwrite --topk 5 --drop 2

wsl -d Ubuntu -- bash -lc "source /home/lc999/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && export PYTHONUNBUFFERED=1 && python /mnt/f/Dev/AIstock/scripts/qlib_full_factor_minute_chain_validate.py --stage backtest --codes 000001.SZ,000063.SZ,000333.SZ,000651.SZ,000858.SZ,600000.SH,600036.SH,600519.SH,601318.SH,601688.SH --daily-start 2018-08-01 --minute-start 2024-01-02 --end 2026-04-28 --output-root /mnt/f/Dev/AIstock/qlib_minute_validation/full_factor_minute_chain_20260428_candidate --reports-dir /mnt/f/Dev/AIstock/reports/qlib_full_factor_minute_chain_20260428 --daily-provider-wsl /home/lc999/data/qlib_bin_20260428_shsz_candidate --snapshot-dir-wsl /mnt/f/Dev/AIstock/qlib_snapshots/qlib_20260428_shsz_candidate --account 10000000 --topk 5 --drop 2 --overwrite"
```

## Evidence

- API calls: not used
- DB checks:
  - `reports/qlib_full_factor_minute_chain_20260428/minute_db_stock_date_bars.csv`
  - `reports/qlib_full_factor_minute_chain_20260428/daily_missing_db_crosscheck.csv`
  - `reports/qlib_full_factor_minute_chain_20260428/daily_missing_db_crosscheck_summary.json`
- Log files: command output in Codex session; structured report files below
- Playwright report/trace: not used
- Screenshots: not used
- Business output summary:
  - Main report: `reports/qlib_full_factor_minute_chain_20260428/report.json`
  - Markdown report: `reports/qlib_full_factor_minute_chain_20260428/report.md`
  - Full factor coverage: `reports/qlib_full_factor_minute_chain_20260428/full_daily_factor_coverage.csv`
  - Source summary: `reports/qlib_full_factor_minute_chain_20260428/full_daily_factor_source_summary.csv`
  - Minute Qlib bars: `reports/qlib_full_factor_minute_chain_20260428/qlib_minute_non_null_bars.csv`
  - Portfolio report: `reports/qlib_full_factor_minute_chain_20260428/minute_chain_portfolio_report.csv`
  - Candidate minute bin: `qlib_minute_validation/full_factor_minute_chain_20260428_candidate/bin`
  - Result: ok=true; 10 stocks; 560 trading days; 1,347,640 minute bars loaded; 132 feature columns; Qlib minute chain portfolio report has 560 rows.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Local time exported as UTC clock | psycopg2 returned timestamptz values displayed as UTC in pandas, causing `01:30/01:31` timestamps | SQL now exports `trade_time AT TIME ZONE 'Asia/Shanghai'` | DB minute export rerun PASS |
| Minute D.features excluded final day in coverage probe | Qlib `end_time='2026-04-28'` at 1min resolves to midnight | Minute coverage probe now uses `2026-04-28 15:00:00` | Qlib minute rows loaded 1,347,640 with zero required NaN |
| Full-calendar daily coverage below 99.9% | 47 rows are suspended days for `000063.SZ`, `000333.SZ`, `000651.SZ` | Cross-checked all 47 against `market.suspend_d`; all absent from `kline_daily_raw` and have `stk_limit` rows | Classified as suspension exceptions |

## Result

- Final status: PASS
- Remaining risks: This is a 10-stock validation, not full-market minute export; the minute execution algorithm used is explicit Qlib TWAP inside `NestedExecutor`, not V24/V25 model execution.
- Need production backend restart: no
- Need dev service restart: no
