# Qlib multi-dataset candidate smoke backtest 20260428

- Module: qlib_data
- Level: L3
- Date: 2026-04-29T19:00:03
- Git commit: 178a0b7
- Operator: lc999

## Scope

- Changed files: `scripts/qlib_multi_dataset_smoke_backtest.py`
- Impacted flows: QE/Qlib candidate data validation, WSL Qlib daily provider, snapshot static factor inputs, Qlib portfolio simulation.
- Business goal: prove the 2026-04-28 SH/SZ candidate can support a small-universe end-to-end Qlib validation that reads factors from multiple exported datasets and produces IC weights, no-leak signals, and portfolio results.
- Out of scope: full-market backtest, minute-execution/V24/V25 simulation, production dataset replacement.
- Protected assets reviewed: only candidate data under `qlib_bin_20260428_shsz_candidate` and `qlib_20260428_shsz_candidate` was read; no historical QE/RD-Agent experiment artifacts or production qlib dirs were overwritten.

## Environment

- Backend port: not used
- Frontend port: not used
- TDX port: not used
- Conda/env: WSL Ubuntu, conda `rdagent-gpu`
- Database: not used by this smoke; data read from generated candidate artifacts
- Browser/headless: not used

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Script syntax | New validation script compiles | `python -m py_compile scripts/qlib_multi_dataset_smoke_backtest.py` | PASS |
| Multi-source feature coverage | Qlib daily bin + daily_basic/moneyflow/bak_basic/cyq_perf/sector_data selected features meet coverage threshold | `reports/qlib_multi_dataset_smoke_20260428/feature_coverage.csv` | PASS |
| No-leak signal generation | Test signal is shifted by one instrument day and covers the target recent window | `reports/qlib_multi_dataset_smoke_20260428/report.json` | PASS |
| Qlib backtest | Qlib engine runs TopkDropout portfolio simulation on the candidate bin using limit fields | `reports/qlib_multi_dataset_smoke_20260428/portfolio_report.csv` | PASS |
| Asset safety | Candidate read-only validation; no old production datasets overwritten | command paths and report paths | PASS |

## Commands

```bash
python -m py_compile scripts/qlib_multi_dataset_smoke_backtest.py

wsl -d Ubuntu -- bash -lc "source /home/lc999/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && export PYTHONUNBUFFERED=1 && python /mnt/f/Dev/AIstock/scripts/qlib_multi_dataset_smoke_backtest.py --num-stocks 20 --topk 5 --drop 2"

wsl -d Ubuntu -- bash -lc "source /home/lc999/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && python /mnt/f/Dev/AIstock/.codex_tmp/qlib_selected_coverage.py"
```

## Evidence

- API calls: not used
- DB checks: not used in this smoke; prior candidate DB completeness validation remains in `reports/qlib_candidate_20260428_validation.json`
- Log files: command output in Codex session; structured outputs below
- Playwright report/trace: not used
- Screenshots: not used
- Business output summary:
  - Main report: `reports/qlib_multi_dataset_smoke_20260428/report.json`
  - Markdown report: `reports/qlib_multi_dataset_smoke_20260428/report.md`
  - Feature coverage: `reports/qlib_multi_dataset_smoke_20260428/feature_coverage.csv`
  - Train IC weights: `reports/qlib_multi_dataset_smoke_20260428/train_ic_weights.csv`
  - Test signal: `reports/qlib_multi_dataset_smoke_20260428/test_signal.parquet`
  - Portfolio report: `reports/qlib_multi_dataset_smoke_20260428/portfolio_report.csv`
  - Result: ok=true; 20 instruments; 35 signal dates; 34 Qlib portfolio rows; final NAV 0.985243.
  - Direct selected-stock Qlib raw field check for 2026-03-10 ~ 2026-04-28 returned 700 rows and zero NaN in `$close`, `$open`, `$volume`, `$up_limit_price`, `$down_limit_price`, `$prev_close`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Parquet index columns not exposed as normal columns | pandas restored `datetime`/`instrument` as MultiIndex when reading static parquet | script now accepts either MultiIndex or columns | rerun reached Qlib backtest |
| Qlib feature index order differed from assumption | Qlib returned `(instrument, datetime)` order in this environment | script now detects datetime/instrument levels instead of assuming order | rerun generated 700 signal rows |
| Qlib calendar had no future row after 2026-04-28 | Qlib daily backtest asks for the calendar point after `end_time` | portfolio simulation end is adjusted to 2026-04-27 while data/signal coverage still validates through 2026-04-28 | final rerun PASS |
| `DataFrame.to_markdown` required missing `tabulate` | WSL `rdagent-gpu` lacks optional markdown dependency | script writes markdown tables without external dependency | final rerun PASS |

## Result

- Final status: PASS
- Remaining risks: This is a small-universe daily Qlib smoke, not a full-market or minute-execution performance proof. Qlib portfolio simulation stops at 2026-04-27 because the candidate calendar ends on 2026-04-28 and no future calendar row exists yet; feature/signal coverage still includes 2026-04-28.
- Need production backend restart: no
- Need dev service restart: no
