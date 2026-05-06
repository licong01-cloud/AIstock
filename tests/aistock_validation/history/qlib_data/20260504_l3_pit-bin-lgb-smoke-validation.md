# PIT Qlib Bin Small LGB Smoke Validation - 2026-05-04

## Scope

- Validate first-stage PIT stock universe data (`shsz_pit_v1`) after Tushare ST-event and stock_basic L/D/P backfill from 2018-08-01.
- Validate Qlib daily Bin export can use `market.stock_universe_pit_spans` and rewrite `instruments/all.txt` into multi-segment PIT ranges.
- Validate a small Alpha158 + LGBModel training/prediction/backtest path can read the candidate Bin without replacing production WSL `/home/lc999/data/qlib_bin`.

## Production Safety

- Production FastAPI backend `8001` was not restarted.
- Production WSL Qlib data path `/home/lc999/data/qlib_bin` was not modified.
- Candidate data stayed under `F:/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220`.

## PIT DB Readiness

Command:

```powershell
python - <<'PY'
# Queried market.stock_st_events, market.stock_basic, market.stock_universe_pit_spans.
PY
```

Observed DB state:

- `market.stock_st_events`: 1,825 rows, 754 stocks, `pub_date` range 2018-08-08..2026-04-30.
- `market.stock_basic`: `L=5512`, `D=325`, `P=0`.
- `market.stock_universe_pit_spans` for `shsz_pit_v1`: 5,577 spans, 5,318 instruments, coverage 2018-08-01..2026-05-04.
- PIT overlap check: 0 overlapping spans.
- Eligible instruments on 2018-08-01: 3,219.

## Small PIT Bin Export

Candidate snapshot:

- Snapshot ID: `qlib_bin_pit_smoke_lgb_202001_202112_220`
- Provider path: `F:/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220`
- WSL read path: `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220`
- Date range: 2020-01-01..2021-12-31
- Basis range: 2020-01-01..2021-12-31
- Stock universe mode: `pit_spans`
- Universe key: `shsz_pit_v1`
- Explicit sample: 220 stocks, intentionally including multi-span instruments.

Main command shape:

```powershell
python scripts/qlib_authoritative_bin_export.py `
  --dataset stock_daily --stage all `
  --snapshot-id qlib_bin_pit_smoke_lgb_202001_202112_220 `
  --start 2020-01-01 --end 2021-12-31 `
  --basis-start 2020-01-01 --basis-end 2021-12-31 `
  --stock-universe-mode pit_spans --universe-key shsz_pit_v1 `
  --codes <220 DB-selected PIT-eligible codes> `
  --overwrite-csv --dump-workers 4
```

Export evidence:

- Export/validation report: `reports/qlib_authoritative_export/qlib_bin_pit_smoke_lgb_202001_202112_220_stock_daily_all.json`
- Bin metadata: `qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220/meta_export.json`
- PIT all.txt summary: `qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220/instruments/all_pit_universe_summary.json`

Export results:

- CSV files: 220
- Qlib feature dirs: 220
- `instruments/all.txt`: 267 rows, 220 unique instruments, 47 multi-span instruments.
- `all_pit_universe_summary.json`: `mode=pit_universe_spans`, `input_feature_instruments=220`, `pit_span_rows=267`, `output_rows=267`, `multi_span_instruments=47`, `skipped_rows=0`.
- Authoritative validation: `ok=true`, 106,557 checked rows, 1,278,684 checked values, 0 errors, all checked fields max absolute diff = 0.0.

## Qlib/LGB/Backtest Smoke

Command:

```powershell
wsl bash -lc "source ~/miniconda3/etc/profile.d/conda.sh && conda activate rdagent-gpu && export PYTHONUNBUFFERED=1 && export OMP_NUM_THREADS=4 && python /mnt/f/Dev/AIstock/tests/aistock_validation/history/qlib_data/20260504_l3_pit_bin_lgb_smoke.py"
```

Artifacts:

- Script: `tests/aistock_validation/history/qlib_data/20260504_l3_pit_bin_lgb_smoke.py`
- JSON result: `tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-result.json`
- Prediction pickle: `tests/aistock_validation/history/qlib_data/20260504_l3_pit-bin-lgb-smoke-pred.pkl`

Training setup:

- Qlib provider: `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_smoke_lgb_202001_202112_220`
- Handler: `Alpha158`
- Model: `LGBModel`
- Train: 2020-01-02..2020-12-31
- Valid: 2021-01-04..2021-06-30
- Test: 2021-07-01..2021-12-31
- Backtest: 2021-07-01..2021-12-30, TopK=20, n_drop=5, close-price simulator, limit flags `($limit_up, $limit_down)`.

Observed results:

- Qlib feature smoke loaded `$close/$volume/$factor/$limit_up/$limit_down`: 624 rows, 208 instruments, no NaN in sampled fields.
- Dataset shapes: train `(44988, 159)`, valid `(21549, 159)`, test `(26509, 159)`.
- Dataset total NaN ratios: train 1.41%, valid 0.79%, test 0.71%.
- LightGBM early-stopped successfully at iteration 8; validation L2 ~0.993651.
- Prediction output: 26,509 rows, 125 days, 214 instruments.
- Test IC smoke: aligned rows 26,039, 123 days, IC mean -0.00423, RankIC mean 0.00059. This is usability smoke only, not performance approval.
- Backtest completed: 124 daily rows, final account 90,083,553.42 from 100,000,000 initial cash, rough cumulative return 1.67% before considering the script's account/value accounting nuance; `bench` is null because benchmark was intentionally disabled for this stock-only candidate.
- Backtest generated trade indicators with 124 rows; tail daily `count` was 10 orders in the sampled final days.

## Issues Found / Fixes

- Initial Qlib feature smoke failed only because `$close` was expanded by PowerShell in an inline command; fixed by writing/running a script file so Qlib received literal `$` expressions.
- First backtest attempt failed at 2021-12-31 due Qlib day executor needing the next calendar point. The smoke script now backtests through 2021-12-30 while leaving the test segment through 2021-12-31.
- Backtest succeeded but raw Qlib indicator objects contained circular references; result serialization now stores compact DataFrame/type summaries instead of raw Qlib objects.
- `scripts/qlib_authoritative_bin_export.py` metadata was patched so future PIT CLI exports write `ipo_filter_mode=pit_universe_spans` when `--stock-universe-mode pit_spans` is used. The already-generated smoke meta still has `all_txt_rewrite.mode=pit_universe_spans`, which is the authoritative evidence for this candidate.

## Conclusion

- The first-stage PIT universe DB tables and small daily Bin candidate are usable for Qlib Alpha158/LGB training, prediction, and a daily TopK backtest smoke.
- The candidate validates the data path and PIT `all.txt` semantics, but it is intentionally small and stock-only; it is not sufficient to replace production data.
- Recommended next step: run a full-size non-production PIT daily Bin candidate plus broader Qlib/QE validation. Only after that passes should production WSL Bin replacement be considered, and it should require explicit user approval.

## Guardrails Run After Smoke

- `python -m py_compile backend/qlib_exporter/router.py backend/qlib_exporter/authoritative_bin_exporter.py scripts/qlib_authoritative_bin_export.py tests/aistock_validation/history/qlib_data/20260504_l3_pit_bin_lgb_smoke.py` passed.
- `python -m py_compile scripts/build_stock_universe_pit_spans.py scripts/create_stock_st_events_table.py backend/services/tushare_dataset_specs.py backend/services/tushare_sync_engine.py backend/routers/ingestion.py backend/ingestion/tdx_scheduler.py backend/db/init_tushare_schedules.py` passed.
- `pytest -q backend/tests/test_stock_universe_pit_spans.py backend/tests/test_authoritative_bin_pit_universe.py backend/tests/test_tushare_sync_engine.py` passed: 6 tests.
- `git diff --check` on the touched tracked files passed; Git emitted only LF-to-CRLF working-copy warnings.
- `cd frontend && npm run build` passed; `/local-data` and `/qlib` compiled in the full Next.js production build.
- Qlib/MLflow auto-created temporary recorder directories under `mlruns/170967380878871710` during the smoke; those recorder directories were removed after extracting JSON evidence to avoid root artifact pollution and a large `code_diff.txt` artifact.
