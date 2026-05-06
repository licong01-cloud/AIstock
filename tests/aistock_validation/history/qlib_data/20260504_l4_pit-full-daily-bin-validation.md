# L4 PIT Full Daily Bin Validation - 2026-05-04

## Scope

- Candidate snapshot: `qlib_bin_pit_full_20180801_20260430`
- Candidate CSV path: `qlib_csv/qlib_bin_pit_full_20180801_20260430/stock_daily`
- Candidate Bin path: `qlib_bin/qlib_bin_pit_full_20180801_20260430`
- Date range: `2018-08-01` through `2026-04-30`
- Universe mode: `pit_spans`, universe key `shsz_pit_v1`
- Production path `/home/lc999/data/qlib_bin` was not modified.

## Pre-export Data Repairs

- `001914.SZ`: inserted/upserted historical `market.stk_limit` rows from old code `000043.SZ` for the renamed-code window `2018-08-01` through `2019-12-13`.
- `688033.SH`: inserted listing-day `market.stk_limit` row for `2019-07-22` with Tushare daily `pre_close=20.37`, no-limit sentinel `up_limit=100000.0`, `down_limit=0.01`.
- `689009.SH`: upserted `market.stk_limit.pre_close` for `2020-10-29` and `2020-10-30` from Tushare daily while preserving Tushare no-limit sentinel values.
- Post-repair check: PIT universe raw daily rows between `2018-08-01` and `2026-04-30` had zero missing `stk_limit` matches.

## Export

- Sharded CSV export: 8 disjoint code shards, 5,318 requested PIT instruments.
- Final CSV files: 5,121 instruments, 0.899 GiB.
- Requested instruments with no price rows: 197, so no CSV/Bin feature files were created for them.
- Dump command completed successfully through WSL `dump_bin.py dump_all`.
- Final Bin files: 61,452 feature `.bin` files, 0.369 GiB.
- Calendar: 1,878 trading days, `2018-08-01` through `2026-04-30`.

## PIT Instruments Audit

- `instruments/all.txt` rewrite mode: `pit_universe_spans`
- Feature instruments written: 5,121
- PIT span rows written: 5,368
- Multi-span instruments: 237
- Overlap check: 0 overlapping spans
- Report: `reports/qlib_authoritative_export/qlib_bin_pit_full_20180801_20260430_stock_daily_dump.json`

## Accuracy Validation

Validation was run as 8 shards with distinct report directories under `reports/qlib_authoritative_export/validate_shards/`.

- Aggregate report: `reports/qlib_authoritative_export/qlib_bin_pit_full_20180801_20260430_stock_daily_validate_aggregate.json`
- Shard count: 8
- Status: PASS
- Stocks in PIT universe checked: 5,318
- Checked rows: 8,237,832
- Checked values: 98,853,984
- Error count: 0
- Max absolute diff for all checked fields: 0.0
- Checked fields: `open`, `high`, `low`, `close`, `volume`, `amount`, `factor`, `up_limit_price`, `down_limit_price`, `prev_close`, `limit_up`, `limit_down`

## Qlib Read Smoke

- Result JSON: `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-qlib-read-smoke.json`
- Provider: `/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_pit_full_20180801_20260430`
- Calendar length: 1,878
- `D.list_instruments("all")` on `2020-01-02`: 3,259
- `D.list_instruments("all")` on `2026-04-30`: 4,871
- Sample feature frame: 6 rows x 5 columns, 0 NaNs
- Patched `688033.SH` listing-day values were readable: `prev_close=20.37`, `up_limit_price=100000.0`, `down_limit_price=0.01`

## LGB Train and Backtest Smoke

- Script: `tests/aistock_validation/history/qlib_data/20260504_l4_pit_full_bin_lgb_smoke.py`
- Result JSON: `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-bin-lgb-smoke-result.json`
- Prediction file: `tests/aistock_validation/history/qlib_data/20260504_l4_pit-full-bin-lgb-smoke-pred.pkl`
- Train segment: `2025-07-01` through `2025-12-31`
- Valid segment: `2026-01-02` through `2026-02-27`
- Test segment: `2026-03-02` through `2026-04-30`
- Backtest segment: `2026-03-02` through `2026-04-29`
- Dataset shapes:
  - Train: 615,525 rows x 159 columns
  - Valid: 166,898 rows x 159 columns
  - Test: 211,445 rows x 159 columns
- Prediction rows: 211,445 across 43 days and 4,942 instruments
- Test IC mean: 0.0179589611
- Test Rank IC mean: 0.0201854738
- TopK backtest: PASS, 42 report rows
- Backtest rough cumulative return from Qlib report returns: 0.0058256761

The LGB run verifies data usability only. It is not a production alpha-quality conclusion.

## Guardrails

- `python -m py_compile backend/qlib_exporter/router.py backend/qlib_exporter/authoritative_bin_exporter.py scripts/qlib_authoritative_bin_export.py`: PASS
- `pytest -q backend/tests/test_stock_universe_pit_spans.py backend/tests/test_authoritative_bin_pit_universe.py backend/tests/test_tushare_sync_engine.py`: PASS, 6 tests
- `cd frontend && npm run build`: PASS
- `git diff --check` on impacted files: PASS with CRLF conversion warnings only

## Replacement Decision

The full daily PIT Bin candidate passed export, direct DB-vs-Bin value validation, Qlib read smoke, and short-window LGB train/backtest smoke. From daily Bin data-correctness and Qlib usability perspectives, it is eligible for production replacement review.

Replacement was not performed. Before replacing `/home/lc999/data/qlib_bin`, take an explicit approval step, back up the current production dataset, copy this candidate to WSL ext4 storage, then rerun the Qlib read smoke and a small QE backtest against the production path.

Residual limits:

- This validation covers daily Bin, not H5 or minute Bin.
- The candidate currently lives under `/mnt/f/Dev/AIstock/...`; production Qlib should still use WSL ext4 for performance.
- The `all.txt` universe contains only instruments with exported feature files, so 197 PIT instruments with no raw price rows were omitted from the candidate feature universe.
