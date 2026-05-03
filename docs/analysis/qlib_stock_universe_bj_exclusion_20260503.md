# Qlib Stock Universe BJ Exclusion Record (2026-05-03)

## Conclusion

The already generated `/home/lc999/data/qlib_minute_authoritative_full_20260428` snapshot is not a valid final QE stock-universe dataset under the new rule. Its metadata and instruments prove that BJ/BSE stocks were included, and ST / delisted filters were not enabled.

This code change makes AIstock Qlib stock exports use the same stock-universe filter family required for QE:

- Stock export exchanges are SH/SZ only. Any `bj` / BSE request fails fast.
- The Qlib UI keeps the BJ checkbox disabled and no longer sends `bj` in any H5/bin/data-check payload.
- Authoritative bin export defaults to `exclude_st=true` and `exclude_delisted_or_paused=true`.
- Authoritative bin stock universe always enforces `stock_basic.list_status = 'L'`.
- Authoritative bin stock universe always enforces `list_date + 365 days <= end`, aligned with `IPO_FILTER_DAYS=365`.
- Future `meta_export.json` files record `exclude_bj=true` and `min_listed_days=365`.

## Existing Snapshot Evidence

```text
path                  /home/lc999/data/qlib_minute_authoritative_full_20260428
meta.exchanges        ["bj", "sh", "sz"]
meta.exclude_st       false
meta.exclude_delisted false
BJ instruments        310
```

Therefore this existing snapshot must be regenerated with the fixed exporter. It must not be repaired by metadata-only edits.

## Code-Level Rules

### authoritative bin exporter

File: `backend/qlib_exporter/authoritative_bin_exporter.py`

- Adds `normalize_stock_export_exchanges()`.
- The default exchange set is `['sh', 'sz']`.
- Any `bj` exchange raises `ValueError` before DB access or dump_bin execution.
- `_exchange_sql_values()` maps only `sh -> SSE` and `sz -> SZSE`; it has no BSE branch.
- `resolve_stock_universe()` enforces:
  - `s.exchange = ANY(['SSE', 'SZSE'])`
  - `s.list_date IS NOT NULL`
  - `s.list_date + 365 days <= end`
  - `s.list_status = 'L'`
  - `market.stock_st` exclusion when `exclude_st=true`.
- Explicit stock codes such as `430047.BJ` or `BJ430047` fail fast.

### AIstock DBReader and UI export

File: `backend/qlib_exporter/db_reader.py`

- Adds `_normalize_stock_export_exchanges()` for H5/multi-dataset stock export paths.
- `get_base_ts_codes()` now applies SH/SZ filtering even when `exchanges` is omitted.
- `get_moneyflow_ts_codes()` now applies SH/SZ filtering even when `exchanges` is omitted.

File: `frontend/src/app/qlib/page.tsx`

- BJ checkbox defaults to false, is disabled, and is labelled as fixed excluded.
- Payload builders no longer push `bj` into `exchanges`.

### CLI export tool

File: `scripts/qlib_authoritative_bin_export.py`

- `--exchanges` help text now states that only `sh,sz` are valid for stock export.
- `--exclude-st` and `--exclude-delisted-or-paused` default to true.
- BJ/BSE is rejected regardless of CLI flags.

## Requirements For Next Full Minute Bin Export

The next QE minute full bin export must satisfy:

```text
required meta.exchanges                  ["sh", "sz"]
required meta.exclude_bj                 true
required meta.exclude_st                 true
required meta.exclude_delisted_or_paused true
required meta.min_listed_days            365
required instruments BJ count            0
```

Required validation after regeneration:

- Count BJ/BSE entries in `instruments/all.txt` and require zero.
- Validate `meta_export.json` stock-universe flags.
- Run authoritative bin-vs-DB sampled field validation.
- Run a small QE/Qlib smoke backtest to confirm data can be loaded and consumed.

## Validation Record

Detailed validation record: `tests/aistock_validation/history/qlib_data/20260503_l2_qlib-stock-universe-bj-exclusion.md`.
