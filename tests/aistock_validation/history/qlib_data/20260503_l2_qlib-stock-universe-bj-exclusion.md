# L2 Validation: Qlib Stock Universe BJ Exclusion (2026-05-03)

## Scope

Changed Qlib stock export universe handling for AIstock QE/Qlib datasets:

- Authoritative bin exporter rejects BJ/BSE and defaults to SH/SZ.
- DBReader stock export helpers reject BJ/BSE and default omitted exchange filters to SH/SZ where needed.
- Qlib frontend disables BJ selection and no longer sends `bj` payload values.
- CLI authoritative exporter defaults to ST/delisted exclusion and rejects BJ/BSE.

## Business Oracles

- BJ/BSE cannot enter stock export through UI, API helper, CLI helper, or explicit stock-code list.
- No silent drop of unsupported exchanges: `bj` and unsupported values raise explicit errors.
- New stock-universe SQL includes SH/SZ, active-listing, and 365-day listing-age filters.
- Existing generated dataset with BJ remains marked invalid and must be regenerated, not metadata-edited.

## Existing Data Evidence

Command:

```powershell
$p='\\wsl$\Ubuntu\home\lc999\data\qlib_minute_authoritative_full_20260428\meta_export.json'
Get-Content $p -Raw
$all='\\wsl$\Ubuntu\home\lc999\data\qlib_minute_authoritative_full_20260428\instruments\all.txt'
(Select-String -Path $all -Pattern 'bj|BJ|\.BJ' -AllMatches).Count
```

Observed:

```text
meta.exchanges        ["bj", "sh", "sz"]
meta.exclude_st       false
meta.exclude_delisted false
BJ instruments        310
```

## Automated Checks

### Python compile

Command:

```powershell
python -m py_compile backend/qlib_exporter/authoritative_bin_exporter.py backend/qlib_exporter/db_reader.py backend/qlib_exporter/router.py scripts/qlib_authoritative_bin_export.py
```

Result: passed.

### Unit tests

Command:

```powershell
python -m pytest backend/tests/test_qlib_export_stock_universe_filters.py -q
```

Result:

```text
4 passed in 0.77s
```

Covered assertions:

- `normalize_stock_export_exchanges(None)` returns `['sh', 'sz']`.
- `normalize_stock_export_exchanges(['sh', 'bj'])` raises `ValueError`.
- Authoritative stock-universe SQL uses `['SSE', 'SZSE']` and does not include BSE.
- Authoritative SQL includes active-listing and 365-day listing-age conditions.
- Explicit `.BJ` stock code fails fast.
- `DBReader.get_base_ts_codes()` defaults to SH/SZ and rejects `exchanges=['bj']`.

### Frontend build

Command:

```powershell
cd frontend
npm run build
```

Result: passed. `/qlib` route built successfully.

## Silent-Fallback Review

Diff scan checked added lines for silent fallback patterns. This change adds explicit `ValueError` / HTTP 400 paths for BJ/BSE and unsupported exchange inputs. No new `except Exception: pass`, fake success, or metadata-only repair path was added.

## Residual Risk

- Full minute bin data has not been regenerated in this validation run.
- The existing `/home/lc999/data/qlib_minute_authoritative_full_20260428` snapshot remains invalid under the new rule until regenerated.
- After regeneration, rerun meta validation, instruments BJ count, sampled bin-vs-DB validation, and a small QE/Qlib smoke backtest.

### CLI fail-fast smoke

Command:

```powershell
python scripts/qlib_authoritative_bin_export.py --dataset stock_daily --stage validate --snapshot-id dry_bj_reject --start 2024-01-01 --end 2024-01-02 --exchanges sh,bj
```

Result: expected non-zero exit before DB/export work, with `ValueError: BJ/BSE stocks are excluded from AIstock QE/Qlib stock exports; use sh/sz only`.
