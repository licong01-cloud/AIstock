# L3 ST PIT Active Derived Universe Implementation - 2026-05-04

## Scope

- Implemented ST-only PIT derived universe key `shsz_st_pit_active_v1`.
- Rule version: `st_pub_next_trade_restore_active_l_v1`.
- Scope: current active SH/SZ stocks only; delisting and paused-listing PIT are intentionally not implemented.
- Added strict `ensure_st_pit_universe()` before PIT Bin/H5 daily/minute/auxiliary export paths.
- Added Tushare post-sync hook: successful `stock_basic`, `stock_st`, or `stock_st_events` sync marks PIT dirty and attempts a non-strict rebuild.
- Added Data Dashboard status card and manual "rebuild ST PIT derived table" action.
- Added H5 Snapshot UI stock-universe mode controls; PIT mode disables legacy static ST/D/P checkboxes.

## Files Changed

- `scripts/build_stock_universe_pit_spans.py`
- `backend/services/stock_universe_pit_service.py`
- `backend/services/tushare_sync_engine.py`
- `backend/routers/stock_universe.py`
- `backend/main.py`
- `backend/routers/__init__.py`
- `backend/qlib_exporter/router.py`
- `backend/qlib_exporter/exporter.py`
- `backend/qlib_exporter/db_reader.py`
- `backend/qlib_exporter/authoritative_bin_exporter.py`
- `scripts/qlib_authoritative_bin_export.py`
- `frontend/src/app/local-data/page.tsx`
- `frontend/src/app/qlib/page.tsx`
- `backend/tests/test_stock_universe_pit_spans.py`
- `backend/tests/test_stock_universe_pit_service.py`
- `backend/tests/test_tushare_sync_engine.py`

## Validation Commands

```powershell
python -m py_compile scripts/build_stock_universe_pit_spans.py backend/services/stock_universe_pit_service.py backend/services/tushare_sync_engine.py backend/routers/stock_universe.py backend/qlib_exporter/router.py backend/qlib_exporter/exporter.py backend/qlib_exporter/db_reader.py scripts/qlib_authoritative_bin_export.py backend/qlib_exporter/authoritative_bin_exporter.py backend/main.py
pytest -q backend/tests/test_stock_universe_pit_spans.py backend/tests/test_stock_universe_pit_service.py backend/tests/test_authoritative_bin_pit_universe.py backend/tests/test_tushare_sync_engine.py
cd frontend; npm run build
python scripts/build_stock_universe_pit_spans.py --scope st_only_active --universe-key shsz_st_pit_active_v1 --rule-version st_pub_next_trade_restore_active_l_v1 --start-date 2018-08-01 --end-date 2026-04-30 --dry-run --write-all-txt --reports-dir reports/stock_universe_pit_validation
python - << service ensure smoke equivalent through PowerShell here-string
temporary backend 127.0.0.1:8012: GET /api/v1/stock-universe/st-pit/status; POST /api/v1/stock-universe/st-pit/ensure; GET /api/v1/stock-universe/st-pit/eligible-codes?trade_date=2026-04-30&ensure=false
direct daily H5 PIT smoke: create `codex_st_pit_smoke_20260504` for 2026-04-30 and clean it up after checking meta/all.txt
git diff --check -- <impacted files>
```

## Results

- `py_compile`: PASS.
- Pytest: PASS, 11 tests.
- Frontend production build: PASS.
- `git diff --check`: PASS; CRLF conversion warnings only.
- ST-only dry-run builder: PASS.
- DB service `ensure_st_pit_universe(start=2018-08-01,end=2026-04-30)` PASS; current state already ready, dirty=false.
- API smoke on temporary port 8012: PASS; temp backend stopped after validation.
- Direct daily H5 PIT smoke: PASS; `meta.json` carries `stock_universe_mode=pit_spans`, `st_pit=true`, `delist_pit=false`, `pause_pit=false`; `instruments/all.txt` was rewritten from PIT spans.

## ST PIT Build Summary

- Universe key: `shsz_st_pit_active_v1`
- Status: `ready`
- Dirty: `false`
- Date range: `2018-08-01` to `2026-04-30`
- Current active SH/SZ stocks: 5,201
- Current D/P stocks excluded by scope: 321
- Source fingerprint SHA256: `d8a6bd97a42d1ff1537990f7e8c3b955b85638c850c0196cfe602f78a50cdbba`
- Classified ST events: 1,886
- `st_negative`: 1,543
- `st_restore`: 343
- Span rows: 5,372
- Eligible instruments: 5,117
- Eligible on `2026-04-30`: 4,880
- Multi-span instruments: 245
- Validation errors: 0 invalid spans, 0 overlaps, 0 event action violations
- Report: `reports/stock_universe_pit/shsz_st_pit_active_v1_summary_20260504_190932.json`

## Business Outcome

- Local data management remains source-table focused.
- ST PIT derived tables are auto-marked dirty and non-strict rebuilt after source-table sync, then strict ensured on H5/Bin export.
- H5/Bin export in PIT mode now prepares/validates ST PIT spans before exporting and writes PIT metadata.
- Data Dashboard can show PIT state and manually rebuild derived rows.
- The implementation does not claim delisted-stock PIT coverage or full survivorship-bias removal.

## Residual Risks

- Minute H5 direct smoke was attempted on a one-stock/one-day sample, but the local Timescale minute query exceeded the interactive validation window and was terminated/cleaned up. `backend/qlib_exporter/db_reader.py` was adjusted to use sargable timestamp bounds instead of `trade_time::date`; full minute validation should be included in the candidate export validation run.
- Selection Center/Paper v2 can consume the new service in a later step; this change provides the service/API but does not force runtime filtering yet.
- Production Qlib data was not replaced.
