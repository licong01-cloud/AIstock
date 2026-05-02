# QE archive realtime warehouse validation

- Module: qe_archive
- Level: L3
- Date: 2026-05-03T01:39:27
- Git commit before change: c93d40b
- Operator: lc999 / Codex

## Scope

- Changed files: `backend/services/qe_archive/models.py`, `backend/services/qe_archive/payload_extractor.py`, `backend/services/qe_archive/repository.py`, `backend/services/qe_archive/archive_service.py`, `scripts/qe_archive_data_quality_smoke.py`, `frontend/src/lib/qe-archive/api.ts`, `frontend/src/app/qe-archive/page.tsx`, `frontend/tests/qe-archive/qe-archive-dashboard.spec.ts`, docs/validation records.
- Impacted flows: QE Archive payload extraction, confirmed backfill API writes, run quality API, data quality smoke, QE Archive UI quality/backfill display.
- Business goal: every archived QE loop should persist queryable all/top/bottom stock summaries, stock trade records, and execution/parser diagnostics in addition to existing config/metrics/curves/factors/raw payload rows.
- Out of scope: direct artifact parser for position snapshots/order fills/model weights; realtime hook enablement in production; production backend restart.
- Protected assets reviewed: no StrategyPackage/HMM/model/Qlib/QE worker assets modified; DB/API payloads only; no direct WSL/remote worker file read.

## Environment

- Backend port: `8011` dev FastAPI, restarted after code change; production `8001` remained running and was not restarted.
- Frontend port: `3011` dev Next.js, restarted after code change.
- TDX port: `19080` existing local service.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL/TimescaleDB through `backend.db.pg_pool.get_conn()` and `.env`.
- Browser/headless: Playwright Chromium headless via `frontend/tests/qe-archive`.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Payload audit | Existing raw payload contains structured symbol/trade fields without reading worker files | `raw_payload_rows=48`; `all_stocks/top_stocks/bottom_stocks/stock_trades` found in 16 enhanced payloads; `stock_trades` keys are `date/type/price/amount/pnl` | Pass |
| Backend extraction/write | Extractor/repository/service handle symbol summaries, trades, and events idempotently | `qe_archive_backend`: 37 tests passed; sample extractor test covers 4 symbol rows, 2 trades, >=2 events | Pass |
| Schema comments | No new uncommented DB columns; existing managed tables/columns fully commented | `qe_archive_data_quality`: 27/27 tables, 458/458 columns commented | Pass |
| API flow | 8011 API dry-run and confirmed write produce DB counts and quality API agrees | `qe_20260502_131502_9b54` 4 loops confirmed-written; all `passed=true`; sample run has 792 symbols, 4322 trades, 3 events | Pass |
| UI E2E | UI displays new symbol/trade/event counts with no console/page/request errors | `qe_archive_ui`: 1 Playwright test passed with mocked APIs | Pass |
| L3 suite | Guardrails/backend/data/UI run as one module validation | `qe_archive_l3`: 4 nox sessions successful | Pass |
| Asset safety | No protected assets modified silently; no production restart | `8001` process stayed running; only dev `8011/3011` restarted | Pass |

## Commands

```powershell
# Targeted backend static/unit regression
$env:PYTHONIOENCODING='utf-8'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_qe_archive_repository_static.py -q

# Module backend and DB smoke
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_data_quality

# Frontend typecheck and QE Archive UI E2E
npm exec tsc -- --noEmit --incremental false
$env:QE_ARCHIVE_UI_MOCK_API='1'
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_ui

# Full QE Archive L3 suite
$env:QE_ARCHIVE_UI_MOCK_API='1'
Remove-Item Env:QE_ARCHIVE_L3_SKIP_UI -ErrorAction SilentlyContinue
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s qe_archive_l3

# 8011/3011 live dev API validation, production 8001 untouched
POST http://127.0.0.1:8011/api/v1/qe-archive/backfill
{"source":"task","task_ids":["qe_20260502_131502_9b54"],"status":"completed","write":true,"confirm_write":"QE_ARCHIVE_WRITE","validate_after_write":true,"min_metrics":60,"min_curves":3000,"min_factors":1,"require_account_summary":true}

C:/Users/lc999/miniconda3/envs/AIstock/python.exe scripts/qe_archive_data_quality_smoke.py --run-id qear_run_61fe6f6dccabca49b1228033 --min-metrics 60 --min-curves 3000 --min-factors 1 --require-account-summary
```

## Evidence

- API calls: `GET /api/v1/qe-archive/health` returned `run_count=16`, `pending_outbox_count=0`; `POST /api/v1/qe-archive/backfill` processed 4 loops; `GET /api/v1/qe-archive/runs/qear_run_61fe6f6dccabca49b1228033/quality` returned new counts.
- DB checks: sample `qe_archive.run_symbol_summary` rows include `002667.SZ`, `600227.SH`; sample `qe_archive.run_trade` rows include `000545.SZ buy 2024-07-02 price=1.75 amount=1887025.0`; events include parser summary, `source.execution_trace`, and `source.trade_diagnostics`.
- Live sample counts: `qear_run_61fe6f6dccabca49b1228033` has `metric_count=67`, `curve_count=3489`, `factor_count_rows=57`, `symbol_summary_count=792`, `trade_count=4322`, `execution_event_count=3`, `raw_payload_count=3`, `passed=true`.
- Playwright report/trace: failing trace during first run under `tmp/playwright-results/...` after strict locator issue; rerun passed and no final trace needed.
- Business output summary: historical backfill via API can now populate structured symbol/trade/event warehouse rows immediately after a loop is archived or re-archived.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Playwright locator `getByText(/symbols 1310/)` strict-mode violation | Two table rows contained the same text | Use `.first()` for the dry-run stats assertion | `qe_archive_ui` passed |
| Playwright expected `4.1K/4,100` but UI rounded compact value to `4K` | `formatCompact` rounds thousands with 0 decimals in quality table | Assert ``trade detail` label and `/4K|4,100/` | `qe_archive_ui` and `qe_archive_l3` passed |

## Result

- Final status: Pass.
- Remaining risks: current source trade payload lacks `quantity/shares/commission/tax/slippage`; these fields remain null unless later artifact/API parsers provide authoritative values. Position snapshots remain future parser work.
- Need production backend restart: no.
- Need dev service restart: done for `8011` and `3011` only to load current code.
