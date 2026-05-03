# Qlib authoritative minute bin export validation 20260503

- Module: qlib_data
- Level: L4
- Date: 2026-05-03T05:22:34
- Git commit: e120730 (pre-change working tree)
- Operator: lc999

## Scope

- Changed files: `backend/qlib_exporter/authoritative_bin_exporter.py`, `backend/qlib_exporter/router.py`, `frontend/src/app/qlib/page.tsx`, `scripts/qlib_authoritative_bin_export.py`, `scripts/qlib_authoritative_csv_bin_audit.py`, `scripts/qlib_authoritative_smoke_backtest.py`, analysis doc.
- Impacted flows: AIstock Qlib day/minute bin export, UI bin export selection, CLI authoritative CSV/bin export, QE/V25 1min provider validation.
- Business goal: produce a full 1min Qlib bin snapshot through DB -> strict CSV -> official dump_bin with all QE/V25 required fields and no silent missing-data fallback.
- Out of scope: restarting production backend 8001, replacing active QE minute provider path automatically, running a new QE experiment.
- Protected assets reviewed: no QE/RD-Agent workspace, model artifact, StrategyPackage manifest, HMM snapshot, or paper ledger was modified.

## Environment

- Backend port: not restarted; API flow was monkeypatched in-process.
- Frontend port: not started; TypeScript static check only.
- TDX port: not used directly.
- Conda/env: WSL `rdagent-gpu` for Qlib dump/validation/backtest; Windows Python for py_compile/API smoke.
- Database: local PostgreSQL via `TDX_DB_PASSWORD`.
- Browser/headless: not used.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 guardrails | No high-risk syntax/type failure | `py_compile`, `npm exec tsc -- --noEmit` | PASS |
| Guardrail scan | No high severity hardcoded/silent-fallback finding in touched files | `scan_quality_guardrails.py --fail-on HIGH`; 2 medium pre-existing/raw-json style findings only | PASS |
| API flow | UI-backed API accepts `stock_minute`, uses `1min`, writes meta | monkeypatched `unified_bin_export_v2`; output `API_SMOKE_OK` | PASS |
| CSV export | Full DB -> CSV strict export succeeds with all fields | 5,515 CSV files, 700,457,459 rows, skipped=0 | PASS |
| dump_bin | Official dump creates Qlib 1min feature bins | 66,180 feature files, 134,807 calendar rows, returncode=0 | PASS |
| Full coverage | No CSV rows missing from bin finite values | CSV-vs-bin audit checked 8,405,489,508 field values, errors=0 | PASS |
| Value samples | DB formula equals Qlib bin values | gap/edge/recent samples all max_abs_diff=0 | PASS |
| Qlib backtest | Full minute provider can run NestedExecutor 1min smoke | minute_nan all 0, portfolio rows=6 | PASS |
| Asset safety | No protected asset modified silently | git status reviewed; reports ignored; WSL data output explicit | PASS |

## Commands

```bash
python -m py_compile backend/qlib_exporter/authoritative_bin_exporter.py scripts/qlib_authoritative_bin_export.py scripts/qlib_authoritative_smoke_backtest.py scripts/qlib_authoritative_csv_bin_audit.py backend/qlib_exporter/router.py
cd frontend && npm exec tsc -- --noEmit
python .codex/skills/verify-aistock-feature/scripts/scan_quality_guardrails.py backend/qlib_exporter/authoritative_bin_exporter.py scripts/qlib_authoritative_bin_export.py scripts/qlib_authoritative_smoke_backtest.py backend/qlib_exporter/router.py frontend/src/app/qlib/page.tsx docs/analysis/P0_qlib_authoritative_bin_export_tool_and_validation_20260503.md --fail-on HIGH

PYTHONPATH=/mnt/f/Dev/AIstock TDX_DB_PASSWORD=*** PYTHONWARNINGS=ignore python scripts/qlib_authoritative_bin_export.py --dataset stock_minute --stage export --snapshot-id qlib_minute_authoritative_full_20260428 --start 2024-01-02 --end 2026-04-28 --basis-start 2024-01-02 --basis-end 2026-04-28 --exchanges sh,sz,bj --csv-root /home/lc999/data/qlib_csv_authoritative --bin-root /home/lc999/data --minute-chunked-export --minute-code-batch-size 100 --minute-chunk-months 3 --resume-csv
PYTHONPATH=/mnt/f/Dev/AIstock TDX_DB_PASSWORD=*** PYTHONWARNINGS=ignore python scripts/qlib_authoritative_bin_export.py --dataset stock_minute --stage dump --snapshot-id qlib_minute_authoritative_full_20260428 --start 2024-01-02 --end 2026-04-28 --basis-start 2024-01-02 --basis-end 2026-04-28 --exchanges sh,sz,bj --csv-root /home/lc999/data/qlib_csv_authoritative --bin-root /home/lc999/data --dump-workers 16
python scripts/qlib_authoritative_csv_bin_audit.py --csv-dir /home/lc999/data/qlib_csv_authoritative/qlib_minute_authoritative_full_20260428/stock_minute_1min --qlib-dir /home/lc999/data/qlib_minute_authoritative_full_20260428 --freq 1min --workers 8 --max-errors 20
python scripts/qlib_authoritative_smoke_backtest.py --minute-provider-uri /home/lc999/data/qlib_minute_authoritative_full_20260428 --day-provider-uri /home/lc999/data/qlib_bin --start 2025-07-08 --end 2025-07-15 --codes 000001.SZ,000063.SZ,600519.SH
```

## Evidence

- API calls: in-process monkeypatched API smoke returned `API_SMOKE_OK` and meta contains `freq_types=["1min"]`, `last_end_dates.stock_minute_1min=2025-07-16`.
- DB checks: sample validations recomputed expected DB values for 2025-07 gap, 688766 prev_close edge, 301449 listing edge, and 2026-04-28 latest date; all max abs diff=0.
- Log/report files: `reports/qlib_authoritative_export/qlib_minute_authoritative_full_20260428_*.json`.
- Playwright report/trace: not run; UI change covered by TypeScript and API smoke.
- Screenshots: not needed.
- Business output summary: full CSV 700,457,459 rows; bin 66,180 field files; full CSV-vs-bin audit error_count=0; Qlib backtest `last_account=1004474.9876913929`.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Full export failed on `688766.SH` missing `pre_close` | `stk_limit.up_limit/down_limit` existed but `pre_close` was null on zero-volume minute dates | Fill only from previous valid daily close and record `previous_daily_prev_close_filled_rows` | 688766 export + DB-vs-bin validation passed, max_abs_diff=0 |
| Full DB coverage validation hit PostgreSQL result-size OOM | Single aggregate query over all stock-date minute counts was too large | Added batch-based validation path and used full CSV-vs-bin audit for final full coverage | CSV-vs-bin audit checked 8,405,489,508 values, errors=0 |
| Official `check_data_health.py --qlib_dir ... --freq 1min` did not run checks by default | Fire default instantiated checker and printed help unless a subcommand is supplied | Did not use it as final oracle; used direct CSV-vs-bin and DB-vs-bin validators | See audit and sample validation outputs |

## Result

- Final status: PASS.
- Remaining risks: full DB-vs-bin value comparison for all 700M minute rows was intentionally not run because it is impractical; deterministic export formulas were validated by strict generation plus targeted DB-vs-bin samples and full CSV-vs-bin coverage.
- Need production backend restart: no; code changes require normal backend/frontend reload before UI/API use.
- Need dev service restart: only if manually testing the UI route in a running dev server.
