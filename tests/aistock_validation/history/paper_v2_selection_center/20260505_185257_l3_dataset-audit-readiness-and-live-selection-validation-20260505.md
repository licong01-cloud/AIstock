# Dataset Audit Readiness And Live Selection Validation 20260505

- Module: paper_v2_selection_center
- Level: L3
- Date: 2026-05-05
- Operator: Codex

## Scope

- Changed files: `backend/services/strategy_package/live_inference.py`, StrategyPackage datetime compatibility files, `backend/tests/selection_center/test_runtime_selection.py`.
- Impacted flows: `market.dataset_date_refresh_audit` readiness, StrategyPackage live selection artifact generation, Paper v2 readiness preflight.
- Business goal: verify whether updated audit rows are accurate enough for Paper v2 selection and paper trading.
- Out of scope: starting/restarting production backend `8001`; UI browser E2E; modifying QE/model/HMM frozen assets.
- Protected assets reviewed: no model weight or QE workspace file was modified; validation created failed Selection Center run records only.

## Environment

- Backend port: not restarted; no temporary backend server started.
- Frontend port: not used.
- TDX port: `19080` probed indirectly by live inference setup.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`; WSL `rdagent-gpu` for live inference.
- Database: local PostgreSQL/TimescaleDB `aistock`.
- Latest completed trading day from DB calendar: `2026-04-30`; previous trading day: `2026-04-29`.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Audit schema | `market.dataset_date_refresh_audit` has required enhanced fields/comments | `local_data_management_audit` | PASS |
| Audit freshness | Paper v2 required datasets are fresh enough | `paper_v2_data_quality`, `tmp/audit_readiness_check_20260505.json` | PASS |
| Physical table match | Latest audit row counts match physical table counts for required datasets | `tmp/audit_readiness_check_20260505.json`, `tmp/audit_required_exact_20260505.json` | PASS |
| Backend regression | Paper v2 + Selection Center + StrategyPackage tests pass | `paper_v2_backend` | PASS |
| 2026-04-30 live selection | Generate current live/latest selection artifact for `pkg_b668...` | `tmp/paper_v2_selection_readiness_20260505.json` | FAIL |
| Paper v2 readiness after selection | Preflight `paper_eaab...` for `2026-04-30` | blocked by live selection failure | BLOCKED |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe tmp/audit_readiness_check_20260505.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe tmp/audit_required_exact_20260505.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_continues_when_node_static_loader_file_is_404 backend/tests/selection_center/test_runtime_selection.py::test_live_inference_materialize_uses_cached_params_when_node_mlruns_params_404 backend/tests/selection_center/test_runtime_selection.py::test_live_inference_load_source_materializes_via_node_api_not_db_workspace -q -p no:cacheprovider
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_backend
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s local_data_management_audit
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m nox -s paper_v2_data_quality
C:/Users/lc999/miniconda3/envs/AIstock/python.exe tmp/paper_v2_selection_readiness_20260505.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe tmp/adj_factor_check.py
```

## Evidence

- DB checks:
  - `tmp/audit_readiness_check_20260505.json`
  - `tmp/audit_required_exact_20260505.json`
  - `tmp/adj_factor_check_20260505.json`
- Business flow check:
  - `tmp/paper_v2_selection_readiness_20260505.json`
- Automated tests:
  - targeted live inference tests: `3 passed`
  - `paper_v2_backend`: `133 passed`
  - `local_data_management_audit`: PASS
  - `paper_v2_data_quality`: PASS with legacy ledger WARN only

## Findings

- The audit table is accurate for the current Paper v2 smoke requirements:
  - `suspend_d`: latest success `2026-05-06`; exact `2026-04-30` success row exists with 98 rows.
  - `stk_limit`: exact `2026-04-30` success row exists with 7,574 rows and matches `market.stk_limit`.
  - `kline_daily_raw`, `daily_basic`, `stock_moneyflow_ts -> market.moneyflow_ts`, `sector_data`, `index_daily`: required `2026-04-29` rows exist and pass.
  - Physical tables are current through `2026-04-30` for daily/moneyflow/sector/index/minute datasets.
- The default smoke's "latest success" for `stk_limit` is `2026-05-01` with zero rows because that non-trading date was recorded as success. Exact `2026-04-30` Paper v2 readiness is still correct, but local data-management policy should decide whether non-trading zero-row success should be `empty_valid` or excluded from latest-success freshness displays.
- `kline_minute_raw` has no audit success row, but current Paper v2 gates validate minute bars directly from `market.kline_minute_raw`; physical latest minute bar is `2026-04-30 15:00:00+08:00` with 1,322,402 rows on that date.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Live selection first failed at `mlruns-params` 404 from node API | RD-Agent node API did not expose historical QE mlruns params, while AIstock-local StrategyPackage runtime cache already had `model/params.pkl` | Added local StrategyPackage model-cache reuse when node `mlruns-params` is unavailable | targeted pytest `3 passed`; next live run advanced beyond asset materialization |
| WSL live inference then failed importing `datetime.UTC` | WSL `rdagent-gpu` is Python 3.10; `datetime.UTC` exists only in Python 3.11+ | Changed StrategyPackage datetime usage to `timezone.utc` for Python 3.10 compatibility | next live run advanced into actual factor/data loading |
| 2026-04-30 live selection still failed | `qe_data_service.load_daily_pv` failed because selected universe included `301599.SZ`, which lacks full-window `adj_factor` coverage even though latest `adj_factor` date exists | Not fixed in this run; requires local data completeness/universe policy work | `tmp/paper_v2_selection_readiness_20260505.json`, `tmp/adj_factor_check_20260505.json` |

## Result

- Final status: audit table readiness is PASS; actual latest-date live selection/Paper v2 readiness is NOT fully guaranteed yet.
- Remaining risk: audit freshness by dataset/date is necessary but not sufficient; live inference also needs symbol-level coverage checks for `adj_factor` and other static-factor source tables.
- Need production backend restart: no restart performed; code changes require the user's normal deployment/restart process before production `8001` uses them.
- Recommended next gate: add a strict data-quality check comparing candidate universe symbols against `adj_factor` historical window coverage before allowing live selection artifact generation.
