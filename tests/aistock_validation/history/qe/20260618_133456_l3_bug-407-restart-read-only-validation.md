# BUG-407 restart read-only validation

- Module: qe
- Level: L3
- Date: 2026-06-18T13:34:56+08:00
- Git commit: 32dca6cf
- Operator: lc999
- Constraint: user explicitly required no full factor independent-metric recompute and no compute-type POST; this run only used GET/API/UI/static read-only checks plus targeted pytest.

## Scope

- Changed files: no code changes in this validation run; BUG-407 code already merged on main.
- Impacted flows: factor transformation DTO/UI, official factor cache inventory, official independent metrics summary, factor correlation status/overview/matrix, QE official factor-cache contract.
- Business goal: after backend/frontend restart, official independent metrics, factor correlation, and QE backtest cache paths must use the same official offline factor-value cache `rdagent_assets/factor_values`; UI must not expose old realtime DTO/cache semantics.
- Out of scope: full factor independent-metric compute, factor-cache compute, correlation recompute, transformation batch run, WSL compute dispatch, production DB writes.
- Protected assets reviewed: `rdagent_assets/factor_values` was read-only; no writes or recompute were triggered.

## Environment

- Backend port: 8001 (`GET /api/v1/health` -> 200)
- Frontend port: 3000 (`GET /quantevolver/factor-transformation` and `/quantevolver/factor-correlation` -> 200 through Playwright)
- TDX port: not used
- Conda/env: local Python invocation from `F:\Dev\AIstock`
- Database: read-only API access only
- Browser/headless: Playwright Chromium headless, screenshots saved under `tests/aistock_validation/history/qe/evidence/bug407_restart_readonly/`

## Design Compliance Matrix

| Design / acceptance item | Implementation refs | Evidence | Result | Gap / exception |
|---|---|---|---|---|
| Official computable factors must be based on available `code_text`, not transformation status or `qe_code_path` | `backend/services/quantevolver/factor_value_pipeline.py` | `pytest backend/tests/quantevolver/test_legacy_factor_paths_removed.py ...` passed; test asserts SQL has `is_available = true`, `code_text IS NOT NULL`, no `transformation_status`, `qe_code_path`, `last_transformation_at` | Pass | None |
| Factor transformation API/UI exposes non-official fields and does not expose realtime DTO names | `backend/routers/quantevolver.py`, `frontend/src/app/quantevolver/factor-transformation/*` | `GET /factor-transformation/status?limit=3&offset=0`: `has_non_official_code_field=true`, `non_official_code_path_field=true`, `has_realtime_code_field=false`, `realtime_code_text_field=false`; Playwright page has no visible old terms | Pass | DB column names remain internally for non-official code compatibility only; public DTO/UI is renamed |
| Correlation uses official offline cache only | `backend/services/quantevolver/correlation_compute_service.py`, `backend/routers/quantevolver_evolution.py` | `GET /correlations/cache-status`: `cached_count=575`, `total_computable=575`, `uncached_count=0`, `cache_source=offline_research_backtest_factor_values`, `cache_root=F:\Dev\AIstock\rdagent_assets\factor_values`, `contains_realtime=false` | Pass | None |
| UI shows unified official cache and current window/counts | `frontend/src/app/quantevolver/factor-correlation/*` | Playwright factor correlation page: shows official window, `575/575`, `rdagent_assets\factor_values`, and copy that independent metrics/correlation/QE backtest share one cache; no console/page/request errors | Pass | None |
| Independent metrics summary is available without recompute | `backend/routers/quantevolver.py`, `backend/services/quantevolver/factor_official_evaluation_service.py` | `GET /official-evaluation/summary`: `ok=true`, `total=778`, snapshot distribution `2026-04-30:575`, `2026-04-10:203`, `calc_engine=qe_eval_v2` | Pass | Historical 203 factors at `2026-04-10` remain in summary because no full recompute was requested |
| Runtime has no active factor compute task | `backend/routers/quantevolver.py` | `GET /factor-cache/active-tasks`: `tasks=0` before and after validation | Pass | None |
| Legacy realtime cache business paths are removed from active official/correlation/QE code | backend/frontend/scripts/tests scoped source scan | `rg factor_values_realtime` excluding logs/history/docs/bugs -> no active source hit; legacy `/factor-values`, `DataSnapshotManager`, legacy env -> no active source hit; FactorValueLoader occurrences use explicit `source="single"` in official paths | Pass | Paper live-data `source="auto"` hits are unrelated to factor cache loader and not part of official factor cache path |
| Restarted UI/API should show full current correlation data | runtime API + UI | `GET /correlations/status`: `status=idle`, `db_correlation_count=164429`, `latest_num_factors=574`, `uncorrelated_factor_count=1`; UI matrix displays `575 x 575` and computed matrix count 574 | Pass | One factor is classified as uncorrelated/no-valid-pair rather than missing cache |

## Commands

```bash
# Health and read-only API checks
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/health
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/factor-transformation/status?limit=3\&offset=0
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/factor-transformation/stats
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/factor-cache/active-tasks
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/official-evaluation/summary
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/cache-status
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/status
Invoke-RestMethod -Method GET http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/overview

# UI read-only validation, via Playwright Chromium from node_repl
# Opened only:
#   http://127.0.0.1:3000/quantevolver/factor-transformation
#   http://127.0.0.1:3000/quantevolver/factor-correlation

# Static old-path scans
rg -n --hidden -S --glob '!frontend/node_modules/**' --glob '!node_modules/**' --glob '!backend/logs/**' --glob '!tests/aistock_validation/history/**' --glob '!tests/aistock_validation/bugs/**' --glob '!docs/**' --glob '!findings.md' --glob '!progress.md' "factor_values_realtime" backend frontend/src scripts tests .github noxfile.py
rg -n --hidden -S --glob '!frontend/node_modules/**' --glob '!node_modules/**' --glob '!backend/logs/**' --glob '!tests/aistock_validation/history/**' --glob '!tests/aistock_validation/bugs/**' --glob '!docs/**' "DataSnapshotManager|data_snapshot_manager|AISTOCK_ENABLE_LEGACY_REALTIME_FACTOR_CACHE|LEGACY_REALTIME_FACTOR_CACHE|/factor-values|factor-values" backend/routers backend/services frontend/src scripts tests .github noxfile.py
rg -n --hidden -S --glob '!frontend/node_modules/**' --glob '!node_modules/**' --glob '!backend/logs/**' --glob '!tests/aistock_validation/history/**' --glob '!tests/aistock_validation/bugs/**' --glob '!docs/**' "FactorValueLoader" backend/routers backend/services frontend/src scripts tests .github noxfile.py

# Targeted regression tests
python -m pytest backend/tests/quantevolver/test_legacy_factor_paths_removed.py backend/tests/quantevolver/test_official_factor_cache_dispatch_route.py backend/tests/test_correlation_compute_independence.py backend/tests/test_factor_cache_wsl_env.py -q
```

## Evidence

- API calls:
  - `/api/v1/health`: 200, `status=ok`.
  - `/factor-transformation/status?limit=3&offset=0`: 200, total 778, new fields present, old realtime fields absent.
  - `/factor-transformation/stats`: 200, `stats.total=778`, `stats.success=778`, `stats.has_original_code=778`, `stats.has_non_official_code=778`.
  - `/factor-cache/active-tasks`: 200, `tasks=[]`.
  - `/official-evaluation/summary`: 200, `total=778`, snapshot distribution `2026-04-30=575`, `2026-04-10=203`.
  - `/correlations/cache-status`: 200, `cached_count=575`, `total_computable=575`, `uncached_count=0`, `cache_source=offline_research_backtest_factor_values`, `cache_root=F:\Dev\AIstock\rdagent_assets\factor_values`, `date_range=2018-08-21~2026-04-30`, `contains_realtime=false`.
  - `/correlations/status`: 200, `status=idle`, `db_correlation_count=164429`, `latest_as_of_date=2026-04-30`, `latest_num_factors=574`, `uncorrelated_factor_count=1`.
  - `/correlations/overview`: 200, `enabled.total=575`, `enabled.correlation_cached=575`, `enabled.correlation_computed=574`, official window `2018-08-01~2026-04-28`, cache root `F:\Dev\AIstock\rdagent_assets\factor_values`, `contains_realtime=false`.
- Local cache inventory:
  - `rdagent_assets/factor_values/single`: 576 parquet files total, including `_merged_panel.parquet`; 575 factor parquet files.
  - Factor parquet total size: 26327.1 MB; all parquet total size including `_merged_panel`: 29148.3 MB.
  - `_meta.json`: 89 factor entries; 486/487 disk/meta gap depending on whether `_merged_panel` is counted. Runtime status correctly uses disk factor parquet count for `cached_count=575`.
  - Top meta windows: `as_of_date=2026-04-30` for all 89 meta entries, `window_train_start=2018-08-01`, `window_backtest_end=2026-04-28`.
- UI evidence:
  - `tests/aistock_validation/history/qe/evidence/bug407_restart_readonly/factor_transformation.png`
  - `tests/aistock_validation/history/qe/evidence/bug407_restart_readonly/factor_correlation.png`
  - Playwright observed no console errors, page errors, request failures, or unexpected API 4xx/5xx on both pages.
- Static evidence:
  - Active source scan has no `factor_values_realtime` hits after excluding logs/history/docs/BUG records.
  - Active source scan has no legacy `/factor-values`, `DataSnapshotManager`, or legacy realtime cache env hits.
  - FactorValueLoader official occurrences are explicit `source="single"`; no official naked default loader remains.
- Tests:
  - `38 passed in 5.52s` for targeted official cache / legacy path / correlation / WSL-env tests.
- Logs:
  - Tail scan showed no new `factor_values_realtime` evidence in current active source/API flow. Existing backend log warnings are Paper/Simulation runtime minute data issues and unrelated to BUG-407 factor cache validation.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `GET /api/v1/quantevolver/factor-cache/stats` timed out at 180s in earlier read-only probing | The endpoint scans large 575-factor / 26GB official cache and computes heavier stats synchronously | No code change in this validation run; record as performance/usability risk for a separate issue if needed | Not rerun to avoid repeated heavy read-only load |

## Result

- Final status: passed for BUG-407 restart read-only validation.
- Business outcome: after restart, UI can read full current correlation data from official cache without recomputing independent metrics; official cache shows 575/575 cached, correlation status shows 574 matrix factors plus 1 no-valid-pair/uncorrelated factor.
- Remaining risks: `factor-cache/stats` is too heavy for a UI/status endpoint under current cache size; `_meta.json` still records only 89 factors while disk cache has 575 factor parquets, but correlation/cache-status already uses disk inventory and exposes the disk/meta gap.
- Need production backend restart: no additional restart required; user already restarted before this validation.
- Need dev service restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.
