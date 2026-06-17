# BUG-375 official full-compute post-merge validation

- Module: qe / quantevolver factor cache and correlation
- Level: L3 post-merge validation, with UI read-only smoke
- Date: 2026-06-15T23:17:17+08:00
- Git commit: 3ba75f22 (`main` == `origin/main`)
- Operator: lc999 / Codex
- PRs: #1117 fix merge `bc11b17b`, #1118 close-sync merge `3ba75f22`

## Scope

- Impacted flows: factor library official independent-metric compute, legacy `/official-evaluation/compute`, `/factor-cache/compute`, official offline single cache, factor-correlation compute/status/overview.
- Business goal: official independent metrics, correlation, and QE backtest paths converge on `rdagent_assets/factor_values` official offline cache; no official path falls back to `factor_values_realtime` or data snapshot/realtime semantics.
- Out of scope: no 575-factor correlation full recompute was submitted in this validation because it dispatches a WSL/compute-node long task and writes correlation DB state.
- Protected assets reviewed: no protected model/QE/StrategyPackage/HMM assets modified; this run added only this validation record.

## Environment

- Backend: existing user-restarted backend on `127.0.0.1:8001`, read-only smoke only.
- Frontend: existing frontend on `127.0.0.1:3000`, read-only Playwright smoke only.
- WSL/compute-node dispatch: not triggered during this post-merge validation.
- Database: read-only API checks plus existing correlation/cache status reads; no DDL.
- Browser/headless: Playwright Chromium headless, opened pages only and did not click compute buttons.

## Design Compliance Matrix

| Design item | Implementation refs | Evidence | Status | Gap / exception |
|---|---|---|---|---|
| BUG-375 UI official metrics buttons must submit official full compute, not snapshot compute | `frontend/src/app/quantevolver/components/FactorList.tsx` `submitOfficialFullCompute` and `/factor-cache/compute` calls | `rg` shows no `/official-evaluation/compute` inside `FactorList.tsx`; focused tests `20 passed`; frontend build passed | passed | Old `activeSnapshot` remains only for legacy full-flow UI panels, not official metrics submission |
| Legacy `/official-evaluation/compute` must be compatibility forwarding layer | `backend/routers/quantevolver.py`, `backend/services/quantevolver/factor_official_evaluation_service.py` | `FactorOfficialEvaluationService.compute()` returns `task_type=official_factor_full_compute`, `legacy_compatibility=official_evaluation_compute_forwarded_to_official_factor_full_compute`, `cache_source=official_offline_backtest_factor_data`; focused tests passed | passed | API path remains for compatibility |
| Official full compute must use backtest-data official cache and code_text semantics | `backend/services/quantevolver/official_factor_full_compute_dispatch_service.py`, `backend/services/quantevolver/official_factor_batch_compute_service.py` | Static refs show `task_type=official_factor_full_compute`, `cache_source=official_offline_backtest_factor_data`, `cache_root=rdagent_assets/factor_values`; runtime API route exists | passed | WSL full recompute not re-triggered in this validation |
| Correlation must consume official offline single cache and not realtime cache | `backend/services/quantevolver/correlation_compute_service.py`, `backend/scripts/run_correlation_compute_wsl.py` | `backend/tests/test_correlation_compute_independence.py` passed; runtime overview reports `cache_source=offline_research_backtest_factor_values`, `cache_root=F:\Dev\AIstock\rdagent_assets\factor_values` | passed | Current correlation DB still has only previous 105-factor result until user triggers recompute |
| Production safety gates | no migration/dependency/runtime ownership changes | `production_ddl_gate=noop`, `production_frontend_dependency_gate=noop`, `production_backend_dependency_gate=noop`; no production restart/DDL done by Codex | passed | User already restarted backend before validation |

## Commands

```powershell
python -m pytest -q backend/tests/quantevolver/test_official_evaluation_cache_source.py backend/tests/quantevolver/test_official_factor_cache_dispatch_route.py backend/tests/quantevolver/test_bug_013_014_factor_eligibility_correlation.py backend/tests/quantevolver/test_official_factor_batch_compute.py
python -m ruff check backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/official_factor_full_compute_dispatch_service.py backend/services/quantevolver/official_factor_batch_compute_service.py backend/routers/quantevolver.py backend/tests/quantevolver/test_official_evaluation_cache_source.py backend/tests/quantevolver/test_official_factor_cache_dispatch_route.py backend/tests/quantevolver/test_bug_013_014_factor_eligibility_correlation.py backend/tests/quantevolver/test_official_factor_batch_compute.py
python -m py_compile backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/official_factor_full_compute_dispatch_service.py backend/services/quantevolver/official_factor_batch_compute_service.py backend/routers/quantevolver.py backend/scripts/run_official_factor_full_compute_wsl.py backend/scripts/run_official_evaluation_wsl.py backend/scripts/run_correlation_compute_wsl.py
python -m nox -s validation_module_registry_l0
python -m nox -s l0
$env:QE_READ_L3_SKIP_UI='1'; python -m nox -s qe_read_l3
npm --prefix frontend run lint
npm --prefix frontend run build
git diff --check
python -m pytest -q backend/tests/test_correlation_compute_independence.py backend/tests/quantevolver/test_official_runtime_validation.py
# Read-only runtime API smoke:
Invoke-RestMethod http://127.0.0.1:8001/openapi.json
Invoke-RestMethod http://127.0.0.1:8001/api/v1/quantevolver/factor-cache/active-tasks
Invoke-RestMethod http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/overview
Invoke-RestMethod http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/status?include_disabled=false
Invoke-RestMethod http://127.0.0.1:8001/api/v1/quantevolver/evolution/correlations/status?include_disabled=true
# Read-only UI smoke:
Playwright Chromium opened /quantevolver/factors and /quantevolver/factor-correlation without clicking compute buttons.
```

## Evidence

- Focused backend regression: `20 passed in 5.62s`.
- Correlation/official runtime regression: `13 passed in 2.28s`.
- Ruff: `All checks passed!`.
- Py compile: passed.
- `validation_module_registry_l0`: `8 passed`, successful.
- `l0`: successful, guardrail scan blocking=0; existing baseline/P2 findings only.
- `qe_read_l3`: successful; backend sub-session `14 passed`.
- Frontend lint: passed with existing `react-hooks/exhaustive-deps` warnings.
- Frontend build: compiled successfully and generated `/quantevolver/factors` and `/quantevolver/factor-correlation` routes.
- `git diff --check`: passed.
- Runtime API smoke: `openapi_ok=true`, `/api/v1/quantevolver/factor-cache/compute` route present, `/official-evaluation/compute` route present, `active_task_count=0`.
- Runtime cache/correlation status: `enabled.total=575`, `enabled.evaluated=575`, `enabled.correlation_cached=575`, `enabled.correlation_computed=105`; `single_cache.cached_count=575`, `cache_source=offline_research_backtest_factor_values`, `cache_root=F:\Dev\AIstock\rdagent_assets\factor_values`, `date_range=2018-08-21~2026-04-30`, `window_train_start=2018-08-01`, `window_backtest_end=2026-04-28`.
- Parquet read-only audit: 575 single parquet files, 0 parquet index errors; 535 factors cover `2018-08-01~2026-04-30`, remaining factors start later due to available data/warmup/listing coverage; `_meta.json` currently records 89 factors and 486 orphan parquet files, so metadata integrity is not fully normalized even though parquet cache exists.
- UI smoke: `http://127.0.0.1:3000/quantevolver/factors` and `/quantevolver/factor-correlation` both returned 200 with no `pageerror`, `console.error`, or `requestfailed`.

## Result

- Final status: post-merge regression and read-only runtime verification passed for BUG-375 contract.
- Current business state: backend/UI can submit official full compute via `/factor-cache/compute`; current cache has 575 enabled factor parquet files, but the current correlation DB still shows the previous 105-factor matrix until the user clicks correlation recompute.
- Production runtime touched by Codex: no restart, no write operation, no full compute submission.
- Production DB/DDL touched by Codex: none.
- Remaining risk: `_meta.json` is incomplete for the 575 parquet cache (`meta_factor_count=89`, `orphan_parquet_count=486`); correlation code can infer missing metadata read-only, but official cache metadata should be normalized by the next full compute/metadata repair flow.
- production_ddl_gate=noop
- production_frontend_dependency_gate=noop
- production_backend_dependency_gate=noop

## Post-merge Full Correlation Recompute (2026-06-16)

- User-authorized action: submitted `/api/v1/quantevolver/evolution/correlations/compute` with `force_recompute=true`, `include_disabled=false`, no `data_date` override.
- Dispatch task: `494f94c7-e114-4ca5-bc15-0f3f62a9b027`, remote WSL task `266`, node `wsl2-5080`.
- Runtime source contract: dispatch logs confirmed `/mnt/f/Dev/AIstock/rdagent_assets/factor_values` with `cache_source=offline_research_backtest_factor_values`; no `factor_values_realtime` cache was used.
- Cache admission: logs reported `575/575` requested factors entered correlation compute, all with `as_of_date=2026-04-30`; `_meta.json` still has the known warning `orphan_parquets=486` and runtime inferred missing metadata read-only from parquet.
- Result: dispatch status `success`; latest metadata `num_factors=575`, `num_high_corr_pairs=1978`, `as_of_date=2026-04-30`, `hdf5_path=/mnt/f/Dev/AIstock/data/correlation_matrices/corr_20260430.h5`.
- DB/API outcome: `/correlations/status?include_disabled=false` reported `db_correlation_count=164429`, `live_high_corr_count_07=1978`, `live_high_corr_count_05=6691`, `latest_computation.num_factors=575`.
- Residual: `uncorrelated_factor_count=1`, identified as enabled factor `quality_structure_composite` (`id=246`), with `correlation_pair_count=0`; factor detail showed `has_cache=false` in the factor list cache view and recent independent metrics have zero recent coverage windows, so it produced no pair rows even though the full recompute task completed successfully.
- Production safety: no backend/frontend restart, no DDL, no dependency change; the action intentionally wrote only the correlation result tables/artifacts as part of the user-authorized full recompute.
