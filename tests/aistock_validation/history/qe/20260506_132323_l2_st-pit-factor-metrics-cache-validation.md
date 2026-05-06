# ST PIT factor metrics cache validation

- Module: qe
- Level: L2
- Date: 2026-05-06T13:23:23
- Git commit at run start: 87c5593
- Operator: lc999

## Scope

- Changed files: ST PIT factor universe service, official factor metrics engine/service, factor cache pipeline/loader/coverage, correlation compute/persist, backfill script, migration, tests, design doc.
- Impacted flows: factor independent metrics, single-factor cache, factor cache coverage checks, factor correlation matrix, QE factor cache reporting.
- Business goal: official factor metrics/cache/correlation all use `shsz_st_pit_active_v1` with daily buy-eligible ST PIT semantics and no silent cache mixing.
- Out of scope: production H5/Bin replacement, historical QE experiment replay, production backend 8001 restart.
- Protected assets reviewed: no H5/Bin/Qlib model/StrategyPackage/Paper ledger assets modified.

## Environment

- Backend port: 8012 for temporary validation app only; production 8001 not touched.
- Frontend port: not used.
- TDX port: not used.
- Conda/env: `C:/Users/lc999/miniconda3/envs/AIstock/python.exe`.
- Database: local PostgreSQL `aistock`, `TDX_DB_PASSWORD` supplied in shell.
- Browser/headless: not used; backend/API/data validation only.

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| L0 compile | Target backend/router/script files compile | `python -m py_compile ...` | PASS |
| L0 guardrail | No P0 guardrail blocker; findings documented | changed-only scan: P0=0, P1=6 planning-file/location findings, P2=101 complexity/timeout review findings | PASS WITH TRIAGE |
| L1 unit tests | ST PIT mask, coverage denominator, metadata, cache mismatch checks pass | `20 passed, 1 numpy warning` | PASS |
| DB migration | New columns and comments exist | migration applied; 7 sampled comments non-empty | PASS |
| PIT data accuracy | service eligible index equals direct SQL join | Jan 2025 rows 88193 == 88193 | PASS |
| API flow | temporary API exposes OpenAPI, ST PIT status, official evaluation summary | 8012 smoke returned 200 for 3 endpoints | PASS |
| Asset safety | no production 8001 restart or protected asset modification | only temp 8012 app; no H5/Bin touched | PASS |

## Commands

```powershell
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m py_compile backend/services/quantevolver/factor_universe_mask_service.py backend/services/quantevolver/data_snapshot_manager.py backend/services/quantevolver/factor_value_pipeline.py backend/services/quantevolver/qe_eval_v2_metric_engine.py backend/services/quantevolver/factor_official_evaluation_service.py backend/services/quantevolver/factor_cache_coverage.py backend/services/quantevolver/factor_value_loader.py backend/services/quantevolver/correlation_engine.py backend/services/quantevolver/correlation_compute_service.py scripts/backfill_factor_cache.py backend/routers/quantevolver.py
C:/Users/lc999/miniconda3/envs/AIstock/python.exe -m pytest backend/tests/test_factor_st_pit_metrics_cache.py backend/tests/test_factor_metrics_authority_static.py -q -p no:cacheprovider
# DB migration and accuracy checks executed through backend.db.pg_pool with local DB password in shell env.
# Temporary validation app on 8012 included backend.routers.quantevolver and backend.routers.stock_universe only.
Invoke-RestMethod http://127.0.0.1:8012/openapi.json
Invoke-RestMethod 'http://127.0.0.1:8012/api/v1/stock-universe/st-pit/status?universe_key=shsz_st_pit_active_v1'
Invoke-RestMethod http://127.0.0.1:8012/api/v1/quantevolver/official-evaluation/summary
```

## Evidence

- API calls: OpenAPI paths=120; ST PIT status=`ready`, dirty=`false`, start=`2018-08-01`, end=`2026-04-27`; official summary keys=`ok,summary,total,calc_engine`.
- DB checks: `market.stock_universe_pit_state` ready/dirty=false; service eligible index rows=88193 and direct SQL join rows=88193 for 2025-01-01~2025-01-31.
- ST PIT sample: `000007.SZ` has multi-span exit/restore rows, proving??????? semantics are represented.
- Migration: `factor_metrics_st_pit_universe_metadata_20260506.sql` applied; sampled new column comments are non-empty.
- Logs: temporary uvicorn 8012 returned HTTP 200 for all three smoke endpoints, then was stopped.
- Guardrail: `20260506_132323_l2_st-pit-factor-metrics-cache-validation-guardrail.md`; no P0 blockers. P1 findings are the known root planning-file pattern from `planning-with-files`; P2 findings are complexity review items on existing high-dimensional QE files.
- Business output summary: official factor path now has a single ST PIT metadata contract from snapshot -> cache -> metric -> correlation.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| `factor_universe_mask_service.py` missing in task worktree | earlier edit landed in root worktree | copied/repaired task-owned file into isolated worktree | target py_compile passed |
| PostgreSQL rejected migration at `ALTER` | SQL file had UTF-8 BOM | rewrote migration as UTF-8 without BOM | migration applied |
| pytest failed: `_group_returns_from_matrices` no `sample_mask` | metric engine used ST PIT mask but helper signature was old | added optional `sample_mask` validation and masking | 20 tests passed |
| full `backend.main` temp startup failed on unrelated `backend.services.rl_execution` import | existing app-level optional RL module missing in current repo state | used temporary scoped validation app with only impacted QE + stock-universe routers | 8012 API smoke passed |

## Result

- Final status: PASS.
- Remaining risks: full production `backend.main` still has unrelated RL import startup blocker in this worktree; not modified because outside current ST PIT factor task.
- Need production backend restart: no.
- Need dev service restart: yes, after merge the backend process must restart to load new code/migration-aware writes.
