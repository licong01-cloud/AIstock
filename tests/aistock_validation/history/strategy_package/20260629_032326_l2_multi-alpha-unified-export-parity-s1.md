# multi-alpha unified export parity S1

- Module: strategy_package
- Level: L2
- Date: 2026-06-29T03:23:26+08:00
- Git commit at run start: 64e80733 (branch later behind origin/main by BUG-545 aftercare commits; no overlapping files)
- Operator: lc999

## Scope

- Changed files: StrategyPackage SourceType/model/repository/promotion/router/tests; strategy_pkg source_type DDL; combine-backtest detail UI; Paper v2 package deep-link selection; F2 design doc.
- Impacted flows: `/strategy-packages/from-multi-alpha-combine-run` automatic component materialization/reuse; explicit legacy component path; multi-alpha parent lineage; combine detail export button; single Alpha resolver regression.
- Business goal: make Multi-Alpha combine run export a one-step StrategyPackage flow symmetric with QE single Alpha export, without silent fallback or downstream package contract changes.
- Out of scope: production DDL apply, service restart, Paper v2 LocalSim admission, manifest schema changes, F-008 downstream contract change.
- Protected assets reviewed: no QE artifacts, prediction store files, HMM snapshots, paper ledger, or production DB rows modified; only source code, migration, tests, and validation record changed.

## Environment

- Backend port: not started; production 8001 not touched.
- Frontend port: not started; production 3000 not touched.
- TDX port: not touched.
- Conda/env: Windows Python from current shell; frontend dependencies installed in task worktree with `npm ci` for typecheck only.
- Database: no writes/DDL; previous design ground truth was read-only.
- Browser/headless: not run; UI verified by TypeScript/static code path only in this pass.

## Design Compliance Matrix

| Design item | Implementation refs | Evidence | Result |
|---|---|---|---|
| F-001 auto component materialization | `backend/services/strategy_package/multi_alpha_promotion.py` `_prepare_component_package`, `_save_component_plans` | pytest auto path creates two single_alpha children and parent without `component_package_ids` | PASS |
| F-002 seed coverage + sha validation | `_find_reusable_component_package`, `_load_leg_evidence`, `_collect_seed_refs` | pytest sha drift and roster mismatch cases assert reason codes | PASS |
| F-003 seed source resolution | `_resolve_leg_seed_sources`, `_build_auto_component_manifest` | pytest fake provenance covers `qear_run_*` experiment and `qe_*_L<idx>` loop paths | PASS |
| F-004 SourceType lineage + DDL | `models.py`, `multi_alpha_promotion.py`, migration/init/schema SQL | pytest parent source assertions and schema DDL assertions | PASS |
| F-005 explicit old path compatible | router/service still accept non-empty `component_package_ids` | existing explicit path tests pass | PASS |
| F-006 fail-loud reason_code/context | promotion `_fail` paths and router error mapping | negative pytest for missing child, scheme, roster, sha, prediction ref, seed unresolved, materialization failure | PASS |
| F-007 combine detail UI button | `frontend/src/app/quantevolver/multi-alpha/combine-backtest/[taskKey]/page.tsx` | `npx tsc --noEmit --pretty false` PASS; payload omits component ids and shows reason/context | PASS |
| F-008 single Alpha no regression | no single-alpha export code changed; resolver test retained | `test_qe_source_resolver.py` PASS | PASS |
| F-009 idempotency | stable component ids + repository save idempotency + reusable scan | pytest repeated auto export reuses children and keeps one parent | PASS |
| F-010 validation evidence | this run record + commands below | feature workflow validate, backend pytest, py_compile, tsc | PASS |

## Commands

```powershell
python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_unified_export_parity_s1_f2_design_20260629.md --tier F2
python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_qe_source_resolver.py -q -p no:cacheprovider
python -m py_compile backend/services/strategy_package/models.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/repository.py backend/routers/strategy_packages.py backend/db/init_trading_core_v2_schema.py
cd frontend; npm ci
cd frontend; npx tsc --noEmit --pretty false
```

## Evidence

- Feature workflow validate: PASS, `tier=F2 design_items=10 matrix_rows=10 warnings=0`.
- Backend pytest: PASS, `23 passed in 3.64s` after adding auto path, idempotency, source lineage, negative path, DDL, and single Alpha resolver regression coverage.
- Python compile: PASS for changed backend/router/schema files.
- Frontend typecheck: initial `npx tsc` failed because local `node_modules` was missing; after `npm ci`, `npx tsc --noEmit --pretty false` PASS.
- UI behavior evidence: static typed code posts `/strategy-packages/from-multi-alpha-combine-run` with `weighting_scheme=ic_weighted`, derived `topk`, `weight_policy.mode=frozen_backtest_terminal_weights`, confirmation token, and no `component_package_ids`; failure message includes backend `reason_code` and `context`; success links to `/paper-v2/packages?package_id=<id>`.
- DB evidence: migration file adds `multi_alpha_combine_run` to `strategy_pkg.package.source_type` CHECK only; no production DDL executed.

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| Existing roster-mismatch tests failed with `multi_alpha_child_package_not_frozen` | Tests mutated frozen manifest payload without recomputing sha; new strict sha drift check correctly failed earlier | Rewrote roster mismatch fixture to save a valid frozen manifest with non-covering seed; added explicit sha-drift test | targeted pytest PASS |
| `npx tsc` failed | task worktree did not have frontend `node_modules` | ran `npm ci` in `frontend` | `npx tsc --noEmit --pretty false` PASS |

## Result

- Final status: PASS for implemented local scope.
- Remaining risks: no live browser E2E or production DB DDL apply in this session; production schema must apply migration before runtime can save `multi_alpha_combine_run` rows.
- Need production backend restart: user-owned after merge/DDL; not performed.
- Need dev service restart: no services were started.
- production_ddl_gate: pending.
- production_frontend_dependency_gate: noop (no dependency file changed; `npm ci` only installed existing lockfile deps in worktree).
- production_backend_dependency_gate: noop.

## Post-Rebase Rerun

- Rebased branch onto latest `origin/main`; this post-rebase evidence was appended without changing implementation code.
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_unified_export_parity_s1_f2_design_20260629.md --tier F2`: PASS, `tier=F2 design_items=10 matrix_rows=10 warnings=0`.
- `python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_qe_source_resolver.py -q -p no:cacheprovider`: PASS, `23 passed in 3.69s`.
- `python -m py_compile backend/services/strategy_package/models.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/repository.py backend/routers/strategy_packages.py backend/db/init_trading_core_v2_schema.py`: PASS.
- `cd frontend; npx tsc --noEmit --pretty false`: PASS.
- `git diff origin/main...HEAD --check`: PASS via `cmd /c`, `exit=0`.
- Working tree clean after restoring `frontend/tsconfig.tsbuildinfo`; no services started/restarted and no production DB writes/DDL applied.

## Target Run Read-Only / In-Memory Evidence After Prediction Ref Fallback

- Context: production DB was read-only; `InMemoryStrategyPackageRepository` was used for package writes, so no production package rows, component edges, DDL, or DML were created.
- Target run: `macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z`, roster_hash `7738e811293948eb`.
- Legs resolved: `a1_plus3_LSTM_h20` with 33 seeds; `new_FUNDGROWTH_h20` with 5 seeds.
- Scheme: `scheme_result_id=41`, `weighting_scheme=ic_weighted`, `topk=25`.
- Local combined prediction fallback: `F:\Dev\AIstock\rdagent_assets\multi_alpha_combine_backtests\macb_7738e811293948eb_20250601_20260310_20260627T191255096216Z\combined_ic_weighted\combined_prediction.pkl`, sha256 `587752c49e7b3f9dd5043ccc4d83d509580272f8b5249aab314b7531e2e4a556`, size `6686116` bytes.
- First in-memory export: `package_id=pkg_ma_c294586c227a79f13ca4177f`, manifest_sha256 `4c9f15c1c4330d17c0f0f9ada80df8a2a683689761387f0bc9d10b926d6ed875`, parent source `source_type=multi_alpha_combine_run`, `source_id=run_id`, component_count `2`, repository records `3`, materialization modes `auto_created_component_package` x2.
- Repeat in-memory export: same package_id, repository records remained `3`, materialization modes `reused_existing_component_package` x2.
- Legacy config compatibility verified by unit coverage: if `backtest_config_json` has `strategy` but lacks `stock_pool` and `execution_algo`, frozen manifest records `strategy` as both audited stock pool and execution algo; missing all values still fails via `multi_alpha_manifest_incomplete`.

## Post-Fallback Final Rerun

- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/multi_alpha_unified_export_parity_s1_f2_design_20260629.md --tier F2`: PASS, `tier=F2 design_items=11 matrix_rows=11 warnings=0`.
- `python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py backend/tests/strategy_package/test_qe_source_resolver.py -q -p no:cacheprovider`: PASS, `26 passed in 3.38s`.
- `python -m py_compile backend/services/strategy_package/models.py backend/services/strategy_package/multi_alpha_promotion.py backend/services/strategy_package/repository.py backend/routers/strategy_packages.py backend/db/init_trading_core_v2_schema.py`: PASS.
- `cd frontend; npx tsc --noEmit --pretty false`: PASS; restored `frontend/tsconfig.tsbuildinfo` from HEAD after typecheck.
- `git diff --check`: PASS.
- No backend/frontend/TDX service started or restarted. No production DDL/DML applied. `production_ddl_gate=pending`; `production_frontend_dependency_gate=noop`; `production_backend_dependency_gate=noop`.

