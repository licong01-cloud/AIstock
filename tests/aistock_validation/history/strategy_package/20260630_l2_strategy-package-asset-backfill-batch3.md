# StrategyPackage Asset Backfill Batch 3 验证记录

- Module: strategy_package / selection_center / simulation_runtime
- Level: L2
- Date: 2026-06-30
- Worktree: `F:\Dev\AIstock_worktrees\strategy-package-asset-backfill-batch3-20260630`
- Branch: `feature/strategy-package-asset-backfill-batch3-20260630`
- Base: Batch 2 `feature/strategy-package-runtime-read-assets-batch2-20260630` commit `56d11d71`
- Design: `docs/architecture/strategy_package_asset_backfill_batch3_f2_design_20260630.md`

## Scope

- Batch 3 only: 存量 StrategyPackage runtime assets 回填固化服务、repository CAS apply 入口、默认 dry-run / gated apply CLI、生产回填 runbook。
- 回填资产仍限定为 runtime 必需的 `params.pkl` 与因子 `.py`；不把 `pred.pkl` / `combined_prediction.pkl` 作为包资产、运行时 authority 或数仓数据。
- No DDL；未修改 `qe_archive`；未启动或重启 backend/frontend/TDX；未执行生产 DML。
- 已固化包的删源 self-contained 运行时判定依赖 Batch 2 tests；本批提供回填计划与 apply 机制。生产 15 包当前只有 2 个源仍可解析，13 个需先恢复源或人工裁决后才能全量 apply。

## Design Compliance Matrix

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/package_asset_backfill.py` `build_plan` / `_manifest_drift_items` / `_ledger_covers`; `backend/services/strategy_package/repository.py` `list` / `validate_manifest_integrity` / `list_package_assets` | `test_plan_report_and_prefix_filter_include_resolution_stats`; `test_frozen_manifest_with_missing_ledger_rebuilds_package_asset_rows`; `test_manifest_drift_is_reported_unrecoverable`; canonical-root production dry-run scanned 15 packages | verified | - |
| F-002 | `PackageAssetBackfillService._plan_freeze`; `scripts/strategy_package_asset_backfill.py` `DryRunPackageAssetStore` / `build_report` | `test_dry_run_apply_and_idempotent_backfill_single_alpha`; `test_dry_run_store_does_not_write_new_blob`; dry-run uses real `PackageAssetFreezeService.freeze_manifest_assets()` and leaves DB unchanged | verified | - |
| F-003 | `scripts/strategy_package_asset_backfill.py` `_validate_apply_gate` / `_db_config` / `main` | `test_prod_apply_requires_flag_and_env_token`; `test_dev_apply_requires_scratch_confirm`; `test_dev_db_config_refuses_non_local_or_non_scratch`; apply report blocks when `unrecoverable` exists | verified | - |
| F-004 | `backend/services/strategy_package/repository.py` `backfill_frozen_manifest_assets`; in-memory repository parity method | `test_dry_run_apply_and_idempotent_backfill_single_alpha`; `test_apply_reports_cas_race_without_partial_write`; `test_apply_blocks_remaining_planned_items_after_failure` | verified | - |
| F-005 | `PackageAssetBackfillService._plan_multi_alpha_parent`; `_desired_parent_manifest`; `_patch_multi_alpha_child_shas` | `test_multi_alpha_backfill_recurses_children_and_patches_parent_child_sha`; `test_multi_alpha_missing_child_is_unrecoverable`; `test_multi_alpha_parent_evidence_missing_child_entry_is_unrecoverable` | verified | - |
| F-006 | `PackageAssetBackfillItem.to_report`; `_reason_code`; `_error_context`; CLI non-zero exit on unrecoverable | `test_missing_source_is_reported_unrecoverable_without_writes`; `test_requested_missing_package_is_explicit_unrecoverable`; production canonical-root dry-run reports 13 `strategy_package_model_params_missing` with per-package context | verified | - |
| F-007 | `scripts/strategy_package_asset_backfill.py`; this validation history | Commands/results below; runbook below; production apply not executed; self-contained runtime oracle covered for frozen packages by Batch 2 and remains pending for the 13 unrecoverable production packages until source restoration | verified | - |

## Commands And Results

```bash
python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_backfill_batch3_f2_design_20260630.md --tier F2
# PASS

python -m compileall -q backend/services/strategy_package scripts/strategy_package_asset_backfill.py
# PASS: exit 0

python -m pytest backend/tests/strategy_package/test_package_asset_backfill_batch3.py backend/tests/strategy_package/test_package_asset_backfill_cli_batch3.py --cov=backend.services.strategy_package.package_asset_backfill --cov=scripts.strategy_package_asset_backfill --cov-branch --cov-report=term-missing -q
# PASS: 32 passed
# Coverage: package_asset_backfill.py lines 89%, branches 76%; strategy_package_asset_backfill.py lines 95%, branches 94%

python -m pytest backend/tests/strategy_package -q
# PASS: 298 passed

python -m pytest backend/tests/selection_center -q
# PASS: 86 passed

python -m pytest backend/tests/simulation_runtime/test_strategy_package_selection_service.py backend/tests/simulation_runtime/test_selection_artifact_hmm_preflight.py -q
# PASS: 16 passed

python -m ruff check backend/services/strategy_package/package_asset_backfill.py backend/services/strategy_package/repository.py scripts/strategy_package_asset_backfill.py backend/tests/strategy_package/test_package_asset_backfill_batch3.py backend/tests/strategy_package/test_package_asset_backfill_cli_batch3.py
# PASS: All checks passed

git diff --check
# PASS: exit 0

git diff --name-only -- backend/migrations; git ls-files --others --exclude-standard backend/migrations
# PASS: no migration/schema changes

git diff --unified=0 -- backend/services backend/tests scripts | rg "^\\+.*(qe_archive|pred\\.pkl|combined_prediction\\.pkl)"
# PASS: no added qe_archive or prediction-artifact authority references
```

## Production Read-only Dry-run Evidence

Default worktree asset root dry-run:

```powershell
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --limit 500 --output tmp\strategy_package_asset_backfill_batch3_dry_run.json
# exit non-zero as expected because all 15 were unrecoverable when worktree-local prediction_store root had no source runs
# counts: unrecoverable=15, asset_count=0
```

Canonical production artifact root dry-run:

```powershell
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --limit 500 --output tmp\strategy_package_asset_backfill_batch3_dry_run_root.json
# exit non-zero as expected because unrecoverable packages were present; no DB writes
# counts: planned_freeze=2, unrecoverable=13, asset_count=40
# source_resolution: resolved_count=2, unrecoverable_count=13, resolution_rate=0.13333333333333333
# reason_counts: strategy_package_model_params_missing=13
```

Resolvable packages in dry-run:

| package_id | planned_asset_count | old_manifest_sha256 | new_manifest_sha256 |
|---|---:|---|---|
| `pkg_c4703dfc2fdf4e548cf8dd3027ef228b` | 13 | `e1a702f164969420b400a577dcb9a2870e30b3d642e1f8b95db9a0bc0771fb82` | `0edd89f833850e57a99d2fb2ba6ba99ec1d1907caab56d0ddb72991df433976d` |
| `pkg_09750b4944ca434db03efd399ccf2144` | 27 | `223e94dab400b8d0ea3a1c216e499740656809366333b42ebb73c21e940a0580` | `2923364b1edf3a15f0aeab736210d547125edf9ab9e31e8411f00e048401f5ec` |

Unrecoverable packages: 13 packages, all `reason_code=strategy_package_model_params_missing`. The report includes per-package attempts and source coordinates in `context`; apply is intentionally blocked while any unrecoverable item remains.

## Business Outcomes Verified

- Dry-run is not static success: it reads source model/factor bytes through the same freezer used by apply and reports source failures explicitly.
- Apply is atomic and idempotent at repository level: CAS on old manifest sha, manifest/ledger/event in one transaction, no partial write after first failure.
- Multi-alpha parent backfill recursively plans child packages first and patches `source_evidence.multi_alpha.legs[*].child_manifest_sha256` to the child frozen manifest sha.
- Missing source, manifest drift, missing child, bad parent evidence, CAS race, and asset sha mismatch all return explicit `reason_code` plus context.
- Already frozen packages with complete ledger are skipped; frozen manifest with missing ledger rebuilds `package_asset` rows after verifying stored asset bytes.

## Production Backfill Runbook

1. Ensure production source assets are restored/available for all target packages; current read-only dry-run shows 13 `strategy_package_model_params_missing` blockers.
2. Run dry-run first with canonical roots and save the JSON report:

```powershell
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --target-db prod --limit 500 --output <approved_report_path>.json
```

3. Proceed to production apply only if `counts.unrecoverable` is absent/zero and the report is approved by the operator.
4. Execute authorized production DML:

```powershell
$env:STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY='I_UNDERSTAND_PRODUCTION_DML'
$env:AISTOCK_PREDICTION_STORE_ROOT='F:\Dev\AIstock\rdagent_assets\prediction_store'
python scripts/strategy_package_asset_backfill.py --env-file F:\Dev\AIstock\.env --target-db prod --apply --confirm-production-dml --operator <operator> --limit 500 --output <approved_apply_report_path>.json
```

5. Re-run dry-run. Expected result after successful full apply: all target packages are `skipped_already_frozen`, `unrecoverable=0`.
6. For rollback, use `package_status_event.reason=strategy_package_asset_backfill_freeze` context `rollback_restore` to restore `strategy_pkg.package.manifest_json` and `manifest_sha256` under a separate user-authorized DML window. Do not delete package asset blobs automatically; keep them as audit evidence.

## Gaps / Residual Risks

- Production apply was not run by design. `production_dml_gate=pending`: user authorization and source restoration are required.
- Current production read-only evidence proves only 2/15 packages are immediately recoverable from available sources. The remaining 13 cannot pass full self-contained deletion-source validation until their model `params.pkl` sources are restored or manually adjudicated.
- No UI validation was needed; no frontend files changed.

## Production Gates

- production_ddl_gate: noop.
- production_dml_gate: pending.
- production_backend_dependency_gate: noop.
- production_frontend_dependency_gate: noop.
- Services: backend/frontend/TDX not started or restarted.
- Production DB: read-only dry-run only; no DML, no DDL.
