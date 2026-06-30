# StrategyPackage Asset Backfill Batch 3 F2 子设计（2026-06-30）

## Background / 背景

总设计 `docs/analysis/strategy_package_asset_freeze_and_candidate_retirement_f2_design_20260630.md` 的 `[3]` 要求把存量 StrategyPackage 从“冻结指针”补齐为“冻结副本”。Batch 1 已让新建包在创建时复制 `params.pkl` 与因子 `.py` 到 package-owned asset store；Batch 2 已让已固化包运行时读取 package-owned assets，而未固化包暂时保留 legacy QE source 路径。

本批只做存量包回填固化：扫描现有 `strategy_pkg.package`，对未固化包从各自 QE / factor catalog 源读取模型参数与因子代码，写入 `PackageAssetStore` 与 `strategy_pkg.package_asset`，并用新的 manifest sha 原子替换 `manifest_json` / `manifest_sha256`。回填本身依赖源仍可达；源缺失必须显式报告 `unrecoverable`，不得静默跳过或把脏包标为成功。

## Scope / 范围

- 新增 StrategyPackage asset backfill service，复用 Batch 1 的 `PackageAssetFreezeService.freeze_manifest_assets()`，不重新实现模型/因子读取逻辑。
- 新增 repository 显式 backfill 方法：compare-and-set 更新现有 package 的 frozen manifest、manifest sha 与 package_asset ledger，并写审计事件。
- 支持 dry-run plan：只读扫描、预估每个 package 的回填动作和不可恢复原因，不写 DB。
- 支持 apply gate：生产 apply 需要 CLI flag + env token；scratch/dev apply 需要本地 dev/scratch DB safety check + confirm flag。
- 支持 multi-alpha parent component recursion：先固化 child package，再按 child 新 sha 修正 parent source_evidence 中 `child_manifest_sha256`，最后固化 parent。
- 产出覆盖率/解析率报告：总数、已固化跳过、计划固化、成功固化、unrecoverable、asset 行数、source failure 明细。

## Non-Goals / 边界

- 不执行生产 DML；脚本默认 dry-run，生产 apply 留给用户授权窗口。
- 不新增或修改 DDL/migration；本批启用既有 `strategy_pkg.package_asset` 表，`production_ddl_gate=noop`。
- 不改 `qe_archive`，不把 `pred.pkl` / `combined_prediction.pkl` 作为包、运行时或数仓 authority。
- 不退役 candidate，不删除 `prediction_ref_*` 字段；这些属于 Batch 5。
- 不启动/重启 backend/frontend/TDX 服务。
- 不改 PaperPortfolio 单 `package_id` 契约；multi-alpha parent 仍是普通 StrategyPackage。

## Architecture / 架构

### 回填状态流

1. 扫描 package list，按 `created_at DESC` 顺序处理，支持 `--package-id-prefix` 和 `--package-id` 精确过滤。
2. 对每个 package 构造 plan：
   - manifest 已含完整 `asset_ref` + `sha256` 且 ledger 完整 -> `skipped_already_frozen`。
   - manifest 已固化但 ledger 缺失 -> 使用 `freeze_manifest_assets()` 读取/校验 existing asset_ref，重建 ledger。
   - manifest 未固化 -> 从源读取 bytes，生成 frozen manifest + asset rows。
   - 源缺失、factor code ambiguity、model missing、manifest drift、component child 不可达 -> `unrecoverable`，记录 `reason_code` 与 context。
3. apply 模式对 `planned_freeze` 执行 repository CAS 更新：
   - expected old manifest sha 必须等于当前 DB sha；不等则 fail-loud `strategy_package_asset_backfill_cas_mismatch`。
   - 更新 `manifest_json` 与 `manifest_sha256`。
   - upsert package_asset rows；同 package/type/ref 不允许 sha 改写冲突。
   - 插入 `package_status_event`，reason=`strategy_package_asset_backfill_freeze`，context 包含 old/new sha、asset_count、operator、rollback_restore。

### Multi-alpha parent recursion

Multi-alpha parent 的 `source_evidence.multi_alpha.legs[*].child_manifest_sha256` 是运行时校验契约。若 child 在本批被固化后 manifest sha 改变，parent 必须在本次回填中同步替换对应 child sha；否则 Batch 2 运行时会触发 `multi_alpha_child_manifest_mismatch`。因此 parent 回填前按 component edge 递归处理所有 child：

- child 已固化且 ledger 完整 -> 复用当前 child sha。
- child 未固化且源可达 -> 先固化 child，再用新 child sha patch parent evidence。
- 任一 child unrecoverable -> parent 也标 `unrecoverable`，reason_code=`strategy_package_asset_backfill_child_unrecoverable`。

### Dry-run 与 apply

Dry-run 不写 DB，但允许读取源 bytes 来证明可固化；为了避免静态成功，dry-run 对每个 planned item 调用同一 freeze path 并汇总 future asset rows。Apply 复用 dry-run plan 的 expected old sha，在单 package transaction 内原子写 manifest + ledger + event。

## Contracts / API/DB/UI/MCP 契约

- Service contract：`PackageAssetBackfillService.build_plan()` 返回 package-level items；`apply_plan()` 返回 updated/skipped/unrecoverable 汇总。
- Repository contract：`backfill_frozen_manifest_assets(package_id, frozen_manifest, assets, operator, expected_old_manifest_sha256)` 是唯一允许替换现有 package manifest 的显式入口；普通 `save_manifest*` 仍禁止 silent replace。
- CLI contract：`scripts/strategy_package_asset_backfill.py` 默认 dry-run；`--apply --target-db prod` 必须带 `--confirm-production-dml` 且 env `STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY=I_UNDERSTAND_PRODUCTION_DML`；`--apply --target-db dev` 必须带 `--confirm-scratch-dml` 且 DB host/dbname 看起来是 local dev/scratch/test。
- Error contract：所有失败 item 输出 `status=unrecoverable`、`reason_code`、`error_type`、`context`；CLI apply 遇到 blocked/unrecoverable 返回非零，不用默认值掩盖。
- DB contract：无 DDL；写入仅限授权 apply 模式的 `strategy_pkg.package`、`strategy_pkg.package_asset`、`strategy_pkg.package_status_event`。

## Design Acceptance Index / 设计验收索引

| id | requirement | refs |
|---|---|---|
| F-001 | 扫描存量 package，识别已固化/未固化/ledger 缺失/manifest drift，不静默漏包 | 总设计 §4.5 |
| F-002 | dry-run 走真实 freeze path 生成可执行 plan 与 coverage/source failure 报告，不写 DB | 总设计 §4.5 |
| F-003 | apply 受生产 DML gate 保护，prod/dev 授权 token 与 DB safety check 缺失时拒绝 | 总设计 Production Gates |
| F-004 | 回填写入幂等且原子：CAS 更新 manifest_json/sha、upsert package_asset、写 status event | 总设计 §4.5 |
| F-005 | multi-alpha parent 递归固化 component child，并同步 parent 中 child_manifest_sha256 | 总设计 §4.5 |
| F-006 | 源缺失/解析失败/child unrecoverable 显式 `unrecoverable` + reason/context，禁止 silent error | 总设计 Scope/No-silent |
| F-007 | 验证与 runbook 覆盖 scratch dry-run/apply/idempotency、删源 self-contained 核验口径、生产回填待授权 | 总设计 Verification Plan |

## Implementation Plan / 实施方案

1. 新增本 F2 子设计并通过 `scripts/aistock_feature_workflow.py validate --tier F2`。
2. 新增 `backend/services/strategy_package/package_asset_backfill.py`：定义 plan item/result dataclass、service 扫描、dry-run freeze、multi-alpha recursion、summary builder。
3. 扩展 `StrategyPackageRepository` 与 `InMemoryStrategyPackageRepository`：新增 `backfill_frozen_manifest_assets()`，沿用 `_validate_asset_records_for_manifest()`、`_upsert_package_asset()`、CAS、status event。
4. 新增 `scripts/strategy_package_asset_backfill.py`：复用 manifest hash repair 脚本的 env/config/gate 模式，默认 readonly dry-run。
5. 新增 tests：service dry-run/apply/idempotency/unrecoverable/multi-alpha recursion/CAS；CLI prod/dev gate。
6. 记录 validation history 和 PR 自审矩阵；不执行生产 DML。

## Verification Plan / 验证方案

- F2: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_backfill_batch3_f2_design_20260630.md --tier F2`
- L0: `python -m compileall -q backend/services/strategy_package scripts/strategy_package_asset_backfill.py`
- L1: `python -m pytest backend/tests/strategy_package/test_package_asset_backfill_batch3.py -q`
- L1 CLI gate: `python -m pytest backend/tests/strategy_package/test_package_asset_backfill_cli_batch3.py -q`
- Regression: `python -m pytest backend/tests/strategy_package -q`
- Asset safety grep: no `qe_archive` changes; no added runtime authority on `pred.pkl` / `combined_prediction.pkl`; no migrations.
- Scratch evidence: tests use in-memory repository and local package asset store to prove dry-run no write, apply writes, rerun idempotent, source missing unrecoverable；真实 production apply 不执行。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/package_asset_backfill.py` `build_plan` / `_manifest_drift_items` / `_ledger_covers`; `backend/services/strategy_package/repository.py` `list` / `validate_manifest_integrity` / `list_package_assets` | `test_plan_report_and_prefix_filter_include_resolution_stats`; `test_frozen_manifest_with_missing_ledger_rebuilds_package_asset_rows`; `test_manifest_drift_is_reported_unrecoverable`; production dry-run scanned 15 packages | verified | - |
| F-002 | `PackageAssetBackfillService._plan_freeze`; `scripts/strategy_package_asset_backfill.py` `DryRunPackageAssetStore` / `build_report` | `test_dry_run_apply_and_idempotent_backfill_single_alpha`; `test_dry_run_store_does_not_write_new_blob`; production dry-run used real freeze path and wrote no DB | verified | - |
| F-003 | `scripts/strategy_package_asset_backfill.py` `_validate_apply_gate` / `_db_config` / `main` | `test_prod_apply_requires_flag_and_env_token`; `test_dev_apply_requires_scratch_confirm`; `test_dev_db_config_refuses_non_local_or_non_scratch`; apply is blocked when unrecoverable packages exist | verified | - |
| F-004 | `backend/services/strategy_package/repository.py` `backfill_frozen_manifest_assets`; in-memory repository parity method | `test_dry_run_apply_and_idempotent_backfill_single_alpha`; `test_apply_reports_cas_race_without_partial_write`; `test_apply_blocks_remaining_planned_items_after_failure` | verified | - |
| F-005 | `PackageAssetBackfillService._plan_multi_alpha_parent`; `_desired_parent_manifest`; `_patch_multi_alpha_child_shas` | `test_multi_alpha_backfill_recurses_children_and_patches_parent_child_sha`; `test_multi_alpha_missing_child_is_unrecoverable`; `test_multi_alpha_parent_evidence_missing_child_entry_is_unrecoverable` | verified | - |
| F-006 | `PackageAssetBackfillItem.to_report`; `_reason_code`; `_error_context`; CLI non-zero exit on unrecoverable | `test_missing_source_is_reported_unrecoverable_without_writes`; `test_requested_missing_package_is_explicit_unrecoverable`; production canonical-root dry-run reported 13 `strategy_package_model_params_missing` without writes | verified | - |
| F-007 | `scripts/strategy_package_asset_backfill.py`; `tests/aistock_validation/history/strategy_package/20260630_l2_strategy-package-asset-backfill-batch3.md` | validation history records commands/results, dry-run coverage/source-resolution report, production DML runbook, and self-contained verification handoff after source restoration | verified | - |

## Rollout / Rollback / 发布回滚

Rollout：合并 Batch 3 后仍不自动写生产。生产回填由用户在授权窗口执行脚本 dry-run，确认 report 中所有 item 非 `unrecoverable` 后再以 prod token 执行 apply。Apply 后重新运行 dry-run，应全部变为 `skipped_already_frozen`。

Rollback：每个 audit event context 记录 old/new manifest sha；如需回滚，必须由单独授权 DML 脚本把 `strategy_pkg.package.manifest_json`/`manifest_sha256` 恢复到备份来源，并保留 `package_asset` rows 作为审计资产，不自动删除 blob 或 ledger。

## Risks / 风险

| risk | impact | mitigation |
|---|---|---|
| 回填前 QE/model/factor 源已清理 | 部分包无法自包含 | item 标 `unrecoverable`，输出 source failure context；不把失败包标成功 |
| child sha 改变后 parent 未同步 | multi-alpha runtime 拒绝 child manifest mismatch | parent recursion 先 child 后 parent，并 patch evidence sha |
| apply 时 package 被并发修改 | 覆盖新 manifest 或丢事件 | repository CAS expected old sha，不匹配即 fail-loud |
| 已固化 manifest 但 ledger 丢失 | runtime 或保护查询缺 ledger | freeze existing asset_ref 校验 blob sha 后重建 ledger |
| 将 prediction artifact 当 runtime asset | 包含无价值/错误资产 | 复用 `_validate_asset_records_for_manifest()` 拒绝 `pred.pkl` / `combined_prediction.pkl` |

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：本批无 migration/schema/comment/index/constraint 变更。
- `production_dml_gate=pending`：回填脚本具备授权 apply 能力，但本 PR 不执行生产 DML；生产执行需用户后续显式授权 token。
- `production_backend_dependency_gate=noop`：无 Python dependency 变更。
- `production_frontend_dependency_gate=noop`：无 frontend 变更。
- Services：不启动/重启 backend/frontend/TDX。
