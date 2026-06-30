# StrategyPackage Runtime Read Package Assets Batch 2 F2 子设计（2026-06-30）

## Background / 背景

总设计 `docs/analysis/strategy_package_asset_freeze_and_candidate_retirement_f2_design_20260630.md` 的 `[2]` 要求：新建包在 Batch 1 已写入包自有 `package_asset` 后，运行时生成 selection signal 必须优先读取包内 `params.pkl` 与因子 `.py`，不得再回 QE 节点、不得再查询 `qe_experiments`。未固化存量包在 Batch 3 回填前保留原有 QE 节点路径，避免一次性切断历史包运行。

本批建立运行时读路径，不新增 DB schema，不修改 `qe_archive`，不触碰回测预测 `pred.pkl` / `combined_prediction.pkl`，不改变 PaperPortfolio 单 `package_id` 契约。

## Scope / 范围

- `QEExperimentRuntimeAssetResolver.load_source_for_strategy_package` 增加 package-aware 路径：当 manifest 已具备完整 `asset_ref` + `sha256` 时，从 `PackageAssetStore` 读取模型与因子字节并物化临时 runtime source。
- `prepare_workspace` 沿用现有 workspace 契约：生成 `model/params.pkl`、`strategy_package_factor_entry.py`、`factor_order.json` 与 runtime `manifest.json`，但 diagnostics 明确标记 `source_workspace_type=strategy_package_asset_store`。
- `StrategyPackageSelectionArtifactService` 单 Alpha 运行时传入 frozen manifest/package_id；多 Alpha 子包在 seed 推理时传入 child manifest/package_id。
- Selection Center 与 simulation runtime 的 preflight 传入 manifest/package_id；已固化包 preflight 不要求 QE node。
- 未固化包继续走原 `qe_experiments` + QE node materialization 逻辑，作为 Batch 3 回填前兼容路径。

## Non-Goals / 边界

- 不回填 15 个生产存量包；回填属于 Batch 3。
- 不改包创建固化路径；Batch 1 已负责新建包固化。
- 不新增或修改 DDL/DML，不执行生产 DB 写入。
- 不改 `qe_archive`、不读取/固化回测预测产物。
- 不退役 candidate、不删除 `prediction_ref` 字段；属于 Batch 5。
- 不启停 backend/frontend/TDX 服务。

## Architecture / 架构

### 运行时 source 分流

1. `manifest_has_frozen_runtime_assets(manifest)` 为 true 时，resolver 不进入 `qe_experiments` 查询、不调用 QE node API。
2. resolver 逐项读取 `manifest.factor_set[*].asset_ref` 与 `manifest.model_asset.asset_ref`。
3. 每个 blob 读取后用 manifest 内 `sha256` 复算校验；缺 blob 或 sha mismatch 直接抛 `PackageAssetInvalidError`，携带 `reason_code`、`package_id`、`asset_kind`、`logical_name`。
4. 字节物化到 `rdagent_assets/strategy_package_runtime/_package_asset_sources/<package_id>/<manifest_sha>/`：
   - 因子：`factors/<factor_name>.py`
   - 模型：`mlruns/package_asset/artifacts/params.pkl`
   - 最小 `conf.yaml`：只满足现有 factor-order builder；完整因子列表来自 frozen `manifest.factor_set`。
5. `QEExperimentRuntimeSource` 增加 provenance 字段：`source_workspace_type`、`package_id`、`manifest_sha256`、`model_params_origin=package_asset`。

### 兼容路径

未固化 manifest 不满足 `manifest_has_frozen_runtime_assets`，仍按现有 `source_type` 访问 `qe_experiments` 并从 QE node 下载 assets。该兼容路径只服务 Batch 3 回填前的存量包，不用于已固化包。

### 多 Alpha

多 Alpha parent 不直接运行模型；每条 leg 使用 child StrategyPackage。Batch 2 在 `_run_seed_live_inference` 中把 child manifest 交给同一 resolver，因此 child 已固化时使用包资产，不再要求 child `run_id == seed_run_id`；显式 `model_params_path` 仍保持兼容。

## Contracts / API/DB/UI/MCP 契约

- API 响应不变；本批无 router contract 变化。
- DB schema 不变；只读取 manifest 内 asset refs 与本地 package asset blob。
- runtime workspace contract 不变：`backend/inference_engine.py` 继续读取 `manifest.json.primary_assets.model_weight_relpath`、`factor_entry_relpath`、`factor_order.json`。
- error contract：缺 blob 使用 `strategy_package_asset_blob_missing`；sha 不符使用 `strategy_package_asset_sha_mismatch`；不完整 asset 使用 `strategy_package_runtime_assets_incomplete`。
- preflight contract：已固化包仍返回 5 个 check，但 `qe_node` check 标记 PASS 且 message 为 package-owned assets 不需要 QE node。

## Design Acceptance Index / 设计验收索引

| id | requirement | refs |
|---|---|---|
| F-001 | 已固化单 Alpha 包运行时从 package-owned asset store 读取模型与因子，不访问 `qe_experiments` / QE node | 总设计 §4.4 |
| F-002 | 运行时读取包资产必须逐项校验 sha，缺 blob / sha mismatch fail-loud | 总设计 §4.4、§8 |
| F-003 | 未固化包保留旧 QE 节点路径，支持 Batch 3 回填前过渡 | 总设计 §4.4 |
| F-004 | 多 Alpha child package 走同一包资产路径，不另起 alpha 分叉；已固化 child 不再依赖 seed run_id 绑定 | 总设计 §4.4 |
| F-005 | 本批无 DDL/DML、无 qe_archive、无 prediction artifact 读写、无服务启停 | 总设计 §3、§9 |

## Implementation Plan / 实施方案

1. 扩展 `QEExperimentRuntimeSource` / `PreparedInferenceWorkspace` provenance 字段，支持 `model_params_origin=package_asset`。
2. 在 `QEExperimentRuntimeAssetResolver` 注入 `PackageAssetStore`，增加 `_source_from_package_assets` 和 `_read_package_asset_bytes`。
3. 物化 package-owned runtime source 到 `_package_asset_sources`，复用现有 `prepare_workspace` / factor entry / model copy 逻辑。
4. 单 Alpha selection artifact 调用 source loader 时传入 `manifest` + `package_id`。
5. Selection Center 与 simulation runtime preflight 传入 `manifest` + `package_id`。
6. MultiAlphaLivePredictionProvider seed 推理传入 child manifest/package_id，并让已固化 child 通过 runtime binding 检查。
7. 增加 Batch 2 单元与集成测试，覆盖无 DB 访问、sha mismatch、缺 blob、legacy fallback、preflight、selection service wiring、多 Alpha child binding。

## Verification Plan / 验证方案

- F2：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_runtime_read_assets_batch2_f2_design_20260630.md --tier F2`
- L0：`python -m compileall -q backend/services/strategy_package backend/services/selection_center backend/services/simulation_runtime`
- L1：`python -m pytest backend/tests/strategy_package/test_runtime_package_assets_batch2.py -q`
- L1：`python -m pytest backend/tests/strategy_package/test_multi_alpha_live_selection.py -q`
- L1：`python -m pytest backend/tests/strategy_package/test_live_inference_preflight.py -q`
- L1：`python -m pytest backend/tests/selection_center/test_runtime_selection.py::test_selection_artifact_service_resolves_qe_evolution_loop_source backend/tests/selection_center/test_live_inference_preflight_wiring.py -q`
- 回归：`python -m pytest backend/tests/strategy_package -q` 与 selection/simulation 相关测试。
- 防误用：grep 确认本批无 `qe_archive`、无 migration、无新增 prediction artifact 写入。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/live_inference.py`; `backend/services/strategy_package/selection_artifact.py`; `backend/tests/strategy_package/test_runtime_package_assets_batch2.py` | `test_frozen_package_runtime_materializes_assets_without_qe_db` 注入 forbidden conn，已固化 manifest 仍完成 source/workspace 物化 | verified | - |
| F-002 | `backend/services/strategy_package/live_inference.py`; `backend/tests/strategy_package/test_runtime_package_assets_batch2.py` | `test_frozen_package_runtime_rejects_asset_sha_mismatch`; `test_frozen_package_runtime_rejects_missing_asset_blob` 校验 reason_code/context | verified | - |
| F-003 | `backend/services/strategy_package/live_inference.py`; `backend/tests/strategy_package/test_runtime_package_assets_batch2.py` | `test_unfrozen_package_keeps_legacy_qe_source_resolution` 证明未固化包仍调用旧 `load_source` | verified | - |
| F-004 | `backend/services/strategy_package/multi_alpha_live.py`; `backend/tests/strategy_package/test_multi_alpha_live_selection.py` | `test_multi_alpha_frozen_child_runtime_does_not_require_seed_run_id_binding` 证明已固化 child 不依赖 child `run_id == seed_run_id` | verified | - |
| F-005 | 本批变更文件；git grep/validation history | 无 `backend/migrations/`、无 `qe_archive` 修改、无 production DDL/DML、无服务启停；`production_ddl_gate=noop` | verified | - |

## Rollout / Rollback

- Rollout：Batch 2 合并后，新建已固化包优先读包自有资产；未回填存量包继续旧路径。生产运行仍需用户重启/部署后生效。
- Rollback：回滚代码即可恢复旧 QE source resolution；已存在 `package_asset` 与 blob 不删除，作为 Batch 1 审计资产保留。
- Batch 3 依赖：存量包完成回填后，Batch 2 路径可覆盖全部包并支持删源验证。

## Risks / 风险

| risk | impact | mitigation |
|---|---|---|
| 包 asset blob 损坏或丢失 | selection artifact 生成失败 | 运行时逐项 sha 校验与 `strategy_package_asset_blob_missing` fail-loud |
| 未回填存量包仍依赖 QE source | 删源后旧包仍可能失败 | 保留 legacy fallback 仅过渡；Batch 3 回填与删源验证解决 |
| 多 Alpha seed 与 child run_id 不一致 | 旧 binding 检查误拦已固化 child | 已固化 child 以 manifest asset refs 为 runtime authority，不再要求 run_id 相等 |
| package asset path 写出 cache root | 本地文件污染 | `_reset_cache_dir` 与写入路径 parent check 拒绝越界 |

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：本批无 migration、无 schema 变化。
- `production_dml_gate=noop`：本批不执行生产回填，不写生产 DB。
- `production_backend_dependency_gate=noop`：无 Python/Conda 依赖清单变化。
- `production_frontend_dependency_gate=noop`：无 frontend 变化。
- runtime activation：未启停任何服务；合并后是否重启由用户窗口执行。
