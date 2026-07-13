# StrategyPackage Asset Freeze Batch 1 F2 子设计（2026-06-30）

## Background / 背景

已合入 `main` 的总设计 `docs/analysis/strategy_package_asset_freeze_and_candidate_retirement_f2_design_20260630.md` 判定：现有 StrategyPackage 只冻结 manifest 指针，运行时仍会回 QE 节点读取 `params.pkl` 与 `factors/<name>.py`；删 QE 源后，新交易日选股会失败。Batch 1 只交付 `[0]` 存储 adapter 与 `[1]` 新建包固化，为 Batch 2 运行时改读包自有资产、Batch 3 存量回填打基础。

本批坚持两条边界：

1. 运行时资产只包括模型 `params.pkl` 与因子代码 `.py`；`pred.pkl` / `combined_prediction.pkl` 不进入包资产、不进入本批 ledger、不作为运行时输入。
2. `strategy_pkg.package_asset` 已存在，本批启用既有 `model_weight` / `factor_code` 类型；不新增 DB 表列，不执行生产 DDL/DML。

## Scope / 范围

- 新增 `PackageAssetStore` adapter：本地内容寻址后端落 `rdagent_assets/package_assets/`，提供 `put/get/exists`；对象存储后端只保留显式 `NotImplementedError`。
- 扩展 manifest 中 `FactorAsset` / `ModelAsset`：新增可选 `asset_ref`、`sha256`、`size_bytes`、`source_uri`；空字段不参与旧 manifest hash，避免存量包 hash drift。
- 新建包路径固化：`from-qe-experiment`、`from-qe-evolution-loop`、`from-multi-alpha-combine-run` 在落库前复制模型与因子字节，写 `package_asset`，并将 asset refs 写回 manifest 后再 freeze。
- 多 Alpha 父包沿用子包已固化资产引用；显式 child 未固化时 fail-loud；自动 child 在保存前执行同一固化流程。
- 存储字节来源：模型优先通过 `ModelStoreService` / `PredictionArtifactStore` 读取 `model_params`；因子代码通过 `aistock_factor_catalog.code_text` 解析，缺失或多版本冲突显式失败。

## Non-Goals / 边界

- 不修改运行时读取逻辑（Batch 2）。
- 不回填生产存量 15 包（Batch 3）。
- 不改因子库删除保护（Batch 4）。
- 不退役 candidate 或删除 `prediction_ref` 字段（Batch 5）。
- 不写 qe_archive，不改变 PaperPortfolio 单 `package_id` 契约，不启停服务，不执行生产 DDL/DML。

## Architecture / 架构

### 写入链路

1. `QEExperimentSourceResolver` 继续只构建只读 manifest，不在 resolver 内写存储或 DB。
2. `PackageAssetFreezeService` 接收 manifest，解析并复制 `params.pkl` 与 factor code：
   - 对无 `asset_ref` 的单 Alpha manifest，读取源字节后写入 `PackageAssetStore`。
   - 对已有 `asset_ref` 的 parent/复用资产，读取包资产存储并校验 sha。
3. Service 层调用 repository 的 atomic save：`save_manifest_with_assets(manifest, assets)`，同一 DB transaction 写 `strategy_pkg.package`、`package_status_event` 与 `package_asset`，避免 DB 半包。
4. `StrategyPackageValidator` 在保存前校验包含资产 ref/sha 的 frozen manifest。

### 失败语义

- 模型源缺失：`DataUnavailableError`，`reason_code=strategy_package_model_params_missing`，包含 `package_id/model_id/source/run_locators`。
- 因子代码缺失：`DataUnavailableError`，`reason_code=strategy_package_factor_code_missing`，包含 `factor_name/package_id`。
- 因子代码多版本冲突：`StrategyPackageValidationError`，`reason_code=strategy_package_factor_code_ambiguous`。
- sha 不匹配：`PackageAssetInvalidError`，`reason_code=strategy_package_asset_sha_mismatch`。
- 多 Alpha child 未固化：`StrategyPackageValidationError`，`reason_code=multi_alpha_child_package_assets_unfrozen`。

## Contracts / API/DB/UI/MCP 契约

- API 请求响应路径保持不变；`POST /strategy-packages/from-qe-experiment` 与 `POST /strategy-packages/from-qe-evolution-loop` 返回的 record 现在对应已固化 manifest。
- `strategy_pkg.package_asset` 复用既有列：`asset_type=model_weight|factor_code`、`asset_ref`、`asset_sha256`、`asset_size_bytes`、`source_uri`、`metadata`。
- manifest schema 向后兼容：旧 manifest 缺少资产字段仍可校验；新增空字段不改变旧 hash；新建包必须有非空 `asset_ref` + `sha256`。
- UI/MCP 不变；本批不让 UI 依赖数仓或 `package_asset`。

## Design Acceptance Index / 设计验收索引

| id | requirement | refs |
|---|---|---|
| F-001 | 本地内容寻址 `PackageAssetStore` adapter + 对象存储显式未实现 | 总设计 §4.2 |
| F-002 | 新建单 Alpha 包固化模型与因子资产，写 manifest asset_ref/sha 与 `package_asset` | 总设计 §4.3 |
| F-003 | 多 Alpha combine promotion 路径不接受未固化 child，自动 child 保存前固化，parent 写资产 ledger | 总设计 §4.3 |
| F-004 | 失败 fail-loud 且不建 DB 半包；不记录 prediction artifacts 为 `package_asset` | 总设计 §8 |
| F-005 | Batch 1 不新增生产 DDL/DML，不启停服务，不触碰 qe_archive | 总设计 §9 |

## Implementation Plan / 实施方案

1. 新增 `backend/services/strategy_package/package_asset_store.py`，实现本地内容寻址与 URI 解析。
2. 更新 `backend/services/strategy_package/models.py` 与 `manifest.py`，加入资产字段并保持旧 manifest hash 兼容。
3. 新增 `backend/services/strategy_package/package_asset_freeze.py`，封装源解析、sha 校验、manifest 更新、ledger record 生成。
4. 更新 `repository.py`，增加 `save_manifest_with_assets`；PostgreSQL 使用单事务，InMemory repo 保持相同语义。
5. 更新 `service.py` 的 QE 建包路径；更新 `multi_alpha_promotion.py` / `components.py` 的 parent/auto child 保存路径。
6. 补单元与服务测试，验证 adapter、单 Alpha、缺源失败、幂等、多 Alpha child/parent 与 prediction artifact 排除。

## Verification Plan / 验证方案

- L0：`python -m compileall -q backend/services/strategy_package backend/routers`，`git diff --check`。
- F2：`python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md --tier F2`。
- L1：新增/扩展 pytest 覆盖 adapter、freeze service、repository atomic save、单 Alpha create、multi-alpha promotion。
- 回归：`backend/tests/strategy_package` 相关目标测试，确保 selection/paper 既有 lifecycle 不回退。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `backend/services/strategy_package/package_asset_store.py`; `backend/tests/strategy_package/test_package_asset_freeze_batch1.py` | `rtk python -m pytest backend/tests/strategy_package/test_package_asset_freeze_batch1.py -q` -> 7 passed；覆盖 `put/get/exists`、sha mismatch、object backend NotImplemented | verified | - |
| F-002 | `backend/services/strategy_package/package_asset_freeze.py`; `backend/services/strategy_package/service.py`; `backend/services/strategy_package/models.py`; `backend/services/strategy_package/manifest.py` | `test_create_from_qe_experiment_freezes_runtime_assets_and_is_idempotent`：新建包内 1 `MODEL_WEIGHT` + 2 `FACTOR_CODE`，manifest `asset_ref/sha` 有值且重复调用幂等 | verified | - |
| F-003 | `backend/services/strategy_package/multi_alpha_promotion.py`; `backend/services/strategy_package/components.py`; `backend/tests/strategy_package/test_multi_alpha_promotion.py` | `rtk python -m pytest backend/tests/strategy_package/test_multi_alpha_promotion.py -q` -> 24 passed；自动 child 固化并写 ledger，显式未固化 child 拒绝 | verified | - |
| F-004 | `backend/services/strategy_package/repository.py`; `backend/tests/strategy_package/test_package_asset_freeze_batch1.py`; `backend/tests/strategy_package/test_multi_alpha_promotion.py` | 缺 factor/model 源失败后 `repo.records == {}` 与 `repo.package_assets == {}`；`rtk rg ... pred.pkl or combined_prediction` 对 `package_asset` 写入路径无命中 | verified | - |
| F-005 | `docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md`; no migration files | `rtk python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_freeze_batch1_f2_design_20260630.md --tier F2` -> PASS；`production_ddl_gate=noop` / `production_dml_gate=noop` | verified | - |

## Rollout / Rollback

- Rollout：合并 Batch 1 后，仅影响新建 StrategyPackage；存量包仍待 Batch 3 回填，运行时仍待 Batch 2 改读。
- Rollback：代码回滚即可停止新建包固化；已写入的 content-addressed blobs 与 `package_asset` 行为审计资产，不在代码回滚中删除。
- 生产门禁：`production_ddl_gate=noop`，`production_dml_gate=noop`。

## Risks / 风险

- 因子库同名多版本可能阻断建包：设计选择 fail-loud，不以任意排序静默选错代码。
- model params 未进入 prediction store 会阻断建包：这是预期治理信号，不能用回测预测或空模型兜底。
- 内容寻址 blob 可能先于 DB transaction 写入；DB 失败会留下孤儿 blob。blob 不可变且无包引用，不构成 DB 半包，后续清理可按引用计数处理。

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：本批复用既有 `strategy_pkg.package_asset`，无新增 DDL。
- `production_dml_gate=noop`：本批不跑生产回填，不写生产 DB。
- `production_backend_dependency_gate=noop`。
- `production_frontend_dependency_gate=noop`。
