# StrategyPackage Asset Backfill Dual-Source Recovery Batch 3 F2 子设计（2026-06-30）

## Background / 背景

PR #1771 的 Batch 3 dry-run 已证明现有回填路径只查中心 `PredictionArtifactStore` / `ModelStoreService`，生产 15 个策略包中仅 2 个可固化、13 个报 `strategy_package_model_params_missing`。新的取证结论是：中心 prediction/model store 自 2026-06-21 后才覆盖新 run，4-5 月老 QE 实验与 candidate 源的 `params.pkl` 从未进入中心库；但运行时 `live_inference.py` 已通过 `QEWorkspaceClient` 从 QE 节点 workspace 下载 `mlruns` params 与 `factors/*.py`。

因此本子设计只扩展 Batch 3 回填的源解析：保留中心库优先级，中心 miss 时通过 QE 节点 API 恢复，再尝试本机 WSL/QE workspace 只读路径。目标是把“中心库缺失”与“源真正不可恢复”区分开，避免把可从节点恢复的老包错误升级为人工裁决。

第 0 步取证必须覆盖 15 个生产包的三源矩阵：中心 store、QE 节点 215（`rdagent-node1` / `192.168.50.215`，只读 SSH/API 核查）、本机 WSL/QE workspace。candidate 源需先解析到底层 `qe_task_id + qe_loop_id + experiment_id`。

## Scope / 范围

- 扩展 `StrategyPackageAssetSource` 的 `model_params_bytes()`：中心库命中保持原路径；中心库 miss 后用 `QEWorkspaceClient.download_mlruns_params()` 从节点取 `params.pkl`；节点 miss 后查本机/WSL workspace。
- 扩展 `factor_code_bytes()`：factor catalog 命中保持原路径；catalog miss 或 ambiguity 记录为中心尝试失败后，按同一 QE 节点 / WSL 顺序取 `factors/<factor>.py`。
- 新增 StrategyPackage 源坐标解析：`qe_experiment`、`qe_evolution_loop`、`candidate_strategy_package` 均解析为 `experiment_id`、`qe_task_id`、`qe_loop_id`、`node_id` 候选。
- 回填 dry-run 输出 `attempted_sources`，每个不可恢复项必须包含中心库、节点、WSL 三类尝试与 miss 原因。
- 保持 PR #1771 的 dry-run/apply DML gate、CAS、ledger 写入、audit event 与 multi-alpha child recursion。

## Non-Goals / 边界

- 不执行生产 DML；生产 apply 仍需 `--apply --confirm-production-dml` 与 `STRATEGY_PACKAGE_ASSET_BACKFILL_APPLY=I_UNDERSTAND_PRODUCTION_DML`。
- 不新增或修改 DDL / migration；本批继续使用既有 `strategy_pkg.package_asset`。
- 不修改 `qe_archive`，不把 `pred.pkl` / `combined_prediction.pkl` 当作包、运行时或数仓权威资产。
- 不启动、不重启 backend/frontend/TDX/QE 服务；节点核查只读。
- 不修改 PaperPortfolio 单 `package_id` 契约，不修改候选退役、factor-library 保护或前端。

## Architecture / 架构

### 双源/三源优先级

1. `central_store`：沿用 `ModelStoreService.get_pointer()` / `pull_params_path()` 与 factor catalog `code_text`。
2. `qe_node`：复用 `QEWorkspaceClient.for_node(node_id)`、`download_mlruns_params(task, loop)`、`download_workspace_file_bytes(task, loop, path)`；不使用 SSH/远端文件遍历作为生产逻辑。
3. `wsl_workspace`：只读本机可达 workspace 根，来源包括环境变量 `AISTOCK_QE_WORKSPACE_ROOTS`、`QE_WORKSPACE_ROOTS`、`QE_WORKSPACE_WIN`、`QE_EXPERIMENTS_ROOT`、`QLIB_RDAGENT_ROOT_WIN/qe_workspace`，以及 `infra.compute_nodes.workspace_base` / `qlib_rdagent_root` 的本机可达路径与 `/mnt/<drive>/...` 到 Windows drive 的显式转换；生产逻辑不硬编码个人目录。

任一源命中即固化为 package-owned asset blob；后续运行时仍只读包自有资产。所有 miss 都进入 `attempted_sources`，最终失败时返回明确 reason_code。

### 源坐标解析

- `qe_experiment`：优先 `source_evidence.experiment_id/qe_task_id/qe_loop_id`，否则用 `source_id/run_id/loop_id` 查 `qe_experiments`。
- `qe_evolution_loop`：优先 `source_id + loop_id`，用 `qe_evolution_loops` 补 `experiment_id/node_id`，并用 `qe_experiments` 补 factor/model metadata。
- `candidate_strategy_package`：查 `strategy_pkg.candidate_strategy_package`，解析 `source_task_id/source_loop_id/source_experiment_id`，再回到底层 QE loop。
- `node_id` 候选：`qe_evolution_loops.node_id`、manifest/custom/result metrics 中的 `execution_node_id/node_id`、默认 QE node；dry-run 可尝试多个 compute node，但每个尝试都必须留痕。

### fail-loud 语义

- `params.pkl` 全源 miss：`reason_code=strategy_package_model_params_missing`，包含 `attempted_sources`。
- factor 全源 miss：`reason_code=strategy_package_factor_code_missing`，包含 factor 名称与 `attempted_sources`。
- 节点 tar 包为空、缺 `artifacts/params.pkl`、含 link/path traversal：显式错误，不落半包。
- 坐标解析失败：作为 source attempt 记录，不伪造 task/loop/node 默认成功。

## Contracts / 契约

- Service contract：`StrategyPackageAssetSource` 仍返回 `PackageAssetBytes(data, source_uri)`，调用方 `PackageAssetFreezeService` / `PackageAssetBackfillService` 无需感知源差异。
- Source URI contract：中心库使用既有 URI；节点源使用 `qe-workspace://node/<node_id>/tasks/<task>/loops/<loop>/...`；本机 workspace 使用 `file://...`。
- Error contract：所有不可恢复错误带 `reason_code`、`package_id`、`source`、`attempted_sources`；`PackageAssetBackfillItem.context.context.attempted_sources` 可直接进入 PR/运行报告。
- Production contract：脚本默认 dry-run；apply gate 沿用 PR #1771；生产写入留用户授权窗口。

## Design Acceptance Index / 设计验收索引

| id | requirement | refs |
|---|---|---|
| F-001 | 第 0 步完成 15 包三源核查表，candidate 源解析到底层 QE 坐标 | Background |
| F-002 | 模型参数解析优先级为中心库 -> QE 节点 -> WSL/local workspace | Architecture |
| F-003 | 因子代码解析优先级为 factor catalog -> QE 节点 -> WSL/local workspace | Architecture |
| F-004 | QE 节点下载复用 `QEWorkspaceClient`，不在生产逻辑里直接 SSH/遍历远端文件系统 | Architecture |
| F-005 | 全源 miss 显式 `unrecoverable`，包含三源尝试与 miss 原因，禁止 silent error | Contracts |
| F-006 | dry-run 重跑给出真实 recoverable/unrecoverable 数与升级清单；生产 apply 留 DML gate | Verification Plan |
| F-007 | 现有 Batch 3 CAS、ledger、audit event、multi-alpha recursion、单 Alpha 回归不破坏 | Scope |

## Implementation Plan / 实施方案

1. 提交本 F2 子设计并运行 `python scripts/aistock_feature_workflow.py validate --tier F2`。
2. 执行第 0 步只读核查：生产 DB 读 15 包坐标；中心 store 读指针；SSH/API 只读核查 215；本机 WSL/workspace 只读核查。
3. 在 `package_asset_freeze.py` 中新增源坐标解析、节点下载、WSL/local fallback 与 `attempted_sources`。
4. 保持 `scripts/strategy_package_asset_backfill.py` 默认 dry-run，并让报告透出新增 context。
5. 增补测试：中心优先、节点 fallback、factor fallback、全源 miss context、安全 tar 校验、candidate 坐标解析。
6. 重跑 dry-run 与 strategy_package/selection/paper 相关回归，记录 validation history 与 PR 自审矩阵。

## Verification Plan / 验证方案

- F2: `python scripts/aistock_feature_workflow.py validate --design docs/architecture/strategy_package_asset_backfill_dualsource_batch3_f2_design_20260630.md --tier F2`
- L0: `python -m compileall -q backend/services/strategy_package scripts/strategy_package_asset_backfill.py`
- L1: `python -m pytest backend/tests/strategy_package/test_package_asset_backfill_dualsource_batch3.py -q`
- Regression: `python -m pytest backend/tests/strategy_package -q`；必要时补 `backend/tests/selection_center` 相关路径。
- L2 dry-run: `python scripts/strategy_package_asset_backfill.py --env-file F:/Dev/AIstock/.env --limit 500 --output <tmp-json>`，确认写 DB 为 0，报告新 recoverable/unrecoverable 数。
- Asset safety grep：无 `qe_archive` 写入变更；无 `pred.pkl` / `combined_prediction.pkl` 作为 runtime asset；无 migration。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | `tests/aistock_validation/history/strategy_package/20260630_l2_strategy-package-asset-backfill-dualsource.md`；`tmp/strategy_package_dualsource_step0_probe_v2_no_hardcoded_roots.json` | 15 包三源核查完成，candidate 源解析到底层 QE 坐标 | verified | - |
| F-002 | `backend/services/strategy_package/package_asset_freeze.py` `model_params_bytes` / `_model_params_from_qe_sources` | `test_central_model_hit_does_not_call_qe_node`、`test_central_miss_qe_node_model_params_archive_hit`、`test_node_miss_local_workspace_model_params_hit`；dry-run `planned_freeze=13` | verified | - |
| F-003 | `backend/services/strategy_package/package_asset_freeze.py` `factor_code_bytes` / `_factor_code_from_qe_sources` | `test_factor_catalog_miss_qe_node_factor_file_hit`、`test_node_miss_local_workspace_factor_code_hit` | verified | - |
| F-004 | `QEWorkspaceClient.for_node` / `download_mlruns_params` / `download_workspace_file_bytes` | grep 证明生产代码无 SSH；mock client 断言节点 API 调用；SSH 仅用于第 0 步只读证据 | verified | - |
| F-005 | `attempted_sources` error context；`PackageAssetBackfillService._plan_freeze` 报告透出 | `test_all_sources_miss_reports_central_node_and_wsl_attempts`；dry-run 两个 unrecoverable 含 central/qe_node/wsl miss | verified | - |
| F-006 | `scripts/strategy_package_asset_backfill.py` dry-run report；validation history runbook | `tmp/strategy_package_asset_backfill_dualsource_dry_run_v4_no_hardcoded_roots.json`：13 planned / 2 unrecoverable / 605 assets；生产 apply 保持 DML gate，未执行 | verified | - |
| F-007 | PR #1771 既有 backfill service/repository/CLI contracts | `python -m pytest backend/tests/strategy_package -q` => 317 passed；selection/paper targeted => 56 passed, 1 skipped | verified | - |

## Rollout / Rollback / 发布回滚

Rollout：合并 stacked PR 后仍不写生产。用户授权窗口先运行 dry-run，确认所有可从节点/WSL 恢复的包均进入 `planned_freeze`，不可恢复包进入人工决策清单；再由用户显式授权生产 apply。

Rollback：本 PR 无 DDL。生产 apply 若执行，仍沿用 PR #1771 audit event 中的 old/new manifest sha 回滚策略；保留 package_asset blob/ledger 作为审计，不自动删除。

## Risks / 风险

| risk | impact | mitigation |
|---|---|---|
| 旧 workspace 已清理 | 包仍不可自包含 | 三源 miss 进入升级清单，不伪造资产 |
| 节点 API 与 SSH 实地结果不一致 | dry-run recoverable 低于手工 find | 生产逻辑只信 QEWorkspaceClient；SSH 仅作为第 0 步证据 |
| factor catalog 代码歧义 | 固化错误因子版本 | 中心歧义记录为 miss，优先用源 workspace 精确 `factors/<name>.py` |
| tar 包路径穿越或 link | 写入非预期文件 | 安全解析，只接受普通文件且必须含 `artifacts/params.pkl` |
| 尝试多个节点耗时 | dry-run 变慢 | 节点候选去重、按坐标 node 优先，失败留痕 |

## Production Gates / 生产门禁

- `production_ddl_gate=noop`：无 schema/migration/comment/index/constraint 变更。
- `production_dml_gate=pending_user_authorized_window`：回填 apply 具备能力但本 PR 不执行生产写入。
- `production_backend_dependency_gate=noop`：无 Python dependency 变更。
- `production_frontend_dependency_gate=noop`：无 frontend 变更。
- Services：不启/重启 backend/frontend/TDX/QE 服务。
