# AIstock 选股推理资产保障改造执行计划（可落地）

## 1. 目标（必须明确且可验收）

### 1.1 总目标

通过对 RD-Agent 资产包固化与 AIstock 资产加载逻辑进行“双端对齐改造”，实现：

- **目标G1（强确定性）**：任意一个被标记 `is_solidified=1` 的 loop，在 AIstock 侧均可**稳定、确定、可重复**地定位到
  - `factor_entry.py`（或等价入口 py）
  - `model.pkl`（或等价权重 pkl）

- **目标G2（强正确性）**：AIstock 选股推理能正确运行并产出非空 `scores`，并成功落库到 `trading.rdagent_signal`。

- **目标G3（强可诊断性）**：当资产缺失或不匹配时，错误信息能够明确指出：
  - 缺失哪个文件
  - manifest 指向的路径是什么
  - bundle 目录结构摘要

### 1.2 非目标（本期不做）

- 不在本期把所有历史 bundle 自动迁移为新结构（可在后续作为批处理任务）。
- 不在本期重做 RD-Agent 的训练/回测逻辑，仅修复“固化打包”和“加载契约”。

## 2. 现状与差异摘要

现状（RD-Agent `solidify_loop_assets` + AIstock `get_strategy_files`）：

- RD-Agent 侧：扁平化拷贝 + workspace_id 前缀避免冲突，但**排除了关键文件**（`read_exp_res.py`、`mlruns/**/params.pkl`）。
- AIstock 侧：按 `factor.py/model.py` 优先，否则随机取第一个 `.py`；模型权重按 `weights/model.pkl` / `mlruns params.pkl` / 根目录 `.pkl` 猜。

差异（导致无法严格保证推理）：

- 缺少“强约定入口文件”和“强约定权重文件”
- 缺少“manifest 映射清单”
- AIstock 不理解“前缀命名规则”，存在选错 py/pkl 风险

## 3. 改造总体策略（强契约闭环）

- **RD-Agent 侧输出**：生成结构化 bundle + `manifest.json`，并将推理入口与权重统一为标准路径。
- **AIstock 侧消费**：优先读取 `manifest.json` 定位资产文件；manifest 不存在时才使用旧 heuristics。

## 4. 两端接口与约定（必须落地）

### 4.1 Bundle 目录结构约定（schema v1）

```
production_bundles/{asset_bundle_id}/
  manifest.json
  workspaces/
    {workspace_id}/
      factor_entry.py
      model.pkl
      config.yaml | conf_*.yaml (可选)
      extras/ (可选)
```

### 4.2 manifest.json 约定（最小必需字段）

- `schema_version`: `1`
- `task_run_id`, `loop_id`
- `primary_workspace_id`
- `primary_assets.factor_entry_relpath`
- `primary_assets.model_weight_relpath`

> AIstock 侧只要拿到上述字段，即可严格定位到推理所需文件。

### 4.3 manifest schema 存放与版本管理（推荐落地）

结论：

- **权威来源建议放在“资产包文件（manifest.json）”里**，作为 bundle 的自描述契约，AIstock 解压后即可离线消费。
- **数据库中建议存“manifest 摘要/索引字段”**，用于检索、审计、联动展示与快速诊断；但不建议把“完整 manifest”只存 DB 而不随 bundle 下发。

原因：

- bundle 是跨系统交付物，必须自包含，不能要求 AIstock 推理时额外依赖 DB 才能定位文件。
- DB 适合做索引（例如 primary workspace、入口文件相对路径、模型权重相对路径、log_dir、source_workspace_path 等），便于 UI 与排障，但 DB 不应替代 bundle 自描述。

推荐实现（两端约定）：

- **manifest.json 内必须包含 `schema_version`**（如 `1`），用于兼容升级。
- RD-Agent 侧在生成 bundle 时写入 manifest.json。
- AIstock 侧在下载解压后：
  - 优先读取 manifest.json 并校验 `schema_version` 是否受支持。
  - 将 manifest 关键字段写入/更新到 AIstock 的 catalog 表（或新增表）用于索引与审计。

## 5. 分工明细（RD-Agent 侧 / AIstock 侧）

### 5.1 RD-Agent 侧（Owner：RD-Agent）

**RD-1 修复漏打包（必须）**

- 移除对 `read_exp_res.py` 的排除
- 移除对 `mlruns/**/params.pkl` 的排除，或将其复制/重命名为 `model.pkl`

**RD-2 输出标准推理入口（必须）**

- 在 bundle 中为 `primary_workspace_id` 生成 `workspaces/{workspace_id}/factor_entry.py`
- 要求：
  - 可被 AIstock `validator.validate_and_load` 成功 import
  - 提供标准 wrapper（`factor_xxx`）或 `Factor` 类

**RD-3 输出 manifest.json（必须）**

- 选择 `primary_workspace_id`：优先 `experiment_workspace`
- 写入 `primary_assets` 指向 `factor_entry.py` 与 `model.pkl`

**RD-3.1 同时打包原始源码（必须，面向未来组合）**

- 在 `workspaces/{workspace_id}/` 下保留原始源码：
  - 推荐路径：`workspaces/{workspace_id}/src/**`
  - 或至少包含：`factor.py`（以及其依赖的其他 `.py`）
- 约定：
  - **推理永远使用 `factor_entry.py`**
  - `src/**` 用于未来“AIstock 人工组合 -> RD-Agent 训练/回测”场景

**RD-4 因子命名区分方案落地（建议）**

- 生成 `fingerprint_md5 = md5(normalize(expression))`
- registry/catelog 中支持同名因子区分与去重（occurrences）

**RD-5 固化前自检（必须）**

- manifest 存在
- factor_entry.py 存在
- model.pkl 存在
- 不满足则不写 `asset_bundle_id` / 不置 `is_solidified=1`

**RD-6 权重文件策略（必须）**

- 约定：对外推理权重固定为 `model.pkl`（由 RD-Agent 选择一个“可推理权重源”复制生成）。
- 同时保留原始权重文件，避免信息丢失：
  - 例如将 `mlruns/**/params.pkl` 等原件放入 `workspaces/{workspace_id}/extras/` 并在 manifest 中记录其来源路径与用途。

### 5.2 AIstock 侧（Owner：AIstock）

**AS-1 读取 manifest 优先定位（必须）**

- 在 `RDAgentAssetService.get_strategy_files` 中：
  - 优先读取 `manifest.json`
  - 直接返回 manifest 指定的 `factor_entry_relpath` 与 `model_weight_relpath`
  - 若缺失/解析失败才走旧 heuristics

**AS-1.1 manifest 入库索引（建议）**

- 将 manifest 的关键字段写入 catalog（或新增表），用于：
  - UI 展示该 loop 的资产入口（factor_entry/model.pkl）
  - 快速排障（比扫描文件系统更快）
  - 与 loop / strategy / model / factor 的强关联

**AS-2 增强校验与错误输出（必须）**

- 缺失资产时打印：manifest 摘要 + bundle 树形摘要 + 关键候选文件列表

**AS-3 回归测试脚本（必须）**

- 新增或扩展现有检查脚本：
  - 给定 strategy_id 或 (task_run_id, loop_id)
  - 下载解压 bundle
  - 读取 manifest
  - 校验文件存在与可 load

## 6. 里程碑与交付物

### M1（RD-Agent）：修复漏文件 + 产出 manifest（最小可用）

- 交付：
  - 新版 bundle 能包含 `manifest.json`、`factor_entry.py`、`model.pkl`
  - 并通过 RD-Agent 自检

### M2（AIstock）：manifest 优先加载

- 交付：
  - AIstock 能按 manifest 定位资产
  - 推理不再依赖“猜测式搜索”

### M3（端到端）：选股推理回归

- 交付：
  - 至少 1 个历史策略 + 1 个 loop 的推理可跑通
  - scores 落库成功

### M4（增强）：因子命名区分（指纹去重）

- 交付：
  - Catalog/前端展示可区分同名因子
  - 不影响推理（推理以 manifest 为准）

### M5（增强）：资产索引入库与可视化（建议）

- 交付：
  - 新增（或扩展）AIstock 表存储每个 loop 的 manifest 摘要与关键路径
  - UI 可直接展示：bundle_id、primary_workspace_id、factor_entry_relpath、model_weight_relpath、source_workspace_path、log_dir 等

## 7. 验收标准（必须可量化）

- **验收A1**：对任意一个 solidified loop：
  - `manifest.json` 可解析
  - `factor_entry.py` 与 `model.pkl` 均存在

- **验收A2**：AIstock 推理：
  - 运行成功（无 ValueError: 未找到策略资产）
  - 输出 `scores` 非空

- **验收A3**：错误可诊断：
  - 人工删除 model.pkl 后再次推理，日志中能明确指出缺失文件与 manifest 指向

- **验收A4（强确定性）**：资产选择无歧义：
  - 在 bundle 中放入多个 `.py` / 多个 `.pkl`（含 pred/ic 等）时，AIstock 仍然严格按 manifest 指向加载，不受干扰

## 8. 风险与应对

- **风险R1**：不同模型框架的权重文件名不统一
  - **应对**：bundle 内统一对外暴露为 `model.pkl`（内部可来源于 params.pkl/other）

- **风险R2**：factor_entry.py 生成逻辑不稳定
  - **应对**：优先复制 workspace 中已验证可用的入口文件，并在 manifest 中记录来源；必要时在 RD-Agent 侧做 import 自检。

- **风险R3**：历史 bundle 无法立刻迁移
  - **应对**：AIstock 侧保留 heuristics fallback，但仅作为兼容路径；新产物必须有 manifest。

## 9. 已确认决策点（进入代码改造阶段）

你已确认：

1. **推理唯一入口**：使用 `factor_entry.py` 作为推理入口，同时打包原始 `factor.py/src/**`。
2. **推理权重入口**：使用 `model.pkl` 作为推理入口权重，同时保留 `params.pkl` 等原始权重到 `extras/`，并由 manifest 记录。

后续代码改造将以此为准。

## 10. 是否需要为每个 loop 建立“文件资产索引表”？（推荐：需要，但存摘要不存全量文件清单）

结论：

- **推荐新增/扩展表，用于存储“manifest 摘要 + 关键路径 + 诊断字段”**。
- 不建议在 DB 中保存“bundle 内全部文件清单”（过大、更新频繁、价值有限）。

推荐字段（示例，供你决定是否新增表或复用现有表扩展字段）：

- `task_run_id`
- `loop_id`
- `strategy_id`（如可关联）
- `workspace_id`（primary workspace）
- `asset_bundle_id`
- `manifest_schema_version`
- `factor_entry_relpath`
- `model_weight_relpath`
- `config_relpath`（可空）
- `source_workspace_path`（RD-Agent 原始 workspace 路径，便于追溯）
- `log_dir` / `log_uri`（RD-Agent 原始 log 目录/可访问 URI，便于定位日志）
- `created_at` / `updated_at`

实现建议：

- RD-Agent 侧：生成 manifest.json 时尽可能写入 `source_workspace_path` 与 `log_dir/log_uri`（如果能稳定确定）。
- AIstock 侧：在同步/导入 catalog 时，将这些字段入库，用于 UI 展示与排障检索。
