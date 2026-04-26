# Paper Trading v2 UI 补齐方案：策略包创建、选股、自选与单包模拟盘

更新日期：2026-04-26

## 目标

本轮补齐 `/paper-v2` 的可操作闭环，使用户可以在新 UI 中按权威流程完成：

1. 从 QE 单次实验或 QE 演进 Loop 创建 StrategyPackage。
2. 用 StrategyPackage 进行权威选股，选股前自动生成 live/latest DB 推理选股 artifact。
3. 查看历史选股记录并动态选择多条单包选股记录做交集、并集或加权融合。
4. 将选股结果按选股当时可追溯价格一键加入自选股票池。
5. 从单个 StrategyPackage 创建模拟盘 v2 组合，配置初始资金、策略运行参数、HMM、行业黑名单、历史回放或实时模拟模式。
6. 查看当前正在运行或已创建的模拟盘策略包列表，并进入运行控制台继续回放/单日运行/账本/绩效验证。

## 关键业务原则

- 选股不得使用 QE 回测 `pred.pkl` 作为权威输入；权威选股必须由 StrategyPackage 保存的 QE 模型在最新/实盘 DB 数据上重新推理。
- UI 可以自动触发 artifact 生成，但不是 silent fallback：如果 DB 数据、模型、因子、WSL 推理、参考价或停牌数据缺失，后端必须返回 fail-fast 错误并展示给用户。
- 多策略包聚合只作用于选股研究，不直接创建模拟盘执行组合；模拟盘执行仍只接受单 StrategyPackage，直到组合 StrategyPackage 或 SelectionBundle 合约稳定。
- HMM、行业黑名单、停牌剔除、TopK 属于运行时可变配置，不写回 StrategyPackage manifest。
- 模拟盘执行策略必须来自回测验证过的执行策略，不提供模拟盘独有执行算法配置。
- 自选股票池加入价必须来自选股结果中可追溯的 `reference_price`；如果缺价格，则整次加入失败，不能用默认价或实时兜底伪装成功。

## 后端补齐设计

### StrategyPackage 来源下拉

新增接口：

```text
GET /api/v1/strategy-packages/qe-sources?source_kind=all&limit=200
```

返回字段：

- `source_kind`: `qe_experiment` 或 `qe_evolution_loop`
- `experiment_id`
- `experiment_name`
- `qe_task_id`
- `qe_loop_id`
- `loop_index`
- `display_name`
- `metrics_summary`: 年化收益、IC、RankIC、夏普、最大回撤等
- `created_at` / `completed_at`

筛选规则：

- 只返回 `qe_experiments.status='completed'` 且有 `result_metrics` 的记录。
- 单次实验默认排除 `is_evolution_loop=true` 的记录。
- 演进 Loop 默认只返回 `is_evolution_loop=true` 且有 `qe_task_id/qe_loop_id` 的记录。
- 已经生成 StrategyPackage 的 source 不再返回，避免重复打包。

### 选股 artifact 自动生成

Selection Center 接收：

```json
{
  "runtime_config": {
    "selection_artifact_config": {
      "auto_generate": true,
      "inference_backend": "wsl"
    },
    "runtime_profile": { ... }
  }
}
```

行为：

- `auto_generate=true` 时，Selection Center 在运行 StrategyPackageRuntime 之前检查对应 `package_id + manifest_sha256 + trade_date + data_source + artifact_runtime_hash` 是否已有权威 artifact。
- 若缺失、失败或不是 `live_qe_model_inference_v1 / authoritative_selection`，调用 `StrategyPackageSelectionArtifactService.generate_from_live_inference` 生成。
- `selection_artifact_config.auto_generate` 只控制编排，不参与 artifact hash，避免同一推理配置产生重复 artifact。
- 只支持 `DB_HISTORICAL`；`TDX_REALTIME` 在当前阶段必须明确失败。

### 选股结果加入自选

新增接口：

```text
POST /api/v1/selection-center/runs/{run_id}/add-to-watchlist
```

请求字段：

- `category_id` 或 `category_name` 必须二选一；没有分类时可自动创建。
- `top_k`: 默认 20，最大 50。
- `on_conflict`: `ignore` 或 `move`。

规则：

- 只允许 `SUCCEEDED` 的 selection run。
- `aggregate_results` 不能为空。
- 每个加入项必须有 `reference_price > 0`。
- `entry_source` 写入策略包名称；`entry_task_id` 写入 `run_id`；`entry_as_of` 写入选股 `trade_date`；`entry_rank` 写入选股排名。
- 后端自选服务返回错误时接口整体失败，不允许部分失败被当作成功。

### TopK 与聚合

- 选股默认 Top20，后端运行配置最大 Top50。
- 多策略包直接运行和已有单包记录聚合均支持 `intersection`、`union`、`weighted_fusion`。
- 聚合已有记录时要求 source run 都是成功的单包选股，且交易日和数据源一致。

## 前端补齐设计

### `/paper-v2/packages`

页面模块：

1. QE 来源创建区
   - 下拉选择“QE 单次实验”或“QE 演进 Loop”。
   - 下拉选项展示：名称、年化收益、IC、最大回撤。
   - 只显示未打包来源。
   - 按钮：预览 Manifest、验证模拟盘就绪、创建策略包。
2. 策略包列表
   - 展示名称、状态、来源、年化收益、IC、RankIC、夏普、最大回撤、manifest hash。
   - 操作：启用选股、启用模拟盘、创建模拟盘、退役。
3. 策略包详情
   - 展示模型新鲜度、状态事件、已验证执行策略。

### `/paper-v2/selection`

页面模块：

1. 选股控制
   - 默认单包模式，TopK 默认 20，最大 50。
   - 数据源下拉：`DB_HISTORICAL`、`TDX_REALTIME`。当前权威推理只支持 DB，选择 TDX 时后端会 fail-fast。
   - HMM 启用后显示模型版本配置和快照下拉。
   - 行业黑名单、停牌剔除可配置。
2. 策略包选择
   - 单包模式只能选一个；聚合模式可多选。
   - 加权融合时可输入每个策略包权重。
3. 结果区
   - 展示 rank、股票、score、目标权重、选股价、原因、追踪字段。
   - 按钮：加入自选股票池。
4. 历史记录
   - 点击历史记录后展示该次选股结果。
   - 支持选择多条成功单包记录并聚合。

### `/paper-v2/portfolios`

页面模块：

1. 单策略包模拟盘启动
   - 选择 StrategyPackage。
   - 初始资金、开始日期、数据源、执行策略。
   - 运行时配置：TopK、停牌剔除、行业黑名单、HMM 配置/快照。
   - 模式：只创建实时模拟盘，或创建后立即历史分钟回放。
   - 历史回放可设置 replay start/end；重跑策略默认 `reject_existing`，重置仍需在运行控制台确认。
2. 运行中/已创建组合列表
   - 展示组合名称、状态、策略包、资金、数据源、开始日期。
   - 操作：运行控制台、详情、暂停/恢复、退役。

## 验证方案

1. 后端单元测试：
   - StrategyPackage QE source 列表。
   - Selection Center auto artifact generation 开关。
   - Selection run 加入自选的失败路径和成功路径。
   - TopK 最大 50 校验。
2. 前端构建：`npm run build`。
3. 非 8001 端口后端验证：启动临时 FastAPI，例如 8011。
4. 非 3000 端口前端验证：启动临时 Next，例如 3011，并设置 `NEXT_PUBLIC_API_BASE=http://127.0.0.1:8011/api/v1`。
5. Playwright/HTTP UI 流程验证：
   - 打开 `/paper-v2/packages`，确认 QE source 下拉可用。
   - 打开 `/paper-v2/selection`，确认 Top20、HMM 下拉、历史记录点击、聚合按钮、自选按钮。
   - 打开 `/paper-v2/portfolios`，确认单包模拟盘启动表单和运行组合列表。
   - 对真实 QE 包 `qe_20260416_002701`、`qe_20260413_084216`、`qe_20260416_082012` 进行 DB_HISTORICAL 选股验证；若数据/模型缺失，记录后端 fail-fast 错误，不写兜底。
