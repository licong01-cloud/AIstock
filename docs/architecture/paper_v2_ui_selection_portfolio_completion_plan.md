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

<!-- paper-v2-ux-live-dashboard-addendum-20260514 -->

## 2026-05-14 UI 语义与实时看板补充决策

### 命名与导航

1. Paper v2 不再设计独立左侧导航；AIstock 已有全局左侧导航，Paper v2 内部页面使用顶部横向二级导航。
2. 当前数据库对象 `paper_v2.portfolio` 在产品语义上不是“多个策略包的投资组合”，而是一个由单个 StrategyPackage 驱动的模拟盘实例。
3. UI 文案将“模拟盘组合”统一改为“模拟账户”或“模拟盘实例”；“策略组合”仅保留给未来多个策略包按权重组合后的新对象。
4. 策略包和模拟盘实例必须提供人类可读名称：
   - StrategyPackage 默认名示例：`2026-05-13 Loop1 小盘LSTM V25`
   - 模拟账户默认名示例：`2026-05-14 LIVE模拟 - Loop1小盘策略`
   - ID、manifest hash、artifact hash 只放在审计详情、复制按钮或高级信息中，不作为主标题。
5. 历史 E2E / smoke / 临时记录默认从主列表隐藏或归档；清理必须先提供预览，不得误删当前 RUNNING 实例。

### 今日信号排序

1. “今日信号”表格必须支持按字段排序，至少覆盖：排名、股票、分数、参考价、候选预览权重、来源。
2. 第一阶段可在前端对后端返回的 Top50 做 client-side sorting；后续当候选数较多时再升级为后端排序、分页和搜索。
3. 排序只改变展示顺序，不改变选股结果、目标仓位或下单意图。

### 信号候选权重与实际目标仓位必须分离

1. “今日信号”显示的是 StrategyPackage selection artifact 的候选清单，`target_weight` 目前是 artifact 生成阶段的 TopK 等权预览字段。
2. Paper v2 日频目标仓位不能以该字段作为真实持仓权重；当 manifest/runtime contract 为 `score_weighted_topk_v2` 时，真实目标仓位必须由 TargetPositionEngine 根据分数、`weight_method`、`min_weight`、`max_weight`、`max_position_ratio`、当前持仓和交易限制重新计算。
3. UI 需要拆分两层含义：
   - 今日信号：候选股票、实时 DB 推理分数、排名、参考价、风险/HMM 追踪字段。
   - 目标仓位与调仓意图：TargetPositionEngine 生成的真实 `target_weight` / `target_quantity` / 买卖意图。
4. 如果继续在今日信号表保留 `target_weight`，列名必须改为“候选等权预览”或“artifact 预览权重”，避免误导为实际持仓目标。
5. “目标仓位与调仓意图”卡片必须使用 live 与 replay 一致的事件合约：`TARGETS_GENERATED` 和 `ORDER_INTENTS_GENERATED`，并展示真实 score-weighted 结果。

### 实时看板事件合约

1. replay/day-runner 已写入 `TARGETS_GENERATED` / `ORDER_INTENTS_GENERATED` 事件并携带数组；live runner 当前只写 `LIVE_RUN_PREPARED` 计数字段会导致看板卡片为空。
2. LIVE `_prepare_live_run()` 在生成 `targets` 与 `intents` 后必须补写与 day-runner 相同的两个事件，或在看板聚合层显式兼容 `LIVE_RUN_PREPARED`；首选补写标准事件，保证 replay/live UI 合约一致。
3. 该改造属于可观测性与 UI 合约修复，不得改变目标仓位计算、下单策略或撮合行为。

### 实时资产曲线

1. “实时资产曲线”应按分钟时间线展示 `paper_v2.intraday_snapshots`，横轴为 `snapshot_time`，纵轴为 NAV / 收益率，可叠加现金、市值和仓位数量。
2. 当前一根柱状图不是业务预期，只是因为后端当前 run 仅持久化了 1 条 intraday snapshot，且前端使用 `pv2-sparkline` 柱状条而不是折线时间轴。
3. LIVE tick 每处理到新的 completed minute bar 后应持续写入新的 intraday snapshot；若当天只有 1 条快照，UI 必须提示“样本不足，等待后续分钟快照”，而不是伪装成完整曲线。
4. 前端应改为时间序列折线/面积图；当样本数少于 2 时显示单点状态卡和最近快照时间。
5. 看板应暴露快照数量、首末时间、最新 NAV、最新收益率，便于判断是运行缺数据还是 UI 展示不足。

### P0 LIVE 执行真实性修复

1. LIVE_ONLY 严格禁止回填订单创建前的分钟线成交；订单创建时必须把已完成的 latest common minute bar 写入 `OrderExecutionState.last_processed_bar_time`。
2. 后续 fill 的 `trade_time` 必须晚于订单创建时刻对应的 LIVE 起始边界；历史 replay/catchup 例外必须显式标记。
3. 订单全部成交后仍需继续按每个新 completed minute bar 做持仓 mark-to-market，并写入 `intraday_snapshots`。
4. 看板需要显示执行数据层级：`local_sim + TDX_REALTIME minute-close`、`minqmt_sim + MINIQMT_REALTIME broker-fill` 等，避免把分钟模拟误认为真实 tick 撮合。
5. miniQMT 模拟盘不能假设天然无误差；它能把撮合交给 miniQMT 仿真账户，但仍需连接、拒单、撤单、时间因果、资产状态来源和事件幂等审查。
