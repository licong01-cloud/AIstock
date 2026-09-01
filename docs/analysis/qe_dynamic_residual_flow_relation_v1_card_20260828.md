# QE `dynamic_residual_flow_relation_v1` 文件型关系候选卡

- 状态：`SOURCE_AND_LEAKAGE_TEST_CANDIDATE`
- 日期：2026-08-28
- 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.10，P1-B
- 执行方案：`docs/analysis/ma_e19_p0_triad_and_alpha_execution_plan_20260824.md` v1.1，A-04
- 主角色：`RELATION_PRIOR`
- 本阶段：只实现冻结 topology、历史动态边权和防泄漏测试；不作为 `DIRECT_ALPHA`，不入库，不运行正式 QE。

## 1. 假设与角色边界

板块 residual return、资金状态和领导扩散之间可能存在跨板块 lead-lag。该候选只回答“历史关系能否作为传播/正则先验”，不能因相关系数、稀疏度或未来图模型收益而自动解释为独立股票/板块 Alpha。

只有未来独立任务在预注册的板块未来收益排序、matched baseline、多个 vintage 和成本后组合中证明直接预测增量，才允许另立 `DIRECT_ALPHA` 身份；本候选本身永久保持 `RELATION_PRIOR`。

## 2. 文件输入合同

输入为 repo 外冻结 Parquet，索引精确为 `(datetime, l2_code_id)`，每个板块日唯一。三个通道必须由上游数据合同预先冻结并带 identity，关系算法不自行选择 residual 或 leadership 模型：

- `residual_return`：小数制、已按独立设计冻结的板块残差收益；
- `flow_state`：有界标准化资金状态；
- `leadership_state`：冻结到 `[-0.5,0.5]` 的领导扩散状态；
- `l2_code_id`：正整数 PIT taxonomy 身份。

缺列、重复板块日、非法 L2、非有限值、`abs(residual_return)>0.5`、`abs(flow_state)>2`、`abs(leadership_state)>0.5` 或时区化日期均显式失败。当前阶段不从原始收益自行去均值/回归，不从 H5、数据库或当前成员快照隐式补齐该面板；正式输入必须带数据准备窗口的 identity/hash/signoff 和三通道构造版本。

## 3. 冻结 topology

- 候选 lag 固定为 `1/5/10/20` 个交易日；
- 训练/验证窗口内，对每个 source-channel-lag → target residual return 计算 full、前半段和后半段相关；
- 三段方向一致时，`stability=min(abs(first_half), abs(second_half))`，`selection=abs(full)×stability`；方向反转或样本不足的关系不进入候选；
- 每个 source-target-channel 只保留最强的一个 lag，再按 target/channel 固定 Top-K 来源；排序同分时按 source 和 lag 确定性裁决；
- topology 输出后，评价期禁止重新选择邻居、channel 或 lag。

## 4. 动态边权

对于 topology 中的冻结边，在预测日 `t` 输出：

```text
long_weight(t)  = Corr(source_channel[u-lag], target_residual[u]), u <= t-1
short_weight(t) = 同一关系的半窗口相关，u <= t-1
stability(t)    = 同号时 min(abs(long), abs(short))，否则 0
effective(t)    = long_weight(t) × stability(t)
```

所有 rolling 结果整体 `shift(1)`，确保预测日自身值不能改变同日边权。评价期允许历史边权更新，但不允许 topology 重选。

## 5. 输出合同

三个显式、互不相同且位于 repo/worktree 外的 Parquet：

1. topology：冻结 source/target/channel/lag、fit 三段相关、稳定分、选择分和 rank；
2. weights：逐预测日、逐冻结边的 long/short/stability/effective weight；
3. receipt：合同版本、角色、输入/topology/weights SHA256、窗口、lags、Top-K、最小样本和行数。

不输出股票 `result.h5`，因为关系先验不是直接 Alpha；不写 catalog、metrics、classification、correlation 或任何数据库表。

## 6. 当前验证与未完成项

当前聚焦验证覆盖：

- 每条 source-target-channel 只冻结一个 lag；
- 已知 1 日传导的可识别性；
- 修改 fit_end 之后数据不改变 topology；
- 修改预测日数据不改变同日 edge weight；
- 动态 materialization 不产生 topology 外的新边；
- 冻结边在评价期缺覆盖时整体 fail loud，不静默删边；
- 外部 topology 的身份、lag、rank、相关系数和稳定分类型/范围校验；
- 非法面板 fail loud；
- 三个 Parquet 与 SHA receipt 一致；
- fit/evaluation 重叠和 repo 内输出 fail loud。

正式文件快筛、跨 vintage 方向稳定性、relation sparsity/turnover、静态 industry-bias 对照、逐通道消融和 QE LOO 均等待数据 signoff 与 D2 结果，不在本阶段执行。

复杂度审查：topology fit 的理论上界为 `O(channel × lag × sector² × fit_dates)`，但节点仅为申万 L2 板块而非股票；正式运行仍须在 repo 外候选文件上记录 wall time、峰值 RSS 和 relation 行数。当前 guardrail 的相关 P2 提示只表示需保留该资源回执，不降低为股票级全连接图，也不作为人工审批门禁。

## 7. 验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| A04-01 三通道与 1/5/10/20 lag | channel derivation / topology fit | known-lag test | IMPLEMENTED | 正式文件待 signoff |
| A04-02 topology 冻结 | source-target-channel 去重、Top-K | freeze tests | IMPLEMENTED | 无 |
| A04-03 预测日前历史 | rolling correlation `shift(1)` | same-day mutation test | IMPLEMENTED | 无 |
| A04-04 测试期不重选 | materializer 只消费 frozen topology | edge-set equality | IMPLEMENTED | 无 |
| A04-05 稳定性与衰减 | half-window stability、lag 字段 | topology/weight contract | IMPLEMENTED | vintage/decay 评价待后续 |
| A04-06 文件身份 | 三 Parquet SHA receipt | artifact builder test | IMPLEMENTED | 数据集 identity 待 signoff |
| A04-07 角色分离 | 仅 relation artifacts，无 `result.h5` | static/docs review | IMPLEMENTED | 不可冒充 DIRECT_ALPHA |
| A04-08 正式快筛/QE | 后续独立任务 | 当前不执行 | DEFERRED | 等待 D2 与数据 signoff |

## 8. DESIGN-COMPLIANCE-001

1. topology fit、动态边权和文件 receipt 均为完整实现；未把 fixture 或单测冒充正式关系验证。
2. 输入、窗口、样本、拓扑和输出路径错误均显式失败，无数据库、静态邻接或当前成员 fallback。
3. 与父设计一致保持 `RELATION_PRIOR`；未改变现有 GAT、组合、执行或数据业务语义。
4. 未新增人工审批、收益门禁、catalog、生产激活或进程控制。
