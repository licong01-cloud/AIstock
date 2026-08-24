# MA-E19 / P0 三联诊断与新 Alpha 统一执行方案

- 文档类型：F2 策略演进执行方案
- 状态：`DESIGN_READY_NO_EXPERIMENT_SUBMITTED`
- 版本：v1.0
- 日期：2026-08-24
- 父蓝图：`docs/analysis/sector_rotation_factors_develop_spec_20260710.md` v6.9
- 概念板块从属设计：`docs/architecture/qe_concept_sector_data_factor_parallel_f2_design_20260813.md`
- 唯一目标：形成更优、可复算、可实施的多 Alpha 长期趋势与板块轮动策略包

## Background / 背景

父蓝图已经从“继续堆横截面腿”调整为先识别瓶颈：模型陈旧、板块选择、板块内排序、beta/主动收益、右尾召回、信号到组合转换分别需要独立证据。MA-E19 是 P0-D1 的首次真实分钟 TWAP canary，但当前只完成 9/12，并在 BUG-1133 修复前生成 portfolio 结果，不能据此裁决 fixed、expanding、rolling 的优劣。

截至 2026-08-24 的只读事实如下：

| 项目 | 当前事实 | 解释边界 |
|---|---|---|
| MA-E19A | `qe_20260819_022744_73a5`，7/9 completed、2/9 failed | completed signal 可读；2026H1 expanding/rolling 缺失 |
| MA-E19B | `qe_20260819_024747_7c51`，2/3 completed、1/3 failed | completed signal 可读；2026H1 fixed 缺失 |
| MA-E19 合计 | 9/12，两个 task 均 terminal `failed`，无活动 loop | 顶层 failed 不覆盖九个成功 loop |
| 执行口径 | LGBM、CE3、h20、seed 123、21 日 purge、Top50/n_drop1、`TWAP/1min`、node1 | 证明任务形状可执行，不证明完整 D1 结论 |
| BUG-1133 | Issue #3616；fix `2757f865…`；registry `verified/closed` | 修复运行态完成；修复前 portfolio 证据不会自动变有效 |
| canonical PIT v2 | submission `dss_d41b1818feb7c81b0124b193b74b0349` 仍为 `BLOCKED_SOURCE_AUDIT_INCOMPLETE`；BUG-1157 source PR #3706 已合入，但 Issue/registry open，runtime/repair/signoff 未完成 | 源码合入不等于数据就绪；不得绕过，依赖 v2 的正式新实验暂缓 |
| 概念板块 | F2 设计完成，预期 builder、因子和测试文件尚不存在 | `DESIGN_READY_NOT_STARTED` |
| 分钟新信号 | 历史 1min candidate/覆盖证据未完成 | 暂缓；分钟 TWAP portfolio 合同不暂缓 |

## Scope / 范围

本方案覆盖：

1. 对 R8～MA-E19 既有证据做不补算、不重跑的决策型综合分析。
2. 定义 `WP-D1R` 的完整确定性 12-arm 重跑。
3. 定义 `WP-D2` 四格 sector oracle 与现实两层诊断。
4. 定义 `WP-D3` benchmark-relative / Brinson 归因。
5. 定义 D1/D2/D3 结果如何触发适应、两层板块、lead-lag、右尾、meta-labeling、动态退出、横截面新 Alpha 或受约束组合构建。
6. 定义五类优先新 Alpha 与一类右尾/趋势持续性特征的研发卡。
7. 明确概念板块和分钟级信号的进入条件。
8. 定义资源、安全、验证、结束标识和下一会话可恢复边界。

## Non-Goals / 边界与非目标

- 本文不提交、恢复、暂停或取消任何 QE task/loop。
- 不启动、停止或重启 AIstock、WSL、RD-Agent、node1、worker 或其他用户进程。
- 不执行数据集构建、candidate activation、node1 distribution、生产切换、DDL/DML 或依赖安装。
- 不访问数据库取得回测输入；数据库只允许未来控制面和结果面按既有合同记录。
- 不补历史 Archive、旧 Loop 制品、长期趋势 F014 指标、状态总账或 UI。
- 不把论文方法、设计完成、部分 canary、BUG 修复、代码合入或数据 source merge写成 Alpha 已验证。
- 不提前占用新的 `MA-E20/MA-E21` 编号；正式提交前重新查询最新任务列表，再绑定工作包与 task id。
- 不开发依赖分钟历史明细的微观结构/盘口/日内资金流信号。

## Architecture / 执行架构

```text
冻结文件数据身份
    |
    +--> WP-D1R: fixed / expanding / rolling 的确定性完整矩阵
    |         |
    |         +--> staleness 可恢复比例与 calendar-regime 剩余弱化
    |
    +--> WP-D2: reality/oracle sector x reality/oracle stock
    |         |
    |         +--> sector selection 与 within-sector ranking 上界
    |
    +--> WP-D3: absolute + active + Brinson
              |
              +--> beta / allocation / selection / interaction

D1R + D2 + D3 结果
    |
    +--> 适应/条件门控
    +--> 两层板块/跨板块 lead-lag/动态关系
    +--> 右尾 LTR/hazard/meta-label/动态退出
    +--> 横截面/正交新 Alpha
    +--> 受约束板块主动权重与稳健组合
```

架构原则：

1. `DIRECT_ALPHA`、`CONDITIONING_STATE`、`RELATION_PRIOR`、`PORTFOLIO_POLICY`、`NEGATIVE_CONTROL` 分开。
2. signal、portfolio、execution 三层证据分开；某层通过不能代替其他层。
3. 论文机制只产生 matched canary 假设，不能跳过简单基线。
4. 当前 combine 冻结 prediction 层可以先验证 OOF gate，无需先联合训练或重写底座。
5. 受约束组合只在 D2/D3 支持 allocation 路线时触发，不建设通用平台。

## Contracts / 契约

### 数据契约

- 训练、预测、回测、组合只读钉住的 bin/H5/Parquet/sidecar。
- 每个 task 保存 dataset id、cutoff、universe rule、文件 SHA、factor set、label、prediction identity。
- 缺文件、hash 漂移、PIT 覆盖不足、taxonomy 不可证明时 fail closed。
- canonical PIT v2 的 `BLOCKED_SOURCE_AUDIT_INCOMPLETE` 不得通过删行、幸存者池、放宽 builder、跳过 terminal evidence 或数据库回退解除。
- BUG-1157 source merge `4fb89674…` 只表示 repair 代码进入主线；worker-scheduler identity、新 repair attempt、candidate signoff 与生产激活必须独立回读，任何一项不得由 source merge 推断。
- 已有 2026-06-30 v1 文件可用于复现既有实验；不得描述为 v2 candidate signoff。

### 执行契约

- 正式 portfolio 固定 `execution_algo=TWAP`、`backtest_freq=1min`、outer day / inner 1min `NestedExecutor`、`TailTWAPWithLimitStrategy`。
- 固定 Top50/n_drop1、费用、交易单位、停牌过滤、涨跌停、投资域与测试窗口。
- 当前 TWAP 是尾盘 TWAP，不得改写为全天 vanilla TWAP。
- `CLOSE_PRICE` 只能是 signal attribution companion；V25/V25_1 不能作为收益真实性基线。
- 任一成交输入缺失即 `NOT_COMPUTABLE`，不得静默降级日频。

### 证据契约

- `signal_evidence`：IC、ICIR、RankIC、RankICIR、bucket/tail recall、prediction identity。
- `portfolio_evidence`：CAGR、Sharpe、MDD、Calmar、turnover、cost、cash/positions、benchmark-relative。
- `execution_evidence`：配置、分钟数据 identity、order/fill/reason、limit/suspend、deterministic ordering。
- failed task 内 completed loop 单独保留；missing arm 不补零、不回填。
- BUG-1133 修复前 MA-E19 portfolio 统一标记 `NONDETERMINISTIC_NOT_FOR_MATCHED_DECISION`。
- 选择偏差分析必须包含失败、放弃和被比较配置；样本不足时 `NOT_COMPUTABLE`。

### 资源契约

- `WP-D1R` 在 node1 使用全局并行度 1；禁止为了缩短时间扩大并行。
- 图模型未来保持 WSL `gpu_serial_graph=1`；本轮 D1R 不占 WSL GPU。
- 只有不同数据/缓存/recorder/workspace 完全隔离且既有调度允许时，两个节点才可并行。
- 不轮询 GPU/NVML/WMI；资源证据由任务自然回执或有界程序层观察取得。
- 后端进程控制始终归用户所有。

### 角色契约

| 角色 | 必须证明 | 不能冒充 |
|---|---|---|
| `DIRECT_ALPHA` | 独立排序、matched 增量、腿表现与 LOO | 降回撤、低相关或模型名 |
| `CONDITIONING_STATE` | 条件切片/组合改进、无未来状态 | 独立收益预测 |
| `RELATION_PRIOR` | 关系消融、传播/正则增量 | 静态邻接直接分数 |
| `PORTFOLIO_POLICY` | 相同 prediction 下成本后改进 | 基础 RankIC 提升 |
| `NEGATIVE_CONTROL` | 方向、冗余或机制边界 | “失败数据”或缺失 |

## Historical Experiment Synthesis / 历史实验综合分析

| 证据组 | 已确认结论 | 对后续任务的约束 |
|---|---|---|
| R8A/R8B | LGBM/LSTM 长周期 RankIC 较高，但 2026H1 明显弱化；Top50 对未来 Top1% 捕获仅略高于随机 | 不能只优化全截面 RankIC；必须保留右尾/近期窗口 |
| R9S/R10/R11 | n_drop/hold 的最佳值依模型和期限变化；固定延长持仓降换手但没有稳定增益 | 退出需要 hazard/meta/continue-hold，而不是继续扫固定持有期 |
| R11B/R12G/MA-E13G | 图模型 h40 效率较好；静态行业关系更像风险正则/关系先验 | 图路线优先动态关系/状态，静态边不直接作 Alpha |
| R12P/MA-E07/MA-E10 | 四腿 equal 和三腿 drop-TCN 都形成全窗口 Pareto | 多腿有价值，但 MA-E12 证明机械加腿会退化；必须 LOO |
| MA-E09/MA-E13R | 近期一年 CAGR 约 12%，明显弱于全窗口 | staleness、regime、beta/active return 必须拆开 |
| MA-E14 | H95/C85 静态阈值 overlay 在三个窗口均退化 | 结束该阈值分支，不否定软条件化状态 |
| MA-E15/MA-E16 | breadth 的 portfolio 增量伴随 RankIC/MDD 代价；MA-E15 panel 混杂，MA-E16 日频仅诊断 | breadth 暂作 `CONDITIONING_STATE/portfolio conversion candidate` |
| MA-E19 | 九臂 signal 未显示 IC 稳定恢复；2026H1 缺失；旧 portfolio 受 BUG-1133 影响 | 必须完整确定性 D1R，不能只补失败三臂或晋级适应模型 |

### MA-E19 当前信号归纳

前三个完整 vintage 中：

- expanding 相对 fixed：平均 `ΔIC≈-0.00135`、`ΔRankIC≈+0.00287`；
- rolling 相对 fixed：平均 `ΔIC≈-0.00494`、`ΔRankIC≈+0.00143`；
- 2024H2 fixed/expanding signal 完全相同，但 portfolio 不同，构成 BUG-1133 的直接业务后果；
- 当前只有 seed 123，不能形成神经网络或多 seed 稳定性结论；
- 2026H1 三臂缺失，不能回答近期弱化是否可恢复。

当前结论只能是：`PARTIAL_SIGNAL_DOES_NOT_YET_SUPPORT_STABLE_REFIT_RECOVERY`。它不是 D1 终局，也不是适应模型的否定结论。

## Work Package WP-D1R / MA-E19R 完整确定性复验

### 目标

在 BUG-1133 修复身份上，以完整 12 arm 回答：新近成熟数据能否稳定恢复信号和可实施收益，以及剩余弱化是否更接近 calendar regime。

### 预注册矩阵

以下窗口从 MA-E19A/B 的实际 `config_json` 回读并原样预注册；`test` 同时是 `backtest` 窗口。`fixed_anchor` 与 `fixed` 使用同一冻结训练/验证窗，只保留原 task 的身份名称差异。

| vintage | refit | train | valid | test/backtest | rolling_train_days |
|---|---|---|---|---|---:|
| 2024H2 | fixed_anchor | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2024-07-01～2024-12-31 | 0 |
| 2024H2 | expanding | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2024-07-01～2024-12-31 | 0 |
| 2024H2 | rolling | 2020-09-11～2023-10-27 | 2023-11-28～2024-05-29 | 2024-07-01～2024-12-31 | 756 |
| 2025H1 | fixed | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2025-01-02～2025-06-30 | 0 |
| 2025H1 | expanding | 2018-08-01～2024-05-07 | 2024-06-06～2024-12-02 | 2025-01-02～2025-06-30 | 0 |
| 2025H1 | rolling | 2021-03-23～2024-05-07 | 2024-06-06～2024-12-02 | 2025-01-02～2025-06-30 | 756 |
| 2025H2 | fixed | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2025-07-01～2025-12-31 | 0 |
| 2025H2 | expanding | 2018-08-01～2024-10-29 | 2024-11-28～2025-05-29 | 2025-07-01～2025-12-31 | 0 |
| 2025H2 | rolling | 2021-09-09～2024-10-29 | 2024-11-28～2025-05-29 | 2025-07-01～2025-12-31 | 756 |
| 2026H1 | fixed | 2018-08-01～2023-10-27 | 2023-11-28～2024-05-29 | 2026-01-05～2026-06-29 | 0 |
| 2026H1 | expanding | 2018-08-01～2025-05-08 | 2025-06-10～2025-12-02 | 2026-01-05～2026-06-29 | 0 |
| 2026H1 | rolling | 2022-03-23～2025-05-08 | 2025-06-10～2025-12-02 | 2026-01-05～2026-06-29 | 756 |

固定项：LGBM、CE3、h20、seed 123、120D cadence、21 日 purge、上述窗口、相同 observation panel、Top50/n_drop1、分钟 TWAP、费用、股票池、风险 policy、2026-06-30 文件 identity。

### 输出

1. 每臂 signal、portfolio、execution 三层 receipt。
2. prediction SHA、order/fill identity、deterministic tie-break version。
3. vintage 内 fixed/expanding/rolling matched delta。
4. full/early/late 汇总与模型年龄衰减曲线。
5. Top20/Top50、tail recall、within-portfolio RankIC、turnover/cost。
6. 同输入重放的 determinism 证明。

### 停止与晋级条件

- 任一臂缺失、hash 漂移、DB data-plane access、分钟覆盖不足：整体 `INCOMPLETE`，不做 D1 裁决。
- expanding/rolling 只在至少 3/4 vintage 的同方向 signal 与风险调整收益改善、且近期窗口不恶化时，记为“可恢复成分存在”。
- 若没有稳定恢复：不启动 DoubleAdapt/Proceed；直接进入 D2/D3 定位其他瓶颈。
- 若稳定恢复：先增加最简单 LSTM cadence/multi-seed matched canary，再决定是否需要适应模型。

### 完成标识

`MA_E19R_END_STATUS=COMPLETE_12_OF_12_DETERMINISTIC`

## Work Package WP-D2 / 四格 Sector Oracle

### 目标

判断收益瓶颈主要位于板块选择还是板块内股票排序，并测量两层模型的上界。

### 四格

| sector | stock | 身份 |
|---|---|---|
| reality | reality | 可部署基线 |
| oracle | reality | sector ceiling，永久不可部署 |
| reality | oracle | within-sector ceiling，永久不可部署 |
| oracle | oracle | 总体诊断上界，永久不可部署 |

每格同时比较 hard Top-M 与 soft continuous gating；申万 L2 为主 taxonomy，只有 PIT L1 文件可验证时才增加 L1。

### 输出与判定

- sector Recall/NDCG、右尾板块捕获、板块内 RankIC、股票 tail recall；
- TWAP portfolio、成本、换手、板块主动暴露与置信区间；
- oracle identity 和 `QE_ONLY_FUTURE_INFORMATION_CEILING` 标记；
- reality→oracle 的连续增量，而非 GO/STOP 标签。

结果触发：sector 增量显著 → 两层板块/lead-lag；stock 增量显著 → 横截面/右尾；两者都低 → 组合/执行；两者都高 → 两层联合路线。

### 完成标识

`P0_D2_END_STATUS=FOUR_CELL_COMPUTABLE`

## Work Package WP-D3 / Benchmark-relative 与 Brinson

### 目标

把绝对收益拆成 beta、主动板块配置、板块内选股与 interaction，重新解释全窗口高收益和近期弱化。

### 输入与输出

- 冻结 benchmark identity；
- 冻结可投资股票池等权对照；
- 同一 TWAP portfolio/holdings；
- PIT taxonomy 与逐日权重 hash；
- absolute return、active return、beta、tracking error、IR；
- Brinson allocation、selection、interaction。

缺 benchmark、逐日权重或 taxonomy 时结果为 `NOT_COMPUTABLE`，不得用当前成分回填历史。

结果触发：allocation 主导 → 两层板块/P1-F；selection 主导 → 股票新 Alpha；beta 主导 → 下调历史绝对 CAGR 的 Alpha 解释强度；成本主导 → 动态退出/换手预算。

### 完成标识

`P0_D3_END_STATUS=ABSOLUTE_ACTIVE_BRINSON_RECONCILED`

## Result Trigger Matrix / 结果触发矩阵

| 观测结果 | 下一工作包 | 不允许的跳跃 |
|---|---|---|
| D1R 稳定恢复 | 简单 cadence → LSTM matched → DoubleAdapt/Proceed canary | 直接建设在线学习平台 |
| D1R 不恢复 | D2/D3、横截面/右尾 | 把单次失败外推为所有适应方法失败 |
| D2 sector ceiling 高 | 两层 soft top-down、cross-sector lead-lag | 把 oracle 收益当现实收益 |
| D2 stock ceiling 高 | participation gap、leadership、cohesion、tail target | 继续堆静态行业边 |
| D3 allocation 主导 | P1-F 受约束主动板块权重 | 无上界证据就建设优化器 |
| D3 selection 主导 | 股票层正交 Alpha、right-tail LTR | 只调组合权重 |
| 主模型 recall 低 | LTR/NDCG/quantile/trend-survival | 先做 meta-label 过滤 |
| recall 足但转化弱 | meta-label take/skip/size/continue-hold、hazard exit | 声称 meta-label 能召回未入候选股票 |
| 专家条件互补 | OOF bounded gate / TRA/MIGA canary | 测试标签训练 gate |
| 新腿仅降风险 | CONDITIONING_STATE 或 PORTFOLIO_POLICY | 升级为 DIRECT_ALPHA |

## New Alpha R&D Cards / 新 Alpha 研发卡

### A-01 `sector_participation_gap_v2`

- 假设：大单参与增强而小单参与减弱，且差值正在加速时，板块后续趋势更可能持续。
- 主角色：`DIRECT_ALPHA`；可另立 `CONDITIONING_STATE` 版本，身份不得混用。
- 输入：冻结日频 moneyflow/amount、PIT L2 membership；金额/单位合同必须固定。
- 公式：`gap=CSRank(large_net_amt/amount)-CSRank(small_net_amt/amount)`；候选为 `mean_5(gap)-mean_20(gap)`。
- 缺失：amount≤0、字段缺失或行业无有效成员时 missing，不填零。
- 快筛：coverage、方向、h20 IC/RankIC/HAC、recent-6m、规模/换手残差、与现有腿相关性。
- QE：matched CE3+candidate、三种子、full/early/late、独立腿、blend、LOO。
- 退出：单位/分母无法证明、方向不稳定且 residual 后消失时保留负结果，不进入正式腿。

### A-02 `leadership_exhaustion_v1`

- 假设：领导集中度过高、breadth 减速且资金背离恶化时，板块趋势更可能衰竭。
- 主角色：`CONDITIONING_STATE`；若用于退出，另立 `leadership_exhaustion_state_v1`。
- 输入：冻结成员收益/排名、breadth、moneyflow、PIT L2。
- 公式族：领导持续性水平/斜率 × breadth deceleration × flow divergence deterioration；原 leadership persistence 方向保留为 `NEGATIVE_CONTROL`。
- 快筛：趋势前/中/后 episode、MFE/MAE、time-to-hit、回撤 hazard；不只看全截面 IC。
- QE：同 prediction 下的无状态 vs exhaustion-aware exit/de-risk；不能以退出改善反推 Alpha。
- 退出：只降低换手但不改善 false early-exit/Calmar，或只在测试窗口选阈值时停止当前公式。

### A-03 `sector_residual_cohesion_break_v1`

- 假设：去市场和去板块公共成分后，成员残差协同水平/斜率突然破裂，预示板块趋势失效或内部轮动。
- 主角色：`CONDITIONING_STATE`；关系消融中可作 `RELATION_PRIOR`。
- 输入：冻结日频收益、PIT L2、市场/板块基准；不得运行期数据库回归。
- 公式族：rolling residual correlation/cohesion level、5/20 日 slope、break z-score。
- 快筛：PIT truncation、coverage、不同成员数稳定性、回撤前 lead time、与 volatility/breadth 的 partial correlation。
- QE：状态切片、hazard、bounded de-risk；若形成独立预测再另立 DIRECT_ALPHA 身份。
- 退出：结果完全由板块规模/波动解释或成员覆盖不足时保持 `NOT_COMPUTABLE/NEGATIVE_CONTROL`。

### A-04 `dynamic_residual_flow_relation_v1`

- 假设：板块间 residual return、资金状态与领导扩散存在稳定 lead-lag，可用于轮动先行预测。
- 主角色：`RELATION_PRIOR`；只有独立板块未来收益排序验证后才可另立 `DIRECT_ALPHA`。
- 输入：冻结日频板块 residual、moneyflow、leadership、PIT taxonomy。
- 设计：lag 1/5/10/20 的有向关系、rolling stability、关系衰减；逐关系/逐通道消融。
- 防泄漏：边只能由预测日前历史构建，测试期不可重新选择邻居或 lag。
- 快筛：lead-lag IC、方向稳定性、relation sparsity、turnover、与静态 industry-bias 对照。
- QE：一层 baseline、静态关系 prior、动态关系 prior、两层 soft gating；完整 LOO。
- 退出：关系在不同 vintage 反向或只在高成本动态图结构下出现时，不升级为正式腿。

### A-05 `pit_fundamental_diffusion_v1`

- 假设：盈利/营收/资本开支等基本面加速度沿板块/产业关系扩散，可形成慢变量趋势确认。
- 主角色：`DIRECT_ALPHA` 或 `CONDITIONING_STATE`，预注册其一。
- 启动条件：冻结 H5/Parquet 含公告可见时间、issuer binding、PIT manifest 和完整 coverage；否则 `DEFERRED_PIT_INPUT_NOT_READY`。
- 输入：只读 PIT 基本面文件和 PIT taxonomy；不修改公告入库、canonical builder 或 event_signal。
- 快筛：as-of truncation、report lag、restatement、coverage、规模/价值/质量暴露、h20/h40。
- QE：matched 单腿、与价格/资金腿相关性、组合 LOO。
- 退出：缺 PIT 证据、覆盖仅幸存者、公告时点不确定时禁止运行。

### A-06 tail / trend-persistence feature family

- 假设：当前 MSE/全截面排序偏重分布中部，需要显式右尾与趋势持续性特征/目标。
- 角色：主模型的 `DIRECT_ALPHA` 辅助目标；hazard/meta 为 `PORTFOLIO_POLICY`。
- 候选：Top-decile/NDCG、quantile/expectile、triple-barrier、趋势存活、time-to-hit、MFE/MAE label。
- 防泄漏：未来路径仅用于 label/weight/evaluation，不能作为决策时输入；未成熟尾部 censored。
- 顺序：先提高 candidate recall，再做 meta-label take/skip/size/continue-hold。
- 退出：若 recall 未提高，meta-label 的 precision 改善不能写成解决右尾漏捕。

## Concept Sector Track / 概念板块路线

- 当前：`DESIGN_READY_NOT_STARTED`。
- PR-A：repo 外 candidate、source audit、PIT/partial/proxy quality、manifest/hash。
- PR-B：六个文件型概念因子与定向测试。
- I1：只有数据和因子通过后才接入 QE；正式 portfolio 仍用分钟 TWAP。
- 概念路线不修改父蓝图 P0 文件、不占 D1R node1 槽、不查询数据库数据面。
- 当前成员静态快照不能回填历史；proxy/partial 必须显式分层。

## Minute Signal Factor Deferral / 分钟信号因子延期

暂缓内容：盘口、微观结构、分钟成交分布、分钟资金流、日内 lead-lag。

重新进入必须同时满足：

1. 不可变历史 1min candidate 和 manifest；
2. W7/W8 或等价 signoff；
3. 股票/交易日/240 bar 覆盖与缺失分类；
4. 停牌、涨跌停、pre-close、factor、费用和交易单位证明；
5. WSL/node1 文件 SHA 一致；
6. QE subprocess DB poison；
7. 明确的经济假设和日频基线。

在此之前，分钟因子为 `DEFERRED_DATA_NOT_READY`。该状态不允许正式 portfolio 回退日频。

## Design Acceptance Index / 设计验收索引

- F-101：当前 task、BUG、数据集和概念实现状态必须绑定 2026-08-24 只读证据。
- F-102：MA-E19 九臂 signal、三臂缺失和旧 portfolio 不可裁决必须分层。
- F-103：WP-D1R 必须是完整 12 arm，不得只补三臂。
- F-104：WP-D2 必须包含四格、hard/soft、不可部署身份和 taxonomy 边界。
- F-105：WP-D3 必须保留 absolute，并报告 active/Brinson 或 `NOT_COMPUTABLE`。
- F-106：结果触发矩阵必须阻止论文方法和平台重写提前晋级。
- F-107：新 Alpha 卡必须包含假设、角色、输入、PIT、公式、快筛、QE、LOO 和退出条件。
- F-108：概念数据/因子必须标记未实现，不能冒充可运行。
- F-109：分钟新信号延期与分钟 TWAP portfolio 合同必须分开。
- F-110：资源、零数据库、进程控制、DDL/DML、依赖和生产边界必须显式。
- F-111：每个工作包必须有稳定结束标识和下一会话可恢复状态。
- F-112：父蓝图与本执行方案必须前后一致且通过至少三轮审核。

## Implementation Plan / 实施方案

### Phase 0：每次执行前实时冻结

1. 查询最新 task 列表，确认没有编号冲突或更新结果。
2. 查询 dataset `status --latest`，记录 submission/state/error，不提交新 intent。
3. 确认 BUG-1133 fix 在运行身份中，或生成明确 runtime blocker。
4. 确认节点资源合同、数据 identity 与 prediction/source assets。

### Phase 1：WP-D1R

按预注册 12-arm 创建新 task；禁止恢复旧 task。运行结束后只在 12/12 与 determinism evidence 完整时生成 D1 结论。

### Phase 2：WP-D2

复用 D1R 冻结数据和 prediction，生成四格 oracle/现实比较。oracle 结果永久不可部署。

### Phase 3：WP-D3

复用相同 holdings 与 TWAP portfolio，计算 absolute/active/Brinson；缺输入时不建补数工程。

### Phase 4：结果触发 P1

只创建触发矩阵命中的最小 canary；每个方向一张 task card，不批量启动所有论文方法。

### Phase 5：新 Alpha 快筛与正式 QE

先完成文件/PIT/公式/方向/相关性快速筛选；只有具备明确经济假设和可复算输入的候选进入 matched QE、三窗口、种子、blend、LOO。

## Verification Plan / 验证方案

### 文档与任务前验证

- 父蓝图、本文、概念 F2 的状态和优先级一致。
- 所有 task id、loop 数、时间、BUG/PR、dataset submission 与 error code 逐项回读。
- signal、portfolio、execution、source merge、runtime、candidate signoff 分开。

### WP-D1R 验证

- 12/12 arm 与矩阵一一对应；无缺失/重复。
- config 固定项和 data/prediction/order identity 一致。
- 相同输入重放结果一致；tie-break version 可回读。
- 2026H1 fixed/expanding/rolling 均有真实结果。
- 无 DB 数据面、无日频降级、无 V25 模型。

### WP-D2/D3 验证

- oracle 永久不可部署；reality/oracle 身份不可混淆。
- active/Brinson 与 absolute 使用相同 holdings/period。
- taxonomy、benchmark、逐日权重和等权池 identity 可追溯。
- 缺输入为 `NOT_COMPUTABLE`，不回填。

### 新 Alpha 验证

- PIT 截断前后历史值不变；缺失不填零。
- 方向在测试前预注册；规模、波动、换手和现有腿相关性分离。
- 快筛、单腿、matched change、blend、LOO、full/early/late 分层。
- 失败公式保留负结果，不扩大为方向死亡。

### 文档交付验证

- `git diff --check`
- `python scripts/aistock_feature_workflow.py validate --design docs/analysis/sector_rotation_factors_develop_spec_20260710.md --tier F2`
- `python scripts/aistock_feature_workflow.py validate --design docs/analysis/ma_e19_p0_triad_and_alpha_execution_plan_20260824.md --tier F2`
- `python scripts/aistock_feature_workflow.py validate --design docs/architecture/qe_concept_sector_data_factor_parallel_f2_design_20260813.md --tier F2`
- `DESIGN-COMPLIANCE-001` 四项逐条审核
- 至少事实、方法、跨文档三轮审核；有阻断则修订后重跑。

## Review Record / 审核记录

1. **Round 1 — 事实与数值**：逐 task 回读 MA-E19A/B 12 个 arm，确认 7/9、2/3、合计 9/12 与无活动 loop；逐臂核对 IC/ICIR/RankIC/RankICIR、历史 absolute portfolio 观察及实际 `config_json` 的 train/valid/test/refit 窗口；复算 expanding/fixed 与 rolling/fixed 的均值差。修订了旧“尚未运行”状态、执行总账和 D1R 窗口歧义。
2. **Round 2 — 方法与因果边界**：逐项检查 signal/portfolio/execution、staleness/regime、absolute/active、sector/within-sector 分离；确认 oracle 永久不可部署、meta-label 不得生成召回、P1-F 只能由 D2+D3 结果触发，论文只提供机制先验。修订了 D1 部分完成与完整 D1R 的状态表达。
3. **Round 3 — 跨文档一致性**：对父蓝图、本文和概念板块 F2 设计交叉核对当前任务、优先级、资源、TWAP、零数据库和生产边界；修订概念设计中的 MA-E16 旧当前态引用，并纠正新 Alpha 研发卡数量为六类。历史快照保留但不再充当当前权威。
4. **Round 4 — 主线漂移复核**：审核期间 `origin/main` 合入 BUG-1157 source PR #3706；重新回读最新 durable dataset status、BUG registry/Issue 和 source diff，确认旧 submission 仍阻断、Issue open 且 runtime/repair/signoff 未完成。父蓝图和本文据此把 source merge 与数据就绪拆开，未改变 D1R 使用既有 v1 文件的边界。
5. **统一门禁重跑**：三个 F2 validator、`git diff --check`、changed-file scope 与 `DESIGN-COMPLIANCE-001` 在所有修订完成后统一执行；receipt 必须绑定最终分支 HEAD。

## DESIGN-COMPLIANCE-001 Review / 设计符合性审核

1. **禁止简化交付**：D1R 保留完整 12 arm，D2 保留四格/hard-soft/taxonomy，D3 保留 absolute/active/Brinson；六类 Alpha 卡均包含输入、PIT、角色、验证与退出条件，不以三臂补跑、单公式或论文复现代替完整工作包。
2. **禁止静默错误**：MA-E19 三臂失败、BUG-1133 后历史 portfolio 不可裁决、PIT v2 source-audit 阻断、概念实现未开始和分钟新信号延期均显式保留；缺文件、identity、benchmark、taxonomy、分钟成交或成熟标签时 `INCOMPLETE/NOT_COMPUTABLE`，不补零、不回退数据库或日频。
3. **禁止改变业务逻辑**：裸 h20、CE3、seed 123、21 日 purge、Top50/n_drop1、费用、分钟 TWAP、文件数据面与资源上限保持不变；oracle、benchmark、tail/meta、gate、概念和新 Alpha 均另立身份，不覆盖旧 task 或历史结果。
4. **禁止私增门禁审批**：D1R→D2→D3 是信息依赖与科研顺序，不是人工审批；结果触发只决定下一个最小 canary，不创建平台、UI、Archive、历史补账或额外 production gate，也不产生合入、进程控制、DDL/DML、依赖或数据激活授权。

## Design Acceptance Matrix / 设计验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-101 | Background、父蓝图 2.5/17 | validation-receipt: `Invoke-RestMethod /api/v1/quantevolver/evolution/tasks`、dataset `status/events`、`20260818_BUG-1133-*.json`、Git tree | VERIFIED | 无 |
| F-102 | Historical Synthesis、父蓝图 2.5.3 | validation-receipt: MA-E19A/B `detail=full` 的 12 行 loop 状态/指标；BUG-1133 registry 与 fix commit | VERIFIED | 无 |
| F-103 | WP-D1R | validation-receipt: MA-E19A/B 实际 `config_json` 回读的 12 组 train/valid/test/refit 窗口、固定项、输出、停止条件和 end marker | DESIGN_READY | 无 |
| F-104 | WP-D2 | validation-receipt: 本文 four-cell、hard/soft、oracle identity、输出与结果触发 | DESIGN_READY | 无 |
| F-105 | WP-D3 | validation-receipt: 本文 absolute/active/Brinson 输入、输出、`NOT_COMPUTABLE` 和结果触发 | DESIGN_READY | 无 |
| F-106 | Result Trigger Matrix | validation-receipt: 本文 10 行观测→最小工作包映射及禁止跳跃 | VERIFIED | 无 |
| F-107 | A-01～A-06 | validation-receipt: 本文六张研发卡均含假设、角色、输入、PIT、公式/设计、快筛、QE 与退出条件 | VERIFIED | 无 |
| F-108 | Concept Sector Track | validation-receipt: `docs/architecture/qe_concept_sector_data_factor_parallel_f2_design_20260813.md` 与五个预期实现路径存在性复核 | DESIGN_READY | 无 |
| F-109 | Minute Signal Deferral | validation-receipt: 本文七项重新进入条件与独立 TWAP execution contract | VERIFIED | 无 |
| F-110 | Contracts、Production Gates | validation-receipt: 本文 zero DB/process/DDL/dependency/activation/no-platform 明细表 | VERIFIED | 无 |
| F-111 | WP-D1R/D2/D3 end markers | validation-receipt: `MA_E19R_END_STATUS`、`P0_D2_END_STATUS`、`P0_D3_END_STATUS` 的稳定定义 | VERIFIED | 无 |
| F-112 | Verification Plan、Review Record | validation-receipt: 三个 F2 validator、`git diff --check`、changed-file scope 与三轮审核记录；提交并同步主线后在最终 HEAD 复验 | VERIFIED | 无 |

## Rollout / Rollback / 发布与回滚

- 本 changeset 只包含父蓝图 v6.9、本文和概念板块 F2 设计的当前进度引用修订。
- 文档合入不启动实验、不激活数据、不改变 runtime。
- 回滚使用文档 PR revert；不得删除现有 task、prediction、receipt 或 dataset control evidence。
- 后续真实实验各自使用新 task identity；失败保留，不覆盖历史。
- 正式 task 编号、合入、数据 candidate、node distribution、进程控制和生产动作均需按当时工作流单独处理。

## Risks / 风险与失败模式

1. **部分结果过度解读**：9/12 被误写成 D1 完成。控制：固定 `PARTIAL_SIGNAL` 与完整重跑要求。
2. **BUG 修复后效应遗漏**：认为 source/runtime 修复自动修复历史结果。控制：历史 portfolio 永久标记旧证据，生成新 run。
3. **重训与 regime 混淆**：单一窗口恢复被写成唯一因果。控制：四 vintage、fixed/expanding/rolling、D2/D3 互证。
4. **oracle 泄漏**：未来信息上界被写成策略。控制：永久不可部署身份。
5. **beta 冒充 Alpha**：高绝对 CAGR 掩盖基准暴露。控制：D3 active/Brinson。
6. **meta-label 越权**：过滤器被写成召回模型。控制：recall/precision/size/exit 分层。
7. **优化器平台化**：尚无 allocation 证据就扩建。控制：P1-F 必须由 D2+D3 触发。
8. **分钟因子与执行混淆**：数据未就绪被解释为日频回测许可。控制：延期仅限新信号，portfolio 继续 TWAP。
9. **概念状态漂移**：设计被误写成实现。控制：PR-A/PR-B/I1 独立状态。
10. **资源回归**：扩大并行导致主机卡顿。控制：D1R node1 并行度 1，图模型 WSL 串行。
11. **数据门禁绕过**：为运行实验放宽 PIT v2。控制：fail closed，不删行、不回退数据库。
12. **历史工程复活**：审核变成 Archive/UI/补账。控制：只消费现有证据，缺失显式记录。

## Production Gates / 生产门禁与当前动作状态

| 状态项 | 本文状态 |
|---|---|
| source/code change | noop |
| experiment submission | noop |
| dataset build/candidate signoff | noop |
| production activation/symlink | noop |
| node1 distribution | noop |
| backend/WSL/RD process control | false / noop |
| DDL/DML | noop |
| dependency install | noop |
| client install | noop |
| UI/Archive/history backfill | noop |
| runtime activation | not applicable |
| merge | pending user confirmation after PR-ready |

## Long Task End Markers / 长任务结束标识

文档任务完成：

`BLUEPRINT_V69_DOC_END=REVIEWED_F2_PASS_PR_READY`

未来实验阶段结束：

- `MA_E19R_END_STATUS=COMPLETE_12_OF_12_DETERMINISTIC`
- `P0_D2_END_STATUS=FOUR_CELL_COMPUTABLE`
- `P0_D3_END_STATUS=ABSOLUTE_ACTIVE_BRINSON_RECONCILED`
- `P1_TRIGGER_DECISION=RECORDED_WITH_EVIDENCE`

任何标识只在对应证据完整时写入；未满足时必须输出准确 blocker 和恢复入口，不得使用近似完成状态。
