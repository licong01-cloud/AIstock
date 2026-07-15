# MiniQMT `ADAPTIVE_IS_L1` 日内执行设计蓝图

> 权威关系：本文是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](../architecture/simulation_platform_unified_authoritative_blueprint_20260715.md) 的 `ADAPTIVE_IS_L1` 算法域下位蓝图，只拥有算法研究目标和阶段验收；不拥有模拟盘平台执行路径、runtime、准入、durability、生产迁移或整体进度的上位权威。

- 日期：2026-07-10
- 状态：F2 Algorithm Domain Blueprint v1.0；算法设计可进入分阶段详细设计，任何平台运行时实现与激活仍须同时通过唯一上位蓝图和对应阶段设计验收
- 范围：Path S / MiniQMT SIM `event_loop` A 路线，多 alpha top-k 日内换仓执行
- 目标算法代码：`ADAPTIVE_IS_L1`
- 当前控制组：BUG-614 protected marketable-limit（下文称 B0）
- 关联生产 SIM：`pkg_ma_8ec5e389fa2c5e484a1ac7e9`，binding slot `ma_8ec5e389_sim_20260703`
- 已合入的可靠性基线：BUG-599、BUG-600、BUG-604、BUG-614
- 设计等级：F2，跨 `simulation_runtime`、`miniqmt_execution_runtime`、`execution_algos`、TCA/ledger 与后续策略资产
- 工作流分级：T3 design-driven；risk level=P1；phase=Algorithm Domain Blueprint
- 目录授权：用户明确要求更新现有 `docs/analysis/` 文档；本文据此使用 FEATURE-WORKFLOW-001 的任务批准目录例外，并作为算法域下位设计，不建立第二份平台级竞争性蓝图

本文是 `ADAPTIVE_IS_L1` 算法域的下位蓝图。每个实施阶段必须同时映射模拟盘平台唯一上位蓝图和本文稳定的 Design Acceptance Index，生成独立详细设计、实现 PR 和验收矩阵，并同步更新上位蓝图 §15；不得把某一阶段的通过描述为整套能力已经完成。

---

## 0. 执行摘要与设计决策

### 0.1 最终决策

如果不受 V25 遗留资产约束，继续扩展现有 V25 两阶段分钟权重模型不是本场景的首选生产路线。本蓝图选择：

> **组合级净额化 + Arrival-Price / Implementation Shortfall 目标 + 约束型滚动时域控制器 + 五档深度感知 micro executor + 独立 Completion Governor。**

具体决策如下：

1. 新建独立算法语义 `ADAPTIVE_IS_L1`，不改变 `V25_TWO_STAGE`、`SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT` 或 `TWAP_LITE_MINIQMT` 的既有语义。
2. B0 继续作为可靠控制组；第一生产候选 B1 是确定性、五档深度感知、真实成交反馈驱动的 adaptive executor。
3. 约束型 MPC 是 B2 challenger；轻量等待成本/深度存活模型是 B3 增强项。模型只能改变候选动作成本，不能绕过硬风险约束。
4. V25 仅作为 B4 macro schedule proposal 参与统一 micro executor 下的实验，不拥有 broker 下单、完成控制或错误降级权。
5. 当前数据不支持以完整 LOB RL、队列位置模型或逐笔成交生存模型作为主路线。
6. “当日完成”定义为 eligible quantity 达到业务 SLO，所有残量均可归因；不承诺停牌、涨跌停无流动性、资金/可卖量不足或 broker 拒绝场景下无条件 100% 成交。

### 0.2 为什么不是继续修补 V25

当前代码事实是：

- V25 是监督式两阶段计划模型，一次生成 240 个分钟权重，不是每个 tick 在线推理的 LOB RL。
- `scripts/rl_execution/v25_gen_plan_data_fullday.py` 使用全日未来最低/最高价生成 oracle attractiveness；`v25_train_joint_fixed.py` 以 KL loss 拟合分钟分布。
- `backend/execution_algos/v25_core.py` 固定前 30 分钟权重约 88.79%、后 210 分钟约 11.21%。
- `V25TwoStageAlgo.compute_step()` 使用分钟模拟成交状态；它不是以 MiniQMT broker trade callback 为实际成交权威的状态机。
- `backend/services/simulation_runtime/bridges.py` 当前明确拒绝 V25 进入 MiniQMT broker execution。

因此，把 arrival benchmark、真实 fill、五档深度、quote age、active child、cancel/replace、deadline 和尾盘阶段全部加入 V25 后，算法语义已经发生根本变化。保留 V25 名称会模糊研究资产与生产执行契约的边界。

---

## 1. Background / 背景与现状证据

### 1.1 业务场景

- 组合规模约 1000 万元。
- 策略为 top-k 每日固定数量换仓，通常仅交易进出 top-k 的标的，并非全组合逐日重建。
- 单日会同时形成约 25–32 个 parent intents，多 alpha 可能指向相同股票。
- 目标是同日完成可交易换仓，同时控制相对决策价/到达价的执行成本和尾部风险。
- SIM 通过 MiniQMT broker、真实 order/trade callback 和 `qmt_strategy_ledger` 作为订单与成交权威。

“小组合”不等于每笔订单的市场冲击天然为零。后续必须按 `Q/ADV`、`Q/分钟成交量`、`Q/五档累计深度`、spread 和波动率验证订单分布，尤其关注 P95/P99 和压力日。

### 1.2 2026-07-07 至 2026-07-10 的生产 SIM 证据

历史记录中的 `submitted/target` 为 child 提交覆盖率，不等同于最终成交率：

| 日期 | submitted / target | 解释 |
|---|---:|---|
| 2026-07-07 | 0 / 25 | 无 child 覆盖 |
| 2026-07-08 | 11 / 25 | 部分依赖人工干预 |
| 2026-07-09 | 5 / 31 | 大量 parent 未形成可成交 child |
| 2026-07-10 | 2 / 32 | pending 算法未被持续有效推进 |

这些数据足以证明当时存在 liveness/child creation 缺陷，但不能单独证明成交成本趋势，也不能用于判定 V25、TWAP 或 adaptive policy 的优劣。后续评价必须使用真实 filled quantity、arrival IS 和 residual opportunity cost。

### 1.3 已修复的可靠性问题与仍需验证的边界

BUG-614 已合入以下基线：

- `SUBMITTING + pending + failed=0` 保持进行中状态，避免误落 `FAILED_RETRYABLE`。
- scheduler 可继续驱动 pending event-loop 算法。
- B0 使用对手价基础上的 marketable-limit、逐 tick 重定价、保护带、tick size/涨跌停约束和 14:55 后更激进的限价。
- 收盘残量使用 loud reason code。

必须纠正旧分析中的两个表述：

1. BUY=`ask1`、SELL=`bid1` 是对手方最优价，不是己方最优价。旧机制的主要问题是快照陈旧、一档容量不足和缺少持续撤改单，而不是买卖方向取错盘口。
2. BUG-614 尾盘仍是 protected limit reprice，不是无条件市价单；它不会在所有市场状态下保证成交。

BUG-614 合入后的生产完成率、实际配置值、quote age、成交价和尾部成本不在本文档 PR 中验证，应由 Phase 0 TCA 基线给出权威结果。

### 1.4 MiniQMT 与 vn.py-style 的实际能力

- `xtdata.get_full_tick`/`subscribe_whole_quote` 可提供最新 tick、累计成交量/金额、证券状态及五档委买卖价量；当前用户环境约每 3 秒更新一次。
- 数据不包含真实队列位置、逐笔委托身份、完整订单事件流或 agent 行为反事实。
- 当前 `VnpyTick` DTO 只显式暴露一档价量，原始五档数组保留在 `raw`。
- 当前 AIstock 已接入的 vn.py 衍生 core 只有 Sniper、BestLimit、TWAP-lite：
  - Sniper：对手一档触价、按一档量提交、活动单撤销后重报；
  - BestLimit：BUY 挂 bid1、SELL 挂 ask1，属于被动报价；
  - TWAP-lite：固定时间/间隔拆分。
- 当前 `get_full_tick` 代码默认 freshness 上限为 300 秒，除非环境配置覆盖；执行级策略不能依赖该宽松默认值。

---

## 2. Scope / 范围

本蓝图覆盖：

1. 多 alpha 目标在 broker 前的 symbol 级净额化与 parent authority。
2. 统一 Parent Execution Intent、Five-Level Quote、Execution State、Child Action 和 Completion State 契约。
3. 确定性 B1 adaptive executor。
4. 约束型 B2 receding-horizon/MPC controller。
5. 可选 B3 轻量预测器及其安全边界。
6. V25、TWAP 作为 macro schedule 候选；B0 作为版本化且每个 revision 不可原地改写的 composite control，在统一实验框架中保持独立身份。
7. MiniQMT quote ingress、broker order/trade callback、OMS/ledger 幂等与恢复。
8. Arrival/decision benchmark、TCA、decision dry-run/action logging、SIM champion-challenger 和晋级规则；这里的 dry-run 不是已退役 compiler/shadow B 路线。
9. 连续竞价、收盘集合竞价、停牌、涨跌停和 broker 异常状态。
10. SIM-only 分阶段发布、显式回滚和生产门禁。

## 3. Non-Goals / 非目标与边界

本蓝图不包含：

- 本文档 PR 中的运行时代码、数据库迁移、模型资产训练或策略激活。
- 直接修改已入库 V25 模型权重、StrategyPackage、QE workspace 或历史实验资产。
- 在当前五档 L1 条件下实现完整 LOB RL、真实队列位置建模或逐笔委托生存模型。
- 使用历史 snapshot 伪造被动单的反事实成交。
- 回退到已退役的 compiler/shadow B 路线。
- 改变 LIVE 硬锁；在完整设计、SIM 证据和单独授权前，`event_loop` 继续 SIM-only。
- 用单一固定参数承诺适配所有股票、板块、价位和市场状态。
- 把 VWAP/TWAP 从研究基线中删除；它们不是默认生产策略，但仍是必要 benchmark/reference schedule。

---

## 4. 优化目标与评价口径

### 4.1 权威 benchmark

每个 parent 必须在 preflight 完成时冻结：

- `decision_price`：策略决定换仓时的可审计基准；
- `arrival_mid`：parent 到达执行系统时的 fresh midquote；
- `decision_time`、`arrival_time`、`deadline`；
- side、target quantity、eligible quantity；
- `eligibility_as_of`、`eligibility_rule_version` 和 eligibility evidence；
- `hard_cost_limit_bps`、hard-cost benchmark 类型/价格、预计强制费用与配置版本。

eligible denominator 冻结后不得因随后未成交、hard limit 触发、broker reject 或行情变化而缩小。后续事件只能被分类为“preflight 原始不可交易”“执行期外生阻塞”“策略/运行失败”，不能改写历史 denominator。不得使用事后全日最低买价或最高卖价作为可实现的生产目标。日内最好价、VWAP、close 只能作为诊断 benchmark。

### 4.2 Signed Implementation Shortfall

令 (s=+1) 表示 BUY，(s=-1) 表示 SELL：

\[
IS_{filled,bps}=s\times\frac{P_{fill}-P_{arrival}}{P_{arrival}}\times 10^4
\]

对 parent 的多次 fill 使用数量加权成交价，分别计算 decision IS 和 arrival IS。parent 总成本必须加入：

- 显性费用与税费；
- delay cost；
- deadline 时未成交残量按权威 mark 计算的 opportunity cost；mark 优先使用 deadline 时 fresh mid，缺失时仅可使用 age 不超过 `deadline_mark_max_age_ms` 的最后 fresh mid，并 loud 记录 benchmark quality，超龄或不存在则 TCA invalid/fail loud；
- cancel/reject/rate-limit 的运行成本与尾部影响。

令 frozen eligible quantity 为 (Q_e)，fills 为 ((q_i,p_i))，deadline residual 为 (R=Q_e-\sum_iq_i)，权威 mark 为 (p_m)，benchmark 为 (b)：

\[
IS_{parent,bps}(b)=
s\times
\frac{\sum_i q_i p_i + R p_m - Q_e b}{Q_e b}
\times 10^4
+\frac{fees}{Q_e b}\times 10^4
\]

归因先统一在人民币口径计算，不能把使用不同分母的 bps 项直接相加。令 decision price 为 (p_d)，arrival mid 为 (p_a)：

\[
C_{delay}=sQ_e(p_a-p_d)
\]

\[
C_{execution}=s\sum_i q_i(p_i-p_a)
\]

\[
C_{opportunity}=sR(p_m-p_a)
\]

则 decision 总成本为 (C_{delay}+C_{execution}+C_{opportunity}+fees)，统一除以 (Q_e p_d) 转成 decision IS bps；arrival 总成本为 (C_{execution}+C_{opportunity}+fees)，统一除以 (Q_e p_a) 转成 arrival IS bps。cancel/reject/rate-limit 只在实际产生费用时进入 `fees`，否则作为独立运行质量指标，不能重复计入 IS。人民币分项加总及统一分母后的 bps 必须分别与 parent total 在预注册容差内一致。

### 4.3 控制目标

控制器求解：

\[
\min E[IS]
+\lambda_1 CVaR_{95/99}
+\lambda_2 D_t^2
+M R_T^2
+\eta C_{cancel/reject}
\]

其中：

- (D_t)：实际累计成交相对 reference completion tube 的落后量；
- (R_T)：deadline terminal residual；
- (M)：must-complete 高惩罚；
- 价格笼子、涨跌停、parent hard limit、资金、可卖量、board lot 和 quote freshness 是硬约束，不能被目标函数权重覆盖。

### 4.4 完成率定义

`eligible_quantity` 仅包含已通过 preflight 且在当时市场规则下可执行的数量。完成率按股数和名义金额分别统计：

\[
completion=filled\_eligible\_quantity / eligible\_quantity
\]

停牌、涨跌停无对手量、T+1 可卖量不足、资金不足、broker 拒绝等残量仍需记录，但必须从“策略可控制失败”和“业务不可交易状态”两个维度分别归因。broker reject 默认属于 eligible-but-unfilled；只有带权威交易规则证据的外生拒绝才可归为执行期外生阻塞，且仍不改变 denominator。

### 4.5 must-complete 与 hard cost limit 优先级

硬价格/成本约束始终高于 Completion Governor。`hard_cost_benchmark_type` 只能选择冻结的 `DECISION_PRICE` 或 `ARRIVAL_MID`，对应正数 benchmark (p_b)。预估强制费用/税费先从 `hard_cost_limit_bps` 扣除，得到非负 `hard_price_budget_bps`；预算小于等于零时禁止启动 parent。价格边界按 side 对称推导：

\[
BUY\_MAX=p_b(1+hard\_price\_budget\_bps/10^4)
\]

\[
SELL\_MIN=p_b(1-hard\_price\_budget\_bps/10^4)
\]

BUY cap 向下取到合法 tick，SELL floor 向上取到合法 tick，再与涨跌停、价格笼子和更严格的 micro guard 取交集。没有合法 marketable price 时进入 `HARD_LIMIT_BLOCKED`，不能通过 round 或 Completion Governor 越界。达到 parent hard cost limit 后，唯一允许的状态转换是：

1. 取消仍可撤的冲突 active child 并 reconcile；
2. parent 进入 `HARD_LIMIT_BLOCKED`，停止新的主动穿价；
3. 通过 loud event 请求拥有权限的上层显式提高 cap、终止 parent 或接受 terminal residual；
4. 未获得新 revision 前，Completion Governor 不得突破原 cap。

提高 cap 必须生成新的 `parent_revision`、授权主体、时间、原因和 policy evidence；不得由算法自行决定。

---

## 5. Architecture / 目标架构

```text
Multi-alpha target weights
          │
          ▼
Portfolio Parent Coordinator
  netting / cash / T+1 / priority / rate budget
          │
          ▼
Parent Execution Intent
  arrival mid / qty / deadline / urgency / hard limits
          │
          ▼
Reference Schedule + Completion Tube
  immediate / short horizon / TWAP-volume / V25 proposal
          │
          ▼
Adaptive Urgency Controller
  B1 deterministic rules → B2 constrained MPC → B3 learned costs
          │
          ▼
Five-Level Micro Executor
  WAIT / PASSIVE / L1..L5 / IOC / terminal protected limit
          │
          ▼
MiniQMT Runtime → OMS/Gateway → Broker
          ▲                         │
          └──── order/trade callbacks ┘
          │
          ▼
Ledger + TCA + Champion/Challenger Evidence
```

### 5.1 所有权边界

| 组件 | 权威职责 | 明确禁止 |
|---|---|---|
| Portfolio Parent Coordinator | 多 alpha 净额化、现金/T+1 依赖、parent 唯一性、并发预算 | 直接调用 broker |
| Reference Schedule | 给出累计目标和允许偏离区间 | 宣称数量已经成交 |
| Adaptive Urgency Controller | 基于真实 residual 和时间选择动作/child 目标 | 绕过硬风险约束 |
| Micro Executor Core | quote 校验、深度/价格计算、动作状态机 | 访问 FastAPI/DB 或静默换算法 |
| MiniQMT Adapter/Runtime | DTO 转换、事件持久化、OMS/Gateway 编排 | 改变 core 决策语义 |
| Broker/ledger | accepted/order/trade/position/cash 权威 | 使用本地模拟覆盖真实回报 |
| Completion Governor | deadline 与交易阶段强制边界 | 承诺业务不可交易状态下必成 |
| TCA | benchmark、成本拆解、实验归因 | 使用 submitted 代替 filled |

### 5.2 Core/Adapter 原则

- 新算法 core 放在 `backend/execution_algos/`，不得直接依赖 FastAPI、PostgreSQL、QMT client 或 UI 对象。
- 新建独立 `AdaptiveExecutionCore` 协议，至少提供 `on_market_data(FiveLevelQuote | ClosingAuctionSnapshot)`、`on_clock(ExecutionClockEvent)`、`on_order(...)`、`on_trade(...)`、`snapshot()` 和 `restore()`。它不直接继承只接受一档 `VnpyTick` 的旧 `VnpyAlgoTemplate`。
- 通过 composition 复用 vn.py-style action DTO、order/trade helper 和 OMS adapter；旧 Sniper、BestLimit、TWAP-lite 的输入和行为保持不变。
- 新 core 使用带 `state_version` 的显式 serializer；不得依赖旧通用 variables dump 推断复杂 ExecutionState。
- MiniQMT adapter 负责把 broker quote/order/trade 转换为标准 DTO，并保持与 core 一致的状态恢复语义。
- QE/Paper 历史 replay 与 MiniQMT 实时执行是不同 capability。没有真实反事实撮合时，历史 replay 只能验证计划、约束和状态机，不能证明被动单成交质量。
- 所有 capability、配置和数据要求必须显式声明；不支持的运行模式必须 loud 拒绝。

---

## 6. Contracts / 契约

### 6.1 `ParentExecutionIntent`

必须包含：

```text
parent_id / plan_id / binding_id / package_id / portfolio_id
symbol / side
target_quantity / eligible_quantity / remaining_quantity
decision_time / decision_price
arrival_time / arrival_mid
eligibility_as_of / eligibility_rule_version / eligibility_evidence
start_time / deadline
urgency_class / alpha_half_life_seconds
hard_cost_limit_bps / hard_cost_benchmark_type / hard_cost_benchmark_price
estimated_mandatory_fee_bps / hard_price_budget_bps
max_participation_rate
policy_id / policy_version / policy_sha256
market / board / lot_size / price_tick
cash_dependency_ids / sell_availability_evidence
parent_scope_id / parent_revision / supersedes_parent_revision
reserved_cash / reserved_sell_quantity
```

唯一 authority 键为 `(parent_scope_id, binding_id, target_trade_date, symbol)`，其中 `parent_scope_id` 使用 account group，缺失时使用 portfolio。`side` 是净额计算结果，不属于唯一键。反向目标先净额化，净额为零时不创建 broker parent，但必须持久化 lineage 和 no-order evidence。

target weight 到 quantity 的转换必须冻结价格来源、lot rounding 和余量处理。日内 target revision 必须通过单调 `parent_revision` 和 compare-and-swap 生效；旧 revision 停止释放新 child，但已存在 child 仍须 reconcile。部分 fill 的 alpha lineage 按预声明的净额贡献规则分配。现金和可卖量在 child working 期间 reservation，成交后结算，撤单/拒绝的未成交部分释放；依赖买单仅按已确认卖出成交释放现金预算。

### 6.2 `FiveLevelQuote`

必须显式字段化：

```text
symbol
schema_version / quote_id / snapshot_hash
exchange_time / received_at / quote_age_ms / source
last_price / prev_close / limit_up / limit_down / stock_status
bid_price[5] / bid_volume[5]
ask_price[5] / ask_volume[5]
cumulative_volume / cumulative_amount
trading_phase / is_duplicate_snapshot
price_unit / volume_unit / volume_multiplier
```

契约要求：

- `quote_id` 由 symbol、exchange time、五档、累计量和 schema version 生成稳定 hash；action 必须引用该 id。
- 有效档位是连续前缀：bid 价格降序、ask 价格升序。未使用尾档统一编码为 `(None, None)`；`(positive_price, 0)` 表示合法 zero depth，不能与空档混淆。
- `volume_unit` 必须显式为股/张，adapter 根据 xtquant instrument metadata 提供转换，不允许 core 猜测“手/股”倍率。
- 价格数组单调、价量非负、时间戳可解析、source 必须为 broker quote。
- zero depth 是可解释的市场无流动性状态，不与字段缺失混为一类。
- quote age 超过 policy 上限、时间戳回退、重复 snapshot 或跨交易日数据不得触发新 child。
- quote age 上限必须由 execution policy 提供；不能依赖 QMT client 的 300 秒默认值。

#### 6.2.1 `ClosingAuctionSnapshot`

14:57–15:00 不复用连续竞价五档语义。统一 market-data union 为 `FiveLevelQuote | ClosingAuctionSnapshot`；后者至少包含：

```text
symbol / schema_version / auction_quote_id / snapshot_hash
exchange_time / received_at / quote_age_ms / source
trading_phase / cancel_allowed
last_price / prev_close / limit_up / limit_down / stock_status
indicative_price(optional) / matched_quantity(optional)
unmatched_side(optional) / unmatched_quantity(optional)
price_source / capability_flags
price_unit / volume_unit / volume_multiplier
```

- `ExecutionClockEvent` 是竞价阶段权威；14:57 后 `cancel_allowed=false`，不能由迟到 quote 改回连续竞价。
- indicative/matched/unmatched 字段只有在 xtquant、券商柜台和市场 capability probe 证明存在且语义稳定时才可使用；不支持时编码为 `None + capability=false`，不得用零或连续竞价五档伪造。
- `price_source` 必须是预声明、可审计且满足 auction freshness 的来源。缺少合法 protected-limit 参考价或必要 capability 时，拒绝新的集合竞价 child，保留 residual 并 loud；不能以“尾盘必成”为由猜价。
- auction action 必须引用 `auction_quote_id`；同一 snapshot 不得重复释放数量。是否允许 native order type、是否可撤和 reject 语义均由 adapter capability 决定。

### 6.3 `ExecutionClockEvent`

deadline、cancel timeout、交易阶段和收盘终止使用独立单调时钟推进，不能依赖 fresh quote 到达：

```text
clock_event_id / monotonic_time / wall_time
trade_date / calendar_version / trading_phase
source / clock_skew_ms / calendar_hash
```

- stale/duplicate quote 禁止新的 price-sensitive submit，但不能阻止 cancel、reconcile、deadline 和 market-phase 收敛。
- `WAIT_ONE_TICK` 同时携带绝对 `valid_until`；被动 timeout 同时受 fresh-tick 数和 wall-clock 秒数约束。
- clock skew 或交易日历失效时冻结新 submit，允许风险收敛动作并 loud。

### 6.4 `ExecutionState`

必须持久化：

```text
parent_id / algo_instance_id / state_version
eligible_target_quantity
filled_quantity / working_quantity / unallocated_quantity
terminal_residual_quantity / remaining_quantity
active_child_ids / cancel_pending_child_ids
last_quote_time / last_clock_time / last_decision_time / last_action
reference_schedule_id / cumulative_target / schedule_deficit
urgency_state / completion_state
submit_count / cancel_count / replace_count / reject_count
last_broker_order_status / last_trade_id
config_sha256 / model_sha256(optional)
parent_revision / state_revision / reconcile_state
```

数量当前态不变量为：

```text
eligible_target_quantity
= filled_quantity
+ working_quantity
+ unallocated_quantity
+ terminal_residual_quantity
```

- `working_quantity` 包含已发出但未终结的 broker leaves；本地 action 仅生成 durable intent，不能增加 filled。
- cancel/reject 的未成交 leaves 在权威 callback/reconcile 后回到 `unallocated_quantity`；cancel/reject 数量只作为历史事件统计，不进入当前态守恒式。
- 每个 child 必须记录 `requested / filled / working_leaves / unfilled_returned`，并满足 child 级守恒。
- 只有 broker trade callback 可以增加 `filled_quantity`。SUBMIT、ACK、ORDER_REPORTED 或本地 action 不得当作成交。
- late fill、重复或乱序 callback 使用 trade id、broker order id 和 state revision 幂等处理；任何 overfill 进入 global fail-closed、冻结新动作并要求人工 reconcile。
- `remaining_quantity` 是 `eligible_target_quantity - filled_quantity` 的派生缓存，不是可独立改写的第五份数量权威；pre-terminal 时 `terminal_residual_quantity=0`，terminal 只能在 broker reconcile 后把未完成量从 working/unallocated 收敛到 terminal residual。
- child 级守恒必须满足 `requested = filled + working_leaves + unfilled_returned`；late fill 发生时按相同 trade id 幂等地从 working 或已返回 unallocated 中回拨，不能重复释放数量。

### 6.5 `ExecutionAction`

允许的低维动作：

```text
WAIT_ONE_TICK
PASSIVE_BBO
MARKETABLE_L1
WALK_TO_L2
WALK_TO_L3
WALK_TO_L4
WALK_TO_L5
NATIVE_FIVE_LEVEL_IOC
CANCEL_ACTIVE
TERMINAL_PROTECTED_LIMIT
NO_ACTION_MARKET_STATE
```

每个动作必须输出：

```text
action_id / parent_id / market_data_id
quote_id(optional, continuous) / auction_quote_id(optional, closing)
child_quantity / limit_price / price_type
valid_until / reason_code / stage
hard_guard_evidence / expected_cost_components
```

`NATIVE_FIVE_LEVEL_IOC` 只有在 xtquant 版本、券商柜台、市场时段和 SIM 能力探测均通过时才可启用；否则配置校验必须拒绝该 action，而不是静默替换。

### 6.6 深度感知定价

令 (h\in(0,1]) 为展示深度 haircut，(q_c) 为本轮 child 数量：

\[
k^*=\min\{k:h\times cumDepth_k\ge q_c\}
\]

- BUY candidate 使用 `ask[k*]`；SELL candidate 使用 `bid[k*]`。
- 若五档不足，只能在 hard cost limit 内提交可覆盖部分或等待下一 fresh quote。
- quote age、最近短时波动和 snapshot-to-ack 延迟用于计算 latency buffer；固定 `cross_ticks` 仅可作为明确配置的保守兜底参数，不能冒充实际穿档数。
- 最终价格依次受交易所规则、涨跌停、parent hard limit 和 micro guard 约束。

### 6.7 配置契约

配置至少包含：

```text
policy_version
quote_age_limit_ms
auction_quote_age_limit_ms / deadline_mark_max_age_ms
depth_haircut
max_child_quantity / max_participation_rate
allowed_actions
passive_enabled / passive_max_fresh_ticks / passive_max_attempts
passive_max_seconds
latency_buffer_mode / volatility_window_seconds
reference_schedule_mode / completion_tube
behind_schedule_thresholds
continuous_final_start
continuous_cancel_cutoff / cancel_latency_p99_buffer_ms
closing_auction_start / market_close
hard_cost_limit_bps
hard_cost_benchmark_type
fee_schedule_version
native_ioc_capability_required
wait_cost_model_ref(optional)
depth_survival_model_ref(optional)
```

所有参数必须严格校验、持久化 snapshot 并计算 hash。缺失必需参数、非法类型、未知 action、模型 hash 不匹配或 market capability 不满足时 fail loud。

### 6.8 reason code 与 stage 契约

至少定义以下家族：

- `ADAPTIVE_IS_QUOTE_*`：缺失、陈旧、时间回退、重复、深度无效；
- `ADAPTIVE_IS_AUCTION_*`：集合竞价 capability、参考价、不可撤语义和 snapshot freshness；
- `ADAPTIVE_IS_CLOCK_*`：clock skew、calendar hash、交易阶段和 deadline；
- `ADAPTIVE_IS_QUEUE_*`：overflow、coalesce、watermark 和 event priority；
- `ADAPTIVE_IS_MARKET_STATE_*`：停牌、临停、涨停买阻塞、跌停卖阻塞、无对手量；
- `ADAPTIVE_IS_PARENT_*`：净额冲突、资金依赖、可卖量、hard limit；
- `ADAPTIVE_IS_ACTION_*`：动作选择、保护触发、能力不支持；
- `ADAPTIVE_IS_BROKER_*`：submit、ack、reject、cancel、trade reconcile；
- `ADAPTIVE_IS_COMPLETION_*`：落后进度、连续竞价终局、集合竞价、收盘残量；
- `ADAPTIVE_IS_TCA_*`：benchmark 缺失、归因不完整、实验分组错误；
- `ADAPTIVE_IS_KILL_*`：parent/binding/global freeze、reconcile 和恢复授权。

任何 terminal residual 必须同时有 `reason_code + stage + parent_id + remaining_quantity + last_broker_state`。

### 6.9 幂等、并发与恢复

- durable child intent 必须先以 client request id、parent revision 和 state CAS 持久化，再调用 broker；若 broker side effect 成功但本地结果持久化失败，重启后通过 client request id、order remark、broker query 和 ledger 进入 `RECONCILE_REQUIRED`，不得重复下单。
- `action_id`、`client_request_id`、`child_order_id`、broker order id 和 trade id 必须支持幂等 reconcile。
- cancel pending 时不得提交冲突 replacement，除非 adapter 证明柜台支持安全 replace 语义。
- duplicate quote 不得重复释放数量。
- 服务重启后从 persisted state、ledger 和 broker query 三方 reconcile；broker 为最终权威。
- 同一 parent 使用单写者状态机；行情 callback 只写入有界队列，不直接调用 broker。order/trade callback 使用不可丢失的高优先级通道，高于可 coalesce 的 quote。
- order/trade 高优先级事件必须先进入带 event/broker id 的 durable inbox/WAL，再由单写者消费；clock phase/deadline transition 必须持久化 high-watermark，并由 scheduler 重发直到应用。inbox/WAL 写失败、满载或无法确认时立即冻结受影响 binding/global 的新 submit、loud 报警，并从 broker query + ledger 做持久化 reconcile，不能继续以内存队列运行。
- quote 队列 overflow 时只允许按 symbol 合并旧 quote 为最新 quote，并增加 loud drop/coalesce metric；order/trade/clock 的权威状态转换不得丢弃。恢复只有在 durable inbox 清空、broker reconcile 和 clock/calendar readiness 全部通过后才可授权。
- DB 逐行/增量写和 BUG-600 watchdog 语义保持不变。

### 6.10 Parent/Child 状态转换

Parent 状态：

```text
CREATED → PREFLIGHTED | PREFLIGHT_REJECTED | PREFLIGHT_FAILED
PREFLIGHTED → ACTIVE
ACTIVE → FILLED | HARD_LIMIT_BLOCKED | RECONCILE_REQUIRED | COMPLETION_FINAL
HARD_LIMIT_BLOCKED → ACTIVE(authorized revision) | RECONCILE_REQUIRED | TERMINAL_RESIDUAL | FROZEN
RECONCILE_REQUIRED → ACTIVE | FILLED | COMPLETION_FINAL | TERMINAL_RESIDUAL | FROZEN
COMPLETION_FINAL → CLOSING_AUCTION | FILLED | TERMINAL_RESIDUAL | RECONCILE_REQUIRED
CLOSING_AUCTION → FILLED | TERMINAL_RESIDUAL | RECONCILE_REQUIRED
任一非终态 → FROZEN（kill）
FROZEN → RECONCILE_REQUIRED（authorized resume）
FILLED | TERMINAL_RESIDUAL → RECONCILE_REQUIRED（late fill/status correction）
```

Child 状态：

```text
INTENT_DURABLE → SUBMITTING → WORKING | REJECTED | UNKNOWN
WORKING → PARTIALLY_FILLED | FILLED | CANCEL_PENDING | UNKNOWN
PARTIALLY_FILLED → FILLED | CANCEL_PENDING | UNKNOWN
CANCEL_PENDING → CANCELED | PARTIALLY_FILLED_CANCEL_PENDING | FILLED | UNKNOWN
PARTIALLY_FILLED_CANCEL_PENDING → CANCELED | FILLED | UNKNOWN
UNKNOWN → broker query/reconcile → 任一权威状态
```

约束：

- `INTENT_DURABLE` 必须先于 broker side effect；submit 结果未知时只能进入 `UNKNOWN/RECONCILE_REQUIRED`。
- preflight 业务拒绝进入 `PREFLIGHT_REJECTED`，基础设施/校验故障进入 `PREFLIGHT_FAILED`，两者均须 loud evidence 且不得创建 broker child。
- 任一非终态出现 broker uncertainty、未知 active child 或未决 cancel 时必须进入 `RECONCILE_REQUIRED`；不能仅靠 wall-clock 推进为 terminal。
- parent/child 状态更新使用 `state_revision` CAS，乱序回调不可覆盖更高权威状态。
- `cancel_pending` 语义不能被 partial fill 清除；只有 broker 确认 canceled、filled 或 reconcile 出其他权威状态后才结束。
- late fill 即使发生在 `CANCELED` 回报之后也必须按 trade id 入账，减少 working/unallocated，并重新评估 parent terminal 状态。
- terminal parent 收到 late fill 时重新打开 reconcile，不得忽略；若造成 overfill，触发 global kill 和人工处置，不自动发反向单“修复”。
- terminal residual 只能从已冻结 denominator 和 broker reconcile 结果产生。只有 deadline/market close、authorized terminate 或 authorized accept-residual 触发，且 `working=0`、`cancel_pending=0`、`unknown=0` 后，才可在单次 state CAS 中把全部 `unallocated_quantity` 移入 `terminal_residual_quantity`；有未决 broker 状态时 parent 必须保持 `RECONCILE_REQUIRED`，不得提前终态化。

### 6.11 Persistence、Event、API 与 Backward Compatibility

权威逻辑记录：

| 逻辑记录 | 主键/幂等键 | 最小内容 |
|---|---|---|
| execution parent | `parent_id`, authority key, revision | intent、eligibility、benchmark、reservation、policy、state quantities |
| child intent/order | `client_request_id`, `child_order_id`, broker id | action、price/qty、状态、working/fill/returned quantities |
| market-data evidence | `market_data_id`（`quote_id` 或 `auction_quote_id`） | 五档/集合竞价 hash、时间、age、source、capability、guard evidence |
| execution event | `event_id` | parent/child、stage、reason、before/after revision、broker context |
| TCA parent result | `parent_id + tca_version` | decision/arrival IS、费用、delay、opportunity、markout、coverage |

Phase 0–3 优先扩展现有 `qmt_strategy_ledger` 和 execution runtime repository；是否新增表由阶段详细设计依据现有 schema 决定，但必须遵守：

- migration 只做向后兼容的 additive 变化，旧 B0 reader/writer 继续工作；
- 新字段无证据时保持 `NULL/unknown` 并 loud，不用默认值伪造完整数据；
- 双写过渡必须有 source-of-truth 和一致性校验，不能长期保留双权威；
- rollback 保留新 evidence，不执行破坏性回填或删除；
- 所有写入逐行/增量并服从现有 watchdog、事务和重试边界。

事件 envelope：

```text
schema_version / event_id / event_type
parent_id / parent_revision / child_id(optional)
occurred_at / received_at / source
stage / reason_code
state_revision_before / state_revision_after
payload / evidence_hash
```

截至 Phase 3 必须完整提供以下 service operations；REST/router 只是 adapter，不能改变语义。阶段所有权不可前移：Phase 0A 只交付 read-only parent/TCA/evidence 查询，Phase 2 只增加不可激活组件的诊断与 kill-state 查询，Phase 3 才交付 policy、parent control 与 kill-switch mutation。

```text
# Phase 0A read-only
GetExecutionParent(parent_id)
ListExecutionParents(binding_id, trade_date, terminal_state)
GetExecutionTca(parent_id, tca_version)
ExportExecutionEvidence(scope, evidence_version)

# Phase 2 component diagnostics
GetExecutionComponentState(algo_instance_id, state_version)
GetKillSwitchState(scope)

# Phase 3 authorized mutation
PreviewPolicyActivation(binding_id, policy_version, expected_revision)
ActivatePolicy(binding_id, policy_version, expected_revision, approval)
PreviewPolicyRollback(binding_id, target_policy_version)
RollbackPolicy(binding_id, target_policy_version, active_parent_mode, approval)
PreviewReviseParentHardLimit(parent_id, expected_revision, new_limit)
ReviseParentHardLimit(parent_id, expected_revision, new_limit, approval)
PreviewTerminateParent(parent_id, expected_revision, reason)
TerminateParent(parent_id, expected_revision, reason, approval)
PreviewAcceptTerminalResidual(parent_id, expected_revision, reason)
AcceptTerminalResidual(parent_id, expected_revision, reason, approval)
PreviewKillSwitchAction(scope, action, expected_revision)
ApplyKillSwitchAction(scope, action, expected_revision, approval)
```

kill-switch action 至少支持 `TRIGGER`、`ACKNOWLEDGE` 和 `RESUME`。所有 mutation operation 需要 RBAC、expected revision、dry-run preview、审计主体和幂等 request id；parent control 只能发起 §6.10 允许的 reconcile/transition，不能直接覆盖数量或 broker 状态。本文不增加 UI；未来 UI 必须调用相同 service contract。

---

## 7. 算法设计

### 7.1 Portfolio Parent Coordinator

处理顺序：

1. 汇总同一 binding/date 的多 alpha target。
2. 按 symbol 计算最终 target 与 current/available quantity。
3. 对反向 alpha 流量净额化并保留 lineage。
4. 使用冻结的 conversion price 把 target weight 转为 quantity，执行 lot rounding，并记录不能分配的余量。
5. 生成唯一 parent，冻结 benchmark、eligibility、reservation 和 policy snapshot。
6. 若日内 target revision 到达，使用 parent CAS/supersession；旧 revision 不再释放新 child，已有 child 继续 reconcile。
7. 建立 sell-to-buy 现金依赖，但不把所有买单无条件串行化；卖单只有 confirmed fill 才释放对应现金 reservation。
8. 根据 urgency、流动性、涨跌停距离和 broker rate budget 排序。

协调器只管理 parent 依赖和资源预算，不决定盘口价格。

### 7.2 Reference Schedule 与 Completion Tube

Reference schedule 是软目标，不是成交事实。允许模式：

- `IMMEDIATE_ARRIVAL`：小订单、深度充分、signal 衰减快；
- `SHORT_HORIZON_ADAPTIVE`：几分钟内滚动完成；
- `POV_OR_VOLUME_CURVE`：订单相对流动性较大；
- `TWAP_REFERENCE`：无短期观点时的研究基线；
- `V25_PROPOSAL`：只作为实验 schedule proposal。

每个 schedule 输出单调不减累计目标 (Q^*(t)) 和允许偏离区间。Completion Governor 可以在落后或 deadline 临近时收紧区间，模型不能扩大硬 deadline。

### 7.3 M1 Micro 与 B1 确定性 Adaptive Urgency

M1 是五档 micro executor 组件；只有在 Parent Coordinator、reference schedule、clock 和 Completion Governor 组装后，完整 composite 才称为 B1。每个 fresh quote 按以下顺序决策：

1. 校验交易阶段、quote freshness、市场状态和 broker active child。
2. 计算 `remaining/depth5`、spread、短时波动、schedule deficit 和剩余时间。
3. 如果 active child 已过期，先进入 cancel/reconcile。
4. 根据 urgency state 选择 WAIT、被动试单或 L1–L5 主动动作。
5. 根据 cumulative depth 与 haircut 决定 child quantity 和 price。
6. 应用所有硬约束并持久化 action evidence。
7. 通过 OMS/Gateway 下单，等待真实 callback 更新状态。

首版 urgency state：

```text
AHEAD_OF_SCHEDULE
ON_SCHEDULE
BEHIND_SCHEDULE
CONTINUOUS_AUCTION_FINAL
CLOSING_AUCTION
MARKET_CLOSED
```

### 7.4 被动分支

`PASSIVE_BBO` 默认关闭，只能在以下条件全部满足时通过实验配置启用：

- spread 足够覆盖潜在等待收益；
- parent 明显提前于 completion tube；
- quote 新鲜且 deadline 预算充足；
- 当前无 cancel pending；
- passive attempts 和 fresh-tick timeout 未超限；
- hard cost limit 与市场状态允许。

超过配置的 fresh quote 周期或 wall-clock 秒数任一门限后，必须显式 cancel/reconcile，再由控制器选择新动作。当前没有队列位置，因此不得宣称能够精确估计被动成交概率。

### 7.5 B2：Constrained Receding-Horizon / MPC

B2 在 B1 稳定后引入。Macro controller 每 30–60 秒或闭合分钟重新计算，micro executor 仍只在 fresh tick 上执行。

MPC 状态：

- remaining quantity/time、schedule deficit；
- spread、五档深度、短时波动、quote age；
- recent fill rate、submit-to-ack、cancel latency；
- parent urgency/alpha half-life；
- market phase 和 hard guard distance。

首版可使用有限动作枚举和可解释成本函数，不要求立即引入通用 QP solver。只有 B1 telemetry 证明数值优化具有稳定增量后，才引入更复杂求解器。

初始化时资产缺失、配置非法、约束不可构造或模型 hash 不匹配时，B2 policy 不得启动。单次 solver timeout、infeasible 或数值 NaN 则进入同一 versioned B2 policy 内预声明的 B1 safety subpolicy，必须 loud 记录 solver evidence；这属于 policy 内安全动作，不是运行时静默更换算法。连续超限触发 kill switch。

### 7.6 B3：轻量预测增强

允许两个可选模型：

1. `wait_cost_model`：预测未来 3/6/15 秒 signed mid move；
2. `depth_survival_model`：预测当前显示深度在 submit/ack 时仍可用的概率。

优先使用可校准 logistic、GBDT 或单调模型。模型输出只能作为 MPC 成本输入；模型缺失、漂移或超出训练域时，系统必须明确停用 B3 policy 并由 operator 选择已验证的 B1/B2 policy，不能在运行中静默切换。

### 7.7 Completion Governor

Completion Governor 独立于预测模型并具有更高优先级：

| 阶段 | 行为 |
|---|---|
| 正常连续竞价 | 允许受控等待、短时被动或主动穿档 |
| `BEHIND_SCHEDULE` | 增大 child、减少等待、提高主动程度 |
| 14:55–`continuous_cancel_cutoff` | 连续竞价最终完成窗，优先 protected aggressive actions |
| `continuous_cancel_cutoff`–14:57 | 按 cancel latency P99 决定撤销或明确 carry；禁止启动无法在边界前收敛的新 child |
| 14:57–15:00 | 收盘集合竞价专用 protected-limit；不可沿用普通 cancel/reprice |
| 收盘后 | 终止新动作，reconcile broker，loud 记录 residual |

具体起始时刻走配置，并由 `ExecutionClockEvent`、交易日历/半日市等市场时段契约校验。14:55 是当前默认候选，不是不可变常量。`continuous_cancel_cutoff` 必须早于 14:57，依据实测 cancel P99 加安全 buffer 计算：

- cutoff 前 active child 必须明确选择 cancel 或 carry；
- cancel pending 未确认时不得提交集合竞价 replacement；
- 14:57 后绝不发送普通连续竞价 cancel/reprice；
- 集合竞价 price source、市场/板块 capability 与不可撤语义必须显式；
- 14:57 前后重启由 clock phase 和 broker reconcile 恢复，不能依赖最后一条 quote。

---

## 8. V25、vn.py 和 MiniQMT 的关系

### 8.1 V25 的保留方式

V25 保留为独立研究资产：

- 不修改已有模型权重、训练产物和历史 workspace；
- 新实验只能通过独立 `V25ProposalAdapter` 把 240 槽输出转换为 `V25_PROPOSAL` reference schedule；
- V25 与 immediate、TWAP、volume curve 使用相同 M1 micro executor；
- 只有真实 TCA 证明 V25 schedule 在 completion 非劣条件下稳定降低 IS，才可作为某些订单分桶的 macro 插件；
- V25 不直接持有 broker child、Completion Governor 或运行时降级权。

`V25ProposalAdapter` 必须满足：

- 固定 `artifact_id/version/hash`、feature schema 和训练 provenance；
- 每次 proposal 记录 `as_of_time`，所有输入特征在该时点必须已经可观测，禁止使用未来分钟；
- 明确 240 槽到交易日历、午休、parent start time 的映射；
- 对 `as_of_time` 前已过槽位使用预声明的 discard + remaining-weight renormalize 语义，不回放过去数量；
- 输出单调累计 schedule、proposal confidence 和适用订单分桶；
- 始终保持 MiniQMT bridge 对 V25 broker execution 的拒绝门，adapter 只生成 schedule proposal。

若现有 artifact 的 day features、计划时点或 provenance 不能满足以上契约，B4 不具备 capability，不进入 Phase 4 实验，也不影响 B0/B1 交付。

### 8.2 vn.py-style 复用

| 现有资产 | 复用内容 | 在新算法中的角色 |
|---|---|---|
| `VnpyAlgoTemplate` | 旧算法的 tick/order/trade/timer 语义 | 保持旧 core 不变；不作为新五档 core 的父类 |
| vn.py-style DTO/helper | action、order/trade helper、OMS adapter | 通过 composition 复用 |
| Sniper | 对手价触发、一档量约束、撤单再报价 | B1 主动 child 的参考，不直接扩写原语义 |
| BestLimit | 己方 BBO 被动报价 | `PASSIVE_BBO` 实验参考 |
| TWAP-lite | 定时切片与撤单 | baseline/reference schedule |
| Iceberg（上游） | 隐藏大单显示量 | 当前小额 top-k 不优先接入 |

新协议和 core 应独立命名，例如 `AdaptiveExecutionCore` / `AdaptiveIsL1Core`，使用 `FiveLevelQuote`、`ExecutionClockEvent` 和显式 state serializer；不得通过修改 `VnpyTick` 或 Sniper 使旧 policy 的行为发生漂移。

### 8.3 MiniQMT quote ingress

目标数据流：

1. `subscribe_whole_quote` callback 接收快照并写入有界 quote 队列；
2. 单写者 event loop 合并同 symbol 最新 quote，按 exchange timestamp/quote id 去重；
3. 多 symbol 不宣称拥有交易所原子快照，而使用 configurable `max_symbol_skew_ms` 和 watermark 形成 decision batch；超过 skew 的 symbol 单独等待或 loud 跳过该 batch；
4. queue overflow 时只 coalesce 可替换 quote，并记录 dropped/coalesced metric；order/trade/clock 高优先级事件不得丢失；
5. core 只收到通过 freshness、schema、单位、capability 和 clock-skew 校验的 `FiveLevelQuote | ClosingAuctionSnapshot`；
6. broker submit/cancel 不在 quote callback 线程执行；
7. order/trade callback 进入不可丢失的高优先级通道，并与 clock event 一起进入相同单写者状态机；
8. exchange/local clock skew、calendar hash 失效或 watermark 超时会冻结新 submit，但继续 cancel/reconcile。

如果推送不可用，polling 必须声明独立 capability、最大附加延迟和严格 freshness gate。

---

## 9. TCA 与实验设计

### 9.1 必须记录的数据

Parent：

- decision/arrival benchmark、目标/eligible 数量、deadline、urgency；
- 多 alpha 净额化前后数量与 lineage；
- schedule/policy/config/model hash。

Market Data/Action：

- exchange/receive time、market-data age、五档价量或集合竞价可用字段、spread、stock status、auction capability；
- action、child quantity/price、深度覆盖率、haircut、guard evidence。

Broker：

- submit/ack/order/trade/cancel/reject 时间；
- partial/full fill、traded price/quantity、commission/tax；
- broker status 与 reconcile evidence。

Outcome：

- decision/arrival IS、effective spread；
- 1/5/15 分钟 markout；
- delay/opportunity cost；
- time-to-first-fill/time-to-complete；
- forced-sweep contribution；
- terminal residual reason。

### 9.2 Champion/Challenger

实验必须分离两个轴：

Macro schedule 轴：

- `IMMEDIATE_ARRIVAL`；
- `SHORT_HORIZON_ADAPTIVE`；
- `TWAP_REFERENCE`；
- `POV_OR_VOLUME_CURVE`；
- 通过 capability gate 后的 `V25_PROPOSAL`。

Micro policy 轴：

- M0：BUG-614 legacy protected marketable child；
- M1：五档 depth-aware micro executor。

Composite system 名称：

- B0：BUG-614 macro/micro control family；每个 control revision 不可原地改写；
- B1：deterministic adaptive controller + M1；
- B2：constrained MPC + M1；
- B3：MPC + learned costs + M1；
- B4：V25 proposal + M1，仅在 V25ProposalAdapter capability 通过后存在。

B2/B3/B4 只有在各自实现和 capability gate 通过后才纳入实验；Phase 4 首轮不无条件要求尚未实现的候选。

B0 的实验身份必须冻结为 `control_revision + policy/config/adapter/code/schema hash`。任何 ingress/adapter 安全语义或代码变化都生成新的 B0 control revision，并重新完成 Phase 0B 观察窗口；不能把不同 revision 拼接为同一不可改写 control，也不能沿用旧 baseline receipt。

隔离变量：

- 比较 macro schedule 时必须固定相同 micro executor；B0 原栈单独保留，不因替换 M1 后继续使用 B0 名称；
- 比较 child policy 时必须固定相同 parent schedule；
- 被动成交只使用真实 SIM callback 证据，不从历史 snapshot 假造；
- Phase 4 详细设计必须预注册随机化单位、组合内订单相互影响的处理、样本量/最小观察窗口、非劣界值、置信水平、多重比较、数据冻结和提前停止规则；
- 按 symbol、side、流动性、时段分层随机，记录 propensity；有现金/同 symbol/组合依赖的 parent 使用 cluster assignment，不能跨策略互相污染；
- 时间顺序 walk-forward，以 parent/day cluster bootstrap 计算置信区间。

### 9.3 晋级规则

具体数值阈值不得在看到实验结果后确定，必须由 Phase 4 依据 B0 方差、业务 completion SLO 和最小经济改善预注册。候选策略必须同时满足：

- eligible completion 对当前 champion 非劣；
- signed arrival/decision IS 达到预注册的经济改善和置信水平；
- P95/P99/CVaR 不恶化；
- forced-sweep ratio 不上升或有可解释成本收益；
- 陈旧 quote 上 unsafe submit 为零、hard-guard violation 为零；broker reject/cancel race 必须 100% reconcile，发生率低于预注册上限；
- terminal residual reason coverage 为 100%；
- 不同流动性、价差、波动、板块和买卖方向分桶无系统性失效。

SIM callback 仅形成 SIM-relative evidence，不外推 LIVE。如果 B2/B3 对 B1 没有稳定增量，保留简单 B1；如果 V25 在统一 micro 下稳定胜出，只将其晋级为对应分桶的 macro 插件。

---

## 10. Failure Modes / 风险与失败模式

| 失败模式 | 检测 | 行为 |
|---|---|---|
| quote 陈旧/时间回退 | exchange/receive time、policy age | 不下单，loud，等待 fresh quote |
| 重复 snapshot | quote id/timestamp/hash | 不重复释放数量 |
| 五档字段缺失 | schema validation | 数据错误 fail loud |
| 合法 zero depth | stock status + depth | 市场无流动性状态，保留 residual |
| active child 未回报 | broker query + timeout | reconcile，禁止冲突 replacement |
| cancel/partial fill race | order/trade idempotency | broker trade 优先，重新计算 remaining |
| submit 成功但本地持久化结果失败 | durable intent + client request id + broker query | `RECONCILE_REQUIRED`，禁止重复 submit |
| callback 重复/乱序/late fill | trade/order id + state revision | 幂等应用；late fill 重算 working/unallocated |
| unknown broker status | status capability + broker query | 冻结 parent 新动作并 reconcile |
| overfill | quantity invariant | global fail-closed、冻结新动作、人工处置 |
| 多 alpha 同股重复 parent | parent uniqueness/netting evidence | preflight 拒绝并 loud |
| target revision 与旧 child 冲突 | parent revision + CAS | 旧 revision 禁止新 child，已有 child reconcile |
| hard cost limit 触发 | decision/arrival cap | 进入 `HARD_LIMIT_BLOCKED`，停止主动穿价并请求显式 parent revision/终止/接受 residual |
| 涨停买/跌停卖 | limit/stock status/depth | 显式 blocked business state |
| 停牌/临停 | trade calendar/stock status | `NO_ACTION_MARKET_STATE` |
| broker disconnect/reject | callback/health probe | 冻结新动作、reconcile、loud |
| quote 队列 overflow/watermark 超时 | queue metrics/max skew | coalesce quote、冻结受影响 submit、loud；不丢 order/trade |
| 高优先级 inbox/WAL 满载或写失败 | durable write/queue health | binding/global freeze，禁止新 submit，broker + ledger 持久化 reconcile 后授权恢复 |
| clock/calendar 失效 | skew/calendar hash | 冻结新 submit，继续风险收敛 |
| DB 增量写失败 | repository error/watchdog | fail closed，不以内存成功替代 |
| 模型缺失/漂移 | model hash/domain monitor | 该 policy 不可运行，显式选择已验证 policy |
| model inference timeout/NaN | inference budget/output schema | loud，使用同 policy 预声明 safety subpolicy；连续超限 kill |
| solver infeasible/timeout/NaN | solver status/budget | loud，使用 B1 safety subpolicy；连续超限 kill |
| broker/order rate limit | token budget/reject code | 停止低优先级 submit，保留 cancel/reconcile |
| duplicate quantity release | action id/parent revision/invariant | 拒绝 action，冻结 parent 并 reconcile |
| 14:57 后继续撤改单 | trading phase guard | 拒绝普通动作，进入集合竞价状态 |
| 收盘仍有 residual | clock + broker reconcile | loud terminal event，完整归因 |

### 10.1 Kill switch 与运行手册契约

必须提供三级 kill switch：

- per-parent：数量不变量、hard limit、unknown broker state 等单 parent 问题；
- per-binding/account：broker disconnect、rate-limit storm、quote/calendar readiness 失败；
- global SIM runtime：overfill、ledger 持久化系统性失败或跨 binding 状态污染。

每个 switch 的阈值、冻结范围、是否撤销 active child、reconcile 顺序、报警渠道、恢复授权和验证命令必须在 Phase 2/3 详细设计中预注册。kill 后默认冻结新 submit；是否撤单取决于交易阶段和 broker 状态，不能在未知状态下盲目重复撤单。运行手册必须覆盖 operator 查询、acknowledge、恢复和 evidence 导出。

---

## 11. Implementation Plan / 分阶段实施方案

每个阶段必须独立生成详细设计，引用本文 Design Acceptance Index，并以独立 worktree/PR 交付。阶段顺序不可通过静默配置绕过。Phase 0 含 0A/0B 两个 gate：0A 与 Phase 1 完成后，Phase 2/3 才可开展代码、harness 和纯 dry-run 开发；在 Phase 0B baseline receipt 与数据冻结完成前，B1 禁止任何 broker side effect、canary、activation 或晋级。

### Phase 0A：Benchmark、TCA Schema 与 Ledger Join

目标：不改变 broker 行为，先建立可计算的 benchmark、parent/broker join 和 observation-only TCA schema。

交付：

- parent decision/arrival benchmark 冻结；
- Path S event-loop 与 qmt ledger 的 TCA join；
- signed IS、opportunity cost、completion 和 markout 公式；
- eligibility freeze、deadline mark 和 BUY/SELL 多 fill 聚合；
- 对 quote age/五档深度当前可用性的 coverage audit；缺失观测由 Phase 1 补齐。

退出条件：所有现有 parent/fill/residual 可关联，BUY/SELL 成本方向正确，TCA schema 与 ledger join 可重复；不要求在 Phase 1 前声称已有完整五档基线。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase0_tca_design.md`。

### Phase 1：五档/集合竞价 Market-Data Contract 与事件输入

目标：建立不改变执行 policy 的强类型数据和时效基础。

交付：

- `FiveLevelQuote` DTO 与 schema validation；
- `ExecutionClockEvent`、交易日历 hash 和 clock-skew gate；
- exchange/receive timestamp、quote age、duplicate detection；
- 五档数组、zero-depth 市场状态；
- `ClosingAuctionSnapshot`、auction capability/freshness 与缺失字段 fail-loud 语义；
- `subscribe_whole_quote` 有界队列与单写者 ingress；
- active symbols watermark/max-skew decision batch；
- quote queue coalesce/overflow telemetry 与 order/trade/clock 高优先级通道；
- capability/配置严格校验。

退出条件：对 fresh/valid quote，新 adapter 保持 BUG-614 下单决策等价；陈旧、重复或非法 quote 被阻止是预期安全差异。由于这些差异改变 ingress 安全语义，必须生成新的 B0 control revision，不能声称与旧 revision 身份相同。所有 quote/clock/queue 用例通过且不产生额外 broker side effect。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase1_quote_contract_design.md`。

### Phase 0B：B0 完整观察窗口与事实校准

目标：使用 Phase 0A + Phase 1 的观测能力，建立 BUG-614 后权威 B0 基线。

交付：

- 至少一个预注册完整观察窗口；
- quote age、snapshot-to-ack、cancel latency 分布；
- `Q/ADV`、`Q/分钟量`、`Q/五档深度` 分桶；
- completion、IS、markout、forced sweep、reject/cancel/reconcile；
- `control_revision + policy/config/adapter/code/schema hash`、数据冻结 hash 和 B0 方差，为 Phase 4 样本量/非劣界提供输入。

退出条件：未归因 terminal residual 为零，关键观测 coverage 达到预注册门限，B0 baseline receipt 可由相同查询重建。若 coverage 不足，先修观测链，不能开始策略晋级。

### Phase 2：M1 五档 Micro Executor 组件

目标：实现不可独立激活的 M1 五档 micro、真实成交反馈状态机和深度感知主动动作；本阶段尚不构成完整 B1。

交付：

- 独立 `AdaptiveExecutionCore`、五档/集合竞价/clock 协议、versioned serializer、registry/capability；
- L1–L5 cumulative-depth 定价与 child sizing；
- active child、partial fill、cancel/reprice、reject/reconcile；
- parent hard limit、tick/lot/limit/cage 保护；
- 组件级 MiniQMT adapter/harness；在 Phase 3 组装前不进入 operator 可选 policy；
- reason code、state persistence 和恢复。

退出条件：目标测试、nox L0/module registry、broker callback harness、数量守恒、幂等/恢复和 fail-closed 证据通过；不得单独声明 B1 ready 或执行 canary。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase2_micro_executor_design.md`。

### Phase 3：组装 B1：Parent Coordinator、Adaptive Urgency 与 Completion Governor

目标：在 M1 上完成组合级净额化、真实 residual 驱动、clock 和交易阶段状态机，首次形成可选择的 B1 composite。

交付：

- 多 alpha symbol parent authority 与 lineage；
- 现金/T+1/并发 budget；
- reference schedule + completion tube；
- deterministic urgency state；
- 14:55–14:57 连续竞价终局、14:57–15:00 集合竞价状态；
- `continuous_cancel_cutoff`、在途/cancel-pending 跨阶段处理；
- parent revision、cash/sell reservation、partial-fill dependency release；
- terminal residual 分类。

退出条件：多 alpha 相同/反向目标、现金依赖、尾盘、停牌/涨跌停、broker reject、重启/reconcile 场景均有业务 oracle 和 loud evidence；B1 通过 SIM 多 symbol canary 后才可进入 Phase 4。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase3_parent_completion_design.md`。

### Phase 4：Champion/Challenger 与 V25 Macro 插件

目标：实施 macro/micro 两轴隔离的 SIM-relative 实验；macro 比较固定同一 micro executor，micro 比较固定同一 parent schedule，不能在同一 contrast 中同时改变两轴。

交付：

- 带冻结 baseline receipt 的 B0 control revision、B1、macro schedule 与 M0/M1 二维实验接口；
- V25ProposalAdapter capability 通过时才加入 B4；否则记录 capability-not-met，不阻塞 B0/B1；
- decision dry-run/action logging（不调用 broker，且不恢复 compiler/shadow B 路线）；
- 分层随机 SIM assignment；
- 预注册指标、cluster bootstrap 和分桶报告；
- 晋级/停止决策记录。

退出条件：随机化单位、干扰处理、样本量、观察窗口、非劣界、置信水平、多重比较、数据冻结和停止规则均预注册；完成率、IS、尾部风险和 forced sweep 有可重复 SIM evidence；未达到晋级标准的策略保持非默认。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase4_experiment_design.md`。

### Phase 5：B2 MPC 与 B3 轻量预测

目标：只在 Phase 4 证明存在稳定 trade-off 后增加模型复杂度。

交付：

- receding-horizon action cost 与 schedule tube；
- solver 性能/超时边界；
- wait/depth 模型数据集、特征契约、校准和漂移监控；
- model/config hash、champion/challenger 和显式停用流程；
- 与 B1 的增量 TCA。
- B2/B3 相对 B1 的独立 challenger experiment，不复用 Phase 4 已冻结数据选择参数。

退出条件：B2/B3 对 B1 completion 非劣、IS 与尾部风险满足预注册晋级标准；solver/model 故障进入同 policy safety subpolicy，不会绕过硬约束。

建议详细设计：`docs/architecture/miniqmt_adaptive_is_phase5_mpc_model_design.md`。

### Phase 6：可选 native IOC 与未来 L2/L3 研究

目标：在券商能力和数据 ROI 明确后评估，不作为当前路线前置依赖。

交付候选：

- xtquant/券商/市场时段 native five-level IOC capability matrix；
- protected price 与 reject semantics；
- 若采购 L2/L3，再评估 queue/fill survival、constrained offline RL 或 residual RL；
- 高保真、可响应 agent 行为的模拟器设计。

退出条件：能力探测、SIM broker evidence、数据授权和独立 ROI 评审全部通过。

---

## 12. Verification Plan / 验证方案

### 12.1 文档 PR 验证

本文档 PR 必须执行：

```powershell
python scripts/aistock_feature_workflow.py validate `
  --design docs/analysis/miniqmt_intraday_execution_strategy_analysis_20260710.md `
  --tier F2
git diff --check
```

### 12.2 实施阶段最小验证矩阵

| 类别 | 必须覆盖 |
|---|---|
| Core unit | 动作选择、深度、hard guard、quantity conservation |
| Quote contract | 五档、时间戳、陈旧、重复、zero depth、跨日 |
| OMS/broker | submit/ack/partial/fill/cancel/reject/race/reconcile |
| Persistence | 增量写、重启恢复、trade id 幂等 |
| Market states | 停牌、临停、涨停买、跌停卖、无对手量 |
| Multi-alpha | 同向合并、反向净额、lineage、现金依赖 |
| Closing | 14:55、14:57、15:00、不可撤单边界 |
| Safety | LIVE 拒绝、配置/模型/能力 fail loud |
| TCA | BUY/SELL signed IS、费用、residual opportunity cost |
| Runtime | 调度不冻结、broker_called、orders/trades 可见、多 symbol |

执行代码阶段至少运行直接 nodeid、相关模块测试、`nox -s l0`、`nox -s validation_module_registry_l0` 和 `git diff --check`。跨模块、真实业务流和长窗口 SIM 证据委托 Validation Center/CI/nightly，不能由 mock-only 测试替代。

### 12.3 数据验证

- market data、action、order、trade 使用稳定 id 端到端 join；
- 随机抽样回放 parent，人工重算 signed IS 与 residual；
- 检查冻结 denominator 的当前态守恒：`eligible_target = filled + working + unallocated + terminal_residual`；cancel/reject 历史量不得重复进入当前态；
- 按 symbol/day 比对 broker ledger 与本地 execution state；
- 对 quote age、snapshot-to-ack、cancel latency 做分布和压力日审计；
- 对模型阶段进行 feature parity hash、时间顺序 walk-forward 和分布漂移检查。

### 12.4 L0–L5、覆盖率与业务 oracle

| 层级 | 本蓝图后续实施的最小范围 |
|---|---|
| L0 | schema/config/static guard、纯 core unit、BUY/SELL 数学方向、状态与数量不变量；无 broker side effect |
| L1 | market-data/clock/serializer/component harness、durable intent、幂等 callback 与 fail-closed fault injection |
| L2 | repository、service operations、OMS/Gateway、migration/backward compatibility 的隔离集成测试 |
| L3 | 独立 MiniQMT SIM binding 端到端，真实 submit/ack/order/trade/cancel/reject、ledger/TCA join 与多 symbol 业务流 |
| L4 | 多日/压力日/nightly，背压、重启、尾盘、长窗口 champion/challenger、分桶和尾部风险 |
| L5 | LIVE-readiness 审计与独立 F2 审批材料；本文和 Phase 0–6 SIM 结果均不授权 LIVE activation |

- API oracle：preview/mutation 的 revision、RBAC、idempotency 与 audit event 可核对；DB oracle：parent/child/market-data/event/TCA 可由 broker ledger 重建，增量写和 watchdog 不退化。
- UI：本文明确不新增 UI，因而当前为 N/A；未来若加入 operator/TCA UI，必须另列 API、真实页面、E2E/截图与权限证据，不能用 mock 替代。
- log oracle：所有失败、kill、reconcile 和 terminal residual 均有稳定 `reason_code + stage + context`；business oracle：broker fill authority、frozen denominator、数量守恒、signed IS、同向/反向多 alpha 与尾盘状态逐项可重算。
- 新增/修改运行时代码的 line coverage 目标不低于 80%，branch coverage 不低于 70%；broker capability、14:57 不可撤语义和异常恢复等不能完全自动化的项目，必须保留受控人工确认与 evidence。
- L4/L5 长任务交给 Validation Center/CI/nightly；本地只保留最小安全门禁和紧凑结果，不以短测通过替代长窗口证据。

---

## 13. Rollout / Rollback / 发布与回滚

### 13.1 发布顺序

1. 代码合入但 policy 不可选、默认关闭。
2. decision dry-run/action logging：生成 action，不调用 broker，且不进入已退役 compiler/shadow B 路线。
3. 独立 MiniQMT SIM binding 小比例 canary。
4. 扩大到分层随机 SIM，保持 B0 control。
5. 达到预注册标准后，B1 可成为 SIM 默认；B2/B3 分别重新走晋级。
6. LIVE 评估必须使用新的 F2 设计、独立授权和生产门禁，不由本文档自动授权。

任何 activation 都必须持久化 policy version、binding、operator、审批证据和生效时间。默认配置、catalog entry 和 StrategyPackage 不得因代码合入自动选择新算法。

### 13.2 显式回滚

- 回滚单位是 versioned execution policy，不回滚 broker ledger 或删除历史 evidence。
- operator 可显式选择已验证 B0/B1 policy；变更必须产生 audit event。
- 运行中 parent 默认 pin 创建时 policy 直到 terminal。回滚时新 parent 使用目标 policy；active parent 由 operator 在 `DRAIN_PINNED`、`CANCEL_AND_RECONCILE`、`FREEZE_NEW_ACTIONS` 中显式选择，不能中途无审计换 policy。
- 自动 kill/freeze/停止扩量触发器至少包括 hard-guard violation、overfill、无法 reconcile 的 broker unknown、系统性 quote/calendar readiness 失败和 ledger 持久化失败；真正的 policy rollback 始终需要 operator 授权，触发器和性能指标都不能自动改写 active parent policy。
- 运行中 policy 不得自动变更。若当前 policy 初始化资产或模型不可用，应 fail closed 并请求明确操作；B2 内预声明 safety subpolicy 仅处理单次 solver/inference runtime failure，并保持相同 policy version。
- DB schema 采用向后兼容扩展时，代码回滚保留新增字段；需要破坏性 DDL 时必须另行批准。
- 任何回滚不得恢复 shadow/compiler B 路线、削弱 LIVE 锁或覆盖 broker 权威状态。

Phase 2/3 详细设计必须定义受权 API/CLI、RBAC、二次确认、审计记录、active-parent 预览和回滚后验证命令；不得依赖人工直接改 DB 或进程内变量。回滚完成必须验证 scheduler、quote readiness、broker reconcile、policy pin 和新 parent policy。

---

## 14. Production Gates / 生产门禁

本文档 PR：

- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- runtime activation：noop
- production DB write：noop

未来实施阶段：

- Phase 0/2/3 若新增持久化字段，必须提供 migration、回滚/兼容策略和生产 DDL 验证证据。
- 新 Python/solver/model 依赖必须经过 backend dependency gate。
- operator UI 若新增 policy 选择或 TCA 页面，必须另列 UI/API/E2E 验收。
- 任何 LIVE capability 变更必须保持默认拒绝，并经独立生产审批。

### 14.1 分阶段门禁矩阵

| 阶段 | 必须通过的 gate |
|---|---|
| Phase 0A | TCA schema/backward compatibility、benchmark freeze、只读重建查询；如有 DDL 则 migration/rollback |
| Phase 1 | quote subscription/poll capability、五档 entitlement、clock/calendar、queue capacity、default-off |
| Phase 0B | 数据冻结、coverage、B0 baseline receipt、无生产 broker 行为变化 |
| Phase 2 | catalog/registry 不可选、core/serializer/OMS callback、kill switch、LIVE deny |
| Phase 3 | policy DML/activation、StrategyPackage/config snapshot、scheduler/restart、broker capability、SIM canary |
| Phase 4 | 实验预注册、独立 binding/account、数据冻结、停止规则、TCA report |
| Phase 5 | solver/model dependency、asset hash、latency budget、drift/timeout、B1 safety subpolicy |
| Phase 6 | broker-native IOC capability、市场/时段矩阵、数据授权、独立 ROI 与生产审批 |

每个阶段必须提供 metrics、alerts 和 operator runbook；依赖、DDL、DML/config、catalog/registry、StrategyPackage、scheduler/restart、quote readiness 和 broker capability 分别报告，不得合并为单一“已部署”状态。

---

## 15. Design Acceptance Index / 设计验收索引

- **F-001**：以 signed decision/arrival IS、尾部风险和 terminal residual 为权威目标。
- **F-002**：多 alpha 在 broker 前形成唯一 symbol parent，并保留 lineage、现金和 T+1 约束。
- **F-003**：定义强类型五档 quote、exchange/receive time、quote age 和 duplicate contract。
- **F-004**：只有 broker trade callback 可以增加真实 filled quantity，并满足 parent/child 当前态数量守恒。
- **F-005**：动作空间限定为 WAIT、被动、L1–L5、能力受控 IOC、cancel 和 terminal protected limit。
- **F-006**：child quantity/price 使用 cumulative depth、haircut 和 latency evidence，不把固定 ticks 当作档位。
- **F-007**：交易所、涨跌停、parent hard limit、micro guard、资金和 lot 是不可绕过的硬约束。
- **F-008**：Completion Governor 独立于模型，覆盖正常、落后、continuous cancel cutoff、集合竞价和收盘。
- **F-009**：停牌、临停、涨跌停、zero depth 与数据/配置错误使用不同业务语义。
- **F-010**：durable intent、action/order/trade/cancel/restart 具备 CAS、幂等、单写者、版本化 serializer 和 broker reconcile。
- **F-011**：schema、配置、policy、adapter/code、模型/artifact 和状态均版本化、持久化并带 hash；B0 control revision 不原地改写，不发生静默算法切换。
- **F-012**：TCA 覆盖 benchmark、费用、delay、opportunity cost、markout、completion 和 residual。
- **F-013**：macro schedule 与 micro policy 分轴，B0/B1/B2/B3/B4 使用预注册、隔离变量的 champion/challenger 验证。
- **F-014**：模型仅作为受约束成本输入，不能直接下单或绕过 Completion Governor。
- **F-015**：新 `AdaptiveExecutionCore` 不改变旧 VnpyTick/core 语义，core/adapter、历史 replay/实时 broker capability 和资产/程序边界明确。
- **F-016**：所有失败与 terminal residual loud，包含 reason code、stage 和 broker context。
- **F-017**：SIM-only 分阶段发布、显式 policy 回滚和 LIVE 独立审批。
- **F-018**：七个主阶段（Phase 0 含 0A/0B gate）均有交付、退出条件、建议详细设计路径和独立验收边界。
- **F-019**：quote/clock/order/trade ingress 定义 watermark、背压、优先级、单位和 clock/calendar 失效语义。
- **F-020**：Phase 0–3 定义 logical persistence、event envelope、service operations、migration 和 backward compatibility。
- **F-021**：V25ProposalAdapter 定义 artifact、as-of、特征可用性、交易日历和已过槽位语义，并保持 broker bridge 拒绝门。
- **F-022**：per-parent/per-binding/global kill switch、阈值、风险收敛、报警、恢复和 runbook 为强制契约。

---

## 16. Design Acceptance Matrix / 设计验收矩阵

本矩阵验证本文蓝图的覆盖完整性，不表示运行时代码已经实现。`ready` 仅表示 master blueprint 条款定义完整、可进入所属 Phase 的详细设计；每个实施 PR 必须把对应行替换为真实代码引用和测试/运行证据。

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| F-001 | §4；runtime owner=Phase 0A/0B | 目标函数、frozen denominator 与 IS 聚合公式审查 | ready | none |
| F-002 | §6.1、§7.1；runtime owner=Phase 3 | authority key、revision、reservation 与 lineage 契约审查 | ready | none |
| F-003 | §6.2；runtime owner=Phase 1 | quote id、五档、单位、空档与时效契约审查 | ready | none |
| F-004 | §6.4、§6.10；runtime owner=Phase 2/3 | broker fill authority、状态机与数量守恒审查 | ready | none |
| F-005 | §6.5；runtime owner=Phase 2 动作契约/Phase 6 native IOC capability | 有限动作空间与能力门禁审查 | ready | none |
| F-006 | §6.6、§7.3；runtime owner=Phase 2 | 深度公式、haircut 与 child sizing 审查 | ready | none |
| F-007 | §4.3–§4.5、§6.6；runtime owner=Phase 2/3 | hard guard 与 must-complete 优先级审查 | ready | none |
| F-008 | §6.2.1、§6.3、§7.7；runtime owner=Phase 1/3 | auction snapshot、clock、cancel cutoff、连续/集合竞价和收盘状态审查 | ready | none |
| F-009 | §6.2–§6.3、§10；runtime owner=Phase 1/3 | 市场状态、clock 与数据错误分类审查 | ready | none |
| F-010 | §6.9–§6.10；runtime owner=Phase 2/3 | durable intent、CAS、幂等、恢复和 reconcile 审查 | ready | none |
| F-011 | §6.7、§9.2、§13；runtime owner=Phase 0B–5 | schema/policy/config/adapter/code/model/artifact 版本、B0 control identity 与显式回滚审查 | ready | none |
| F-012 | §9；runtime owner=Phase 0A/0B | TCA 字段、指标和 frozen baseline 审查 | ready | none |
| F-013 | §9.2–§9.3；runtime owner=Phase 4/5 | macro/micro 隔离、预注册和分层实验审查 | ready | none |
| F-014 | §7.5–§7.7；runtime owner=Phase 5 | MPC/模型权限、Completion Governor 优先级与 safety subpolicy 审查 | ready | none |
| F-015 | §5.2、§8.2；runtime owner=Phase 2 | 独立 core 协议、adapter/capability/资产边界审查 | ready | none |
| F-016 | §6.8、§10；runtime owner=Phase 0A–6 | TCA、market data、broker、model/solver、capability 的 loud reason/stage/terminal context 审查 | ready | none |
| F-017 | §13–§14；runtime owner=Phase 3–6 | SIM 发布、active-parent 回滚、LIVE 与生产门禁审查 | ready | none |
| F-018 | §11–§12；runtime owner=Phase 0–6 | Phase 0A/1/0B 依赖、分阶段退出条件与验证审查 | ready | none |
| F-019 | §6.3、§6.9、§8.3；runtime owner=Phase 1 | watermark、背压、事件优先级和 clock/calendar 审查 | ready | none |
| F-020 | §6.11、§14.1；runtime owner=Phase 0A/1/2/3 | persistence、market-data evidence、event/service、migration/backward compatibility 审查 | ready | none |
| F-021 | §8.1；runtime owner=Phase 4（有条件） | V25 proposal as-of/calendar/artifact/capability 审查 | ready | none |
| F-022 | §10.1、§13；runtime owner=Phase 2/3 | kill switch、risk convergence、恢复与 runbook 审查 | ready | none |

---

## 17. 后续详细设计输入规则

每阶段详细设计必须：

1. 声明引用的 F-编号和本阶段不拥有的 F-编号。
2. 给出代码 ownership、API/DB/config/event schema 和 backward compatibility。
3. 给出完整 failure matrix、直接 nodeid、集成/运行 evidence 和生产门禁。
4. 明确对 BUG-599/600/604/614 的不变式证明。
5. 对所有无法满足的条款停止实施并请求批准调整，不能自行删除蓝图条款。
6. 实施完成时产出 `design_item / implementation_refs / test_or_evidence / status / gap_or_exception` 矩阵。

---

## 18. 研究与官方资料

- Perold (1988), [The Implementation Shortfall: Paper versus Reality](https://doi.org/10.3905/jpm.1988.409150)
- Almgren & Chriss, [Optimal Execution of Portfolio Transactions](https://doi.org/10.21314/JOR.2001.041)
- CFA Institute, [Trading Costs and Electronic Markets](https://www.cfainstitute.org/insights/professional-learning/refresher-readings/2026/trading-costs-and-electronic-markets)
- AQR, [Trading Costs](https://www.aqr.com/Insights/Research/Working-Paper/Trading-Costs)
- BestEx Research, [Adaptive Optimal IS Framework](https://www.bestexresearch.com/insights/adaptive)
- McAuliffe et al. (2026), [Model Predictive Control for Trade Execution](https://arxiv.org/abs/2603.28898)
- Li et al., [M3T: Hierarchical Deep Reinforcement Learning for VWAP Strategy Optimization](https://arxiv.org/abs/2212.14670)
- Arroyo et al., [Deep Attentive Survival Analysis in Limit Order Books](https://arxiv.org/abs/2306.05479)
- Zhang et al., [Towards Generalizable Reinforcement Learning for Trade Execution](https://www.ijcai.org/proceedings/2023/0553.pdf)
- Byun et al., [Practical Application of Deep Reinforcement Learning to Optimal Trade Execution](https://www.mdpi.com/2674-1032/2/3/23)
- vn.py, [AlgoTrading built-in algorithms](https://github.com/vnpy/vnpy_algotrading/tree/main/vnpy_algotrading/algos)
- 迅投知识库, [股票行情与 `get_full_tick`/`subscribe_whole_quote`](https://dict.thinktrader.net/dictionary/stock.html)
- 迅投知识库, [XtQuant trader API](https://dict.thinktrader.net/nativeApi/xttrader.html)
- 上海证券交易所, [2026 交易规则](https://www.sse.com.cn/lawandrules/sselawsrules2025/stocks/exchange/c/c_20260424_10816482.shtml)
- 深圳证券交易所, [2026 交易规则](https://docs.static.szse.cn/www/lawrules/rule/trade/current/W020260424690713155663.pdf)
