# MiniQMT 唯一执行链路与 vn.py 架构参考统一设计（2026-06-08）

> 状态：详细设计方案 / P0 架构统一整改方案
> 工作分支：`docs/miniqmt-unified-vnpy-execution-design-20260608`
> 适用范围：AIstock MiniQMT SIM 单策略/多策略模拟盘、未来 MiniQMT Live 执行层基础、Paper v2 MiniQMT 入口、simulation_runtime MiniQMT 入口、qmt_strategy ledger/OMS、operator 清仓/撤单/重置信号入口。
> 非目标：本设计不改造 `V25_TWO_STAGE` / `V25_1_SMALL_CAP` MiniQMT live 化，不启动独立 vn.py 服务，不在本分支改后端/前端代码，不重启生产服务，不写生产数据库。

## 0. 一句话结论

MiniQMT 未来只能保留 **一条同时支持 N=1 和 N>1 的执行链路**：

```text
AlphaSignalBook（一个或多个 alpha 信号源）
  -> MiniQMTExecutionRuntime（唯一执行运行时）
  -> StrategySlotTarget / RebalanceIntent / ExecutionPlan
  -> vn.py-derived AlgoInstance（Sniper / BestLimit / TWAP 等）
  -> MiniQMTGateway（XtQuant / MiniQMT broker adapter）
  -> Durable OMS / qmt_strategy ledger / reconciliation
```

硬结论：

1. **单策略不是单独路径**，只是同一 `MiniQMTExecutionRuntime` 下 `strategy_slot_count = 1` 的特例。
2. **多策略不是另一个产品路径**，只是同一 runtime 下多个 `StrategySlot` 共同竞争资金、持仓和交易规则后的执行结果。
3. **StrategyPackage / alpha 层只产生信号和意向**，不得知道 broker、账号、MiniQMT order remark、价格类型、执行算法内部状态。
4. **MiniQMT 执行层是唯一能连接 MiniQMT 下单的策略执行边界**。operator 清仓、撤单、重置、换信号也必须进入同一 runtime 的 `OperatorCommand`，不得形成第二条 raw-order 业务路径。
5. **vn.py 不是口号**：本设计把 vn.py / vnpy_algotrading 的组件、源码文件、固定 commit、行号、状态机语义、保留行为和禁止偏离项列为实现规格。后续 PR 若没有 source mapping、attribution、characterization tests，不得声称“复用 vn.py 逻辑”。
6. **策略数量只受资金容量和交易规则限制**，不得再出现固定 `max_concurrent_packages=1/2/64` 这种业务门槛。资金不足只能阻断具体 slot/intent，并给出显式状态。

## 1. 设计边界与非目标

### 1.1 本次必须解决的问题

此前 MiniQMT 路径反复出现“按 vn.py 参考实现”但落地后变成多版本、多路径、多语义的问题：

- Paper v2 `MiniQMTSimBackend` 单策略路径、simulation_runtime 多策略路径、qmt_strategy managed order 手工路径、raw QMT router 都能在不同层次触达 MiniQMT。
- 部分路径只做一次性 `submit/query/reconcile`，没有长期运行的事件循环和订单生命周期状态机。
- 单策略、多策略、operator 手动动作之间边界不清，容易让某个 hotfix 修一条链路但另一条链路继续遗漏。
- `V25_*`、`SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT` 的“回测执行策略 / MiniQMT broker 执行策略 / Paper path 兼容策略”边界不够清晰。
- 资金不足、卖出未成交、历史挂单、submit 失败后 reconcile 成功覆盖失败、deterministic batch 失败缓存等场景容易被误判为成功。

本设计目标是先形成一个不可再分叉的产品级执行架构。后续 issue 只能按此架构拆阶段实现，不能新增“临时 MiniQMT 路径”。

### 1.2 本次不做的事

| 非目标 | 说明 |
|---|---|
| 不改造 V25 live 化 | `V25_TWO_STAGE` / `V25_1_SMALL_CAP` 暂时仍是 QE / LocalSim minute replay / 研究回测执行模型，不能作为 MiniQMT broker order policy 直接运行。 |
| 不启动独立 vn.py 服务 | 用户明确不能接受未来多启动一套 vn.py 服务；AIstock 后端进程内也不能并行维护第二套不可追溯生产 runtime。 |
| 不复制整套 vn.py MainEngine 作为黑盒 | 直接黑盒嵌入会绕开 AIstock 的 StrategyPackage、资金分仓、ledger、审计、生产门禁。正确方式是把 vn.py 的职责边界、事件顺序和算法源码语义移植到 AIstock 唯一 runtime。 |
| 不删除旧路径代码 | 旧路径先降级为 compatibility wrapper / read-only / admin-only，在 L0-L5 验证完成和用户确认后再按单独 chore issue 删除。 |
| 不写生产 DB / 不重启服务 | 本分支是 docs-only 设计。实现阶段若需要 DDL，必须通过 issue/PR/production gate 单独执行。 |

## 2. 证据基线

### 2.1 AIstock 现有权威文档

| 证据 | 关键结论 | 本设计如何继承或修正 |
|---|---|---|
| `docs/architecture/strategy_package_platform_boundary_contract_20260520.md:13` | StrategyPackage 只保存 alpha core。 | 本设计把 alpha 信号层收敛为 `AlphaSignalBook`，禁止 broker/order/execution 字段进入信号合同。 |
| `docs/architecture/strategy_package_platform_boundary_contract_20260520.md:30` | MiniQMT Strategy Ledger 负责多策略虚拟账户、资金分仓、订单/成交归因、对账。 | ledger 保留，但从“可以手工发 order 的服务”收敛为唯一 runtime 的 OMS/持久化子系统。 |
| `docs/architecture/strategy_package_platform_boundary_contract_20260520.md:119` | 合法执行链路是 SelectionRun/TargetPortfolio -> Unified Strategy Engine -> ValidatedExecutionPolicy -> Broker child orders。 | 本设计把 MiniQMT broker child order 的唯一落点定义为 `MiniQMTExecutionRuntime`，禁止 `SelectionOrderBuilder` 直连 broker。 |
| `docs/architecture/strategy_package_platform_boundary_contract_20260520.md:130` | 明确禁止 `SelectionRun -> SelectionOrderBuilder -> broker order` 等路径。 | 后续 guardrail 必须扫描和测试这个禁令，不得在新路径中重开。 |
| `docs/architecture/simulation_remediation_project_design_20260521.md:93` | 缺少 StrategyPackage -> Daily Selection -> Target/Rebalance -> ValidatedExecutionPolicy -> MiniQMT ManagedOrder 执行桥。 | 本设计把“执行桥”升级为“唯一 MiniQMTExecutionRuntime”，避免桥只是又一层旁路。 |
| `docs/architecture/simulation_remediation_project_design_20260521.md:170` | Shared Decision Engine 负责 selection、target、rebalance、execution plan。 | 本设计把 Shared Decision Engine 输出明确改为 broker-neutral `AlphaSignalBook` / target / plan，执行层再转 broker intent。 |
| `docs/architecture/simulation_remediation_project_design_20260521.md:213` | 共享链路最后一个统一产物是 `ExecutionPlan`。 | 仍保留 `ExecutionPlan`，但 MiniQMT 侧必须继续进入长期运行的 event/algo/OMS runtime，不再把 `ExecutionPlan -> submit_batch` 作为终点。 |
| `docs/architecture/simulation_remediation_project_design_20260521.md:640` | MiniQMT 必须禁止 `SelectionOrderBuilder`，所有订单来自 shared ExecutionPlan。 | 本设计新增：所有订单还必须可追溯到 `execution_runtime_id`、`algo_instance_id`、`strategy_slot_id`。 |
| `docs/architecture/miniqmt_execution_priority_and_qe_migration_design_20260529.md:15` | 后续设计必须在主体章节明确复用 vnpy_algotrading 源码语义和状态机，不能泛泛参考。 | 本设计新增第 4 章 vn.py 参考矩阵，作为后续实现验收规格。 |
| `docs/architecture/miniqmt_execution_priority_and_qe_migration_design_20260529.md:88` | 不直接整体接入 vn.py runtime，而是复制/改造成熟算法 core 与状态机。 | 本设计采用“AIstock 唯一 runtime + vn.py-derived component semantics”，不允许独立 vn.py 服务。 |
| `docs/architecture/vnpy_execution_source_inventory_20260529.md:21` | Sniper/BestLimit/TWAP 应最大化直接复用 vnpy_algotrading 代码语义和结构。 | 后续算法 issue 必须按该 inventory 的 file-level mapping 和 attribution gate 执行。 |
| `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md:498` | 已识别单策略和多策略是两条执行路径。 | 本设计升级为强制唯一 runtime：单策略只能是 N=1 slot。 |
| `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md:500` | 已识别 vn.py-style 执行策略只在 Paper v2 path 生效的问题。 | 本设计要求 Paper v2、simulation_runtime、operator command 均调用同一 algo runtime。 |
| `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md:603` | 旧路径迁移策略是先添加 unified path，完整验证前不删除 legacy。 | 本设计沿用“不提前删除”，但要求 legacy 不再作为产品执行入口。 |

### 2.2 AIstock 当前代码事实

| 当前事实 | 代码位置 | 架构含义 |
|---|---|---|
| 当前 shared adapter 明确说明不导入 vn.py EventEngine/MainEngine/gateway。 | `backend/services/trading_core/miniqmt_vnpy_execution.py:1` | 这只是 vn.py-style adapter，不是完整事件驱动执行 runtime。 |
| `UnifiedMiniQMTVnpyExecutionAdapter.execute_intent()` 当前按 `start -> update_tick -> update_timer` 一次性生成动作。 | `backend/services/trading_core/miniqmt_vnpy_execution.py:199` | 缺长期 event loop、真实 broker order/trade callback 驱动、EOD tail policy。 |
| simulation_runtime 的 `MiniQMTExecutionBridge` 可以在 plan 上识别 vn.py-style policy。 | `backend/services/simulation_runtime/bridges.py:131` | 已有向统一执行靠拢的基础，但 bridge 仍是一次性建请求并 `submit_batch`。 |
| `QmtManagedOrderSubmitter.cancel_child()` 在 batch preflight 前没有 broker order 可撤。 | `backend/services/simulation_runtime/bridges.py:381` | 这不是 vn.py 式真实 active order cancel/replace，只是生成 managed request 的 preview 阶段。 |
| `submit_plan()` 最终直接调用 `QmtManagedOrderService.submit_batch()`。 | `backend/services/simulation_runtime/bridges.py:188` | 订单生命周期由 batch submit 主导，缺 algo instance 级事件持久化。 |
| Paper v2 day runner 对 `portfolio.broker_backend == "minqmt_sim"` 进入 `_run_minqmt_sim_orders()`。 | `backend/services/paper_trading_v2/day_runner.py:527` | Paper v2 仍保留自己的 MiniQMT 执行入口。 |
| Paper v2 MiniQMT 路径可选择 vn.py-style adapter。 | `backend/services/paper_trading_v2/day_runner.py:1148` | 说明 Paper v2 和 simulation_runtime 已有重复适配逻辑，未来应合并到 runtime。 |
| Paper v2 `_run_minqmt_vnpy_style_intent()` 构造 `MiniQMTLiveAlgoAdapter`。 | `backend/services/paper_trading_v2/day_runner.py:1424` | 这是另一套 adapter owner，未来必须退化为 runtime client。 |
| `MiniQMTSimBackend.bind_capacity()` 当前 account-group 返回 `max_concurrent_packages=64`，legacy 返回 1。 | `backend/services/paper_trading_v2/broker/minqmtsim.py:603` | 与“策略数量只由资金容量决定”冲突；后续必须删除固定数量门槛。 |
| vn.py-style registry 已列出 `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT`。 | `backend/execution_algos/vnpy_style/registry.py:124` | 这些可作为第一批 MiniQMT 执行算法资产，但需要 runtime 化和 parity tests。 |
| `QmtManagedOrderService` 是 MiniQMT managed order broker-calling boundary。 | `backend/services/qmt_strategy_ledger/order_service.py:44` | 未来应成为 runtime OMS/Gateway 的下单子系统，不应被多个产品路径直接绕用。 |

### 2.3 vn.py / vnpy_algotrading 源码基线

本设计使用固定提交作为后续实现的源码规格，不允许只写“参考 vn.py”：

| 上游项目 | 固定提交 | 用途 |
|---|---|---|
| `vnpy/vnpy` | `1b78494979deb4c4996f6b864f234d9839f2f239`（2026-05-17） | `MainEngine`、`EventEngine`、`BaseGateway`、`OmsEngine` 职责边界。 |
| `vnpy/vnpy_ctastrategy` | `6ef76981624bf55b2ea978f8587f74d633aafc72`（2026-05-06） | CTA strategy engine 的事件注册、order-id 到 strategy 映射、strategy template 的 buy/sell/cancel_all 语义。 |
| `vnpy/vnpy_algotrading` | `4133987530eb28f3538d1983545d81c4f83d7d59`（2025-06-13；本仓已有 inventory） | Sniper、BestLimit、TWAP、AlgoTemplate、AlgoEngine 的可移植执行算法语义。 |

后续实现 issue 必须在 PR body 中保留以上提交或声明更新后的固定提交，并说明 diff 风险。不能引用浮动 `master/main` 作为验收依据。
## 3. 不可变硬规则

### 3.1 唯一路径规则

1. MiniQMT 产品执行入口只有一个：`MiniQMTExecutionRuntime`。
2. Paper v2 MiniQMT、simulation_runtime MiniQMT、多策略包 MiniQMT、单策略 MiniQMT、operator 清仓/撤单/重置都只能调用 runtime API。
3. `QmtManagedOrderService.submit_batch()`、`XtQuantQMTClient.place_order()`、raw `/qmt/order` 不得被策略产品路径直接调用。
4. 历史 `MiniQMTSimBackend`、Paper v2 day runner、qmt_strategy router 可以保留为 compatibility layer，但只能转发到 runtime 或只读展示，不得各自实现交易语义。
5. 若 runtime 不可用，应 fail-fast，并返回 `RUNTIME_UNAVAILABLE` / `EXECUTION_PATH_NOT_CANONICAL`，不能静默 fallback 到旧路径。

### 3.2 Alpha 与执行隔离规则

`AlphaSignalBook` 是策略包/信号层的唯一输出。它可以表达：

- 信号日期、as-of、cutoff、source package/release、signal hash。
- 买入候选、卖出候选、目标权重、rank、score、置信度、替代候选、原因。
- alpha 侧的行业/风格/风险暴露标签，供执行层做组合约束。

`AlphaSignalBook` 明确禁止字段：

```text
broker_account_id
account_group_id
strategy_name
order_remark
qmt_order_id
order_type
price_type
limit_price
execution_algo_code
execution_policy_id
execution_policy_sha256
tail_policy_id
cash_freeze
position_lot
available_quantity
broker_can_sell
MiniQMT native status/raw packet
```

这些字段全部属于 execution/binding/OMS/ledger 层。alpha 层变化不得修改执行层状态机；执行策略变化也不得污染 alpha signal artifact。

### 3.3 策略数量规则

1. `strategy_slot_count` 没有固定上限。
2. 能运行几个策略只由以下条件决定：account group 可用资金、每个 slot 的最小资金、最小买入 lot、费用模型、A 股交易规则、T+1、涨跌停、停牌、可卖数量、用户显式资金分配或风险预算。
3. 不允许出现产品硬门槛：`max_concurrent_packages=1`、`max_concurrent_packages=2`、`max_concurrent_packages=64`、`if package_count > N reject`。
4. 资金不足时只允许产生 slot/intent 级显式状态：`SKIPPED_INSUFFICIENT_CAPITAL`、`PARTIAL_CAPITAL_ALLOCATED`、`MIN_LOT_NOT_REACHED`、`SELL_PROCEEDS_REQUIRED` 等。

### 3.4 V25 边界规则

1. 本设计不改造 V25 MiniQMT live 化。
2. `V25_TWO_STAGE` / `V25_1_SMALL_CAP` 当前保留在 QE / LocalSim minute execution / 回测语义中。
3. MiniQMT broker execution policy 近期只能接受 `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT` 这类 vnpy-style asset 或后续经同等流程移植的 MiniQMT asset。
4. 若 MiniQMT path 收到 `V25_*`，必须 fail-fast：`MINIQMT_UNSUPPORTED_EXECUTION_ALGO`，不能降级 TWAP，不能用最新价整笔提交。

## 4. vn.py 参考矩阵：哪些必须复用，哪些不能复制

### 4.1 vn.py framework 组件映射

| vn.py 组件 | 上游源码定位 | 必须保留的逻辑/职责 | AIstock 目标组件 | 禁止做法 |
|---|---|---|---|---|
| `MainEngine` | `https://github.com/vnpy/vnpy/blob/1b78494979deb4c4996f6b864f234d9839f2f239/vnpy/trader/engine.py#L81`；`send_order` 在 `#L254` | 统一持有 event engine、gateway、engine/service；订单必须通过 engine/gateway boundary。 | `MiniQMTExecutionRuntime`：唯一 runtime owner，持有 EventDispatcher、MiniQMTGateway、OMS、AlgoEngine、RiskEngine、TailPolicyEngine。 | 在 Paper v2 / simulation_runtime / router 各自创建 broker 下单逻辑。 |
| `EventEngine` | `https://github.com/vnpy/vnpy/blob/1b78494979deb4c4996f6b864f234d9839f2f239/vnpy/event/engine.py#L33`；`put` 在 `#L105`，`register` 在 `#L111` | 事件队列、注册 handler、启动/停止；订单/成交/行情/定时器统一进入事件通道。 | `MiniQMTExecutionEventLoop`：持久化事件分发，支持 tick/order/trade/timer/reconcile/eod/operator command。 | 只在 submit 时同步 query 一次，把 reconcile 当事件循环替代品。 |
| `BaseGateway` | `https://github.com/vnpy/vnpy/blob/1b78494979deb4c4996f6b864f234d9839f2f239/vnpy/trader/gateway.py#L33`；`on_tick/on_trade/on_order/send_order` 在 `#L93/#L101/#L109/#L197` | gateway 是真实柜台事实的入口/出口，负责发单、撤单、推送 order/trade/tick。 | `MiniQMTGateway`：封装 XtQuant/MiniQMT native submit/cancel/query/callback，保留 raw packet。 | 在算法 core 或 StrategyPackage 内直接调用 `XtQuantQMTClient.place_order`。 |
| `OmsEngine` | `https://github.com/vnpy/vnpy/blob/1b78494979deb4c4996f6b864f234d9839f2f239/vnpy/trader/engine.py#L360`；事件注册在 `#L384` | 订单、成交、持仓、账号事实集中管理并通过事件更新。 | `MiniQMTOmsLedger`：以 `qmt_strategy` 表为 durable OMS，维护 active/terminal order、trade、position lot、cash、cost、reconcile。 | 用 run status 或 batch status 代替订单/成交事实。 |
| `CtaEngine` | `https://github.com/vnpy/vnpy_ctastrategy/blob/6ef76981624bf55b2ea978f8587f74d633aafc72/vnpy_ctastrategy/engine.py#L69`；`orderid_strategy_map` 在 `#L88`，`send_order` 在 `#L466`，`cancel_all` 在 `#L510` | strategy 与 order id 的归因映射、事件注册、统一 send/cancel。 | `StrategySlotExecutionEngine`：`strategy_slot_id/order_intent_id/algo_instance_id/native_order_id` 映射。 | 用 `strategy_name/order_remark` 字符串拼接作为唯一归因，不落库 mapping。 |
| `CtaTemplate` | `https://github.com/vnpy/vnpy_ctastrategy/blob/6ef76981624bf55b2ea978f8587f74d633aafc72/vnpy_ctastrategy/template.py#L113`；`buy/sell/send_order/cancel_all` 在 `#L143/#L164/#L227/#L255` | 模板把策略回调和 buy/sell/cancel 封装在 engine boundary。 | 只借鉴 execution algo template 语义，不把 alpha 策略写成 CtaTemplate。 | 把 alpha 信号层和 broker buy/sell 混在一个 StrategyPackage 类里。 |
| `AlgoEngine` | `https://github.com/vnpy/vnpy_algotrading/blob/4133987530eb28f3538d1983545d81c4f83d7d59/vnpy_algotrading/engine.py#L33`；事件注册在 `#L79`，order/trade/tick/timer routing 在 `#L86/#L102/#L111/#L94`，order-id mapping 在 `#L226` | 对 tick/timer/order/trade 分发到 owning algo；order id 到 algo 映射。 | `MiniQMTAlgoRuntime`：每个 parent intent 生成一个或多个 `ExecutionAlgoInstance`，通过 order-id mapping 接收事件。 | `execute_intent()` 一次性跑完就丢失 algo instance 状态。 |
| `AlgoTemplate` | `https://github.com/vnpy/vnpy_algotrading/blob/4133987530eb28f3538d1983545d81c4f83d7d59/vnpy_algotrading/template.py#L13`；`update_tick/order/trade/timer` 在 `#L49/#L54/#L63/#L71`，`buy/sell/cancel_all` 在 `#L129/#L154/#L183` | active order 维护、成交累计、timer 驱动、只有 running 状态才执行 tick/timer。 | `VnpyDerivedAlgoCore` + `ExecutionAlgoInstance`。core 生成 action，runtime 负责 submit/cancel/query/persist。 | core 里直接访问 DB、MiniQMT、FastAPI、StrategyPackage。 |

### 4.2 vnpy_algotrading 算法移植规格

| 算法 | 上游源码定位 | 必须保留的行为 | AIstock 适配 | 验收测试 |
|---|---|---|---|---|
| Sniper | `https://github.com/vnpy/vnpy_algotrading/blob/4133987530eb28f3538d1983545d81c4f83d7d59/vnpy_algotrading/algos/sniper_algo.py#L8`；核心 `on_tick` 在 `#L36` | 有 active order 时先撤单并返回；买入只在一档卖价不高于保护价时提交；卖出只在一档买价不低于保护价时提交；成交量不超过剩余量与盘口一档量。 | `SNIPER_MINIQMT` core 只输出 submit/cancel action；MiniQMTGateway 负责真实发单；A 股 lot/涨跌停/撤单频率在 runtime risk gate 处理。 | `active order -> cancel_all no submit`；`ask_price_1 <= price -> buy`；`bid_price_1 >= price -> sell`；盘口量限制。 |
| BestLimit | `https://github.com/vnpy/vnpy_algotrading/blob/4133987530eb28f3538d1983545d81c4f83d7d59/vnpy_algotrading/algos/best_limit_algo.py#L10`；`on_tick` 在 `#L60`，order terminal reset 在 `#L84` | 无 active order 时买挂一档买价/卖挂一档卖价；盘口价变化时撤单；terminal order 清空 active order 和 order price；子单量在 min/max 随机范围和剩余量内。 | `BEST_LIMIT_MINIQMT` 使用可注入 deterministic random provider 以保证回放可复现；runtime 处理 cancel/replace 和最大撤单次数。 | `no active long -> bid_price_1 submit`；`no active short -> ask_price_1 submit`；quote change -> cancel_all；terminal clears state。 |
| TWAP | `https://github.com/vnpy/vnpy_algotrading/blob/4133987530eb28f3538d1983545d81c4f83d7d59/vnpy_algotrading/algos/twap_algo.py#L9`；初始化在 `#L39`，`on_timer` 在 `#L62` | `order_volume = volume / (time / interval)`；timer_count 和 total_count 每秒推进；总时间结束 finish；未到 interval 不提交；到 slice 前先 cancel_all；买入需 ask 不高于 price，卖出需 bid 不低于 price。 | `TWAP_LITE_MINIQMT` 使用 AIstock board-lot rounding 和交易时间窗；timer 由 runtime 事件循环驱动，不由同步 for-loop 伪造。 | slice 数量、timer 计数、interval gate、finish-on-time、cancel before slice、quote guard。 |
| Iceberg | `vnpy_algotrading/algos/iceberg_algo.py`（inventory only） | display volume、interval、quote cross cancel/reprice。 | 未来资金规模或单票参与率需要时再移植。 | 不是第一阶段验收项。 |
| Stop | `vnpy_algotrading/algos/stop_algo.py`（inventory only） | 条件触发、terminal handling。 | 未来做条件单/止损策略时纳入。 | 不是第一阶段验收项。 |

### 4.3 “复用 vn.py”在本项目中的精确定义

后续实现 PR 只有满足以下条件，才能写“复用 vn.py 逻辑”：

1. PR body 或 issue context pack 写明上游 repo、commit、file path、license。
2. 复制或实质派生的 AIstock 文件有 header 或集中 attribution mapping。
3. 每个移植行为都有 characterization test，测试名能看出对应上游行为。
4. 如果因 A 股规则偏离上游，必须写偏离原因，例如 board lot、涨跌停、T+1、撤单上限、MiniQMT status 57、卖出 proceeds 释放时点。
5. 算法 core 不导入 vn.py runtime、MiniQMT client、DB、FastAPI；core 只输出 action，由 runtime 执行。
6. runtime 的事件顺序必须与 vn.py 组件语义一致：tick/timer/order/trade 都进入 event loop；order/trade 通过 order-id mapping 路由到 owning algo instance；terminal 事件更新 OMS 后再驱动 algo 状态。
## 5. 目标分层架构

```text
Layer A: Alpha Signal Layer（完全 broker-neutral）
  - StrategyPackage alpha core
  - DailySelectionEvidence / AlphaSignalBook
  - signal hash / rank / score / target preference / sell preference
  - 禁止 broker/order/execution state

Layer B: Runtime Binding Layer（仍不下单）
  - StrategyRuntimeRelease
  - AccountGroupBinding / StrategySlotBinding
  - capital allocation / risk budget / execution policy activation
  - binding_hash / policy_hash / tail_policy_hash

Layer C: MiniQMTExecutionRuntime（唯一产品执行链路）
  - EventLoop / Gateway / OMS / AlgoRuntime / RiskEngine / TailPolicyEngine
  - StrategySlotTargetEngine
  - RebalanceIntentEngine
  - ExecutionPlanCompiler
  - CapacityAllocator（funds-only）
  - OperatorCommandEngine

Layer D: MiniQMTGateway + qmt_strategy OMS/Ledger
  - XtQuant/MiniQMT native submit/cancel/query/callback
  - order/trade/status/cash/position lot/cost/reconciliation
  - raw packet and diagnostic persistence

Layer E: Projection / UI / Ops
  - Paper v2 dashboard
  - strategy slot PnL / position / error / order diagnostic
  - unattended scheduler status
  - L0-L5 validation evidence
```

### 5.1 AlphaSignalBook 合同

建议数据结构：

```text
AlphaSignalBook
  signal_book_id
  trade_date
  as_of
  cutoff_date
  alpha_source_type: strategy_package | advisory_book | manual_alpha_upload | future
  source_ids[]
  alpha_core_hashes[]
  runtime_profile_hash
  signal_hash
  items[]
  metadata: broker-neutral only

AlphaSignalItem
  symbol
  action_preference: BUY | SELL | HOLD | AVOID
  target_weight_hint
  target_rank
  score
  confidence
  replaceable_group_id
  reason_codes[]
  alpha_tags[]
```

`AlphaSignalBook` 不表达“买 1000 股 10.23 元限价单”，只表达“希望提高/降低某标的 exposure”。具体 quantity、price、child order、tail retry 都由 execution runtime 根据资金、持仓、交易规则、执行策略决定。

### 5.2 MiniQMTExecutionRuntime 合同

```text
MiniQMTExecutionRuntime
  execution_runtime_id
  account_group_id
  mode: SIM | LIVE_PENDING_APPROVAL | LIVE
  trade_date
  event_loop_state
  gateway_state
  oms_state
  active_strategy_slots[]
  active_algo_instances[]
  runtime_config_hash
```

核心输入：

- `AlphaSignalBook[]`：来自一个或多个 alpha source。
- `AccountGroupBinding`：MiniQMT account / mode / cash source / slot allocation。
- `ExecutionPolicyActivation`：MiniQMT 可用算法及参数。
- `TailHandlingPolicy`：未成交/尾盘/撤单/追单/隔日释放策略。
- `OperatorCommand`：清仓、撤单、重置信号、停用 slot 等。

核心输出：

- `StrategySlotTarget`：每个 slot 的目标持仓。
- `RebalanceIntent`：每个 slot 的买/卖/保留/跳过意图。
- `MiniQMTExecutionPlan`：带 hash/idempotency 的执行计划。
- `ExecutionAlgoInstance`：每个 parent intent 的算法实例。
- `ChildOrder`：由 algo action 转出的 broker 子单。
- `OrderEvent` / `TradeEvent` / `ReconcileEvent` / `TailEvent`。
- `StrategySlotLedgerProjection`：按策略 slot 的持仓、现金、PnL、成本、可卖量。

### 5.3 OperatorCommand 合同

保留 AIstock 侧可以直接连接 MiniQMT 执行订单的能力，但必须变成 runtime 命令，而不是 raw broker 下单路径：

| 命令 | 用途 | 必须经过的 runtime 子系统 |
|---|---|---|
| `CANCEL_ALL_OPEN_ORDERS` | 撤销 MiniQMT 当前账号组所有 open orders。 | Gateway query/cancel、OMS terminalization、strategy slot attribution。 |
| `FLATTEN_ALL_POSITIONS` | 清空所有 broker 持仓，用于换新 alpha 前重置。 | Position sync、sellable lot/T+1、risk gate、sell-first execution policy、tail retry。 |
| `FLATTEN_STRATEGY_SLOT` | 只清某个策略 slot 的 AIstock 归因持仓。 | Strategy lot ledger、broker can_sell aggregation、same-stock conflict resolver。 |
| `RESET_STRATEGY_SLOT` | 停用并重置 slot 状态，不直接乱改 broker。 | Cancel open orders、terminalize intents、position/cash settlement snapshot。 |
| `REPLACE_ALPHA_SIGNAL_BOOK` | 更换某 slot 的 alpha source。 | 生成新的 signal_book_id 和 target plan，不修改 execution runtime 代码。 |

operator 命令必须像策略订单一样持久化 `operator_command_id`、原因、审批人、raw broker 结果、终态，不允许“手工调用 raw qmt 下单后再人工解释”。

## 6. 关键状态机设计

### 6.1 交易日 lifecycle

```text
PREPARE
  -> load account group / slots
  -> load alpha signal books
  -> validate trading calendar / suspend / limit / T+1 source
  -> sync broker orders/trades/positions
  -> terminalize stale previous-day orders
  -> build targets and rebalance intents
  -> compile execution plan

OPEN_EXECUTION
  -> create algo instances
  -> subscribe ticks/timers
  -> send child orders through MiniQMTGateway
  -> process order/trade callbacks or query snapshots
  -> retry/cancel/replace under TailHandlingPolicy

MIDDAY_RECONCILE
  -> sync broker facts
  -> update slot ledger and cash
  -> preserve submit failures; reconcile success cannot erase submit failure
  -> decide retry or hold

TAIL_HANDLING
  -> cancel stale open orders if policy requires
  -> reprice/retry remaining sell/buy if policy permits
  -> mark unfilled residual with explicit reason

EOD_FINALIZE
  -> sync final broker facts
  -> terminalize today orders
  -> release stale cash freezes
  -> persist slot PnL/cost/position projection
  -> produce validation evidence packet
```

### 6.2 成功状态不能被误用

`SimulationDailyRun` / session / slot / plan / batch / order 各自有独立状态。不得出现“reconcile 成功覆盖 submit 失败”：

| 层级 | 成功含义 | 不能代表 |
|---|---|---|
| runtime tick 成功 | 本次事件处理没有崩溃。 | 不代表有信号、不代表已下单、不代表已成交。 |
| execution plan 成功 | 目标和意图已编译并持久化。 | 不代表 broker 接受订单。 |
| submit batch 成功 | broker 接受了部分或全部 child orders。 | 不代表全部成交。 |
| reconcile 成功 | broker facts 查询/同步完成。 | 不代表 submit 失败已修复，不代表订单交易完成。 |
| no rebalance | 目标持仓与现有 slot 持仓确实一致且有 evidence。 | 不代表信号缺失、历史 target 复用、selection 过期。 |

### 6.3 重试语义

1. deterministic plan/batch 的失败结果不能被作为“成功缓存”复读。
2. retry 时要区分：
   - `PRE_SUBMIT_FAILURE`：未触达 broker，可重建同一 deterministic plan 并修复输入后提交。
   - `BROKER_REJECTED`：已触达 broker，必须记录 native rejection，按 policy 决定是否修正价格/数量/资金后新 child order。
   - `PARTIAL_ACCEPTED`：部分 broker order 已存在，retry 只能处理 residual，不能重复已接受数量。
   - `UNKNOWN_AFTER_TIMEOUT`：先 query/sync，再决定补单或终止。
3. retry key 必须包括 `execution_runtime_id`、`plan_id`、`intent_id`、`algo_instance_id`、`child_order_seq`、`trade_date`。
4. 同一 native `qmt_order_id` 不允许被两个 slot 或两个 algo instance 认领。

### 6.4 换仓资金模型

全仓换股或资金紧张时，runtime 必须支持 SELL-first 和卖出 proceeds 参与预检：

```text
current holdings + target holdings
  -> classify sells first
  -> broker can_sell / T+1 check
  -> estimate sell proceeds with price guard and fee model
  -> allocate buy cash from available cash + allowed same-batch sell proceeds
  -> if sell order unfilled, dependent buy order stays WAITING_FOR_SELL_PROCEEDS or scaled/skipped
```

买入失败不能把卖出成功回滚；卖出未成交不能假装 cash 已释放；当日无法完成的 residual 要进入 tail/eod policy，而不是让第二天继续保留 AIstock stale open order。
## 7. 单策略与多策略统一方式

### 7.1 StrategySlot 是唯一策略运行单位

```text
AccountGroup
  account_group_id
  broker_account_id
  mode
  total_cash_snapshot
  slots[]

StrategySlot
  strategy_slot_id
  alpha_source_id / signal_book_id
  capital_budget
  target_risk_budget
  slot_state
  slot_ledger_id
```

- 单策略 = 一个 account group 下一个 slot。
- 多策略 = 同一个 account group 下多个 slot。
- 不允许另建 `exclusive_account` 产品路径；legacy exclusive 只能作为旧数据兼容 wrapper。
- slot 数量不被代码硬门槛限制；slot 是否能运行由 capital allocator 和 risk gate 判断。

### 7.2 同股票多策略持仓

同一个 broker account 下多个 slot 可能持有同一股票：

- Broker authority 只知道合并持仓和合并可卖量。
- AIstock authority 维护 strategy slot lot、cost、T+1、realized/unrealized PnL。
- Sell 冲突按策略 slot 的可卖 lot 和 operator policy 分配。
- Broker callback 中没有 slot 信息时，通过 `order_intent_id/algo_instance_id/order_remark/native_order_id` mapping 反查归因。

### 7.3 Alpha 源替换

未来策略包逐渐淘汰或换新 alpha source 时：

1. 新 alpha source 只生成新的 `AlphaSignalBook`。
2. Runtime 不因 alpha source 类型变化而改代码；只校验 signal contract。
3. 若需要清空旧仓位，operator 先提交 `FLATTEN_ALL_POSITIONS` 或 `FLATTEN_STRATEGY_SLOT`。
4. 清仓完成后再 `REPLACE_ALPHA_SIGNAL_BOOK`，生成新的 target/plan。

## 8. 当前多路径收敛方案

### 8.1 未来唯一产品路径

```text
Paper v2 UI / scheduler
simulation_runtime scheduler
MiniQMT multi-strategy package runner
operator command API
        |
        v
MiniQMTExecutionRuntime API
        |
        v
MiniQMTGateway + qmt_strategy OMS/Ledger
        |
        v
XtQuant / MiniQMT SIM or approved LIVE
```

### 8.2 旧路径处理

| 当前路径 | 未来处理 | 删除条件 |
|---|---|---|
| `backend/services/paper_trading_v2/day_runner.py::_run_minqmt_sim_orders` | 降级为 compatibility wrapper：构造 `AlphaSignalBook` / runtime request 后调用 `MiniQMTExecutionRuntime`。 | L0-L5 全通过、UI/API 已切换、用户确认后单独 chore 删除旧执行分支。 |
| `backend/services/simulation_runtime/bridges.py::MiniQMTExecutionBridge` | 保留为 runtime client/adapter，不再自己 build child order semantics。 | runtime API 覆盖 plan submit/preview/reconcile 后，桥只做 DTO 转换。 |
| `backend/services/trading_core/miniqmt_vnpy_execution.py` | 算法 core 可保留，但 `execute_intent()` 同步 for-loop 不能作为生产 lifecycle；改为 runtime-owned algo instance。 | 迁移到 event-driven instance 后，同步 helper 只保留为 characterization/fake broker 测试。 |
| `backend/services/qmt_strategy_ledger/order_service.py` | 作为 OMS/Gateway 下单与 ledger 子系统保留。 | 不删除；但产品路径不得直接绕 runtime 调它下单。 |
| `backend/routers/qmt_strategy_ledger.py` managed order 手工接口 | 改为 admin/operator command 或 read-only diagnostic，默认不开放策略下单。 | operator command runtime 化后关闭手工 submit。 |
| `backend/routers/qmt.py` raw order | 仅保留 emergency/admin 审批路径或禁用；不得用于模拟盘策略执行。 | 有安全替代命令后默认禁用 raw submit。 |

## 9. 验收场景矩阵

| 场景 | 期望行为 | 必须验证的风险 |
|---|---|---|
| N=1 单策略 | 通过同一 runtime 创建 1 个 slot，走同一 signal/target/algo/OMS。 | 无单独 day_runner 私有下单语义。 |
| N>1 多策略 | 多个 `AlphaSignalBook` 分配到多个 slot，同一 runtime 统一资金、可卖量、订单归因。 | 不因 slot 数量触发固定上限。 |
| 资金不足 | 只阻断具体 slot/intent，记录 `SKIPPED_INSUFFICIENT_CAPITAL`。 | 不把整个 account group 判失败；不静默空订单成功。 |
| 最小 lot 不足 | 该 symbol/slot 标记 `MIN_LOT_NOT_REACHED`。 | 不生成无效 0 股/非整手订单。 |
| 同股票多 slot 买入 | risk/capacity 合并预检，order/trade 分 slot 归因。 | 不超 account cash；不丢 slot attribution。 |
| 同股票多 slot 卖出 | 按 slot lot 和 broker can_sell 分配可卖量。 | 不超 broker account-level can_sell。 |
| 全仓换股 | SELL-first；buy 依赖 sell proceeds 或缩放。 | 虚拟现金低时不能永远买不出去，也不能提前假设卖出成交。 |
| 卖单挂单未成交 | 按 tail policy 撤单/重挂/终止 residual。 | 第二天 AIstock 不保留 stale open order；先同步 broker 后释放。 |
| 买单未成交 | 按 policy 决定当日重挂、尾盘取消、或 residual skipped。 | 未成交不能算调仓成功。 |
| submit 超时 | 先 query broker，进入 `UNKNOWN_AFTER_TIMEOUT`。 | 不能重复提交造成双单。 |
| broker reject/status 57 | 保存 raw packet、资金快照、policy、child order diagnostic。 | 不返回空数组伪成功。 |
| broker disconnect | runtime 停止新下单，进入 reconcile-only/retry window。 | 重连后不重复已接受订单。 |
| 后端重启 | 从 durable OMS/algo state 恢复，先 sync broker facts。 | 不重复 deterministic batch；不丢 active order。 |
| operator 清仓 | `FLATTEN_ALL_POSITIONS` 进入同一 runtime。 | 不绕开 strategy slot ledger；不绕开风险/交易规则。 |
| 更换 alpha source | 只替换 signal book，执行层代码不变。 | alpha 改动不影响 gateway/algo/OMS。 |
| 切换执行算法 | 只创建新 execution policy activation。 | 不修改 StrategyPackage，不修改 signal book。 |
| 非交易日/收盘后 | 不提交新 broker order，可执行 reconcile/terminalize。 | 不因无 tick 伪造成交。 |

## 10. 分阶段实施计划

### Phase 0：冻结设计和 issue epic

写入范围：docs / issue workflow metadata。

- 以本文作为 P0 architecture epic 设计基线。
- 通过 `scripts/aistock_issue_workflow.py` 创建 MiniQMT 唯一执行链路 epic 和子 BUG/feature issues，同步 GitHub。
- 每个 issue context pack 必须引用本文第 3、4、9、11 章。
- 标记旧文档冲突项：固定 capacity、exclusive account product path、Paper v2 私有 MiniQMT 执行分支均被本文 supersede。

验收：文档合入；issue/GitHub 同步；无生产变更。

### Phase 1：合同和禁用门禁

写入范围：contracts/models/tests，少量 router/service guard。

- 新增 `AlphaSignalBook` / `AlphaSignalItem` 合同和 forbidden-field negative tests。
- 新增 `MiniQMTExecutionRuntimeRequest` / `OperatorCommand` / `StrategySlotTarget` DTO。
- 所有 MiniQMT 产品执行入口检测 canonical runtime gate。
- MiniQMT path 收到 `V25_*` fail-fast。
- `max_concurrent_packages` 不再作为 account group slot 数量限制；保留 legacy metadata 只读兼容。

验收：

- `test_miniqmt_signal_contract.py`
- `test_miniqmt_path_uniqueness.py`
- `test_miniqmt_rejects_v25_broker_execution.py`
- grep guard：`broker_account_id` 不出现在 `AlphaSignalBook` payload。

### Phase 2：durable runtime skeleton

写入范围：`backend/services/miniqmt_execution_runtime/`、repository/migrations、tests。

- 实现 `MiniQMTExecutionRuntime`、`MiniQMTExecutionEventLoop`、`MiniQMTGateway` interface、`MiniQMTOmsLedger` interface。
- 先用 fake broker 跑 L2/L3，不接生产 MiniQMT。
- 建表或扩展字段（如需要）：`execution_runtime`、`execution_algo_instance`、`execution_event`、`operator_command`、`child_order` mapping。
- runtime state 可恢复：active order、algo instance、pending tail actions 都可从 DB 重建。

验收：

- `test_miniqmt_execution_runtime_event_loop.py`
- `test_miniqmt_runtime_restart_recovery.py`
- `test_miniqmt_event_ordering.py`
- production DDL gate 单独记录，不在代码合入后遗漏。
### Phase 3：vn.py-derived algo instance 化

写入范围：`backend/execution_algos/vnpy_style/`、runtime algo adapter、tests。

- 将当前同步 `execute_intent()` 能力迁为 runtime-owned `ExecutionAlgoInstance`。
- Sniper/BestLimit/TWAP 继续使用 `vnpy_algotrading` 固定 commit 的源码语义。
- 增加上游行为 characterization tests；若有偏离，测试名与 migration note 写明原因。
- `timer` 不再用一次性 for-loop 伪造，改由 runtime event loop 驱动。

验收：

- `test_miniqmt_vnpy_algo_parity_sniper.py`
- `test_miniqmt_vnpy_algo_parity_best_limit.py`
- `test_miniqmt_vnpy_algo_parity_twap.py`
- import boundary：algo core 不导入 DB、FastAPI、MiniQMT、vn.py runtime。

### Phase 4：Paper v2 / simulation_runtime 收敛到 runtime

写入范围：Paper v2 MiniQMT adapter、simulation_runtime bridge、qmt_strategy router guards、tests。

- Paper v2 `_run_minqmt_sim_orders()` 改为 runtime client。
- simulation_runtime `MiniQMTExecutionBridge` 改为 runtime client，不再自建 broker child order semantics。
- qmt_strategy managed order submit router 下沉为 admin-only 或 runtime internal API。
- raw QMT order router 不再是模拟盘策略入口。

验收：

- 单策略 N=1 和多策略 N>1 用同一 runtime evidence。
- grep guard：产品路径不得直接调用 `XtQuantQMTClient.place_order` 或 `QmtManagedOrderService.submit_batch`。
- `test_miniqmt_single_multi_same_runtime.py`

### Phase 5：资金容量和 SELL-first 模型

写入范围：capacity allocator、risk/preflight、cash/lot ledger、tests。

- 删除固定 slot 数量门槛。
- 资金不足变为 slot/intent 状态。
- SELL-first proceeds 模型进入 batch preflight 和 tail policy。
- 部分成交、卖单未成交、买单依赖卖出 proceeds 的 residual 处理持久化。

验收：

- `test_miniqmt_capacity_funds_only.py`
- `test_miniqmt_sell_first_rebalance_cash_model.py`
- `test_miniqmt_unfilled_sell_blocks_dependent_buy.py`
- `test_miniqmt_no_fixed_strategy_count_gate.py`

### Phase 6：operator command runtime 化

写入范围：operator command service/router/UI、tests。

- `FLATTEN_ALL_POSITIONS`、`CANCEL_ALL_OPEN_ORDERS`、`RESET_STRATEGY_SLOT`、`REPLACE_ALPHA_SIGNAL_BOOK` 进入 runtime。
- 所有 command 均持久化、可审计、可重启恢复。
- UI 展示 command status 和 broker raw diagnostic。

验收：

- `test_miniqmt_operator_flatten_all.py`
- `test_miniqmt_operator_cancel_all.py`
- `test_miniqmt_operator_replace_alpha_signal_book.py`
- Playwright operator flow。

### Phase 7：L0-L5 验证和 legacy 退役

写入范围：validation history、compat flags、legacy deprecation issue。

- L0 unit / static。
- L2 fake broker runtime。
- L3 unattended MiniQMT SIM stub。
- L4 dual backend / restart recovery。
- L5 real MiniQMT SIM 交易时段验证。
- 只有在全量验收和用户确认后，才允许单独 chore issue 删除 legacy 执行路径。

验收：

- 验证记录写入 `tests/aistock_validation/history/...`。
- DESIGN-COMPLIANCE-001 item-by-item。
- PR body 明确 production gates、DDL、backend/frontend dependency、restart requirement。

## 11. 反回归门禁

### 11.1 必须新增的 grep/static guard

```text
# 禁止策略产品路径绕 runtime 直接下单
rg "place_order\(" backend/services backend/routers
rg "submit_batch\(" backend/services backend/routers

# 禁止重新启用 SelectionOrderBuilder -> broker order
rg "SelectionOrderBuilder" backend/services backend/routers

# 禁止 AlphaSignalBook 带 broker/order 字段
rg "broker_account_id|order_remark|qmt_order_id|price_type|execution_algo_code" backend/services/*signal* backend/tests/*signal*

# 禁止 MiniQMT broker execution 接受 V25
rg "V25_TWO_STAGE|V25_1_SMALL_CAP" backend/services/miniqmt_execution_runtime backend/services/simulation_runtime backend/services/paper_trading_v2

# 禁止固定策略数量门槛
rg "max_concurrent_packages|package_count\s*>|strategy_count\s*>" backend/services backend/routers
```

说明：grep 不是唯一验收，但必须作为 PR preflight 的红线扫描。命中 legacy compatibility 代码时，必须有明确注释、只读/禁用 gate 和 tests 证明非产品路径。

### 11.2 必须新增或迁移的测试

| 测试 | 目的 |
|---|---|
| `test_miniqmt_signal_contract.py` | AlphaSignalBook broker-neutral forbidden fields。 |
| `test_miniqmt_path_uniqueness.py` | Paper v2、simulation_runtime、operator 均进入同一 runtime。 |
| `test_miniqmt_execution_runtime_event_loop.py` | tick/order/trade/timer/operator/reconcile 事件顺序。 |
| `test_miniqmt_vnpy_algo_parity_*.py` | Sniper/BestLimit/TWAP 与上游关键行为一致。 |
| `test_miniqmt_capacity_funds_only.py` | 策略数量由资金容量决定，无固定上限。 |
| `test_miniqmt_sell_first_rebalance_cash_model.py` | 全仓换股 SELL-first 和 proceeds 预检。 |
| `test_miniqmt_stale_order_terminalization.py` | 历史挂单第二天自动同步/释放，不保留 AIstock stale open。 |
| `test_miniqmt_submit_failure_not_overwritten_by_reconcile.py` | reconcile success 不能覆盖 submit failure。 |
| `test_miniqmt_failed_deterministic_batch_retry.py` | 失败 batch 不被缓存成复读失败/成功。 |
| `test_miniqmt_operator_flatten.py` | operator 清仓走同一 runtime 和 ledger。 |
| `test_miniqmt_rejects_v25_broker_execution.py` | V25 不进入 MiniQMT broker order。 |
| `test_miniqmt_raw_order_not_product_path.py` | raw qmt/managed manual 入口不被模拟盘使用。 |

### 11.3 验收状态定义

MiniQMT 模拟盘 run 只有在以下条件全部满足时才能标为“交易执行完成”：

1. 当日 `AlphaSignalBook` 存在且 cutoff 合法。
2. target/rebalance intent 是基于当前 slot 持仓和当日信号生成。
3. 每个 intent 有终态：`FILLED`、`PARTIAL_UNFILLED_TERMINAL`、`SKIPPED_WITH_REASON`、`REJECTED_TERMINAL`、`CANCELLED_BY_POLICY` 等。
4. 所有 broker active orders 已同步；历史 open 已 terminalized 或确认为仍有效且同日。
5. cash/position/lot/PnL projection 与 broker facts 对账。
6. submit failure、broker reject、timeout、partial fill 不被 run-level success 覆盖。

`NO_REBALANCE` 只有在目标持仓与 slot 当前持仓确实一致、且使用当日 signal/evidence 后才合法；“没有交易、没有信号”不能默认算正常。

## 12. 与旧设计的兼容和覆盖关系

| 旧文档/旧实现 | 本设计关系 |
|---|---|
| `docs/architecture/simulation_remediation_project_design_20260521.md` | 继承 shared decision、ExecutionPlan、验收矩阵；进一步强化 MiniQMT 唯一 runtime、alpha/execution 隔离、vn.py 源码规格。 |
| `docs/architecture/miniqmt_execution_priority_and_qe_migration_design_20260529.md` | 继承 MiniQMT-first、Sniper/BestLimit/TWAP 优先、不要泛泛参考 vn.py；本设计把它扩展为完整 runtime 架构。 |
| `docs/architecture/paper_v2_miniqmt_unified_autorun_design_20260602.md` | 继承 account group/slot、legacy 不提前删除、F2M 修复矩阵；本设计废止固定 capacity 和单/多策略双路径。 |
| `backend/services/paper_trading_v2/broker/minqmtsim.py::bind_capacity` | 当前固定 `max_concurrent_packages=64/1` 与本设计冲突；后续必须迁为 funds-only capacity。 |
| `UnifiedMiniQMTVnpyExecutionAdapter` | 当前作为过渡 adapter 可复用算法 core；不能被认定为最终 vn.py-like runtime。 |
| `V25_*` MiniQMT 执行诉求 | 暂时不纳入本设计实施；未来若要 live 化，必须作为 `ExecutionAlgoInstance` 的新 asset 进入同一 runtime，而不是新路径。 |

## 13. 未来 issue 拆分建议

后续不建议用一个巨型 PR 一次性改完，应建立一个 P0 epic 并按强依赖顺序拆分：

1. `BUG/EPIC: MiniQMT unique execution runtime architecture gate`：登记本文，建立 context pack、GitHub epic、legacy path inventory。
2. `P0: AlphaSignalBook broker-neutral contract`：信号合同和 forbidden field tests。
3. `P0: MiniQMTExecutionRuntime durable event loop skeleton`：runtime/event/gateway/OMS/algo instance 基础模型。
4. `P0: vn.py-derived algo instance parity`：Sniper/BestLimit/TWAP characterization tests 和 runtime adapter。
5. `P0: Paper v2 and simulation_runtime converge to runtime`：单/多策略路径收敛。
6. `P0: funds-only capacity and SELL-first cash model`：移除固定策略数量门槛。
7. `P1: operator commands through runtime`：清仓、撤单、重置、换信号。
8. `P1: stale order terminalization and tail policy`：历史挂单、未成交、EOD 策略。
9. `P1: UI/diagnostic alignment`：Paper v2 展示 runtime/slot/algo/order diagnostic。
10. `chore: legacy path deprecation`：仅在 L0-L5 和用户确认后执行。

所有 issue 必须使用 `scripts/aistock_issue_workflow.py` 创建和同步 GitHub；每个 issue 都必须说明是否触碰 MiniQMT、是否需要 DDL、是否需要后端重启、是否需要生产依赖。

## 14. DESIGN-COMPLIANCE-001 预检清单

本设计不是功能完成声明，但后续实现合入前必须逐项证明：

| 项 | 要求 | 本设计门禁 |
|---|---|---|
| 完整实现 | 不得交付简化版、POC、mock-only。 | runtime 必须覆盖 signal/target/algo/order/trade/reconcile/tail/operator。 |
| 唯一路径 | 不得保留多个产品执行路径。 | Paper v2/simulation_runtime/operator 都调用 `MiniQMTExecutionRuntime`。 |
| 设计一致 | 不得把 StrategyPackage 当运行环境快照。 | `AlphaSignalBook` broker-neutral negative tests。 |
| vn.py 复用 | 不得只保留算法名字。 | source mapping + attribution + characterization tests。 |
| 无 silent fallback | 算法/数据/资金/订单失败必须显式状态。 | `MINIQMT_UNSUPPORTED_EXECUTION_ALGO`、`SKIPPED_*`、`REJECTED_*` 等。 |
| 可恢复 | 重启不重复下单、不丢 active order。 | durable event/algo/order mapping。 |
| 资金安全 | 不超资金、不超可卖、不假设未成交 proceeds。 | funds-only capacity、SELL-first、dependent buy gate。 |
| 生产门禁 | DDL/依赖/重启状态必须报告。 | 每 PR production gates 必填。 |

## 15. 本分支交付物边界

本分支只交付本文档：

- 不修改后端代码。
- 不修改前端代码。
- 不修改数据库 schema。
- 不触碰 `.env`。
- 不启动或重启 backend/frontend/TDX/MiniQMT。
- 不关闭任何 issue。

后续实现必须基于本文创建 issue/PR，不得在未登记上下文的情况下直接开始大改。
