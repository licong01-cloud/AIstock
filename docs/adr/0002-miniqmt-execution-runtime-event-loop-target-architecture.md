# ADR 0002：MiniQMT 执行 Runtime 目标架构 —— durable 事件循环（A）为实盘目标，同步编译器（B）为模拟盘过渡

- Status: Accepted
- Date: 2026-06-23
- Supersedes: 无（细化 ADR 0001 §Trading Core 方向与 `miniqmt_unified_vnpy_execution_runtime_design_20260608.md` §4.1/§5.2）
- Owner: 战略 session（架构决策）；A 方案真实开发交独立 Codex 窗口
- 关联详细设计: `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md`（A 方案详细设计，本 ADR 的落地规格）

## 背景

`miniqmt_unified_vnpy_execution_runtime_design_20260608.md` §4.1/§5.2 要求 MiniQMT 执行链路由一个 **durable、回调驱动的 `MiniQMTExecutionRuntime` 事件循环**承载（vn.py EventEngine 语义：常驻进程整天消费券商真实 order/trade/tick 回调，runtime 自持 durable OMS）。

当前 main 的实现（经 2026-06 多轮审计与代码核实）**不是**该架构，而是一个**同步 per-tick 编译器（下称 B）**：
- `backend/services/miniqmt_execution_runtime/client.py:486-488`：按 intent 实例化 runtime，`on_tick` 喂一次提交时快照 + `for index in range(_timer_iterations): on_timer(...)` 固定个合成 timer，发完子单即弃；无常驻进程、无真实回调。
- `client.py:741/744/747`：gateway `sync_orders/sync_trades/sync_positions` 直接 `return []`，runtime 不摄取券商事实。
- `backend/services/miniqmt_execution_runtime/repository.py:152-156`：runtime 自带 OMS 是 JSON 文件（注释明写 "Production Postgres/Timescale DDL intentionally not introduced"）；真正 order/trade/cash/lot 事实在 `qmt_strategy_ledger`（Postgres）。

正确性目前靠 **scheduler 周期 reconcile + 收盘 fresh reconcile（BUG-446）+ 状态归一（BUG-470）+ ledger 幂等（BUG-447/272/382）+ pre-run 失败 durable 审计（BUG-484）** 在外层补回来。BUG-291/337/369/396/414/446/470/484 这一串本质都是在补"看着像 vn.py runtime、底层是轮询编译器"的差。

### 为什么现在必须决策

模拟盘 SIM 与实盘 LIVE 是**同一份 xtquant 代码**，仅 `MINIQMT_MODE`（SIM/LIVE，`backend/infra/qmt_client.py`）+ 账户不同；`MiniQMTSimBackend`（`broker/minqmtsim.py:240`）走真 `place_order`，无伪撮合。区别只在**成交动态**：SIM 账户成交快/确定，实盘成交是部分（status 55）/延迟/概率性。

后果：
- **执行算法价值**：B 下算法只收到"提交时一次 tick + 合成 timer"，收不到盘中真实 tick/成交回调 → TWAP/VWAP/Sniper/BestLimit/POV **退化为提交时一次性算好的静态切片表**，失去自适应（追价/跟量/狙击/改撤）价值。对 500–1000 万、A 股中小盘流动性场景，执行质量是真钱。
- **实时风控**：盘中越限即时反应、亏损阈值击穿即撤、断连处置、kill-switch —— 这些实时反应型风控只有事件循环能真做；B 顶多按轮询节奏延迟反应。
- **SIM 不能证明 B 的实盘执行时效**：SIM 成交快，恰好不压 B 最弱的"时效/部分成交/反应性"轴；sim 跑通 ≠ B 的实盘执行时效 OK。

## 决策

1. **B 继续承载现有模拟盘功能**，尽快完成多 Alpha / 各策略包收益的模拟盘验证，不被本决策阻塞。B 在 sim 与"日频再平衡 + 老实限价"实盘语义下可用，作为**显式过渡方案**。
2. **正式立项 A（durable 事件循环 runtime）为 MiniQMT 实盘目标架构**，独立研发，详见 A 方案详细设计文档。**A 是真实开发，禁止任何简化版、禁止用合成 timer / 查一次 / JSON 文件 OMS 冒充事件循环。**
3. **A 以 flag 门控的"第二 runtime 实现"方式落地**（`MINIQMT_EXECUTION_RUNTIME=compiler|event_loop`，默认 `compiler`=B），住在 main 但默认 inert；**避免长命分支漂移、支持增量与影子运行**。允许在独立 worktree/feature 分支开发，但最终以 flag 门控形态合入 main。
4. **接缝冻结为 A/B 共用契约**：vn.py-style 算法核（只输出 action、零 I/O，`backend/execution_algos/vnpy_style/`）+ `qmt_strategy_ledger`（durable OMS 权威）。**A 复用同一批核与账本，是 additive，不是重写。** 任何 PR 不得破坏该接缝。
5. **切换前硬性要求**：A 与 B **影子/并行运行**（同输入各自计算执行结果与账，自动对账差异）+ 在 SIM 账户充分压实盘成交动态（部分成交流/延迟/拒单/撤单/断连）→ 通过后**灰度切**（按 portfolio/策略槽逐步），可一键回退 B。
6. **实盘准入硬门槛（优先级高于 A/B 之争，独立推进）**：
   - **盘前风控层**（pre-trade 仓位限额 / 买力 / 价格笼子 / 防胖手指 / 提交时 kill-switch）—— 目前代码不存在（grep 为空），实盘前必须建；盘前类可先在 B 上建。
   - **`test_order_service_preflight::test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots` 当前失败**（分仓间现金超额校验挂）—— 对实盘是真金超杠杆风险，实盘前必须 triage 修复。
   - **断连/券商掉线**盘中处置。
7. **禁止"简化-补丁-看似一致"复发**：A 每阶段必须满足详细设计文档中的"阶段验收硬门禁"与"开发偏航阻断规则"（grep/static guard 强制），任一不达标即停止合入。

## 目标架构（摘要，规格见 A 详细设计文档）

- **EventLoop（常驻、durable）**：进程内事件引擎，消费 tick/order/trade/timer/operator/reconcile/eod 事件；非"submit 后查一次"。
- **MiniQMTGateway**：包装真实 xtquant 回调（on_order/on_trade/on_tick/on_account），保留原始包；为 A 提供事件源（替换 B 中 `return []` 的 sync stub）。
- **MiniQMTOmsLedger**：runtime 自持 durable OMS，落 `qmt_strategy` 持久表（order/trade/position-lot/cash/cost/reconcile 事实），与现有 `qmt_strategy_ledger` 统一为同一权威，不再有装饰性 JSON OMS。
- **AlgoRuntime**：每个 parent intent 一个 `ExecutionAlgoInstance`，由**真实 timer/tick/fill 事件**驱动现有 vn.py-style core 产生 submit/cancel/replace 动作；禁止 `for range(_timer_iterations)` 合成循环。
- **RiskEngine（实时）**：事件驱动的盘中风控与 kill-switch。
- **Recovery**：从 durable OMS + 券商 reconcile 恢复，进程重启不丢生命周期。

## 权威对象

- 执行动作语义来源：`backend/execution_algos/vnpy_style/`（vn.py-style 核，A/B 共用，不得分叉）。
- OMS / 账本权威：`qmt_strategy_ledger`（Postgres、幂等）。
- 唯一产品执行入口：`MiniQMTExecutionRuntime`（A 形态）；禁止策略产品路径直连 `XtQuantQMTClient.place_order` / `QmtManagedOrderService.submit_batch`（沿用 0608 §3.1）。

## 禁止事项

- 禁止把 A 简化成"提交后查一次 + 合成 timer"——那就是 B，违背本 ADR。
- 禁止 runtime 自带第二套非 durable OMS（JSON 文件等）；OMS 必须落 `qmt_strategy` 持久表。
- 禁止分叉 vn.py-style 算法核（A/B 必须共用同一核）。
- 禁止在没有影子/并行运行验证的情况下把任何 portfolio 切到 A。
- 禁止在缺失盘前风控层与未修复 cash-overcommit 失败的情况下接入任何实盘账户（`MINIQMT_MODE=LIVE`）。
- 禁止用 sim 通过来声称 A/实盘执行时效达标。
- **★禁止 MiniQMT 路线依赖 TDX 行情(2026-06-23 新增, 关闭已发现偏移)**:MiniQMT 的 pre-trade tradability 报价、分钟线、执行期 tick 必须端到端走 MiniQMT/xtquant 券商行情;**TDX 实时行情仅限 LocalSim**。当前 `scheduler.py:375-377` 把 pre-trade tradability provider 写死成 TDX(对所有 backend 含 MINIQMT_SIM),`assert_broker_market_source_match` 未覆盖该 provider——属"标称 MINIQMT_REALTIME 实则 TDX"的偏移,必须关闭:pre-trade tradability provider 按 broker_backend 选源、`assert_broker_market_source_match` 扩展覆盖 tradability 报价源、MiniQMT 代码路径禁止 `fetch_tdx_realtime_quotes`/`TDX_REALTIME`、并有 minqmt_sim 调 TDX 即失败的回归测试。该 pre-trade 闸修复作为独立 P0 bug 推进;A 设计已纳入为不可变规则(详细设计 §3 规则 9 / §10 grep guard)。

## 分阶段落地

A 的分阶段、阶段验收硬门禁、开发偏航阻断规则、反回归 grep guard、影子运行与切换方案，全部以
`docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` 为准，本 ADR 不重复。原则：
- Phase 0 冻结接缝契约 + 立 epic；Phase 1 Gateway 真回调事件源；Phase 2 durable EventLoop 骨架 + runtime-owned OMS 落库；Phase 3 算法实例事件化（真 tick/timer/fill 驱动现有核）；Phase 4 实时 RiskEngine + kill-switch；Phase 5 影子/并行运行 + 自动对账；Phase 6 灰度切 + 回退；Phase 7 B 降级为显式 fallback / 退役评估。
- 每阶段必须 flag 门控、默认 inert、不影响 B 承载的模拟盘。

## 后果（Consequences）

- **正向**：实盘执行算法可发挥自适应价值；实时风控可落地；执行 runtime 的代码与设计/名义一致,消除"看似一致实则不同"的技术债；A additive 不推倒重来。
- **负向 / 接受的代价**：A 投入大（数周级,数据见详细设计 Phase 估算）；过渡期 B 的实盘执行算法只能做静态切片,这一局限被**显式接受**为过渡;需维护 A/B 双 runtime 至灰度切换完成;影子运行期有额外算力/对账成本。
- **风险控制**：flag 门控 + 影子运行 + 灰度 + 一键回退,把切换风险降到可控;实盘准入硬门槛独立于 A/B 推进,确保即便仍用 B 也不裸奔上实盘。

## 关联文档

- `docs/adr/0001-ai-stock-trading-core-direction.md`（Trading Core 方向、不引入第二平台、Fail-Fast 红线）
- `docs/architecture/miniqmt_unified_vnpy_execution_runtime_design_20260608.md`（原始统一 runtime 设计，§4.1/§5.2 为 A 的源规格）
- `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md`（A 方案详细设计，本 ADR 落地规格）
- B 补丁证据链（说明为何需要 A）：BUG-291/337/369/396/414/446/470/484。
- 实盘准入相关：`test_order_service_preflight`（cash-overcommit 失败）、盘前风控缺口。
