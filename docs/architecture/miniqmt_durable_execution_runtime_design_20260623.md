# MiniQMT Durable 执行 Runtime（A 方案）详细设计（2026-06-23）

> 权威关系：本文是 [`simulation_platform_unified_authoritative_blueprint_20260715.md`](simulation_platform_unified_authoritative_blueprint_20260715.md) 的 durable event-loop/OMS 下位专项契约。本文中的历史 `A/B`、shadow、fallback 和人工 live gate 术语不得解释为保留第二条 SIM 产品执行路线；当前平台术语与迁移边界以上位蓝图 §6 为准。

> 本文是 ADR 0002 的落地规格。**A = durable、回调驱动的 MiniQMTExecutionRuntime 事件循环**，是 MiniQMT 实盘目标架构。
> 本文交独立 Codex 窗口做**真实开发**：**不允许任何简化版**，不允许用"合成 timer / 提交后查一次 / JSON 文件 OMS"冒充事件循环，
> 不允许偏离接缝契约。每阶段受 §9 阶段验收硬门禁与 §10 反回归门禁强制约束，任一不达标即停止合入。

## 0. 一句话结论

把 MiniQMT 执行从"按 intent 同步编译一次性发单 + 外部 scheduler 轮询补账（B）"升级为"**常驻 durable 事件循环消费券商真实 order/trade/tick 回调，驱动每个 parent intent 的算法实例实时产生 submit/cancel/replace 动作，OMS 落 `qmt_strategy` 持久表**（A）"，从而让 vn.py-style 执行算法发挥自适应价值、支持实时风控；A 与 B 共用同一批算法核与账本，flag 门控并存，影子运行验证后灰度切换。

## 1. 设计边界与非目标

### 1.1 本设计必须解决
- 执行算法收到**盘中真实 tick 与真实成交回调**，据此动态追价/跟量/狙击/改撤（非提交时静态切片）。
- runtime 持有 **durable 生命周期与 OMS**（落 `qmt_strategy` 持久表），进程重启不丢、可审计、支持 operator 盘中干预。
- **实时风控 / kill-switch**：事件驱动的盘中越限/亏损/断连处置。
- 与 B 并存、可影子运行、可灰度切、可一键回退。

### 1.2 本设计不做的事
- 不重写 vn.py-style 算法核（复用 `backend/execution_algos/vnpy_style/`，A/B 共用）。
- 不引入 vn.py 本体 runtime（沿用 ADR 0001：不引入第二平台，移植语义而非进程）。
- 不改 B 的对外行为（B 继续承载模拟盘；A 默认 inert）。
- 不在本阶段做 LocalSim 的事件化（A 仅针对 MiniQMT 执行链路）。
- 不做盘前风控的全部实现（盘前风控作为独立实盘准入项，见 §12；本设计只定义 A 内的实时风控挂载点）。

## 2. 证据基线

### 2.1 当前 B 代码事实（A 必须替换/绕过的点，file:line）
- `backend/services/miniqmt_execution_runtime/client.py:486-488`：`on_tick` 一次 + `for index in range(_timer_iterations): on_timer(...)` 合成循环 —— **A 必须删除此同步循环作为产品生命周期**。
- `client.py:740-747` / `:838-842`：gateway `sync_orders/sync_trades/sync_positions` `return []` —— **A 必须实现为真实券商回调/查询事件源**。
- `backend/services/miniqmt_execution_runtime/repository.py:152-156`：`JsonFileMiniQMTExecutionRuntimeRepository`，"Production DDL intentionally not introduced" —— **A 必须以 `qmt_strategy` 持久表替换为权威 OMS**。
- `backend/services/miniqmt_execution_runtime/runtime.py:137`（`recover`）/ `:867`（`_terminalize_orphaned_active_algos`）：仅 restart 兜底，无常驻事件循环 —— **A 必须提供常驻事件循环 + 事件驱动恢复**。
- `backend/services/paper_trading_v2/broker/minqmtsim.py:225-240`：真 `place_order`（无伪撮合）；`_ORDER_PENDING={48,49,50,51}` / `_ORDER_PARTIAL={52,53,55}` —— A 复用真实下单/撤单/查询与状态码。
- `backend/infra/qmt_client.py`：`MINIQMT_MODE`=SIM/LIVE 开关；A 与 B 共用同一 client，SIM/LIVE 仅模式与账户差异。

### 2.2 源规格与权威
- `miniqmt_unified_vnpy_execution_runtime_design_20260608.md` §4.1（vn.py 组件映射）、§5.2（MiniQMTExecutionRuntime 合同）、§6（状态机）、§10.8（阶段验收硬门禁）—— A 在其上**真正实现**而非编译器近似。
- ADR 0001（不引入第二平台 / Fail-Fast 红线）、ADR 0002（A 为目标、B 为过渡、接缝冻结、flag 门控、影子运行、实盘准入门）。
- BUG-446/447/470/484：B 在 scheduler+ledger 层补出的成败/对账/状态/审计语义，A 必须**等价或更强**地内建（不得回归）。
- 订单状态码权威（BUG-470 已归一，A 复用，禁止再分叉）：非终态 open-like = {48,49,50,51,52,53,55}；终态 = {54 撤,56 成,57 拒}；未知 → loud（`MINIQMT_RUNTIME_UNKNOWN_BROKER_ORDER_STATUS`）。

## 3. 不可变硬规则

1. **真事件驱动**：算法实例只能由真实 tick/order/trade/timer 事件驱动；**禁止合成 timer 循环、禁止"提交后查一次"作为生命周期**。
2. **durable OMS 单一权威**：runtime OMS 落 `qmt_strategy` 持久表，与 `qmt_strategy_ledger` 统一为同一权威；**禁止第二套非 durable OMS（JSON/内存）作为事实源**。
3. **算法核不分叉**：A 复用 `backend/execution_algos/vnpy_style/` 的核（只输出 action、零 I/O）；A 只新增"驱动核的事件循环 + 执行 action 的 gateway/oms"，**禁止在 A 里重写算法逻辑**。
4. **幂等**:所有 order/trade/cash 落库锚定券商 traded_id/order_id,`ON CONFLICT DO NOTHING`/once 语义(沿用 BUG-447/272/382),事件重放不双计。
5. **Fail-Fast / no silent**:券商回调异常、未知状态、断连、对账冲突一律 loud(reason_code + 持久事件),禁吞、禁软化、禁 fallback 默认(ADR 0001 红线)。
6. **成败由 ledger 事实判定**:成功不得覆盖 submit 失败(0608 §6.2);run/batch status 不作事实替身。
7. **flag 门控 + 默认 inert**:`MINIQMT_EXECUTION_RUNTIME=compiler|event_loop`,默认 `compiler`(=B);A 未灰度的 portfolio 行为与 B 完全一致。
8. **SIM/LIVE 同码**:A 不得对 SIM/LIVE 走不同执行逻辑分支(仅账户/模式/风控阈值差异);实盘成交动态由测试覆盖,不靠 sim 证明。
9. **★MiniQMT 行情源端到端隔离(2026-06-23 新增, 关闭已发现偏移)**:MiniQMT 路线的 **pre-trade tradability 报价、分钟线、执行期 tick 全部走 MiniQMT/xtquant 券商行情;TDX 实时行情仅限 LocalSim**。
   - 已发现偏移(必须关闭):`backend/services/simulation_runtime/scheduler.py:375-377` 把 `ProductionSimulationRunContextProvider._pre_trade_tradability_provider` 默认写死成 `PreTradeTradabilityProvider(realtime_quote_fetcher=fetch_tdx_realtime_quotes, realtime_quote_source="TDX_REALTIME.batch_quote")`,**对所有 backend(含 MINIQMT_SIM)统一用 TDX**;`assert_broker_market_source_match`(`market_data.py:74`)目前只约束分钟线源,**未覆盖 tradability provider** → 标称 `MINIQMT_REALTIME` 实则 pre-trade 闸用 TDX。
   - 硬规则:pre-trade tradability provider 必须**按 broker_backend 选源**(minqmt_sim/minqmt_live → xtquant/券商行情;localsim → TDX);`assert_broker_market_source_match` 必须**扩展覆盖 tradability 报价源**;**MiniQMT/event_loop 代码路径禁止出现 `fetch_tdx_realtime_quotes` / `TDX_REALTIME`**;必须有测试断言 minqmt_sim 调 TDX 即失败、localsim 才允许 TDX。
   - 范围说明:该 pre-trade 闸在执行 runtime 的**上游**(共享 run-context provider),其修复作为独立 P0 bug 推进(MiniQMT 禁 TDX);A 设计在此**显式纳入为不可变规则**,确保 A 接线后不会静默继承/复活该偏移。

## 4. A 目标分层架构与合同

### 4.1 EventLoop（常驻 durable 事件引擎）
- 进程内常驻循环（或异步任务），消费事件类型：`MarketTick`、`OrderUpdate`、`TradeFill`、`AlgoTimer`、`OperatorCommand`、`ReconcileTick`、`EndOfDay`。
- 事件入队/出队有 durable 落地（崩溃可重放未处理事件或从 OMS+券商 reconcile 恢复，二选一并在 §6 定义）。
- 单进程内串行处理每个 runtime 的事件，保证算法实例状态一致；多 runtime 可并发但互不串状态。
- **禁止**：把 EventLoop 实现成"被外部 scheduler 每 30s 调一次 run_once 并 query 一次"——那是 B。EventLoop 必须自持节奏并消费真实回调。

### 4.2 MiniQMTGateway（真实券商事件源）
- 包装 `XtQuantQMTClient` 的回调：`on_order`/`on_trade`/`on_tick`/`on_account`/`on_disconnect`，把券商原始包转成上述事件投入 EventLoop；保留 raw payload 以便审计。
- 替换 B 中 `sync_orders/sync_trades/sync_positions` 的 `return []`：A 的 gateway 必须返回真实券商 order/trade/position，且通过回调实时推送。
- 下单/撤单/改单仍走真实 `place_order/cancel_order`（与 B 同 client，SIM/LIVE 同码）。
- 断连：`on_disconnect` → 触发实时风控（§4.5）与 reconcile，不静默。

### 4.3 MiniQMTOmsLedger（runtime-owned durable OMS）
- 落 `qmt_strategy` 持久表（order/trade/position-lot/cash/cost/reconcile facts），与现有 `qmt_strategy_ledger` 统一；**删除/退役 `JsonFileMiniQMTExecutionRuntimeRepository` 作为事实源**（可保留为只读调试快照，但不得是权威）。
- 幂等：fill→ledger 锚定 traded_id；cash freeze/unfreeze on submit/cancel/reject；position-lot T+1。复用 `qmt_strategy_ledger` 既有幂等实现，不重写。
- 提供 API/UI/监控可查的运行态（child order 状态随券商实时收敛——内建 BUG-470 的 runtime child reconcile，不再靠 recover 补）。

### 4.4 AlgoRuntime（事件化算法实例）
- 每个 parent intent → 一个 `ExecutionAlgoInstance`，绑定一个 vn.py-style core（Sniper/BestLimit/TWAP/…）。
- 由真实 `MarketTick`/`TradeFill`/`AlgoTimer` 驱动 core 的 `on_tick`/`on_trade`/`on_timer`，core 输出 submit/cancel/replace action，runtime 执行并经 gateway 下达。
- 子单与算法实例的生命周期由 OMS durable 跟踪；全部子单终态 → 实例终态；operator cancel → 撤所有活跃子单并终结实例（内建 BUG-358 语义）。
- **禁止**：一次性算完 N 个子单提交后丢弃实例。实例必须存活到执行窗口结束并持续反应。

### 4.5 RiskEngine（实时风控挂载点）
- 事件驱动:每个 OrderUpdate/TradeFill/Tick 后评估盘中风控(越限、亏损阈值、敞口、断连),可发 kill-switch(撤全部活跃子单 + 阻断新单)。
- 本设计定义**挂载点与事件接口**;具体盘前/盘中风控规则集作为独立实盘准入项(§12)落地,但 A 必须预留实时评估钩子(B 无法提供)。

### 4.6 Recovery（事件驱动恢复）
- 进程重启:从 durable OMS 重建活跃算法实例 + 子单状态,并对券商做一次 reconcile(orders/trades/positions)对齐真相,再恢复事件消费;不丢生命周期、不重复下单。
- 与 B 的 `recover()` 区别:A 的恢复是常驻循环的一部分且事件化,不是"下次有人调 run_once 才补"。

## 5. A/B 接缝与 flag 门控

- **冻结接缝(Phase 0 必须先做)**:抽出 A/B 共用契约——(1) 算法核接口(action 输入/输出);(2) `qmt_strategy_ledger` OMS 写读接口;(3) gateway 下单/撤单/查询接口。A 与 B 都只通过该接缝交互。
- `MINIQMT_EXECUTION_RUNTIME=compiler|event_loop`(默认 compiler):
  - `compiler` → 现有 B 路径,行为零变化。
  - `event_loop` → A 路径(常驻 EventLoop)。
  - 切换粒度:全局默认 + 可按 portfolio/策略槽覆盖(灰度)。
- A 开发可在独立 worktree/feature 分支进行,但**最终以 flag 门控形态合入 main**,默认 inert,不影响 B 承载的模拟盘。

## 6. 关键状态机（事件驱动版，沿用 0608 §6 语义）

- **交易日 lifecycle**:CREATED → PRECHECKING → SUBMITTING(事件循环启动,算法实例激活)→ INTRADAY_RUNNING(持续消费 tick/fill,算法改撤)→ RECONCILING → 终态(SUCCEEDED / FAILED_TERMINAL / NO_REBALANCE)。
- **成功不覆盖失败**(0608 §6.2):任何终态判定以 OMS 事实为准;submit 失败 + 后续 reconcile 成功 不得翻成 SUCCEEDED 而掩盖。
- **重试语义**:retryable 残差(SELL_PROCEEDS_REQUIRED 等)由事件循环内的依赖图驱动(SELL 成交事件 → 释放现金 → 触发依赖 BUY),**取代 B 的"排序+下次 tick 重评"**;有 attempt 上限 + 退避 + 收盘终结。
- **收盘终结**:EndOfDay 事件触发,以新鲜券商 reconcile 后的事实判终态(内建 BUG-446 语义);Asia/Shanghai 时区(内建 BUG-463 修复)。
- **未终结订单**:部分成交 55 计入 open(内建 BUG-470);算法实例据真实 fill 事件继续推进或在收盘终结。

## 7. 影子 / 并行运行与灰度切换

1. **影子运行(shadow)**:同一 binding,A 与 B 同时接收同输入;A 的下单走"影子模式"(可对 SIM 账户真发,或 dry-run 记录意图),与 B 的执行结果与账逐项自动对账(子单数/价/量/成交/cash/positions),差异 loud 报告。
2. **并行验证矩阵**:覆盖 §8 全部实盘成交动态场景,A 与 B 对账无致命差异 + A 通过全部验收门禁。
3. **灰度切**:按 portfolio/策略槽逐个把 `MINIQMT_EXECUTION_RUNTIME` 切 `event_loop`;每个切换后观察期;**一键回退** `compiler`。
4. **切换不可逆动作前置检查**:in-flight 子单/算法实例状态迁移方案;切换当日不得有未终结的跨 runtime 订单歧义。

## 8. 验收场景矩阵（A 必须全部覆盖，含实盘成交动态）

- 单 parent intent 全成交;分批部分成交(55 流式)直至全成;部分成交后收盘未全成 → 正确终态。
- 子单被拒(57)→ 算法实例据 reject 事件改价/重试/终结(不静默)。
- 算法主动撤单改价(BestLimit 追价 / TWAP 改未成子单);Sniper 等待盘中价格触发后才发。
- SELL 部分成交 → 释放现金事件 → 触发依赖 BUY(funds-only,内建 0608 §6.4 / BUG-296/300)。
- operator cancel 盘中:撤所有活跃子单 + 终结实例 + durable 审计。
- 券商断连(on_disconnect)→ 实时风控/kill-switch + reconcile + loud,不丢单不重复下单。
- 进程重启:从 durable OMS + 券商 reconcile 恢复活跃实例,不重复下单、不丢生命周期。
- 未知状态码 → loud(`MINIQMT_RUNTIME_UNKNOWN_BROKER_ORDER_STATUS`)。
- 收盘终结时区正确(Asia/Shanghai);非交易日不起循环。
- 幂等:同一 trade/order 回调重放不双计 cash/lot。
- A vs B 影子对账:同输入下执行结果与账差异在容许阈内(或差异均可解释为 A 的自适应改进)。

## 9. 分阶段实施 + 阶段验收硬门禁 + 开发偏航阻断

> 沿用 0608 §10.8 的"统一通过定义 + 验收矩阵 + 偏航阻断 + 一次性整改完成标准"机制,并强化。每阶段 flag 门控、默认 inert、不影响 B。

- **Phase 0 接缝冻结 + epic**:抽出并冻结 §5 接缝契约;建 issue epic;**门禁**:接缝契约有测试锁定;A/B 都只经接缝交互(grep guard)。
- **Phase 1 Gateway 真事件源**:实现 on_order/on_trade/on_tick/on_account/on_disconnect → 事件;替换 `sync_* return []`。**门禁**:gateway 不再有 `return []` stub(grep);回调→事件有单测;断连产生 loud 事件。
- **Phase 2 durable EventLoop 骨架 + OMS 落库**:常驻事件引擎 + runtime OMS 落 `qmt_strategy` 表;退役 JSON OMS 作为事实源。**门禁**:无合成 timer 循环作为生命周期(grep `range(_timer_iterations)` 在 event_loop 路径=0);OMS 事实源是 Postgres(无 JsonFile 作权威)。
- **Phase 3 算法实例事件化**:ExecutionAlgoInstance 由真实 tick/timer/fill 驱动现有 vn.py-style 核;实例存活至窗口结束。**门禁**:Sniper/BestLimit/TWAP 在真实 tick 流下产生与 0608 algo 规格一致的改撤行为(characterization test);算法核未被分叉(attribution 不变)。
- **Phase 4 实时 RiskEngine + kill-switch**:事件驱动盘中风控挂载 + kill-switch。**门禁**:断连/越限触发 kill-switch 有测试;实时评估钩子存在。
- **Phase 5 影子/并行运行 + 自动对账**:A/B 同输入对账。**门禁**:§8 全场景影子对账无致命差异;对账报告 durable。
- **Phase 6 灰度切 + 回退**:按 portfolio 切 event_loop + 一键回退。**门禁**:切换/回退演练通过;in-flight 迁移无歧义。
- **Phase 7 B 降级评估**:B 降为显式 fallback 或退役评估(不强制删 B,保留回退能力)。

### 9.x 开发偏航阻断规则（强化,杜绝"简化-看似一致"复发）
- 任一 PR 若**用合成 timer / 提交后查一次 / JSON OMS / sync 返回 [] / 分叉算法核**来"近似"事件循环,**直接拒绝**,记为偏航。
- 每阶段 PR 必须附:对应 §10 grep guard 全绿证据 + characterization/影子对账证据 + flag inert 证据(默认 compiler 行为零变化)。
- 不允许"先合一个能跑的简化版,后续再补"——A 的阶段交付物必须是该阶段的真实形态,缺失即不达标、不合入。

## 10. 反回归门禁（grep / static guard，CI 强制）

- `event_loop` 路径中 `for .* in range(_timer_iterations)` 出现次数 = 0。
- A 的 gateway 中 `return []`（针对 sync_orders/trades/positions）出现次数 = 0。
- `JsonFileMiniQMTExecutionRuntimeRepository` 不作为 `event_loop` 路径的权威 OMS（仅允许只读调试）。
- 算法核 `backend/execution_algos/vnpy_style/` 的 attribution/source map 未变（A 不得分叉核）。
- 订单状态判定统一走 `is_open_like_order_status`/`is_terminal_order_status`（BUG-470 权威谓词），A 内无新分叉的状态集字面量。
- `MINIQMT_EXECUTION_RUNTIME` 默认 `compiler`；未设置时 B 行为零变化（测试锁定）。
- 无 silent：A 路径无 `except: pass` / 裸 fallback；券商异常/未知状态/断连均 loud + reason_code。
- **★MiniQMT 行情源隔离(规则 9)**:MiniQMT/`event_loop` 代码路径中 `fetch_tdx_realtime_quotes` / `TDX_REALTIME` 出现次数 = 0；pre-trade tradability provider 按 broker_backend 选源(非写死 TDX);`assert_broker_market_source_match` 覆盖 tradability 报价源;有测试断言 `minqmt_sim` 调 TDX 即失败、`localsim` 允许 TDX。

## 11. Issue / PR 证据模板

- Issue context pack:阶段号 + 本阶段真实形态定义 + 对应 §9 门禁 + §10 grep guard 列表 + 影子对账/characterization 计划。
- PR body:改动文件(限本阶段 scope)+ 门禁全绿证据 + flag inert 证据(默认 compiler 行为不变)+ 影子对账/characterization 结果 + 反回归 grep 输出 + production gates。
- Validation history:l0 + paper_v2_l3 + 新增 A 专用测试套 + 影子对账报告链接。

## 12. 实盘准入门（独立于 A/B,优先级高,实盘前必须）

- **盘前风控层**:pre-trade 仓位限额 / 买力 / 价格笼子 / 防胖手指 / 提交时 kill-switch（目前代码不存在）。盘前类可先在 B 上建;A 提供实时盘中风控挂载点(§4.5)。
- **cash-overcommit 失败修复**:`test_order_service_preflight::test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots` 当前失败,分仓间现金超额校验,实盘前必修。
- **断连/掉线**盘中处置(A 内建,B 需补)。
- **实盘成交动态测试**:在 SIM 账户压 §8 全场景;sim 通过不等于实盘执行时效达标,必须显式覆盖部分成交/延迟/拒单/撤单/断连。
- **接入实盘账户(`MINIQMT_MODE=LIVE`)的前置**:上述全部 + A 影子运行验证通过 + 灰度方案就绪。

## 关联文档
- ADR 0001 / ADR 0002
- `miniqmt_unified_vnpy_execution_runtime_design_20260608.md`（§4.1/§5.2/§6/§10.8 为源规格）
- B 补丁证据链:BUG-291/337/369/396/414/446/470/484
- 算法核:`backend/execution_algos/vnpy_style/`；OMS:`qmt_strategy_ledger`
