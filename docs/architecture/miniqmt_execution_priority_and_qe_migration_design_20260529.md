# MiniQMT 执行算法优先验证与 QE 回测迁移方案

> 日期：2026-05-29  
> 状态：设计方案 / 待拆分 issue 实施  
> 范围：Paper v2 MiniQMT SIM 执行算法、订单/成交/成本审计、QE 回测执行模型迁移  
> 非目标：不直接集成 vn.py 运行时；不推翻 AIstock 现有 Paper v2/QE/StrategyPackage 架构；不修改 `.env`；不重启生产 `8001/3000/19080`；不写生产数据库；不修改 StrategyPackage frozen asset、HMM snapshot、QE workspace 或模型权重。

## 0. 关键结论

1. **优先级应调整为 MiniQMT 实盘模拟执行闭环优先**：1000 万以内资金规模下，短期不把市场冲击、AC Optimal、大额 Iceberg 作为主线；先解决下单成功率、拒单诊断、资金释放、成本统计和可撤可追的 child-order 执行。
2. **Sniper / BestLimit 应早于 V25 live 化**：先实现 `SNIPER_MINIQMT`、`BEST_LIMIT_MINIQMT`、`TWAP_LITE_MINIQMT`，让 MiniQMT SIM 形成真实订单样本；V25 后续作为 schedule/meta execution 层接入。
3. **QE 不应现在直接复用 MiniQMT 初版算法作为正式排名算法**：先在 MiniQMT SIM 验证订单状态、撤单重报、手续费和拒单语义，再把稳定语义迁移到 QE shadow model；否则 MiniQMT adapter 一变，QE 需要反复返工。
4. **MiniQMT 能提供真实成交金额与 broker 返回的费用字段，但不应假设能拆分印花税/过户费**：当前本地类型显示成交回报有 `traded_amount` 和 `commission`；未看到独立 `stamp_tax` / `transfer_fee` 字段。AIstock 应保存 broker aggregate cost，并用版本化 fee model 估算拆分和对账。
5. **若 QE 当前执行成本模型无法表达 broker aggregate、卖出印花税、部分成交、未成交偏离和成本版本，则必须更新**：否则回测净值与 MiniQMT SIM/未来实盘偏离，QE 结果参考价值不足。
6. **vn.py 的事件驱动执行模块比 AIstock 当前 MiniQMT 执行闭环更成熟**：后续设计必须在主体章节中明确直接复用 `vnpy_algotrading` 的 Sniper/BestLimit/TWAP 源码语义和状态机，AIstock 只做适配、审计、风控和持久化，不允许把“参考 vn.py”降级为最后一章的泛泛建议。

## 1. 背景与修正后的前提

用户补充的实盘规模假设为：未来 AIstock 实盘资金量约 **1000 万人民币以内**。在该规模下，短期不把市场冲击模型作为首要约束，优先目标从“大资金低冲击执行”调整为：

1. 提高下单成功率与诊断透明度。
2. 避免资金不足、卖出资金未释放、委托状态误判等无值守事故。
3. 优先验证更贴近实际可成交的 MiniQMT 执行算法，例如 Sniper / BestLimit / TWAP-lite；这些算法应优先从 `vnpy_algotrading` 的成熟源码迁移和改造，而不是只按概念重写。
4. 用 MiniQMT SIM 的真实订单、成交、拒单、撤单、手续费数据校准回测执行模型。
5. 在 MiniQMT 执行语义稳定前，不急于把所有新算法直接迁移进 QE 实验，避免 MiniQMT 路径变化导致 QE 回测反复返工。

现状关键差异：

| 场景 | 当前执行语义 | 问题 |
|---|---|---|
| QE / LocalSim minute replay | 可通过 `MinuteExecutionEngine` 使用 TWAP/VWAP/POV/V25 等算法 | 回测语义相对完整 |
| Paper v2 MiniQMT SIM | 生成 `OrderIntent` 后直接进入 MiniQMT broker-authoritative 提交 | 跳过 V25/TWAP/VWAP 等分钟执行算法，接近整笔最新价委托 |
| MiniQMT 成交/拒单诊断 | 查询 orders/trades，并部分写入 metadata | 缺完整回调、失败订单诊断包、资金快照关联 |
| 成本统计 | `query_stock_trades` 已可读取 `commission` 字段 | 尚未确认是否包含全部真实费用，印花税/过户费字段未独立暴露 |

当前代码证据：

| 事实 | 代码位置 | 影响 |
|---|---|---|
| MiniQMT portfolio 路径在 `broker_backend == "minqmt_sim"` 时进入 `_run_minqmt_sim_orders` | `backend/services/paper_trading_v2/day_runner.py` | 当前不走 LocalSim 的 `MinuteExecutionEngine.execute_order` 完整分钟执行链路 |
| LocalSim 路径才调用 `MinuteExecutionEngine.execute_order` | `backend/services/paper_trading_v2/day_runner.py` | V25/TWAP/VWAP 的回测语义尚未自然落到 MiniQMT child orders |
| MiniQMT MARKET 当前映射为 `_LATEST_PRICE` | `backend/services/paper_trading_v2/broker/minqmtsim.py` | “整笔最新价”更接近简单提交，不是可撤可追的执行算法 |
| `qmt_client.get_trades()` 返回 `traded_amount` 和 `commission` | `backend/infra/qmt_client.py` | 可以进入 Paper v2 fill/cost audit，但需标注费用口径 |
| `qmt_client.get_positions()` 返回 `cost_price` 和 `market_value` | `backend/infra/qmt_client.py` | 持仓成本/市值字段理论上可展示，缺失时应诊断 broker 原始值 |



## 1.1 vn.py 成熟度判断与 AIstock 差距

结论：vn.py/VeighNa 的执行算法模块在事件驱动交易执行、订单生命周期、可插拔 gateway、UI/CSV/外部调用和常用算法模板方面更成熟；AIstock 当前优势在 StrategyPackage、QE、Paper v2 组合管理、审计和未来研究闭环，但 MiniQMT live/SIM 执行层还不完整。因此本方案不是推翻 AIstock，而是把 vn.py 成熟执行模块的代码语义和工程边界迁入 AIstock 的执行层。

本次源码核对基线：

| 项目 | 证据 |
|---|---|
| 上游仓库 | `https://github.com/vnpy/vnpy_algotrading` |
| 本地审计路径 | `F:\Dev\AIstock_artifacts\vnpy_source_audit_20260529\vnpy_algotrading` |
| 审计 commit | `4133987530eb28f3538d1983545d81c4f83d7d59` |
| license | MIT License，允许复制、修改、分发，但必须保留 copyright 和 permission notice |
| 算法文件 | `vnpy_algotrading/algos/sniper_algo.py`、`best_limit_algo.py`、`twap_algo.py` |
| 框架文件 | `vnpy_algotrading/template.py`、`engine.py`、`base.py` |

AIstock 与 vn.py 的能力对比：

| 维度 | vn.py / vnpy_algotrading | AIstock 当前状态 | AIstock 缺口 | 本方案要求 |
|---|---|---|---|---|
| 事件模型 | `on_tick` / `on_order` / `on_trade` / `on_timer` 清晰分发 | MiniQMT 路径以 submit/query 为主，回调审计不完整 | 缺统一 live execution event loop 和 callback collector | 复用 vn.py 事件语义，AIstock adapter 实现事件分发 |
| 算法模板 | `AlgoTemplate` 统一 `buy/sell/cancel_all/finish` | Trading Core 有 minute execution，但 MiniQMT 路径未复用 | core 与 broker adapter 边界不清 | 建 `vnpy_style` core + MiniQMT adapter |
| 订单状态 | `vt_orderid`、active order、order/trade callback 驱动 | 有 order execution state，但失败诊断和 active terminal 语义不足 | rejected/partial/timeout/cancel-replace 不够透明 | 建 active order state machine |
| 常用算法 | 已有 Sniper、BestLimit、TWAP、Iceberg 等 | MiniQMT 当前不真正执行这些 live child-order 算法 | 缺可撤可追、按盘口/时间片执行 | 第一批迁移 Sniper/BestLimit/TWAP |
| 网关边界 | gateway 负责真实发单和成交回报 | MiniQMT client 是 broker authority | 两者边界相似，但对象模型不同 | 不引入 vn.py gateway，保留 MiniQMT authority |
| 风控职责 | vn.py 有交易前检查和 gateway 限制 | AIstock 需组合级资金/持仓/涨跌停/审计 | 资金释放、成本、组合偏离比 vn.py 更业务化 | 风控和审计留在 AIstock 平台层 |
| 成本/TCA | vn.py 执行算法本身不负责完整研究 TCA | AIstock 有 QE/Paper 研究闭环 | MiniQMT 成本落库和对账不足 | AIstock 增强 cost audit/report |
| 回测一致性 | vn.py 侧更偏交易执行 runtime | AIstock 有 QE 回测和 execution policy | live 与 QE execution 语义未对齐 | MiniQMT 先验证，再 QE shadow |

### 1.2 为什么不直接整体接入 vn.py runtime

不是因为 vn.py 不成熟，而是因为整体接入会让 AIstock 同时存在两套生产运行边界：

| vn.py runtime 模块 | 成熟价值 | 直接整体接入的问题 | AIstock 复用方式 |
|---|---|---|---|
| `EventEngine` | 成熟的事件分发 | 会和 Paper v2 scheduler/live session 并行，调度和恢复语义重复 | 复用事件类型和回调顺序，在 AIstock adapter 中实现 |
| `AlgoEngine` | 算法生命周期和 active algo 管理 | 会引入第二套 algo registry、持久化和 UI 状态 | 复用模板职责，AIstock 持久化 algo state |
| Gateway/MainEngine | 多柜台/多网关抽象 | MiniQMT 已由 AIstock `qmt_client` 和 broker backend 管理，直接替换风险高 | 不接 gateway，只把 adapter 输出转成 MiniQMT native request |
| OrderData/TradeData | 标准订单成交对象 | 字段和 Paper v2 repository、MiniQMT raw status 不一致 | 建映射 DTO，保留 raw native payload |
| RiskManager | 交易前检查 | 不覆盖 AIstock 组合级资金、StrategyPackage、成本审计 | vn.py 风控思想 + AIstock risk gate |

因此“参考复用 vn.py”在本方案中必须解释为：**复制/改造上游成熟算法 core 与状态机，保留上游行为证据；不把 vn.py runtime 作为第二个生产引擎直接运行。**

### 1.3 直接复用代码的前置约束

后续 issue 不允许只写“参考 vn.py 后重新实现”。必须满足：

1. 在 issue 中记录上游仓库、commit、文件路径、license。
2. 在迁移文件头或 attribution 文件中记录 derived-from 信息。
3. 对 Sniper/BestLimit/TWAP 建 characterization tests，证明 AIstock core 的 tick/order/trade/timer 行为与上游关键分支一致。
4. 若因为 A 股/miniQMT 规则需要偏离上游行为，必须在 migration notes 和测试名中写明差异，例如 board lot、涨跌停、撤单上限、资金释放、broker status 57。
5. 不允许把最后结果伪装为“成熟代码复用”，但实际只保留算法名字和参数。


## 2. 设计原则

### 2.1 先 MiniQMT 验证，再 QE 迁移

短期采用 **MiniQMT-first，QE-follow**：

```text
MiniQMT SIM 执行算法 PoC/小规模验证
  -> 真实订单/成交/拒单/撤单/成本审计
  -> 固化 execution semantics + 参数 schema
  -> 再迁移到 QE / LocalSim 回测执行模型
  -> 用样本外验证决定是否成为 validated execution policy
```

原因：

- MiniQMT adapter、柜台返回、订单状态码、手续费字段、可用资金释放时点都仍在被真实运行验证。
- 如果先大规模改 QE，后续 MiniQMT adapter 或交易规则变化，会造成 QE 执行模型重复调整。
- QE 的参考价值来自与实际执行一致；应等待 MiniQMT 侧形成稳定的订单状态机和成本口径后再迁移。

### 2.2 不做静默 fallback

任何执行算法不可用、模型缺失、数据缺失、成本口径缺失，都必须产生显式状态或 fail-fast，不允许：

- V25 静默降级到 TWAP。
- MiniQMT 执行失败伪装成 run 成功。
- 成本字段缺失时用 0 成本伪装精确成本。
- 未成交订单被当成已完成调仓。

### 2.3 broker-authoritative 不等于无算法

MiniQMT 是成交、现金、持仓、订单状态的权威；但执行算法仍应负责：

- 什么时候发单。
- 发多少。
- 用什么价格类型/限价。
- 何时撤单重报。
- 何时停止、延后或降风险。

最终成交价、成交量、手续费以 MiniQMT 返回为准。

## 3. 资金规模下的执行算法优先级

1000 万以内资金量下，不优先实现 Almgren-Chriss / 大额 Iceberg / 市场冲击最优执行。推荐优先级如下。

### P0：执行基础设施与诊断，不属于算法但必须先做

| 项目 | 优先级 | 说明 |
|---|---:|---|
| MiniQMT 原始回调接入 | P0 | 采集 `on_stock_order`、`on_stock_trade`、`on_order_error`、`on_cancel_error`、`on_order_stock_async_response` |
| 失败订单诊断包 | P0 | 关联 intent、native request、native response、order/trade snapshots、cash/position before-after、status_msg 原文和截断状态 |
| 活动订单状态机 | P0 | active/terminal 状态、partial fill、timeout、cancel replace、deferred |
| 成本字段审计 | P0 | 持久化 `traded_amount`、`commission`、估算/拆分 tax/transfer fee 的口径标记 |
| 资金释放状态 | P0 | sell-before-buy 不能只是排序；必须等待或重查可用资金 |

没有 P0 基础设施时，任何 Sniper/V25/TWAP 都无法可靠无值守运行。

### P1：优先验证 Sniper / BestLimit / TWAP-lite

| 算法 | 优先级 | 适用场景 | 原因 |
|---|---:|---|---|
| `SNIPER_MINIQMT` | P1 | 小中单、希望快速成交、盘口满足条件时触发 | 用户资金规模较小，可优先追求成交确定性和少量价格控制 |
| `BEST_LIMIT_MINIQMT` | P1 | 正常调仓，挂买一/卖一或对手一档限价 | 比整笔最新价更可控，能形成撤单重报价差控制 |
| `TWAP_LITE_MINIQMT` | P1 | 单票金额较大或需要分散执行 | 简单稳定，作为基线和兜底候选，但不能作为其他算法失败后的静默 fallback |

推荐第一阶段不实现复杂 VWAP/POV，不优先 Iceberg。Sniper 和 BestLimit 更符合 1000 万以内、A 股中小盘/普通调仓的实际需求。

### P2：V25 live adapter 与回测对齐

| 算法 | 优先级 | 说明 |
|---|---:|---|
| `V25_LIVE_SCHEDULE_MINIQMT` | P2 | 将 V25 的 240 分钟权重计划转换为 MiniQMT child-order schedule |
| `VWAP_MINIQMT` | P2 | 在成交量曲线数据稳定后引入 |
| `POV_MINIQMT` | P2 | 在实时成交量/盘口数据稳定后引入 |

V25 不应再作为“孤立回测算法”。它的正确定位是 **智能 schedule / meta execution**：生成执行权重计划，再交给 MiniQMT adapter 下 child orders。

### P3：大额或复杂算法

| 算法 | 优先级 | 说明 |
|---|---:|---|
| `ICEBERG_MINIQMT` | P3 | 资金规模扩大或单票成交额占比明显时再做 |
| `AC_OPTIMAL` | P3 | 当前 1000 万以内不优先；需要冲击成本模型 |
| `SBB_EMA` 等择时类执行 | P3 | 等基础算法和成本闭环稳定后再评估 |


## 4. vn.py 源码复用与 AIstock 适配边界

后续 Sniper/BestLimit/TWAP-lite 第一版实现必须直接从 `vnpy_algotrading` 对应源码迁移核心语义，并保留 derived-from 证据。这里的“复用”不是只引用算法名称，也不是按概念重新自研；必须用源文件映射、状态变量、关键分支、回调顺序和 characterization tests 验收。AIstock 只在 broker 适配、A 股规则、风控、审计、成本统计、持久化和 QE 迁移层做必要改造。

### 4.1 复用原则

| 原则 | 要求 | 验收方式 |
|---|---|---|
| 最大范围直接复用 | 优先保留上游围绕 `cancel_all`、`finish`、`on_tick`、`on_order`、`on_trade`、`on_timer` 的核心分支 | migration notes 标注保留/改造点；characterization tests 覆盖关键分支 |
| 保留上游状态语义 | `vt_orderid`、`order_price`、`timer_count`、`total_count`、`order_volume` 等状态语义不得随意重命名或弱化 | attribution 文件记录 name mapping |
| core 与平台边界分离 | 算法 core 不直接访问 raw MiniQMT status、DB、API、FastAPI；这些职责放在 adapter/audit/risk 层 | import-boundary test 验证 core 不 import qmt/db/fastapi |
| A 股差异显式记录 | board lot、涨跌停、可卖数量、MiniQMT 拒单码等差异必须写入 migration note | 单测覆盖差异行为 |
| 不引入第二套 runtime | 不把 vn.py `EventEngine`/`AlgoEngine` 作为 Paper v2 并行生产调度器 | dependency/import scan 和 runtime wiring review |

### 4.2 源文件到 AIstock 文件的直接映射

| 上游文件 | 复用对象 | AIstock 目标文件 | 复用方式 | 必须保留的行为/状态 | 必须剥离或适配 |
|---|---|---|---|---|---|
| `vnpy_algotrading/algos/sniper_algo.py` | Sniper 算法 core | `backend/execution_algos/vnpy_style/sniper_core.py` | 复制并改造成去 vn.py runtime 的 core | `vt_orderid` active-order 语义；有活动委托先 `cancel_all`；买入只在 `ask_price_1 <= price`；卖出只在 `bid_price_1 >= price`；`on_order`/`on_trade` 终态与 `finish` | `BaseEngine`、vn.py `TickData/OrderData/TradeData`、gateway `buy/sell` 调用 |
| `vnpy_algotrading/algos/best_limit_algo.py` | BestLimit 算法 core | `backend/execution_algos/vnpy_style/best_limit_core.py` | 复制并改造成 pure core，保留随机子单和追价逻辑 | `vt_orderid`、`order_price`、`min_volume/max_volume`；买入用 `bid_price_1`；卖出用 `ask_price_1`；价格变化触发 `cancel_all`；active order 终态清理与 `finish` | vn.py random source 改为可注入 deterministic source；vn.py object/gateway 类型 |
| `vnpy_algotrading/algos/twap_algo.py` | TWAP 时间片 core | `backend/execution_algos/vnpy_style/twap_lite_core.py` | 复制并改造 timer slicing core | `time`、`interval`、`order_volume = volume / (time / interval)`、`timer_count`、`total_count`、每片前 `cancel_all`、终态 `finish` | vn.py `get_tick/get_contract/round_to` 改为 AIstock tick/contract/board-lot helper |
| `vnpy_algotrading/template.py` | AlgoTemplate 生命周期和 helper | `backend/execution_algos/vnpy_style/base.py` | 提取不依赖 vn.py engine 的生命周期语义 | `update_tick/update_order/update_trade/update_timer` 顺序；`buy/sell/cancel_order/cancel_all` action 语义；`finish` 终态 | `algo_engine` 依赖、UI event side effect、日志 side effect、MainEngine 依赖 |
| `vnpy_algotrading/base.py` | AlgoStatus 和基础常量 | `backend/execution_algos/vnpy_style/base.py`、`backend/execution_algos/vnpy_style/models.py` | 复制/映射 enum 名称和状态语义，并保留 attribution | active/stopped/finished 生命周期概念 | vn.py app 包装和 UI 依赖 |
| `vnpy_algotrading/engine.py` | AlgoEngine 的 algo 管理职责 | `backend/services/paper_trading_v2/execution/minqmt_live_algo_adapter.py` | 不直接复制 runtime engine；复用 active-order routing、event dispatch、lifecycle 设计 | algo lifecycle、tick 分发、order/trade/timer/cancel 路由 | BaseEngine/MainEngine/EventEngine runtime、UI/CSV/setting 持久化 |
| `vnpy_algotrading/algos/iceberg_algo.py` | Iceberg 未来候选 | 本文档 inventory；后续资金规模触发再建 issue | 仅登记 inventory | timer、cancel-replace、visible volume 语义 | 当前 1000 万以内资金规模不优先 |
| `vnpy_algotrading/algos/stop_algo.py` | Stop 未来候选 | 本文档 inventory；后续条件单需求再建 issue | 仅登记 inventory | trigger + order state 语义 | 不进入第一阶段 |

### 4.3 AIstock 目标目录

```text
backend/execution_algos/vnpy_style/
  attribution.py              # repo/commit/license/source file/name mapping
  base.py                     # derived responsibilities from template.py/base.py
  models.py                   # tick/order/trade/timer/action DTOs; no vn.py object dependency
  sniper_core.py              # derived from vnpy_algotrading/algos/sniper_algo.py
  best_limit_core.py          # derived from vnpy_algotrading/algos/best_limit_algo.py
  twap_lite_core.py           # derived from vnpy_algotrading/algos/twap_algo.py

backend/services/paper_trading_v2/execution/
  minqmt_live_algo_adapter.py # core action -> MiniQMT submit/cancel/query
  minqmt_order_state.py       # active/terminal/partial/rejected/timeout/deferred
  minqmt_execution_audit.py   # native request/response/status_msg/cash/position/cost
  minqmt_execution_report.py  # execution quality / broker cost reconciliation
```

### 4.4 复用边界

| vn.py 内容 | 是否直接复用 | AIstock 处理 |
|---|---|---|
| Sniper/BestLimit/TWAP 算法 core | 是，复制/改造 | 保留上游分支、状态变量和生命周期顺序 |
| `AlgoTemplate` 生命周期 | 是，抽取不依赖 engine 的部分 | AIstock action DTO 承接 submit/cancel/fail-fast 副作用 |
| `AlgoEngine` 管理职责 | 部分设计复用 | 不运行第二套 engine；Paper v2 scheduler/runtime 仍是所有者 |
| `EventEngine` 事件模型 | 只复用语义 | 由 AIstock MiniQMT adapter 分发 tick/order/trade/timer/error |
| Gateway/MainEngine | 不复用 | 保持 AIstock MiniQMT broker-authoritative 边界 |
| UI/CSV/setting 持久化 | 不复用 | 由 AIstock Paper v2 UI/API 和 issue workflow 管理 |

### 4.5 职责分界

| 职责 | vn.py 来源 | AIstock 归属 |
|---|---|---|
| 下单/撤单/追价核心 | Sniper/BestLimit/TWAP core | 复制/改造到 `vnpy_style/*_core.py` |
| tick/order/trade/timer 回调 | `AlgoTemplate.update_*` | MiniQMT adapter 调用 pure core |
| 柜台提交 | vn.py Gateway | AIstock `qmt_client` / `MiniQMTSimBackend` |
| native status 诊断 | gateway/order callback 语义 | AIstock 持久化 raw order/trade/error/status_msg |
| 费用/滑点/TCA/对账 | 非 vn.py 算法职责 | AIstock Paper v2 repository + report |
| QE shadow 语义迁移 | AIstock 职责 | AIstock QE/Paper v2 对齐 |

### 4.6 禁止项

- 禁止把 `SNIPER_MINIQMT` 写成同名自研算法。
- 禁止缺少 vn.py 源文件、commit、license 或 derived-from 证据。
- 禁止把 vn.py `EventEngine`/`AlgoEngine` 作为 Paper v2 第二套生产 runtime。
- 禁止用 vn.py gateway 替换 AIstock MiniQMT broker backend。
- 禁止 core 模块 import DB、FastAPI、MiniQMT client 或生产配置。
- 禁止没有 characterization tests 就宣称行为与 vn.py 一致。

## 5. Sniper / BestLimit / TWAP-lite 源码迁移设计

本节要求直接从 `vnpy_algotrading` 对应源码迁移核心语义。AIstock 可以因 A 股和 MiniQMT 规则增加风控、审计、board lot、撤单上限和成本字段，但必须保留上游核心行为映射。

### 5.1 `SNIPER_MINIQMT`

上游行为来源：`vnpy_algotrading/algos/sniper_algo.py`。

上游关键语义：

| 上游语义 | AIstock 迁移要求 |
|---|---|
| 有活动 `vt_orderid` 时先 `cancel_all()`，等待下一轮 | 有 active child order 时先发 cancel action，不叠加新委托 |
| 买入：`ask_price_1 <= price` 才发单 | 买入只在卖一不高于 limit/target price 时发限价单 |
| 卖出：`bid_price_1 >= price` 才发单 | 卖出只在买一不低于 limit/target price 时发限价单 |
| order volume 取剩余数量和盘口一档数量较小值 | child quantity 取 remaining、盘口量、board lot、max_child_order 的最小可成交单位 |
| order 非 active 后清空 `vt_orderid` | terminal order 后清空 active child order 并继续/结束 |
| trade 数量达到目标后 `finish()` | filled >= target 后 terminal success，否则继续等待 |

AIstock 扩展参数：

```json
{
  "algo_code": "SNIPER_MINIQMT",
  "source": "vnpy_algotrading.sniper_algo",
  "limit_price": 0,
  "max_price_chase_bps": 30,
  "timeout_seconds": 20,
  "cancel_replace_limit": 3,
  "max_child_order_value": 500000,
  "allow_partial_fill": true,
  "final_unfilled_action": "DEFER_OR_FAIL"
}
```

验收重点：characterization tests 必须覆盖 active order 先撤单、买卖盘口触发条件、一档盘口量截断、terminal 后继续/结束、timeout 后不伪装成功。

### 5.2 `BEST_LIMIT_MINIQMT`

上游行为来源：`vnpy_algotrading/algos/best_limit_algo.py`。

上游关键语义：

| 上游语义 | AIstock 迁移要求 |
|---|---|
| 买入无活动单时按 `bid_price_1` 挂单 | 买入默认 join bid，可配置 opponent/join |
| 卖出无活动单时按 `ask_price_1` 挂单 | 卖出默认 join ask，可配置 opponent/join |
| 当前挂单价与最新一档价格不一致时 `cancel_all()` | 价格变化触发 cancel-replace，但受 cooldown 和 max_cancel_replace 约束 |
| `min_volume/max_volume` 随机生成挂单量 | 保留随机量语义，但加 deterministic seed 以便测试和回放 |
| order 非 active 后清空 `vt_orderid/order_price` | terminal 后清空 active state，等待下一 tick |
| 成交达到目标后 `finish()` | filled >= target 后 terminal success |

AIstock 扩展参数：

```json
{
  "algo_code": "BEST_LIMIT_MINIQMT",
  "source": "vnpy_algotrading.best_limit_algo",
  "quote_side": "JOIN_OR_OPPONENT",
  "min_child_volume": 100,
  "max_child_volume": 5000,
  "reprice_interval_seconds": 10,
  "max_cancel_replace": 5,
  "random_seed_mode": "run_deterministic",
  "timeout_seconds": 120
}
```

验收重点：characterization tests 必须覆盖买/卖一档价格选择、价格变化撤单、随机量范围、terminal 清空、partial fill 后继续。

### 5.3 `TWAP_LITE_MINIQMT`

上游行为来源：`vnpy_algotrading/algos/twap_algo.py`。

上游关键语义：

| 上游语义 | AIstock 迁移要求 |
|---|---|
| `order_volume = volume / (time / interval)` | 按总量、持续时间、间隔生成固定 child slice |
| timer 每秒累计，达到 interval 后执行一次 | 用 Paper v2 scheduler/timer event 驱动，不依赖 vn.py EventEngine |
| 每个 interval 先 `cancel_all()` | 下一片前先撤销未完成 active child order，并记录 partial/unfilled |
| 买入仅当 `ask_price_1 <= price` | 买入切片仍需满足限价和盘口条件 |
| 卖出仅当 `bid_price_1 >= price` | 卖出切片仍需满足限价和盘口条件 |
| 达到总执行时间后 `finish()` | time exhausted 后按 filled/unfilled 输出 terminal success/partial/deferred/fail |

AIstock 扩展参数：

```json
{
  "algo_code": "TWAP_LITE_MINIQMT",
  "source": "vnpy_algotrading.twap_algo",
  "duration_seconds": 600,
  "interval_seconds": 60,
  "max_child_order_value": 500000,
  "price_mode": "LIMIT_WITH_BEST_QUOTE_CHECK",
  "cancel_before_next_slice": true,
  "final_unfilled_action": "DEFER_OR_FAIL"
}
```

验收重点：characterization tests 必须覆盖 timer_count/total_count、slice quantity、每片前 cancel_all、盘口条件、执行时间结束后的终态。

### 5.4 AIstock 必须新增但不改变上游核心语义的扩展

| 扩展 | 原因 | 不允许破坏的上游语义 |
|---|---|---|
| board lot / A 股最小交易单位 | A 股股票交易约束 | 仍以上游 child volume 为上限，再做合法化 |
| 涨跌停/停牌/no quote 状态 | A 股市场状态 | 业务 no-fill/wait，不伪装成交 |
| broker status/raw msg 诊断 | MiniQMT 柜台返回复杂 | 不吞掉上游 order/trade 状态 |
| 资金释放与预算重算 | 组合调仓需要 | 不让买单在 cash 不足时盲目提交 |
| 成本与 TCA | QE/Paper 研究闭环需要 | 不把成本逻辑混入 core 决策 |
| deterministic random seed | BestLimit 可测试可复现 | 保留 min/max random volume 语义 |


## 6. MiniQMT 成本数据能力分析

当前本地 `xtquant` 类型和 AIstock `qmt_client` 显示：

- `XtTrade` 包含 `traded_amount`：成交金额。
- `XtTrade` 包含 `commission`：手续费字段。
- `qmt_client.get_trades()` 已读取并返回 `commission`。
- `XtAsset` 包含 `cash`、`frozen_cash`、`market_value`、`total_asset`、`fetch_balance`。
- `XtPosition` 包含 `avg_price`、`market_value`、`last_price` 等持仓成本和市值字段。

但需要注意：

1. `commission` 是否包含印花税、过户费、规费，取决于 MiniQMT / 券商柜台返回口径，不能假设。
2. 本地 xtquant `XtTrade` 未看到独立 `stamp_tax` / `transfer_fee` 字段。
3. 若柜台只返回合计 `commission`，AIstock 需要把字段命名为 `broker_reported_commission` 或 `broker_reported_fee_total`，并保存 `cost_breakdown_source="broker_reported_aggregate"`。
4. 若需要精确拆分印花税、过户费，需要额外做费用模型估算，并与 broker reported aggregate 对账。
5. 成本精确统计要以成交回报为主，以费用模型为解释和回测校准补充。

建议新增成本口径：

| 字段 | 含义 | 来源 |
|---|---|---|
| `trade_amount` | 成交金额 | MiniQMT `traded_amount` |
| `broker_reported_commission` | broker 返回手续费/费用字段 | MiniQMT `commission` |
| `broker_cost_schema` | broker 费用口径版本 | AIstock 标注 |
| `estimated_stamp_tax` | 按 A 股规则估算印花税 | AIstock fee model |
| `estimated_transfer_fee` | 按市场规则估算过户费 | AIstock fee model |
| `estimated_broker_commission` | 按账户费率估算佣金 | AIstock fee model |
| `cost_reconciliation_delta` | broker reported 与估算合计差额 | AIstock reconciliation |
| `cost_precision_level` | `broker_aggregate` / `estimated_breakdown` / `broker_breakdown` | AIstock 标注 |

### 6.1 精确成本统计的推荐口径

MiniQMT 能否“获取真实交易手续费、印花税等成本”的答案应拆成两层：

1. **真实 broker aggregate**：成交回报里的 `commission` 是 MiniQMT/柜台返回字段，应作为真实来源保存；但字段名不应直接解释为“纯佣金”，除非券商文档或实测确认。
2. **估算 cost breakdown**：印花税、过户费、规费、账户佣金费率应由 AIstock fee model 按市场规则和账户配置估算，生成可解释拆分。
3. **每日 reconciliation**：用 `broker_reported_fee_total` 对比 `estimated_stamp_tax + estimated_transfer_fee + estimated_broker_commission + estimated_exchange_fee`，输出差异和阈值告警。
4. **回测使用版本化成本模型**：QE 不能只用固定 `rate/slippage`；至少要记录 `fee_model_version`、买卖方向、交易市场、成交金额、最低佣金、卖出印花税规则和成本精度等级。

在未确认券商拆分字段前，禁止在 UI、报告或 QE 结果中声称“已精确拆分印花税/过户费”；可以声称“已保存 broker 返回聚合费用，并提供估算拆分与对账”。

## 7. QE 回测执行模型迁移策略

### 7.1 不立即全量迁移

现阶段不建议把 Sniper/BestLimit 的初版直接塞进 QE 作为正式实验选项。原因：

- MiniQMT 的真实订单状态、成本口径、撤单重报行为仍需验证。
- Sniper/BestLimit 依赖盘口/tick，如果 QE 当前主要是分钟线数据，直接迁移会产生数据降维误差。
- 如果用分钟 close 模拟 Sniper，可能制造“看似精确、实则伪造”的执行效果。

### 7.2 推荐三阶段迁移

#### Phase A：MiniQMT-only 验证

- 只在 Paper v2 MiniQMT SIM 启用。
- 保存完整 broker audit 和成本数据。
- 输出每日 execution quality report。
- QE 不使用该算法，只记录为 future candidate。

#### Phase B：QE 影子回放模型

- 用 MiniQMT SIM 真实结果统计 fill probability、slippage、reject、partial fill、timeout。
- 在 QE 中新增 `miniqmt_calibrated_shadow_v1`，只用于对照分析，不作为正式策略排名依据。
- 对同一 prediction_hash 跑 TWAP baseline、BestLimit shadow、Sniper shadow、V25 baseline。

#### Phase C：正式 QE execution policy

- MiniQMT 算法参数稳定。
- 成本口径稳定。
- 影子回放在样本外验证通过。
- 才允许创建 `validated_execution_policy` 并开放 QE 正式选择。

### 7.3 QE 成本模型更新条件

若当前 QE 执行算法不满足以下条件，就需要更新：

- 能区分买入/卖出费用。
- 能计入印花税只对卖出收取。
- 能记录 broker-reported aggregate cost 与 estimated breakdown。
- 能计入部分成交和未成交带来的组合偏离。
- 能用 MiniQMT 成交样本校准 slippage / fill probability。
- 能版本化成本模型，保证历史实验可复现。

## 8. V25 的重新定位

按 vn.py 架构，V25 仍有价值，但不是优先级最高。

### 8.1 V25 的价值

V25 可以作为智能执行计划层：

- 生成 240 分钟权重计划。
- 根据开盘缺口、涨跌停状态、日内特征调整执行节奏。
- 与 Sniper/BestLimit 组合：V25 决定每个时间片目标数量，Sniper/BestLimit 决定 child order 如何落地。

### 8.2 V25 当前不应承担的职责

- 不直接处理 MiniQMT 连接。
- 不直接管理现金、持仓、可卖数量。
- 不直接解释柜台拒单。
- 不绕过平台风控。
- 不在模型缺失时 fallback 到 TWAP。

### 8.3 V25 能否提升收益

V25 提升的不是 alpha，而是执行收益：

```text
净收益改善 = 滑点下降 + 成交率提高 + 未成交损失下降 + 调仓偏离下降 - 额外撤单/等待成本
```

对 1000 万以内资金规模，V25 仍可能有效，但必须和更直接的 Sniper / BestLimit 对比。如果 Sniper/BestLimit 在成交确定性和滑点上已经足够好，V25 可能只在以下场景增值：

- 单票金额较大。
- 开盘跳空明显。
- 涨跌停附近。
- 需要避免开盘瞬时成交风险。
- 需要根据日内时间分布降低未成交概率。

因此 V25 优先级降为 P2：先完成 MiniQMT 基础状态机与 Sniper/BestLimit，再把 V25 接入为 schedule 层。

## 9. 推荐实施路线

### 9.1 第一阶段：MiniQMT 可靠执行与诊断闭环

交付：

- MiniQMT callback collector。
- broker audit persistence。
- failed order diagnostics。
- cost audit fields。
- active order state machine。
- sell cash release wait / budget recompute。

验证：

- 失败订单可还原完整链路。
- 买单不会在卖出资金未释放时盲目全量提交。
- 成本字段能从 trades 中读取并进入 Paper v2 fills / TCA。

### 9.2 第二阶段：Sniper / BestLimit / TWAP-lite MiniQMT SIM

交付：

- `SNIPER_MINIQMT` live adapter。
- `BEST_LIMIT_MINIQMT` live adapter。
- `TWAP_LITE_MINIQMT` live adapter。
- execution quality report。

验证：

- 同一目标组合在三种算法下对比成交率、滑点、撤单次数、拒单率、成本。
- 只在 MiniQMT SIM 验证，不进入 QE 正式实验排名。

### 9.3 第三阶段：QE 影子校准

交付：

- `miniqmt_calibrated_shadow_v1` 回测成本/成交模型。
- fixed-prediction execution comparison。
- broker cost reconciliation report。

验证：

- 同一 prediction_hash 下对比 TWAP、BestLimit shadow、Sniper shadow、V25。
- 不允许数据偷看：只使用历史上已发生的 MiniQMT 样本校准未来样本。

### 9.4 第四阶段：V25 live schedule 与正式 QE policy

交付：

- `V25_LIVE_SCHEDULE_MINIQMT`。
- V25 + Sniper/BestLimit child-order adapter。
- QE 正式 validated execution policy。

验证：

- V25 实盘模拟与 QE 回测执行事件一致。
- V25 不再只是回测算法，也不再被 MiniQMT 路径跳过。

## 10. 验收指标

| 指标 | 目标 |
|---|---|
| 失败订单诊断覆盖率 | 100% rejected/cancelled/timeout 订单有诊断包 |
| 成本字段覆盖率 | 100% fills 有 `trade_amount` 与 cost precision 标注 |
| broker 成本对账 | 每日输出 broker aggregate 与 estimated breakdown 差异 |
| 买单资金不足拒单率 | 应显著下降，理想为 0 |
| 未成交显式状态 | 100% partial/unfilled 不伪装成功 |
| Sniper/BestLimit 对比 | 至少 20 个交易日或足够订单样本后再决定 QE 迁移 |
| QE 迁移门槛 | 样本外 shadow 模型表现稳定，且 MiniQMT 语义冻结 |

## 11. 风险与约束

| 风险 | 缓解 |
|---|---|
| MiniQMT `commission` 口径不明 | 保存 broker aggregate，增加 cost_precision_level，不伪称拆分精确 |
| Sniper 依赖 tick/盘口，QE 数据不匹配 | MiniQMT-only 先验证，QE 先 shadow，不直接正式排名 |
| 频繁撤单触发柜台/监管限制 | 增加 max_cancel_replace、cooldown、日内撤单计数 |
| 资金等待导致错过买入 | 先卖后买 + cash recompute + budget clip，而非盲目全量买入 |
| 回测与实盘再次漂移 | 每个 execution_model_version 固化 schema、参数、样本窗口、hash |

## 12. 后续 issue 拆分建议

1. `BUG/P1: MiniQMT failed-order diagnostics and broker audit are incomplete`。
2. `BUG/P1: MiniQMT buy legs can be submitted before sell cash is released`。
3. `FEATURE/P1: Add SNIPER_MINIQMT and BEST_LIMIT_MINIQMT live execution adapters`。
4. `FEATURE/P1: Persist broker-reported costs and cost precision for MiniQMT fills`。
5. `FEATURE/P2: Add MiniQMT-calibrated shadow execution model for QE`。
6. `FEATURE/P2: Add V25 live schedule adapter for MiniQMT child orders`。

所有 issue 必须通过 `scripts/aistock_issue_workflow.py` 创建/同步，不得手工绕过 BUG JSON/GitHub 同步。



## 12.1 更新后的 issue 拆分建议（与 vn.py 源码复用一致）

1. `FEATURE/P1: Add vn.py source inventory and MIT attribution gate for execution algos`。
2. `FEATURE/P1: Port vnpy_algotrading Sniper core with characterization tests`。
3. `FEATURE/P1: Port vnpy_algotrading BestLimit core with characterization tests`。
4. `FEATURE/P1: Port vnpy_algotrading TWAP core as TWAP_LITE_MINIQMT with characterization tests`。
5. `FEATURE/P1: Add MiniQMT live algo adapter and active order state machine`。
6. `FEATURE/P1: Add MiniQMT failed-order diagnostics and broker audit persistence`。
7. `FEATURE/P1: Add MiniQMT cost capture, fee precision, and reconciliation report`。
8. `FEATURE/P2: Add MiniQMT-calibrated QE shadow execution model`。
9. `FEATURE/P2: Add V25 schedule seam for Sniper/BestLimit child orders`。

执行要求：以上 issue 必须通过 `scripts/aistock_issue_workflow.py` 创建/同步。第一批建议只并行处理 1、2、5、6；它们决定 license、源码迁移、adapter 和 runtime 安全边界。

每个后续实施 issue 的 closure requirements 必须引用第 4.2 节对应源文件映射，并补充 `source_inventory`、attribution、license notice 和 characterization test 证据。



## 13. DESIGN-COMPLIANCE-001 文档验收矩阵

| design_item | implementation_refs | test_or_evidence | status | gap_or_exception |
|---|---|---|---|---|
| 根据 1000 万以内资金规模重排执行算法优先级 | 本文第 0、3、9 节 | 文档明确 P0/P1/P2/P3，降低市场冲击和 AC/Iceberg 优先级 | 完成 | 后续需 issue 化实施 |
| 回答是否先 MiniQMT 验证再迁移 QE | 本文第 2、7、9 节 | 明确 MiniQMT-first、QE-follow，分 Phase A/B/C | 完成 | QE shadow 需后续开发 |
| 回答 vn.py 是否更成熟以及 AIstock 缺陷 | 本文第 1.1、1.2 节 | 明确 vn.py 在事件驱动、算法模板、订单生命周期、常用算法方面更成熟；AIstock 保留 QE/Paper/审计优势 | 完成 | 后续实现需按对比缺口拆 issue |
| 将 vn.py 复用要求前移到详细设计主体 | 本文第 4、5 节 | 明确 Sniper/BestLimit/TWAP 必须从 `vnpy_algotrading` 源码语义迁移，不再只放在最后章节 | 完成 | 真正代码迁移需保留 attribution 和 characterization tests |
| 分析 Sniper / BestLimit / TWAP-lite 落地路径 | 本文第 3、5 节 | 给出上游源码映射、参数 schema、行为、适用场景 | 完成 | 参数需用真实样本校准 |
| 分析 MiniQMT 成本字段与印花税/过户费口径 | 本文第 6、6.1 节 | 结合本地 `xtquant` 类型和 `qmt_client` 字段，区分 broker aggregate 与 estimated breakdown | 完成 | 未做真实柜台文档确认，不声称拆分字段存在 |
| 分析 QE 执行成本模型是否需要更新 | 本文第 7.3、9.3、10 节 | 列出更新条件、成本模型版本化和样本外验证要求 | 完成 | 具体 schema 需 issue 实施 |
| 回答是否可以直接复用 vn.py 代码 | 本文第 1.1、1.3、4、5 节 | 明确 MIT license gate、source inventory、derived-from、characterization tests 和 adapter 边界 | 完成 | 真正复制代码前必须执行 issue 化 license gate |
| 补充严格项目验收矩阵 | 本文第 10、13 节 | 覆盖失败诊断、成本、broker 对账、Sniper/BestLimit 样本、QE 迁移门槛和文档一致性 | 完成 | 每个实施 issue 需截取相关行形成 closure requirements |
| 更新 issue 拆分 | 本文第 12、12.1 节 | 拆出 source inventory、算法 port、adapter、diagnostics、cost、QE shadow、V25 seam | 完成 | issue 创建必须走 `scripts/aistock_issue_workflow.py` |
| 保持方案前后一致 | 本文第 0、1.3、4、5、12.1、13 节 | 关键结论、设计原则、算法设计、issue 拆分、验收矩阵都指向源码迁移 + AIstock adapter | 完成 | 旧的泛泛“参考”表述已替换为源码复用边界 |
| 使用独立 worktree 和分支落地 MD 文档 | `docs/architecture/miniqmt_execution_priority_and_qe_migration_design_20260529.md` | worktree `F:\Dev\AIstock_worktrees\miniqmt-execution-priority-20260529`，branch `docs/miniqmt-execution-priority-20260529` | 完成 | 本文档不触碰生产运行 |
