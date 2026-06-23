# MiniQMT LIVE Readiness Gap Investigation (2026-06-23)

> 调查角色：实盘准入调查员。
> 范围：按 ADR 0002 禁止事项与 `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` §12，只调查并登记缺口，不实施修复。
> 结论：MiniQMT 仍禁止接入 `MINIQMT_MODE=LIVE`；三道实盘准入门均存在未关闭缺口。

## 0. 权威与安全边界

- ADR 0002 明确列出实盘准入硬门槛：盘前风控层、cash-overcommit 修复、断连/券商掉线处置（`docs/adr/0002-miniqmt-execution-runtime-event-loop-target-architecture.md:36`）。
- ADR 0002 禁止在缺失盘前风控层与未修复 cash-overcommit 的情况下接入任何实盘账户（`docs/adr/0002-miniqmt-execution-runtime-event-loop-target-architecture.md:63`）。
- 详细设计 §12 将实盘准入门定义为独立于 A/B、优先级高、实盘前必须完成（`docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md:163`）。
- 本轮未启动/重启任何服务，未写生产 DB/DDL，未做任何 `MINIQMT_MODE=LIVE` 尝试；只做本地只读调查、一个失败测试复现、文档与 BUG 登记。

## 1. 准入门一：pre-trade 风控 / kill-switch

### 现状

A event_loop 已有实时风控挂载点，但它不是完整盘前风控层：

- `ConfigurableMiniQMTRiskEngine` 配置项覆盖 `kill_on_disconnect`、`max_child_order_quantity`、`max_child_order_notional`、`max_total_exposure`、`max_symbol_exposure`、`max_loss`、`price_bands`（`backend/services/miniqmt_execution_runtime/risk.py:128`）。
- A 在 `submit_child_order` 前调用 kill-switch 与 pre-submit 风控（`backend/services/miniqmt_execution_runtime/runtime.py:332`、`backend/services/miniqmt_execution_runtime/runtime.py:357`）。
- A 的断连事件会写入 `GATEWAY_DISCONNECTED` 并交给风险引擎（`backend/services/miniqmt_execution_runtime/runtime.py:707`、`backend/services/miniqmt_execution_runtime/risk.py:209`）。
- A kill-switch 会暂停 runtime、撤活跃 child order、终结活跃算法实例并写 `RISK_KILL_SWITCH_TRIGGERED`（`backend/services/miniqmt_execution_runtime/runtime.py:983`、`backend/services/miniqmt_execution_runtime/runtime.py:1063`）。

B/compiler 与实际下单链路仍只有局部 preflight，不满足 §12 的盘前风控：

- `QmtManagedOrderService.preview_order()` 仅覆盖 symbol/side/order_type/quantity、买入 board lot、买入正价格、order_remark 去重、虚拟策略现金、卖出 T+1 可用数量（`backend/services/qmt_strategy_ledger/order_service.py:236`、`backend/services/qmt_strategy_ledger/order_service.py:267`、`backend/services/qmt_strategy_ledger/order_service.py:275`）。
- `MiniQMTSimBackend.submit_order_intent()` 最终直接调用 `_qmt_client.place_order(...)`（`backend/services/paper_trading_v2/broker/minqmtsim.py:225`、`backend/services/paper_trading_v2/broker/minqmtsim.py:240`）。
- `XtQuantQMTClient.place_order()` 仅 `_require_connected()` 后调用 `order_stock(...)`，未看到产品侧价格笼子、防胖手指、买力、提交时 kill-switch（`backend/infra/qmt_client.py:1224`、`backend/infra/qmt_client.py:1235`、`backend/infra/qmt_client.py:1244`）。
- grep 证据：`rtk rg -n 'pre_trade_risk|price_collar|fat_finger|buying_power|kill_switch|kill-switch|submit-time|submit_time' backend/services/qmt_strategy_ledger backend/services/paper_trading_v2/broker/minqmtsim.py backend/infra/qmt_client.py backend/routers/qmt.py` 无输出。

### 缺口

- B/compiler 与 MiniQMTSim/qmt_client 提交路径缺少统一盘前风控层：仓位限额、真实/账户组买力、价格笼子、防胖手指、提交时 kill-switch。
- A 的 realtime RiskEngine 只覆盖 event_loop 内部实时挂载点，不能替代 B fallback/compiler 路径的实盘准入门。
- raw/admin 诊断路由虽已有管控开关，但 qmt_client 本身仍没有一层统一 submit-time risk gate；未来任何绕过上层 service 的调用都会裸到 `order_stock`。

### 严重度

- 建议 P0：ADR 0002 将该门列为接 LIVE 的禁止事项；缺口未关前禁止实盘。

### 对实盘的风险

- 可能提交超量、超额、价格偏离、无买力或 kill-switch 已触发后的真实券商订单。
- 若 B 作为 fallback 保留，灰度回退到 compiler 仍可能绕过 event_loop 风控。
- 依赖券商柜台拒单会把产品侧风险控制后置，无法满足实盘准入的 submit-before-broker 要求。

### 建议 BUG 与 scope

- 已登记：BUG-500 / GitHub Issue #1550：`MiniQMT LIVE pre-trade risk layer missing on compiler submit path`。
- 建议 scope：`qmt_strategy_ledger` 订单 preflight、`MiniQMTSimBackend` 提交前钩子、`qmt_client.place_order` 防绕过 gate、相关 qmt/paper_v2 测试。

## 2. 准入门二：cash-overcommit 失败

### 现状

复现命令：

```powershell
rtk python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py::test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots -q
```

结果：失败，`broker.place_order_calls == 0` 断言实际为 `1`。

关键事实：

- 测试构造一个 account group，`cash_limit=15000`，两个 slot 各 `allocated_cash=7500`（`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:337`、`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:343`、`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:352`、`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:362`）。
- 测试将两个 virtual account 的 cash 都改成 `10000`，随后提交两笔各 `10000` 的 BUY，总需求 `20000 > 15000`（`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:370`）。
- 当前结果 `preflight_passed is False`，但仍有一次 broker call；测试要求全批次拒绝且 `broker.place_order_calls == 0`（`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:380`、`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py:381`）。
- 根因路径：`submit_batch()` 将 capacity residual 视为 non-compensating，不触发全批次 hard preflight failure（`backend/services/qmt_strategy_ledger/order_service.py:427`、`backend/services/qmt_strategy_ledger/order_service.py:428`、`backend/services/qmt_strategy_ledger/order_service.py:429`）。
- group 超额后只给后续 BUY 增加 `SKIPPED_INSUFFICIENT_CAPITAL` / `scope="account_group"`（`backend/services/qmt_strategy_ledger/order_service.py:817`、`backend/services/qmt_strategy_ledger/order_service.py:820`）。
- `_is_non_compensating_batch_residual()` 将 `_is_capacity_residual_skipped()` 归为非硬失败，导致前面 cash-fit 的 BUY 仍提交到 broker（`backend/services/qmt_strategy_ledger/order_service.py:1557`、`backend/services/qmt_strategy_ledger/order_service.py:1567`、`backend/services/qmt_strategy_ledger/order_service.py:1571`）。

### 缺口

- 分仓间账户组现金门未实现“对本批次 account-group 过额时 broker 零提交”的实盘准入语义。
- 当前逻辑在模拟盘 capacity allocator 语义下会跳过后续残差，但实盘准入指定的 characterization test 要求该 overcommit batch 不得产生任何 broker side effect。
- 0608 设计中 capacity residual 可以表达 slot/intent 级跳过；但 ADR 0002 对 LIVE 准入明确点名该失败测试为真金超杠杆风险，本轮按 ADR 作为更高优先级门禁处理。

### 严重度

- 建议 P0：ADR 0002 明确写为实盘前必须 triage 修复；复现证明不是测试环境问题。

### 对实盘的风险

- account group 现金上限已经被 batch 总需求突破时，系统仍能先发出至少一笔真实券商委托。
- 若多个 slot 的虚拟现金被膨胀或不同步，可能形成跨策略槽的真金超额买入/超杠杆风险。
- 风险在 broker 前已经产生 side effect，后续 skip/partial 状态不能完全抵消实盘委托风险。

### 建议 BUG 与 scope

- 已登记：BUG-501 / GitHub Issue #1551：`MiniQMT account-group cash overcommit batch can still submit broker order`。
- 建议 scope：`backend/services/qmt_strategy_ledger/order_service.py`、`backend/tests/qmt_strategy_ledger/test_order_service_preflight.py`；Tier2 需先明确策略：LIVE admission 下是 account-group overcommit 全批次拒绝，还是引入严格的预分配且测试更新需经设计批准。

## 3. 准入门三：断连 / 券商掉线处置

### 现状

A event_loop 已有断连到 kill-switch 的形态：

- `record_disconnect_event()` 写 `MINIQMT_GATEWAY_DISCONNECTED`，把 runtime 置为 `PAUSED`、gateway 置为 `DISCONNECTED`（`backend/services/miniqmt_execution_runtime/runtime.py:707`、`backend/services/miniqmt_execution_runtime/runtime.py:725`）。
- RiskEngine 默认 `kill_on_disconnect=True`，遇到 `GATEWAY_DISCONNECTED` 返回 `MINIQMT_RISK_DISCONNECT_KILL_SWITCH`（`backend/services/miniqmt_execution_runtime/risk.py:128`、`backend/services/miniqmt_execution_runtime/risk.py:209`）。
- kill-switch 会撤活跃 child orders、终结活跃 algo instances 并写 durable 事件（`backend/services/miniqmt_execution_runtime/runtime.py:983`、`backend/services/miniqmt_execution_runtime/runtime.py:1012`、`backend/services/miniqmt_execution_runtime/runtime.py:1045`、`backend/services/miniqmt_execution_runtime/runtime.py:1061`）。

B/compiler 与 qmt_client 仍是被动探测/调用点错误传播：

- qmt_client 只做 `trader.subscribe(account)` best-effort，未在生产 client 中注册/扇出 `on_disconnected` callback（`backend/infra/qmt_client.py:268`）。
- `_probe_connection_locked()` 注释说明探测仅在 `status()` 被调用时发生，例如 UI 打开/刷新（`backend/infra/qmt_client.py:699`、`backend/infra/qmt_client.py:727`）。
- `status()` 只记录断连/恢复日志并更新 `_last_status_connected`，没有冻结 portfolio/slot、撤单、reconcile 或 durable alert（`backend/infra/qmt_client.py:751`、`backend/infra/qmt_client.py:772`、`backend/infra/qmt_client.py:775`、`backend/infra/qmt_client.py:778`）。
- `_require_connected()` 与 `place_order()` 只在当前调用点失败；没有影响全局运行态（`backend/infra/qmt_client.py:794`、`backend/infra/qmt_client.py:1224`、`backend/infra/qmt_client.py:1235`）。
- `MiniQMTSimBackend.ensure_connected()` 会自动尝试连接并要求 SIM mode；`submit_order_intent()` 对当前调用的 `QMTNotAvailableError` 映射为 `BrokerConnectivityError`，但不会冻结其它 in-flight slot/order（`backend/services/paper_trading_v2/broker/minqmtsim.py:178`、`backend/services/paper_trading_v2/broker/minqmtsim.py:198`、`backend/services/paper_trading_v2/broker/minqmtsim.py:225`、`backend/services/paper_trading_v2/broker/minqmtsim.py:250`）。
- `/api/v1/qmt/disconnect` 是手工断开接口；`reload_client()` 对 reload 期间 disconnect 失败只打 debug，不是生产盘中断连处置策略（`backend/routers/qmt.py:102`、`backend/routers/qmt.py:113`、`backend/routers/qmt.py:115`、`backend/routers/qmt.py:160`）。
- grep 证据显示生产路径没有 B/compiler disconnect monitor + freeze/kill-switch 组合；`on_disconnected` 主要出现在 PoC 脚本，不在 qmt_client 生产提交链路。

### 缺口

- B/compiler fallback 路径缺少“券商断连即冻结新单、处理活跃订单、告警、重连后 reconcile”的生产策略。
- qmt_client 缺少生产级断连事件源与 durable alert；当前状态探测依赖 UI/status/query 调用，可能在无人值守盘中延迟发现。
- 当前错误传播只覆盖单次 submit/query/cancel，不会对同 account group/portfolio/strategy_slot 的其它 in-flight 任务形成统一 kill-switch 或 freeze。

### 严重度

- 建议 P1，Tier2 可按实盘硬门提升为 P0：A 已有 event_loop 断连处理，但 B fallback/compiler 仍是实盘准入缺口；ADR §12 明确 “A 内建，B 需补”。

### 对实盘的风险

- 券商掉线后，系统可能继续生成/排队新单，直到某个调用点碰到连接错误才失败。
- 断连期间活跃委托状态不明，缺少统一 cancel/reconcile/alert，可能导致重复提交、漏撤或账实偏离。
- 灰度回退 B 时，event_loop 的断连 kill-switch 语义会丢失。

### 建议 BUG 与 scope

- 已登记：BUG-502 / GitHub Issue #1552：`MiniQMT compiler path lacks production disconnect freeze kill-switch handling`。
- 建议 scope：`backend/infra/qmt_client.py` 断连事件/状态源，`paper_trading_v2/broker/minqmtsim.py` 与 `simulation_runtime` freeze/alert/reconcile 挂载点，qmt_strategy_ledger 活跃订单查询/撤单协作，以及对应测试。

## 4. BUG 登记汇总

| BUG | GitHub Issue | Severity | 主题 | 本轮状态 |
| --- | --- | --- | --- | --- |
| BUG-500 | #1550 | P0 | B/compiler submit path 缺 unified pre-trade risk layer | 已登记，不修 |
| BUG-501 | #1551 | P0 | account-group cash overcommit batch 仍会 broker submit | 已登记，不修 |
| BUG-502 | #1552 | P1 | B/compiler 断连缺 freeze / kill-switch / alert / reconcile | 已登记，不修 |

## 5. Tier2 建议决策

1. 先确认 BUG-501 的 policy：LIVE admission 是否强制 account-group overcommit 全批次零 broker side effect；若允许 capacity allocator，需改 ADR/测试并补 broker-before-submit 分配证明。
2. BUG-500 与 BUG-502 都是 B fallback/compiler 的实盘准入门；即使 A event_loop 可灰度，也不能用 A 的实时钩子声称全系统 LIVE ready。
3. 在三门关闭前，所有 MiniQMT 实盘账户接入仍应保持禁止；SIM dry-run/影子运行证据不能替代这三道门。

## 6. 本轮验证与生产门

- `rtk python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py::test_submit_batch_rejects_account_group_cash_overcommit_across_strategy_slots -q`：失败，复现 BUG-501，`broker.place_order_calls` 实际为 1。
- `rtk rg -n 'pre_trade_risk|price_collar|fat_finger|buying_power|kill_switch|kill-switch|submit-time|submit_time' backend/services/qmt_strategy_ledger backend/services/paper_trading_v2/broker/minqmtsim.py backend/infra/qmt_client.py backend/routers/qmt.py`：无输出，支持 BUG-500 缺口。
- `production_ddl_gate=noop`
- `production_frontend_dependency_gate=noop`
- `production_backend_dependency_gate=noop`
- 未启动/重启服务；未连接 LIVE；未写生产 DB/DDL。
