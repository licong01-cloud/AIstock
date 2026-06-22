# MiniQMT runtime A Phase 0 接缝契约冻结

> 本文是 `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md` Phase 0 交付物。
> A(event_loop) 与 B(compiler) 必须共享同一算法核、同一 gateway 边界、同一 `qmt_strategy_ledger` OMS 权威；A 只能新增事件循环与适配层，不得分叉业务语义。

## 1. 接缝总览

| 接缝 | 权威代码 | A 允许做什么 | A 禁止做什么 |
|---|---|---|---|
| vn.py-style 算法核 | `backend/execution_algos/vnpy_style/` | 用真实 tick/order/trade/timer 事件调用 core，并执行返回的 action | 重写 Sniper/BestLimit/TWAP 逻辑、在 core 中访问 DB/MiniQMT/FastAPI、用合成 timer 冒充生命周期 |
| Gateway 下单/撤单/查询/回调 | `backend/services/miniqmt_execution_runtime/gateway.py` | 包装真实 `XtQuantQMTClient` 回调为 event_loop 事件，保留 raw packet | 新建绕过 runtime 的产品下单路径；在 event_loop path 里用 `return []` 静默吞掉 broker 事实 |
| Durable OMS | `backend/services/qmt_strategy_ledger/` | 通过 qmt_strategy_ledger 记录 order/trade/position-lot/cash/reconcile facts | 把 JSON 文件、内存 dict 或 runtime-local projection 当作 event_loop 事实源 |
| Runtime flag | `backend/services/miniqmt_execution_runtime/config.py` | `MINIQMT_EXECUTION_RUNTIME=event_loop` 时启用 A | 未设置时改变 B 行为；非法 flag 静默 fallback |

## 2. 算法核接口签名

冻结协议：`backend/services/miniqmt_execution_runtime/contracts.py::MiniQMTVnpyAlgoCoreContract`。

必须保持：

```python
start() -> list[VnpyAction]
update_tick(tick: VnpyTick) -> list[VnpyAction]
update_order(order: VnpyOrderUpdate) -> list[VnpyAction]
update_trade(trade: VnpyTradeUpdate) -> list[VnpyAction]
update_timer() -> list[VnpyAction]
get_data() -> VnpyAlgoSnapshot
audit_metadata() -> dict[str, Any]
```

验收点：

- core 只输出 `VnpyAction`，不直接发单、撤单、写 DB 或读 `xtquant`。
- source attribution 仍来自 `backend/execution_algos/vnpy_style/attribution.py`，固定上游 commit `4133987530eb28f3538d1983545d81c4f83d7d59`。
- `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py` 继续防止 core 引入 runtime/broker 依赖。

## 3. Gateway 接口签名

冻结协议：`backend/services/miniqmt_execution_runtime/gateway.py::MiniQMTGateway`。

必须保持：

```python
connect(*, runtime_id: str) -> None
sync_orders(*, runtime_id: str) -> list[dict[str, Any]]
sync_trades(*, runtime_id: str) -> list[dict[str, Any]]
sync_positions(*, runtime_id: str) -> list[dict[str, Any]]
submit_child_order(order: MiniQMTChildOrder) -> MiniQMTGatewayOrderAck
cancel_child_order(order: MiniQMTChildOrder, *, reason: str) -> MiniQMTGatewayCancelAck
```

Phase 1 在 `backend/services/miniqmt_execution_runtime/gateway.py::MiniQMTGatewayEventSource` 上追加真实事件源方法，不改变既有 B 调用签名：

```python
on_order(raw_order: dict[str, Any]) -> MiniQMTExecutionEvent
on_trade(raw_trade: dict[str, Any]) -> MiniQMTExecutionEvent
on_tick(raw_tick: dict[str, Any]) -> MiniQMTExecutionEvent
on_account(raw_account: dict[str, Any]) -> MiniQMTExecutionEvent
on_disconnect(raw_event: dict[str, Any]) -> MiniQMTExecutionEvent
```

验收点：

- B 的 compiler path 可以继续使用现有 `sync_*` 兼容逻辑。
- A 的 event_loop gateway 必须把 `sync_*` 返回真实 broker facts 或 loud failure，不允许 `return []` 作为事实缺失默认值。
- 断连必须写 durable loud event，包含 `reason_code`。

## 4. qmt_strategy_ledger OMS 接口签名

冻结协议：`backend/services/miniqmt_execution_runtime/contracts.py::MiniQMTStrategyLedgerOmsContract`。

event_loop OMS 的权威写读只能来自以下 qmt_strategy_ledger 能力：

```python
get_virtual_account(strategy_id: str) -> VirtualAccount
upsert_order_ledger(order: OrderLedgerRecord) -> OrderLedgerRecord
get_order_ledger(account_id: str, qmt_order_id: str) -> OrderLedgerRecord | None
list_order_ledger(account_id: str, *, trade_date=None, strategy_id=None, open_only=False) -> list[OrderLedgerRecord]
upsert_trade_ledger(trade: TradeLedgerRecord) -> tuple[TradeLedgerRecord, bool]
create_position_lot(lot: PositionLotRecord) -> PositionLotRecord
list_position_lots(strategy_id: str, symbol: str | None = None) -> list[PositionLotRecord]
append_cash_entry_once(entry: CashLedgerEntry) -> tuple[CashLedgerEntry, bool]
```

验收点：

- order/trade/cash/lot/reconcile facts 必须继承 qmt_strategy_ledger 的幂等语义。
- `JsonFileMiniQMTExecutionRuntimeRepository` 只能保留给 compiler 兼容测试或只读调试快照；不得作为 event_loop 权威 OMS。
- 订单状态判定统一使用 `is_open_like_order_status` / `is_terminal_order_status` / `is_partial_order_status`。

## 5. A 模块布局

Phase 1/2 新增代码必须按下列布局落地：

```text
backend/services/miniqmt_execution_runtime/
  config.py          # flag 门控，默认 compiler
  contracts.py       # Phase 0 接缝协议
  gateway.py         # 既有 gateway + Phase 1 real event source adapter
  models.py          # 事件与 runtime DTO；仅 additive
  runtime.py         # B 兼容 runtime helpers + Phase 2 event_loop skeleton
  oms.py             # event_loop OMS facade，底层 qmt_strategy_ledger
  repository.py      # B/测试兼容仓库；event_loop 不以 JSON 为权威
```

## 6. Phase 0 验收证据

- GitHub epic: https://github.com/licong01-cloud/AIstock/issues/1501
- Characterization tests:
  - `backend/tests/miniqmt_execution_runtime/test_miniqmt_phase0_seam_contracts.py`
  - `backend/tests/miniqmt_execution_runtime/test_miniqmt_vnpy_algo_import_boundary.py`
- Flag inert evidence:
  - 未设置 `MINIQMT_EXECUTION_RUNTIME` 时 `get_miniqmt_execution_runtime_kind({}) == compiler`
  - 非法值 loud fail，`reason_code=MINIQMT_EXECUTION_RUNTIME_UNSUPPORTED`
