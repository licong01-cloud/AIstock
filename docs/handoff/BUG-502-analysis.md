# BUG-502 analysis - MiniQMT disconnect freeze / kill-switch handling

## 独立根因

Issue #1552 与当前代码一致：`backend/infra/qmt_client.py` 的断连探测主要发生在 `status()`/查询类 API；下单路径在 `place_order()` 前只做 `_require_connected()`，断连通常只影响当前调用。`backend/services/paper_trading_v2/broker/minqmtsim.py::submit_order_intent()` 捕获 `QMTNotAvailableError` 后只抛出本次 `BrokerConnectivityError`，没有把受影响的 MiniQMT adapter/策略槽置入 freeze/kill-switch 状态，也没有定义“重连后必须先查询 broker orders/trades 快照并 reconcile 成功，才允许恢复提交”的语义。

`backend/services/qmt_strategy_ledger/order_service.py` 的 B/compiler managed-order 路径同样只把 broker 调用异常返回给当前 submit/batch；没有账户级断连 freeze、alert telemetry，也没有 reconnect-reconcile gate。对照 `docs/adr/0002-miniqmt-execution-runtime-event-loop-target-architecture.md` 与 `docs/architecture/miniqmt_durable_execution_runtime_design_20260623.md`，目标 event_loop A 尚未默认启用；本 BUG 只在当前 B/compiler 过渡链路补断连安全护栏，不解锁 LIVE、不改变 `MINIQMT_EXECUTION_RUNTIME=compiler` 默认 inert 行为、不触碰 LocalSim。

## 修复方案

1. MiniQMTSim broker adapter 增加断连 freeze 状态：
   - 任一 `QMTNotAvailableError` 或 pre-submit 连接探测失败都会记录结构化 freeze 事件；
   - 后续新下单先进入 freeze gate；broker 仍未恢复时直接 loud `BrokerConnectivityError`；
   - 错误上下文包含 `reason_code=MINIQMT_BROKER_DISCONNECTED_FREEZE`、portfolio/package/account_group/strategy_slot、freeze payload 与 alert。
2. 重连恢复必须先 reconcile：
   - freeze 后的下一次 submit 会先尝试 broker `status()`/`connect()`；
   - 连接恢复后必须成功查询 `get_orders(cancelable_only=False)` 与 `get_trades()`；
   - 两个快照查询都成功才清除 freeze，并记录 `MINIQMT_BROKER_RECONNECTED_RECONCILED`；
   - 任一查询失败则保持 freeze，loud 返回 `MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED`，不提交新单。
3. B/compiler `QmtManagedOrderService` 增加同口径 broker disconnect freeze：
   - 对 `submit_order()`、`submit_batch()`、`_submit_preflighted_order()` 统一做 freeze preflight；
   - broker 断连异常会将该 account/mode scope 冻结，后续同 scope 的提交在 broker 调用前拒绝；
   - 恢复必须先查询 managed broker 的 orders/trades 快照；
   - 提供 `broker_disconnect_freeze_status()` telemetry，供 API/监控只读暴露。

## 验收断言

- 断连后的下一次 submit 不调用 `place_order`，返回 `reason_code=MINIQMT_BROKER_DISCONNECTED_FREEZE`。
- 重连后 submit 先调用 `get_orders` 与 `get_trades`，两者成功后才允许新订单提交。
- reconnect reconcile 查询失败时保持 freeze，返回 `MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED`，不提交新单。
- Managed-order B/compiler path 与 MiniQMTSim adapter 都有明确 telemetry/alert；无 `except: pass` 或 silent fallback。
- 不改 LocalSim，不解锁 LIVE，不削弱 BUG-446/447/448/470/478。

## 验证证据

- `rtk python -m pytest backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q` -> passed, `130 passed`.
- `rtk python -m ruff check backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/qmt_strategy_ledger/order_service.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py` -> passed.
- `rtk git diff --check` -> passed.
- `rtk python -m nox -s l0` -> passed.
- `rtk python -m nox -s validation_module_registry_l0` -> passed.
- `rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"` -> passed.

## production gates

- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
