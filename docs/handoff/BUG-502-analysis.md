# BUG-502 分析 - MiniQMT disconnect freeze / reconnect-reconcile

## 独立根因

Issue #1552 与当前代码一致：MiniQMT compiler/B 路径在 broker 断连时只把当前调用异常返回给 submit 调用方，没有把受影响的 account/mode scope 进入 freeze / kill-switch 状态，也没有定义“重连后必须先查询 broker orders/trades 快照并 reconcile 成功，才能恢复新单提交”的语义。

具体缺口：

- `backend/services/paper_trading_v2/broker/minqmtsim.py` 在 `QMTNotAvailableError` 后只抛当前 `BrokerConnectivityError`，后续提交可能继续尝试。
- `backend/services/qmt_strategy_ledger/order_service.py` managed-order 路径在 `place_order()` 异常后缺少 account/mode scope 级冻结、告警上下文、重连前 reconcile gate。
- 现有 `qmt_client.status()` / 查询 API 会暴露连接状态，但 compiler submit 路径没有把断连转化为持久安全闸。

本修复只补 MiniQMT compiler/B 断连安全能力，不解锁 LIVE，不改变 LocalSim，不削弱 BUG-446/447/448/470/478/501/500。

## 修复方案

1. MiniQMTSim adapter 增加 disconnect freeze：
   - 第一次 broker 断连记录结构化 freeze payload。
   - 后续新单先进入 freeze gate；broker 未恢复时不调用 broker。
   - 错误上下文包含 `MINIQMT_BROKER_DISCONNECTED_FREEZE`、portfolio/package/account_group/strategy_slot、freeze payload 和 alert。
2. reconnect-reconcile gate：
   - freeze 后下一次 submit 先尝试 `status()` / `connect()`。
   - broker 恢复后必须成功查询 `get_orders(cancelable_only=False)` 和 `get_trades()`。
   - orders/trades 快照均成功后才清除 freeze 并记录 `MINIQMT_BROKER_RECONNECTED_RECONCILED`。
   - 任一查询失败则保持 freeze，返回 `MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED`，且不提交新单。
3. `QmtManagedOrderService` managed-order 路径增加同口径安全闸：
   - `submit_order()`、`submit_batch()`、`_submit_preflighted_order()` 统一检查 freeze。
   - broker 断连异常会冻结对应 account/mode scope。
   - active freeze 优先于 BUG-500 flag-gated pre-trade risk 和 BUG-501 account-group cash hard gate，原因是连接断开时所有新单必须 fail-closed，不能被风控或资金错误掩盖真实阻断。
   - freeze 被清除前，新单不触发 broker 调用；reconnect-reconcile 成功后恢复正常 submit。

## 三道闸顺序

当前 submit 路径顺序为：

1. `MINIQMT_BROKER_DISCONNECTED_FREEZE`：account/mode scope 已冻结时最先拦截，并尝试 reconnect-reconcile；失败则不进入后续风控/资金计算，避免掩盖断连真实原因。
2. `miniqmt_pre_trade`：BUG-500 flag-gated 逐单风控层，默认 inert；仅配置启用后执行 kill-switch / price collar / fat-finger / buying-power 拒单。
3. `account_group_cash_hard_gate`：BUG-501 always-on account-group 聚合购买力硬闸，批次聚合超额时拒绝致超额订单且不调用 broker。

第一次断连发生在通过风控/资金闸后的 broker 调用阶段；该异常会立即建立 freeze。后续 submit 再按上面的顺序由 freeze gate 最先拦截。

## 验收断言

- 断连后的下一次 submit 不调用 `place_order`，返回 `MINIQMT_BROKER_DISCONNECTED_FREEZE`。
- 重连后 submit 先调用 `get_orders` 与 `get_trades`；两者成功后才允许新单提交。
- reconnect reconcile 查询失败时保持 freeze，返回 `MINIQMT_BROKER_RECONNECT_RECONCILE_FAILED`，且不提交新单。
- active freeze 与 BUG-500/BUG-501 同时存在时，freeze reason_code 优先暴露，不被 pre-trade 或资金错误覆盖。
- MiniQMTSim adapter 与 managed-order compiler path 都有明确 telemetry/alert；无 silent fallback。
- 不改 LocalSim，不解锁 LIVE，不执行生产 DB/DDL 或服务操作。

## 验证证据

- `rtk python -m pytest backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/qmt_strategy_ledger/test_order_service_submit_fake_qmt.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py -q`
- `rtk python -m ruff check backend/services/paper_trading_v2/broker/minqmtsim.py backend/services/qmt_strategy_ledger/order_service.py backend/tests/paper_trading_v2/test_minqmtsim_backend.py backend/tests/qmt_strategy_ledger/test_order_service_preflight.py backend/tests/simulation_runtime/test_lifecycle_scheduler.py`
- `rtk git diff --check`
- `rtk python -m nox -s l0`
- `rtk python -m nox -s validation_module_registry_l0`
- `rtk cmd /c "set AISTOCK_HOSTED_CI=1&& set PAPER_V2_L3_SKIP_UI=1&& python -m nox -s paper_v2_l3"`

## Production gates

- production_ddl_gate: noop
- production_frontend_dependency_gate: noop
- production_backend_dependency_gate: noop
