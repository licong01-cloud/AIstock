# 2026-06-25 MiniQMT SIM dependent-buy 失败根因只读调查

## 0. 范围与约束

- 调查目标：解释 `simrun_004f058011cff9e6`（L2）和 `simrun_d807c4ce90a3cbe9`（L16）今日 `FAILED_TERMINAL` 的直接原因，并判断 dependent-buy 缺口是否影响 A（MiniQMT event_loop）上线。
- 执行方式：只读。未改代码、未启停服务、未撤单、未写生产 DB/DDL。
- 证据来源：
  - GET `/api/v1/simulation-runtime/runs?trade_date=2026-06-25&broker_backend=minqmt_sim&limit=200`
  - GET `/api/v1/simulation-runtime/runs/{run_id}`
  - GET `/api/v1/qmt/orders`
  - GET `/api/v1/qmt/orders?cancelable_only=true`
  - GET `/api/v1/qmt/trades`
  - GET `/api/v1/qmt/positions`
  - 只读 SQL：`paper_v2.simulation_daily_run`、`qmt_strategy.order_batch`、`qmt_strategy.order_intent`、`qmt_strategy.order_ledger`、`qmt_strategy.order_status_event`、`market.kline_minute_raw`

## 1. 今日 run 结论

| 维度 | L2 | L16 |
| --- | --- | --- |
| run_id | `simrun_004f058011cff9e6` | `simrun_d807c4ce90a3cbe9` |
| strategy_slot_id | `codex_final_ms_l2_20260603` | `codex_final_ms_l16_20260603` |
| binding_id | `simbind_ba4e3e683e00e306` | `simbind_85004cb870e0cd98` |
| execution_plan_id | `plan_155ed6d546048ca4` | `plan_162e2def628062c4` |
| qmt_batch_id | `qmtbatch_9047788863e2e4196442fec5` | `qmtbatch_361455b91c8ace9fbccf8dc3` |
| run 状态 | `FAILED_TERMINAL` | `FAILED_TERMINAL` |
| broker_called | `true` | `true` |
| qmt_batch_status | `PARTIAL` | `PARTIAL` |
| submitted / failed | 6 / 8 | 20 / 23 |
| failed code | `SELL_PROCEEDS_REQUIRED=5`, `SKIPPED_INSUFFICIENT_CAPITAL=3` | `SELL_PROCEEDS_REQUIRED=20`, `SKIPPED_INSUFFICIENT_CAPITAL=3` |
| post-close reason | `miniqmt_post_close_open_orders_terminal_failed` | `miniqmt_post_close_open_orders_terminal_failed` |

关键修正：背景里“今日卖单成交=0”与只读 broker 事实不符。GET `/api/v1/qmt/trades` 显示今日 47 笔成交，其中 `order_type=24` 卖出成交 46 笔、`order_type=23` 买入成交 1 笔；GET `/api/v1/qmt/orders` 显示 26 笔委托中 24 笔 `order_status=56` 已成、2 笔 `order_status=50` open-like。

## 2. 2 笔残留 open-like 卖单

GET `/api/v1/qmt/orders?cancelable_only=true` 当前返回 2 笔可撤 open-like 卖单：

| qmt_order_id | strategy_slot | symbol | side | broker price_type | broker price | qty / traded | status | broker order_time | remark |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `1098908839` | L2 | `002943.SZ` | SELL | 50 | 53.08 | 1100 / 0 | 50 | `2026-06-25T09:21:03+08:00` | `codex_final_ms_l2_20-155ed6d5-vn02-002943-S` |
| `1098917718` | L16 | `301303.SZ` | SELL | 50 | 17.39 | 1100 / 0 | 50 | `2026-06-25T09:37:35+08:00` | `codex_final_ms_l16_2-162e2def-vn11-301303-S` |

只读 SQL 对账补充：

- `qmt_strategy.order_intent` 中这两笔原始 intent 都是 SELL，`price_type=5`，计划限价分别为 `52.43` / `17.46`。
- `qmt_strategy.order_ledger` 中 broker 同步后的委托 `price_type=50`，价格分别为 `53.08` / `17.39`，`traded_volume=0`，`order_status=50`。
- `qmt_strategy.order_status_event` 对这两笔都有 `SUBMITTED` 和 `STATUS_SYNC`，`status_msg` 为空，诊断为 `broker_status_msg_missing`；没有 broker 拒单码。
- GET `/api/v1/qmt/positions` 显示这两只股票仍在持仓中，且 `can_sell=0`，符合“卖单挂住后冻结可卖量”。

定性：今天不是“卖单提交逻辑整体失败”。大部分 SELL 已成交；两笔残留是 broker 接受后仍 open-like 的未成交/冻结委托。基于当前可读证据，无法证明是 AIstock 下单接口失败；更像市场时序/队列/SIM 撮合语义下的挂单未成交。`market.kline_minute_raw` 分钟线显示日内高点触及过相关价格，但分钟线高点不等价于该委托在队列中必成交，且 broker 最终事实仍是 `traded_volume=0`、`status=50`。

需要单独注意但本轮不修：intent `price_type=5` 到 broker ledger `price_type=50` 的映射/回报差异需要后续确认 xtquant 语义；但它不是今日 dependent-buy 未闭合的唯一证据，因为同批大量 SELL 也通过同路径成交。

## 3. dependent-buy 二阶段为何未闭合

### 3.1 B 路径当前机制

代码证据：

- `backend/services/qmt_strategy_ledger/order_service.py:1715` 将 batch 内 SELL 排在 BUY 前。
- `backend/services/qmt_strategy_ledger/order_service.py:1039` 到 `backend/services/qmt_strategy_ledger/order_service.py:1068`：当 BUY 的累计冻结资金超过当前 `available_cash`，但小于等于 `available_cash + same_batch_sell_proceeds` 时，不假设卖出回款已到账，而是追加 `SELL_PROCEEDS_REQUIRED`。
- `backend/services/qmt_strategy_ledger/order_service.py:553` 到 `backend/services/qmt_strategy_ledger/order_service.py:564`：该类 BUY 不提交 broker，返回“dependent buy deferred until same-batch sell proceeds are reconciled”。
- `backend/services/qmt_strategy_ledger/order_service.py:1291` 到 `backend/services/qmt_strategy_ledger/order_service.py:1326`：只有再次调用 `submit_batch` 时，才会找到同一 `batch_id` 或 logical batch 的旧 `PARTIAL` 批次。
- `backend/services/qmt_strategy_ledger/order_service.py:1328` 到 `backend/services/qmt_strategy_ledger/order_service.py:1418`：`_retry_dependent_buy_batch` 只对旧批次里的 deferred BUY 重跑 preflight；若真实现金已对账足够才提交，否则继续等待。

这说明 B 的 dependent-buy 是“再次 submit 时重试”的机制，不是 SELL 成交回调自动触发。

### 3.2 今日为何没有重试闭合

代码证据：

- `backend/services/simulation_runtime/scheduler.py:5481` 到 `backend/services/simulation_runtime/scheduler.py:5493`：已有 broker side effect 的 run 只有在 `FAILED_RETRYABLE`、存在 deferred dependent buy、且没有 open-order evidence 时，才会重新提交旧计划。
- `backend/services/simulation_runtime/scheduler.py:6021` 到 `backend/services/simulation_runtime/scheduler.py:6056`：submit-result gate 如果 `open_order_count > 0`，状态先变为 `PENDING`，reason=`miniqmt_open_orders_pending_after_reconciliation`。
- `backend/services/simulation_runtime/scheduler.py:3158` 到 `backend/services/simulation_runtime/scheduler.py:3170`：收盘终结时如果 fresh reconcile 后仍有 `open_order_count > 0`，直接 `FAILED_TERMINAL`，reason=`miniqmt_post_close_open_orders_terminal_failed`。

今日事实：

- L2 fresh reconcile：`open_order_count=1`，open order=`1098908839 / 002943.SZ / SELL / 1100 / traded 0`。
- L16 fresh reconcile：`open_order_count=1`，open order=`1098917718 / 301303.SZ / SELL / 1100 / traded 0`。
- L2 residual summary：`dependent_buy_count=5`、`capacity_residual_count=3`、`noncompensating_residual=true`。
- L16 residual summary：`dependent_buy_count=20`、`capacity_residual_count=3`、`noncompensating_residual=true`。
- 两个 run 的 submit gate 都是 `PENDING`，reason=`miniqmt_open_orders_pending_after_reconciliation`；15:00 fresh reconcile 后仍有 open order，于是 post-close 终结为 `FAILED_TERMINAL`。

结论：二阶段没有闭合的直接原因不是“所有 SELL 都没成”，而是 B scheduler 把 dependent-buy 重试绑定在“run 已进入 FAILED_RETRYABLE 且 open_order_evidence=0”的下一轮重提交流程上。今日每个 run 都至少有 1 笔 open-like SELL 挂到收盘，所以没有进入可重试窗口；收盘后按 fresh broker facts 终结失败。

## 4. 为什么不是 `SUCCEEDED_WITH_CAPACITY_RESIDUAL`

代码证据：

- `backend/services/simulation_runtime/scheduler.py:5668` 到 `backend/services/simulation_runtime/scheduler.py:5676`：`terminal_capacity_residual` 要求 `capacity_residual_count > 0` 且 `dependent_buy_count == 0`。
- `backend/services/simulation_runtime/scheduler.py:6029` 到 `backend/services/simulation_runtime/scheduler.py:6056`：存在 open order 时 submit gate 优先 `PENDING`；有 dependent-buy residual 时不会走 capacity residual 成功。
- `backend/services/simulation_runtime/scheduler.py:5785` 到 `backend/services/simulation_runtime/scheduler.py:5847`：residual summary 会把 `SELL_PROCEEDS_REQUIRED` 归为 `dependent_buy_count`，把 `SKIPPED_INSUFFICIENT_CAPITAL` 归为 `capacity_residual_count`。

今日两个 run 均同时有 `dependent_buy_count>0` 和 `open_order_count=1`。因此按现有 B 终态规则，不满足 `SUCCEEDED_WITH_CAPACITY_RESIDUAL`，收盘后归类为 `FAILED_TERMINAL` 是当前代码的预期结果。

## 5. 对 A（MiniQMT event_loop）的影响判定

判定：**A 需新增 dependent-buy 事件驱动任务，作为 canary 前置硬门；不建议修 B 专属 scheduler 闭环。**

理由：

1. B 当前 SIM 主链路仍是 compiler-style submit：
   - `backend/services/simulation_runtime/bridges.py:270` 到 `backend/services/simulation_runtime/bridges.py:278`：`MiniQMTExecutionBridge.submit_plan` 调用 runtime client 的 `submit_managed_vnpy_order_requests`。
   - `backend/services/miniqmt_execution_runtime/client.py:411` 到 `backend/services/miniqmt_execution_runtime/client.py:442`：该方法最终通过 `_ManagedOrderRequestRuntimeGateway.from_requests(...).submit_managed_batch(order_service=managed_order_service)` 走 `QmtManagedOrderService.submit_batch`，所以 dependent-buy 逻辑属于 B 提交/重试层。
2. A event_loop 不应复用 B compiler lifecycle：
   - `backend/services/miniqmt_execution_runtime/client.py:593` 到 `backend/services/miniqmt_execution_runtime/client.py:605`：`EVENT_LOOP` runtime 明确拒绝 compiler-style managed lifecycle，reason_code=`MINIQMT_EVENT_LOOP_REQUIRES_REAL_CALLBACKS`。
3. A 已有真实 broker 回调入口，但没有 dependent-buy 编排：
   - `backend/services/miniqmt_execution_runtime/gateway.py:394` 到 `backend/services/miniqmt_execution_runtime/gateway.py:435`：A gateway 有 `on_order` / `on_trade`，会把 broker 回调送入 runtime。
   - `backend/services/miniqmt_execution_runtime/runtime.py:611` 到 `backend/services/miniqmt_execution_runtime/runtime.py:685`：`record_trade_event` 会更新 child order、写 fill、驱动 vn.py core 的 trade update。
   - `backend/services/miniqmt_execution_runtime/oms.py:80` 到 `backend/services/miniqmt_execution_runtime/oms.py:126`：trade callback 可落 `qmt_strategy.trade_ledger`，并要求 broker trade id，属于 loud/idempotent 写账。
   - 但在 `backend/services/miniqmt_execution_runtime/**` 和 `backend/execution_algos/**` 中未发现 `SELL_PROCEEDS_REQUIRED`、`ACCOUNT_GROUP_SELL_PROCEEDS_REQUIRED`、`dependent_buy` 或 sell-proceeds deferred queue 逻辑。现有 A 事件循环能感知 SELL 成交，但没有“SELL 回款释放后提交原本 deferred BUY”的父级协调器。

因此，今日问题不能简单归类为“B scheduler 专属，无需 A 修复”。B 的“下一轮 submit 才重试”不应继续投入；但 A 如果要承接真实再平衡计划，必须有事件驱动的 dependent-buy 能力，否则遇到“卖出融资买入”的计划时，要么假设回款造成假成功/透支，要么永远不提交 BUY。

## 6. 建议新增 D 任务草案

建议标题：`MiniQMT event_loop dependent-buy sell-proceeds event-driven coordinator`

建议性质：A go-live canary 前置开发任务，插在 D4（真实 event_loop canary 切换）之前，或作为 D4 的硬验收子项；不并入 B scheduler 修复。

候选 scope：

- `backend/services/miniqmt_execution_runtime/models.py`
- `backend/services/miniqmt_execution_runtime/runtime.py`
- `backend/services/miniqmt_execution_runtime/oms.py`
- `backend/services/miniqmt_execution_runtime/repository.py`
- `backend/services/miniqmt_execution_runtime/gateway.py`
- `backend/services/miniqmt_execution_runtime/client.py`
- `backend/services/miniqmt_execution_runtime/shadow.py`
- `backend/tests/miniqmt_execution_runtime/**`
- 只在需要复用 preflight contract 时只读参考 `backend/services/qmt_strategy_ledger/order_service.py`；不把 B scheduler 轮询逻辑搬进 A。

关键验收：

1. A 接收同一 execution plan 时，先提交 SELL；BUY 若需要未对账卖出回款，记录 durable `DEFERRED_BUY_WAITING_FOR_SELL_PROCEEDS`，不得触 broker。
2. `on_trade` 收到 SELL 成交回调后，基于 qmt_strategy trade/cash/position 事实释放可用回款；现金足够才提交对应 BUY。
3. 部分成交只释放部分现金；未成交或撤单后 BUY 保持 deferred/blocked，并给明确 reason_code，不假成功。
4. EOD 仍未满足的 deferred BUY 进入显式 residual/failed 状态，携带依赖 SELL、已成交回款、缺口金额。
5. shadow 模式也能产出同样 dependency/cash-release 证据，但不得 broker mutation。
6. 所有拒绝/失败都 loud：包含 run_id/runtime_id、parent_intent_id、dependent_sell_order_id、released_cash、required_cash、reason_code。

禁止项：

- 禁把 A 退化成 B 的“一次性 timer + batch submit”。
- 禁绕过 qmt_strategy ledger/cash facts 直接用估算回款提交 BUY。
- 禁为了让 SIM 成功去放宽 broker-authoritative、submit_result_gate、pre-trade risk 或 residual 观测性。

## 7. 残留卖单处置建议（未执行）

当前 `/api/v1/qmt/orders?cancelable_only=true` 仍显示 2 笔可撤 open-like 卖单：

- `1098908839`：`002943.SZ`，L2，SELL 1100，成交 0。
- `1098917718`：`301303.SZ`，L16，SELL 1100，成交 0。

建议仅在用户授权后走受控 operator command，不直接用 raw `/api/v1/qmt/order` 或手工 DB 修改。可选方案：

1. 分 strategy slot 执行 `CANCEL_ALL_OPEN_ORDERS`，分别清理 L2 与 L16；
2. 若运营确认这 2 笔就是全部遗留，也可不带 `strategy_slot_id` 执行一次 `CANCEL_ALL_OPEN_ORDERS` 清理当前账户所有 open orders。

operator command 必须包含：

- `command_type`: `CANCEL_ALL_OPEN_ORDERS`
- `account_group_id`: `ag_minqmt_62266303_sim`
- `trade_date`: `2026-06-25`
- `runtime_config_hash`: 由运营按当前 MiniQMT runtime 配置提供
- `confirm_text`: `EXECUTE CANCEL_ALL_OPEN_ORDERS`
- `reason`: `post-close cleanup for 2026-06-25 MiniQMT SIM FAILED_TERMINAL open orders`

## 8. 总结

- 卖单未成交定性：不是全部卖单失败，也不是 pre-run/TDX/BUG-499 类问题；是 2 笔 broker 已接受但收盘仍 open-like 的 SELL 残留，导致 B 的 dependent-buy 重试窗口一直未打开。
- dependent-buy 未闭合定性：B 当前机制依赖再次调用同一 batch 的 `submit_batch`；scheduler 又要求 `FAILED_RETRYABLE && no open-order evidence` 才重提交流程。今日有 open SELL 挂到收盘，因此收盘 fresh reconcile 后直接 `FAILED_TERMINAL`。
- A 影响：这是 A 上线前必须补齐的业务能力，但不应修 B scheduler 专属闭环。A 需要 event-driven dependent-buy coordinator：由 SELL `on_trade` + qmt_strategy ledger/cash facts 驱动 deferred BUY 提交。
- 运营处置：2 笔残留卖单建议用户授权后走 `/api/v1/simulation-runtime/miniqmt/operator-commands` 的 `CANCEL_ALL_OPEN_ORDERS` 受控撤单；本次未执行任何撤单。
