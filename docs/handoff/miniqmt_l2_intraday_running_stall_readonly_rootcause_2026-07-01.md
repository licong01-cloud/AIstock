# MiniQMT SIM L2 `INTRADAY_RUNNING` 停滞只读根因报告

## 0. 只读边界

- 调查对象：`simrun_be0af928d096734b`，`account_group=ag_minqmt_62266303_sim`，`account_id=62266303`，`mode=SIM`，`trade_date=2026-06-30`，`strategy_slot=codex_final_ms_l2_20260603`，`binding=simbind_80fb713d5d54b1ed`。
- 本窗口只做只读取证：MCP broker monitor `qmt/account`、`qmt/orders`、`qmt/trades`；PostgreSQL `default_transaction_read_only=on`；代码锚点静态读取。
- 本窗口未改产品代码、未启停服务、未写生产 DB、未发券商订单、未撤券商订单、未跑任何 operator 命令。
- 除本 handoff 文档外无修复动作；是否登记新 BUG 与执行处置由战略 session 决定。
- `production_ddl_gate=noop`；`production_backend_restart_gate=noop`；`production_frontend_gate=noop`；`broker_order_mutation_gate=noop`。

## 1. 结论摘要

1. L2 不是 BUG-562 的 no-side-effect `RECONCILING` 类型。L2 当前 DB 证据是 `status=INTRADAY_RUNNING`、`broker_called=true`、`submitted_intents=10`、`failed_intents=3`、`qmt_batch_status=PARTIAL`、`qmt_batch_id=qmtbatch_a0f8165fdbdb6bfd59f14fa8`，`updated_at=2026-06-30T16:34:50.624216+08:00`。
2. 10 笔 broker submit 的真实归宿：9 笔 `order_status=56` 已成交/终态；唯一 open-like 残留是 `002851.SZ / order_id=1082221896 / status=50 / traded_volume=0`。当前 broker monitor 返回 `cancelable_stale_warning=false`，与任务背景中的 `true` 不一致；以当前只读接口为准记录差异。
3. “10 笔是否都在 2026-06-30 16:33 前后提交”的答案是否定的：5 笔 SELL 的 broker `order_time_iso` 在 14:26:44/14:36:30，DB `submitted_at` 在 14:36:32；5 笔 BUY 的 broker `order_time_iso` 在 16:24:12/16:33:58，DB `submitted_at` 在 16:34:00-16:34:01。缺陷核心不是“10 笔全部收盘后 replay”，而是“已有计划/残差 BUY 在收盘后仍被 submit/retry”。
4. L2 停在 `INTRADAY_RUNNING` 的直接机制：submit 后 reconcile 的 `submit_result_gate.status=PENDING` 会把 run 写成 `INTRADAY_RUNNING`。若 2026-06-30 当日 post-close terminalizer 正常覆盖它，当前 002851 open-like 订单应导致 `FAILED_TERMINAL`；但 payload 无 `miniqmt_post_close_terminalization`，说明该终结器没有落过证据。
5. 不是 `_MINIQMT_STALE_ACTIVE_STATUSES` 漏掉 `INTRADAY_RUNNING`；代码明确包含。真正缺口是 same-day post-close terminalizer 只扫 `run.trade_date == trade_date`，跨日后 background `trade_date` 已变成 2026-07-01，不会补偿终结 2026-06-30 的 `INTRADAY_RUNNING`；历史 stale terminalizer 即使运行，有 broker side-effect 时也只转 `FAILED_RETRYABLE`，不是 broker-authoritative post-close 终态。
6. 归类：新卡死类型，属于组合缺陷 `(a) 收盘后 persisted/existing plan residual submit/retry 缺时间窗硬门` + `(b) MiniQMT `INTRADAY_RUNNING` 跨日缺 broker-authoritative post-close 补偿出口`。不是 BUG-562 范围。

## 2. Broker 监控归宿表

只读来源：`qmt/account`、`qmt/orders?account_id=62266303&mode=SIM&strategy_name=codex_final_ms_l2_2026`、`qmt/trades?...`。当前账户：`provider=xtquant`，`connected=true`，`frozen_cash=50154.0`。

状态码锚点：`F:\Dev\AIstock\backend\services\qmt_strategy_ledger\models.py:18` 定义 `50` 为 open-like；`F:\Dev\AIstock\backend\services\qmt_strategy_ledger\models.py:19`-`F:\Dev\AIstock\backend\services\qmt_strategy_ledger\models.py:27` 定义 48/49/51/52/53/55/54/56/57；`F:\Dev\AIstock\backend\services\qmt_strategy_ledger\models.py:37`-`F:\Dev\AIstock\backend\services\qmt_strategy_ledger\models.py:38` 将 48/49/50/51/52/53/55 归为 open-like、54/56/57 归为 terminal。

| side | symbol | order_id | status | lifecycle | traded_volume | broker order_time_iso | DB submitted_at | 备注 |
|---|---|---:|---:|---|---:|---|---|---|
| SELL | 300196.SZ | 1082219100 | 56 | terminal filled | 1700 | 2026-06-30T14:26:44+08:00 | 2026-06-30T14:36:32.205377+08:00 | 有成交明细 |
| SELL | 300706.SZ | 1082219102 | 56 | terminal filled | 700 | 2026-06-30T14:26:44+08:00 | 2026-06-30T14:36:32.320786+08:00 | 有成交明细 |
| SELL | 301387.SZ | 1082219105 | 56 | terminal filled | 500 | 2026-06-30T14:26:44+08:00 | 2026-06-30T14:36:32.381566+08:00 | 有成交明细 |
| SELL | 301511.SZ | 1082219106 | 56 | terminal filled | 300 | 2026-06-30T14:36:30+08:00 | 2026-06-30T14:36:32.443367+08:00 | 有成交明细 |
| SELL | 301603.SZ | 1082219107 | 56 | terminal filled | 400 | 2026-06-30T14:26:44+08:00 | 2026-06-30T14:36:32.514208+08:00 | 有成交明细 |
| BUY | 002049.SZ | 1082221895 | 56 | terminal filled | 600 | 2026-06-30T16:24:12+08:00 | 2026-06-30T16:34:00.931806+08:00 | 收盘后 BUY |
| BUY | 002851.SZ | 1082221896 | 50 | open-like | 0 | 2026-06-30T16:33:58+08:00 | 2026-06-30T16:34:00.998000+08:00 | 唯一残留 open-like；当前 `cancelable_stale_warning=false` |
| BUY | 002971.SZ | 1082221897 | 56 | terminal filled | 800 | 2026-06-30T16:24:12+08:00 | 2026-06-30T16:34:01.063410+08:00 | 收盘后 BUY |
| BUY | 300481.SZ | 1082221898 | 56 | terminal filled | 3000 | 2026-06-30T16:24:12+08:00 | 2026-06-30T16:34:01.125673+08:00 | 收盘后 BUY |
| BUY | 300576.SZ | 1082221899 | 56 | terminal filled | 1100 | 2026-06-30T16:33:58+08:00 | 2026-06-30T16:34:01.192650+08:00 | 收盘后 BUY |

补充：

- `qmt/trades` 当前返回 15 条成交明细，对应 9 个已成交订单；`1082221896` 无成交。
- `qmt_strategy.order_ledger.raw_json->>'order_time_iso'` 对 `1082221896` 仍记录 `2026-06-30T16:24:12+08:00`，而当前 broker monitor 返回 `2026-06-30T16:33:58+08:00`；归宿判断以当前 broker monitor 为 broker-authoritative，报告同时保留 ledger/monitor 时间差异。
- 当前 `qmt/account` 返回 broker `frozen_cash=50154.0`；DB `qmt_strategy.virtual_account` 对 L2 返回 `frozen_cash=48396.0`、`cash=44945.58`、`market_value=437122.0`、`status=ENABLED`、`updated_at=2026-06-30T16:34:50.807665+08:00`。

## 3. DB run / binding / payload 证据

只读 DB 连接确认：`SHOW transaction_read_only = on`。

### L2 与 L16 对照

| run_id | slot | status | broker_called | submitted_intents | failed_intents | qmt_batch_status | updated_at | BUG-562 适配性 |
|---|---|---|---|---:|---:|---|---|---|
| `simrun_be0af928d096734b` | `codex_final_ms_l2_20260603` | `INTRADAY_RUNNING` | `true` | 10 | 3 | `PARTIAL` | 2026-06-30T16:34:50.624216+08:00 | 不适配；已有 broker side-effect |
| `simrun_0143264657762a1e` | `codex_final_ms_l16_20260603` | `RECONCILING` | `false` | 0 | 0 | null | 2026-06-30T09:25:18.099692+08:00 | 适配 BUG-562 no-side-effect 恢复 |

L2 payload keys 中没有：

- `miniqmt_no_side_effect_reconciling_recovery`
- `miniqmt_post_close_terminalization`
- `stale_active_terminalization`

这说明 L2 既没有走 BUG-562 no-side-effect recovery，也没有走 post-close/stale terminalization 的 durable evidence 路径。

### L2 07-01 run / binding 状态

- `paper_v2.simulation_daily_run` 对 `strategy_slot_id='codex_final_ms_l2_20260603'` 在 `2026-06-30/2026-07-01` 的统计只有：`2026-06-30 INTRADAY_RUNNING n=1`；无 `2026-07-01` run。
- `simulation_release_binding` 中 `simbind_80fb713d5d54b1ed` 为 `effective_from=2026-06-30`、`effective_to=2026-06-30`；按 `2026-07-01` 查询 L2 active binding 返回空。
- 因此“07-01 零新 run”不能只归因于卡死 run；当前直接证据还包括 07-01 没有 active binding。卡死 run 会污染 active 视图和前置风险，但绑定 roll-forward/日切是否缺失是另一个需要战略 session 决定是否拆分的调查点。

## 4. Submit 时机与触发链定位

### 4.1 时机判断

不是 10 笔全部在 16:33 前后提交：

- SELL：5 笔 DB `submitted_at` 在 14:36:32，broker `order_time_iso` 多数为 14:26:44，1 笔为 14:36:30。
- BUY：5 笔 DB `submitted_at` 在 16:34:00-16:34:01，broker `order_time_iso` 为 16:24:12 或 16:33:58，明确是收盘后 BUY side-effect。

因此，本次不应定性为“10 笔全量收盘后 replay”。更精确的归类是：已有 2026-06-30 execution plan/batch 的 BUY residual/retry 在收盘后仍进入 broker submit。

### 4.2 触发链代码锚点

正常调度窗口锚点：

- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:99`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:105`：默认 `execution` 窗口为 `09:25-15:00`，`post_close_reconcile` 为 `15:00-15:30`。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6903`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6906`：background scheduler 用 `_trade_date(now)` 计算当前 trade_date。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6945`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6950`：`eod_reconcile` 窗口调用 `post_close_reconcile_once(...)`，不是 submit。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:7129`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:7133`：`_trade_date(now)` 默认返回自然日，除非 env 覆盖。

已有 plan 的 replay/retry 路径锚点：

- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3488`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3493`：若同 key 已有 run 且有 `execution_plan_id`，进入 existing plan 分支。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4057`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4068`：`_existing_plan_result(...)` 读取既有 execution plan 并继续处理。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:5683`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:5724`：`_should_submit_existing_plan(...)` 决定既有 plan 是否可 submit；该函数只看 `submit` flag、run status、broker side-effect/residual evidence，不在函数内部重新做当前时钟是否仍在 `09:25-15:00` 的硬门。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4237`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4244`：existing plan 分支在满足条件时调用 `submit_persisted_execution_plan(...)`。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:5764`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:5785`：已有 broker side-effect 且状态在 `SUBMITTING/INTRADAY_RUNNING/RECONCILING/SUCCEEDED/FAILED_RETRYABLE` 时，另有 reconcile 路径，不应无条件重 submit。

只读 DB 当前没有 append-only 的 scheduler tick 调用栈，不能单凭当前 payload 断言“具体是服务重启 tick”还是“某条恢复/重试 API 调度”。可以确定的是：这不是 raw `/qmt/order` 诊断入口；它收敛在 simulation lifecycle scheduler 的既有 `execution_plan_id` replay/retry 路径。收盘后 BUY 能落到 broker，说明当时有调用者让 existing-plan submit 路径带 `submit=True` 运行，且该路径缺少自身的交易时间窗硬防线。

## 5. 为什么停在 `INTRADAY_RUNNING`

### 5.1 直接出口条件

- `F:\Dev\AIstock\backend\services\simulation_runtime\models.py:202`-`F:\Dev\AIstock\backend\services\simulation_runtime\models.py:215` 定义 `SimulationDailyRunStatus`，其中 `INTRADAY_RUNNING` 不是终态。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6198`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6208`：submit 后 reconcile 若 `submit_result_gate["status"] == "PENDING"`，`next_status = INTRADAY_RUNNING`；`SUCCEEDED` 才转 `SUCCEEDED`，否则转 `FAILED_RETRYABLE`。
- L2 当前有唯一 open-like order `1082221896/status=50/traded=0`，符合 reconcile 后 gate 仍 pending 的业务表象。

### 5.2 Post-close terminalizer 本应如何终结

- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:192`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:201`：`_MINIQMT_STALE_ACTIVE_STATUSES` 包含 `INTRADAY_RUNNING`，不是扫描集合漏掉。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2511`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2529`：`post_close_reconcile_once(...)` 调用 `_terminalize_post_close_miniqmt_runs(...)`。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2836`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2859`：post-close terminalizer 只在 `_is_post_close_reconcile_time(as_of_time)` 为真时运行，且只处理 `run.trade_date == trade_date` 的 run。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3039`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3055`：MiniQMT post-close terminalization 要求 broker side-effect evidence，然后 fresh reconcile，再调用 terminal status 判定。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3171`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3175`：post-close 判定只要求 `as_of_time >= 15:00`。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3178`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:3194`：`open_order_count > 0` 应返回 `FAILED_TERMINAL`，batch succeeded 返回 `SUCCEEDED`，capacity residual 可返回 `SUCCEEDED`，retryable buy residual 返回 `FAILED_RETRYABLE`。

按上述代码，如果 2026-06-30 当日 post-close terminalizer 在 16:34 后 fresh reconcile 到 `1082221896/status=50`，预期应写 `FAILED_TERMINAL` + `miniqmt_post_close_terminalization`。当前 DB 仍是 `INTRADAY_RUNNING` 且无该 payload key，说明它没有成功覆盖这条 run。

### 5.3 为什么过夜没有补偿

- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6903`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:6906`：跨到 2026-07-01 后，background scheduler 默认 `trade_date=2026-07-01`。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2836`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2859`：post-close terminalizer 要求 `run.trade_date == trade_date`，因此 2026-07-01 的 post-close 不会补扫 2026-06-30 的 L2 run。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2131`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2172`：普通 `SimulationLifecycleScheduler.run_once(...)` 会先跑 stale historical terminalize，再跑 post-close terminalize。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2700`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:2749`：historical stale active terminalizer 只处理 `run.trade_date < scheduler_trade_date`，有 broker side-effect 时转 `FAILED_RETRYABLE`，不是 fresh broker-authoritative post-close terminal status。
- 当前 L2 payload 无 `stale_active_terminalization`，说明即使该 historical stale 路径存在，当前它也没有落到这条 run。

因此，停滞的直接原因是：L2 在 2026-06-30 16:34 后进入 `INTRADAY_RUNNING`，同日 post-close terminalization 没有落证；跨日后 same-day terminalizer 不再扫描它，historical stale 路径也没有把它转走，并且设计上 historical stale 对有 side-effect 的 MiniQMT run 也不是 broker-authoritative 终结器。

## 6. 与 BUG-562 的边界

BUG-562 的 no-side-effect 恢复入口代码锚点：

- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4332`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4402`：`recover_no_side_effect_reconciling_run_after_operator_cleanup(...)` 只处理 no-side-effect stale runtime cleanup 后的恢复。
- `F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4404`-`F:\Dev\AIstock\backend\services\simulation_runtime\scheduler.py:4437`：guard 明确要求 `MINIQMT_SIM`、`RECONCILING`、`broker_called=false`、`submitted_intents=0`；否则拒绝。

L16 `simrun_0143264657762a1e` 满足该范围；L2 `simrun_be0af928d096734b` 不满足，因为它是 `INTRADAY_RUNNING` 且 `broker_called=true/submitted_intents=10`，已有 broker side-effect。将 L2 归入 BUG-562 会掩盖真实问题：已有 side-effect 后的收盘后 BUY submit 与跨日终结缺口。

## 7. 缺陷归类与修复归属建议

### 7.1 归类

- 主类：新 MiniQMT lifecycle state-machine 卡死缺陷。
- 子类 A：existing/persisted execution plan submit/retry 缺少内层交易时间窗硬门，导致 caller 在收盘后带 `submit=True` 时仍可把 BUY residual 送到 broker。
- 子类 B：`INTRADAY_RUNNING` missed same-day post-close 后缺少跨日 broker-authoritative terminalization 补偿；post-close terminalizer 只处理当前 `trade_date`，historical stale path 不等价。
- 非归类：不是 BUG-562 no-side-effect `RECONCILING`，也不是 `_MINIQMT_STALE_ACTIVE_STATUSES` 未包含 `INTRADAY_RUNNING`。

### 7.2 建议修复模块

只给方向，不实施：

1. `backend/services/simulation_runtime/scheduler.py`
   - 在 `_existing_plan_result(...)` / `_should_submit_existing_plan(...)` 或更靠近 `submit_persisted_execution_plan(...)` 的 MiniQMT 分支增加硬时间窗 gate：超过 execution window 后，禁止 persisted plan submit/retry，把流程导向 reconcile/terminalization。
   - 对 `FAILED_RETRYABLE` + deferred/residual BUY 的重试分支增加“当前时间仍在可提交窗口内”的显式条件与 durable evidence。
   - 增加 missed same-day post-close 的跨日补偿：对历史 MiniQMT active side-effect run 做 fresh broker reconcile 后，用 `_miniqmt_post_close_terminal_status(...)` 得出 broker-authoritative 终态，而不是只用 stale historical `FAILED_RETRYABLE`。
2. `backend/services/miniqmt_execution_runtime` / `backend/services/qmt_strategy_ledger`
   - 检查 `order_time_iso` 在 broker monitor 与 `order_ledger.raw_json` 对 `1082221896` 的差异来源，避免 stale/open-like 判断依赖不一致时间。
   - 检查 `status=50` + `cancelable_stale_warning=false` + frozen_cash 的同步语义，明确收盘后 open-like SIM 单是否需要次日 broker snapshot 才能解除。
3. Ops/API 层
   - 对 active run summary 明确区分 `INTRADAY_RUNNING`、post-close missed、historical stale with side-effect，避免 UI 只显示“仍在跑”而无处置建议。

## 8. 对 A 路线的影响

- 如果 A/event_loop 上线后仍复用 `SimulationLifecycleScheduler` 的 run 状态机、existing plan replay/retry 外壳、post-close terminalizer 和 qmt ledger 状态分类，那么本次 `INTRADAY_RUNNING` missed post-close 后跨日不补偿的缺口会同样影响 A。
- event_loop adapter 可能改变下单执行细节，但不能自动修复 scheduler 层的两个共享缺口：收盘后 persisted plan submit gate 与跨日 broker-authoritative terminalization。
- 同一 slot 切 event_loop 后，如果出现 `status=50` open-like stale 单、frozen_cash 未释放、run 停在 `INTRADAY_RUNNING` 且同日 post-close 未覆盖，A 路线仍可能遇到相同“slot 看似 active / 资金冻结 / 次日 readiness 污染”的问题。

## 9. 运营风险

1. 订单风险：`002851.SZ / order_id=1082221896 / status=50 / traded_volume=0` 是当前唯一 open-like 残留。当前 `cancelable_stale_warning=false`，但 status code 本身仍 open-like；是否需要撤单或等待 broker/SIM 日切，只能由具备 operator 权限的会话根据 broker authority 决定，本窗口未操作。
2. 资金风险：broker account `frozen_cash=50154.0`，L2 virtual account `frozen_cash=48396.0`。这会污染下一交易日 L2 的资金前置检查，尤其是 strategy-level cash/freeze 逻辑。
3. run 占用风险：`INTRADAY_RUNNING` 不在 ops terminal set 内。`F:\Dev\AIstock\backend\services\simulation_runtime\ops.py:24`-`F:\Dev\AIstock\backend\services\simulation_runtime\ops.py:30` 的 terminal set 只含 `SUCCEEDED/FAILED_RETRYABLE/FAILED_TERMINAL/CANCELLED`，`F:\Dev\AIstock\backend\services\simulation_runtime\ops.py:336` 会把非终态计为 active。因此 L2 这条 run 会污染 active run 视图。
4. 07-01 零新 run 的直接证据：当前 DB 没有 2026-07-01 L2 run，同时也没有 2026-07-01 active binding。不能只说“卡死 run 导致新 run 起不来”；更准确是“卡死 run 占 active 视图并污染资金/订单状态，但 07-01 未起新 run 还需要同时调查 binding roll-forward / scheduler window / run creation 条件”。

## 10. 建议给战略 session 的处置方向

只建议，不执行：

1. 登记一个新 BUG，范围聚焦 `backend/services/simulation_runtime/scheduler.py`，标题建议：MiniQMT existing-plan post-close BUY retry plus missed cross-day terminalization leaves side-effect run `INTRADAY_RUNNING`。
2. 将 BUG-562 与本问题分开：BUG-562 继续处理 L16 no-side-effect `RECONCILING`；L2 是 side-effect-bearing run，不能走 no-side-effect recovery。
3. 对现有 L2 run，先由 operator 权限会话只读确认 broker authority，再决定是否撤/终结/清 freeze；任何 DB 状态推进必须以 broker evidence 为前置。
4. 修复设计应同时覆盖：收盘后 submit gate、existing plan residual retry gate、historical active side-effect broker-authoritative terminalization、payload durable evidence、A/event_loop 共用路径。
5. 另行确认 2026-07-01 L2 active binding 未生成的原因；该问题可能与卡死 run 相关，但当前证据不足以把零新 run 完全归因于卡死 run。

## 11. 本窗口 gates

- `production_ddl_gate=noop`
- `production_backend_restart_gate=noop`
- `production_frontend_gate=noop`
- `production_db_write_gate=noop`
- `broker_submit_gate=noop`
- `broker_cancel_gate=noop`
- `operator_command_gate=noop`
- 本窗口只读完成，未做修复。
