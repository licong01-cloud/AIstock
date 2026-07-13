# BUG-463 根因分析 - LocalSim scheduler timezone / terminalizer / cash / trading-day gate

- 日期：2026-06-22
- Lane：L2（BUG-463 / GitHub #1428）
- Worktree：`F:\Dev\AIstock_worktrees\BUG-463-p0-paper-v2-localsim-scheduler-timezone-localsim-20260622`
- 约束：仅修 LocalSim 与共享时区/交易日基础设施；不改 MiniQMT runtime/ledger/execution 文件；不启动、重启、停止服务。

## Issue 摘要

GitHub #1428 指出：

1. scheduler 使用 naive `datetime.now()` 和无时区交易窗口；UTC 部署时 A 股窗口/EOD 判断错位。
2. stale/post-close terminalizer 只覆盖 MiniQMT，LocalSim 历史或盘后非终态 run 可能跨日卡住。
3. LocalSim cash-fit 在 `context.cash is None` 时静默默认 `0.0`，把依赖缺失伪装成资金不足。
4. background 外层已有交易日 gate，但 inner lifecycle `run_once` / roll-forward 缺少交易日 gate。

## 当前代码对抗性确认

- `SimulationLifecycleBackgroundScheduler.run_once()` 原先使用 naive `datetime.now()`，并直接用传入时间做窗口比较；UTC host 或 aware UTC `as_of_time` 会把 UTC 时间当作 A 股本地时间。
- `_compute_schedule_windows()` 原先返回无时区 `start_at/end_at`，窗口状态没有 Asia/Shanghai 语义。
- `_local_sim_snapshot_time()` 在无 fills/events 时将 A 股 15:00 fallback 写成 UTC 15:00，快照时间偏移 8 小时。
- `_terminalize_stale_miniqmt_active_runs()` 与 `_terminalize_post_close_miniqmt_runs()` 只扫描 `MINIQMT_SIM`，`LOCAL_SIM` 缺少对应收口。
- `_cash_fit_localsim_execution_plan()` 原先使用 `float(context.cash if context.cash is not None else 0.0)`，缺失现金上下文会被静默降级为 0 现金。
- `SimulationLifecycleScheduler.run_once()` 原先没有可注入交易日 gate；直接调用 API/内部生命周期时仍可能在非交易日触发 stale、roll-forward、selection、plan 或 submit。

## 与 issue 的分歧

- issue 中 “`time(15,0)` tagged tzinfo=UTC” 对应的是 LocalSim fallback snapshot time；post-close window 判断本身是 naive `time(15, 0)` 比较。两者都需要统一到 Asia/Shanghai。
- “镜像 MiniQMT 版”不应理解为复用 MiniQMT fresh broker reconcile。LocalSim 没有 MiniQMT broker-authoritative ledger，因此方案只在 `LOCAL_SIM` payload 上做本地、fail-closed 的终态判定，不改变 `_run_minqmt_*`、`_persist_minqmt_*`、`reconcile_minqmt_*`、`_terminalize_*_miniqmt_*` 语义。

## 修复方案

1. 新增 `SCHEDULER_TZ=Asia/Shanghai`，统一 scheduler now、`as_of_time` 归一化、窗口 `start_at/end_at`、post-close 判断和 LocalSim fallback snapshot time。
2. 新增 LocalSim stale terminalizer：扫描历史 `LOCAL_SIM` active 状态；无 side effect 置 `CANCELLED`，有 side effect 置 `FAILED_RETRYABLE`；写入结构化 evidence 与 `reason_code`。
3. 新增 LocalSim post-close terminalizer：盘后扫描当日 `LOCAL_SIM` active 状态；持久化成功/无调仓可成功终结，capacity residual 终结为 `FAILED_TERMINAL`，无 side effect 取消，有 side effect 但缺 durable persistence 则 fail-closed 为 `FAILED_TERMINAL`；写入 `reason_code`。
4. LocalSim cash-fit 在 `context.cash is None` 时抛 `DataUnavailableError`，context 包含 `reason_code=LOCALSIM_CASH_CONTEXT_MISSING`、run/plan/binding 追踪信息和 required action；调度器把该错误持久化为 LocalSim pre-submit retry failure，不调用 broker。
5. `SimulationLifecycleScheduler` 增加可注入交易日 gate，生产 singleton 接入 `TradingCalendarStatusService`；非交易日或 calendar 不可用时 fail loud，阻止 inner `run_once` 的 stale/post-close/roll-forward/plan/submit。

## 回归覆盖

- Asia/Shanghai window：UTC `2026-05-21T01:22:00Z` 被归一为上海 09:22，planning window active；窗口 payload 带 `timezone=Asia/Shanghai`。
- Asia/Shanghai EOD：UTC `2026-05-21T07:05:00Z` 被归一为上海 15:05，post-close reconcile 生效。
- LocalSim stale terminalization：历史 LocalSim planning run 在下一交易日 tick 前被取消并写入 `LOCALSIM_STALE_ACTIVE_WITHOUT_BROKER_SIDE_EFFECT`。
- LocalSim post-close terminalization：当日 LocalSim active run 有 durable persistence 时，盘后终结为 `SUCCEEDED` 并写入 `LOCALSIM_POST_CLOSE_PERSISTED_SUCCESS`。
- `context.cash is None`：LocalSim submit fail-closed 为 `FAILED_RETRYABLE`，写入 `LOCALSIM_CASH_CONTEXT_MISSING`，且 broker 未被调用。
- 非交易日 gate：inner `SimulationLifecycleScheduler.run_once` 在非交易日抛 `SIMULATION_LIFECYCLE_NON_TRADING_DAY`，不会加载 context、不会创建 daily run。

## Production gates

- `production_ddl_gate=noop`
- `production_backend_dependency_gate=noop`
- `production_frontend_dependency_gate=noop`
- 本 lane 不启动、重启、停止服务，不写生产 DB；合并后如需运行新代码，由用户执行服务重启。
