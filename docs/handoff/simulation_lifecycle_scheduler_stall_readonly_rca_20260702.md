# SimulationLifecycleScheduler 2026-07-02 盘中不更新只读 RCA

> 口径：除特别标注 UTC 外，时间均为 Asia/Shanghai。本窗口只读取证；未改代码、未启停服务、未重启 backend/QMT、未写生产 DB、未发/撤券商订单、未跑 operator/apply。唯一产物为本文件，位于专用 worktree `F:\Dev\AIstock_worktrees\sim-lifecycle-scheduler-stall-rca-20260702`。

## 结论速览

1. **两个 scheduler 不是同一套线程，也不是同一套 `run_once`。**
   - `SimulationLifecycleBackgroundScheduler` / `SimulationLifecycleScheduler` 驱动 `paper_v2.simulation_daily_run` 和 `simulation_release_binding`，后台线程名为 `simulation-runtime-lifecycle-scheduler`。
   - `PaperTradingV2SessionScheduler` 是 legacy Paper v2 session scheduler，后台线程名为 `paper-v2-session-scheduler`，驱动 legacy session tick / auto-run；`bootstrap-status` 显示的就是它，不驱动 `simulation_daily_run`。
2. **13:01 后不是线程死，也不是所有 binding 变成不 eligible。**本轮取证看到 simulation lifecycle 线程持续运行到 14:21+，日志没有 `Simulation runtime scheduler run_once crashed`；DB/日志显示 active binding 仍包含 L2/L16 MiniQMT 与两个 LocalSim。
3. **MiniQMT daily_run 在 12:59:56/12:59:57 后不再更新的直接原因，是 tick 过程中遇到未被 per-binding 捕获的 `LiveInferencePreflightError`，导致整个本轮 tick 提前 abort。**错误从 13:00:48 起每分钟重复，package 为 `pkg_378eb9c91e104c64935404e257e932ee`，reason_code 为 `strategy_package_model_code_missing`。
4. **不需要 backend restart 才能恢复这一类 tick。**线程活着，重启不能修复缺失 model-code 或行情/quote gate。最小恢复方向是先移除当前阻塞：修复/替换/临时停用触发 `LiveInferencePreflightError` 的 eligible binding/package；然后确认 MiniQMT realtime quote 不再 stale。是否执行由战略 session 决定。
5. **这是 Bug。**主 Bug 方向不是“线程停了”，而是 `SimulationLifecycleScheduler.run_once()` 只捕获 `DataUnavailableError` / `RuntimeConfigInvalidError`，对 `LiveInferencePreflightError` 这类选择/资产 preflight 错误没有 per-binding durable failure + continue，导致一个 binding/package 阻断后续 MiniQMT/LocalSim binding，并且运行状态主要只落日志/内存，不落到对应 `simulation_daily_run`。

## 只读取证范围

- 代码锚点：专用 worktree 下 `backend/services/simulation_runtime/**`、`backend/services/paper_trading_v2/**`、router、QMT client、market data gate。
- 运行状态：只读 GET `127.0.0.1:8001` status/list APIs；只读进程与端口查询。
- 日志：只读 `F:\Dev\AIstock\backend\logs\aistock.log` / `errors.log`。
- DB：只读 psycopg2 连接，`conn.set_session(readonly=True, autocommit=True)`，执行 SELECT。

## 两套 scheduler 的代码关系

### SimulationLifecycleScheduler：驱动 simulation_daily_run

代码路径：`backend/services/simulation_runtime/scheduler.py`

- 后台 wrapper：`SimulationLifecycleBackgroundScheduler` 定义在 `scheduler.py:6829`。
- 后台线程启动：`start()` 在 `scheduler.py:6850`，线程名固定为 `simulation-runtime-lifecycle-scheduler`（`scheduler.py:6861`-`scheduler.py:6864`）。
- 后台 loop：`_run_loop()` 每轮调用 `self.run_once()`，外层 `except Exception` 只打 `logger.exception("Simulation runtime scheduler run_once crashed")`，不退出线程（`scheduler.py:7024`-`scheduler.py:7030`）。
- wrapper `run_once()`：根据交易窗口调用 inner `self.lifecycle_scheduler.run_once(...)` 或 `post_close_reconcile_once(...)`（`scheduler.py:6904`-`scheduler.py:6960`）；inner 抛出的异常在 `scheduler.py:7014`-`scheduler.py:7021` 被记录到 `last_result.errors` 并写 warning。
- inner `SimulationLifecycleScheduler.run_once()`：列出 eligible `simulation_release_binding`（`scheduler.py:2174`-`scheduler.py:2181`），逐 binding 调用 `_run_binding()`（`scheduler.py:2199`-`scheduler.py:2221`），但只 per-binding 捕获 `DataUnavailableError` / `RuntimeConfigInvalidError`（`scheduler.py:2223`-`scheduler.py:2248`）。
- 真正 binding tick 入口：`_run_binding()` 在 `scheduler.py:3492`，按“load context -> run selection -> validate evidence -> build plan -> submit/reconcile”推进（`scheduler.py:3550`-`scheduler.py:3578`、`scheduler.py:3658`-`scheduler.py:3725`）。
- selection 入口：`_run_selection_once_per_release()` 调 `selection_service.run_selection(...)`（`scheduler.py:5163`-`scheduler.py:5199`）。
- 生产 singleton：`simulation_lifecycle_background_scheduler = SimulationLifecycleBackgroundScheduler(lifecycle_scheduler=simulation_lifecycle_scheduler)`（`scheduler.py:7233`-`scheduler.py:7235`）。
- API：`GET /api/v1/simulation-runtime/scheduler/status` 走 `backend/routers/simulation_runtime.py:158`-`backend/routers/simulation_runtime.py:164`，服务层状态投影在 `backend/services/simulation_runtime/ops.py:60`-`backend/services/simulation_runtime/ops.py:95`。

### PaperTradingV2SessionScheduler：legacy session scheduler

代码路径：`backend/services/paper_trading_v2/scheduler.py`

- 类定义：`PaperTradingV2SessionScheduler` 在 `scheduler.py:30`。
- 线程启动：`start()` 在线程名 `paper-v2-session-scheduler` 上跑 `_run_loop()`（`scheduler.py:52`-`scheduler.py:67`）。
- status/bootstrap：`status()` 和 `bootstrap_status()` 在 `scheduler.py:79`-`scheduler.py:109`。
- `run_once()`：恢复 auto-run portfolios 后列出 `list_tickable_sessions(...)` 并调用 `PaperTradingSessionRunner.tick(...)`（`scheduler.py:111`-`scheduler.py:129`、`scheduler.py:207`-`scheduler.py:260`）。
- loop：`_run_loop()` 捕获自己的异常并继续（`scheduler.py:444`-`scheduler.py:450`）。
- API：`GET /api/v1/paper-v2/session-scheduler/status` / `bootstrap-status` 在 `backend/routers/paper_trading_v2.py:1168`-`backend/routers/paper_trading_v2.py:1179`。
- autostart 分离：`backend/main.py:294`-`backend/main.py:312` 先按 `ENABLE_PAPER_TRADING_V2_SCHEDULER` 启动 legacy scheduler，再按 `ENABLE_SIMULATION_RUNTIME_SCHEDULER` 启动 simulation runtime scheduler，两个 env/实例互不相同。

## 运行状态证据

### 进程与服务

- backend：PID `164416`，`python.exe`，StartTime `2026-07-02 12:36:18`，端口 `0.0.0.0:8001` LISTENING。
- frontend：PID `119456`，`node.exe`，StartTime `2026-07-02 12:36:19`，端口 `3000` LISTENING。
- TDX Go：PID `100712`，StartTime `2026-07-02 12:36:23`，端口 `19080` LISTENING。
- MiniQMT 当前可见进程：`XtMiniQmt.exe` PID `127096`，StartTime `2026-07-02 11:56:35`；`miniquote.exe` PID `93196`，StartTime `2026-07-02 11:56:36`。这里只记录当前观测，不推断用户何时重启或因果。

### API 只读状态

- `GET /api/v1/simulation-runtime/scheduler/status`：返回 `scheduler=simulation_lifecycle_scheduler`、`autostart=true`、`default_submit=true`、`context_provider_mode=production`、`miniqmt_quote_source=MINIQMT_REALTIME.broker_quote`。该 API 的 ops 投影没有暴露 underlying `running/thread_alive/last_run_at`，但日志证明线程活着。
- `GET /api/v1/paper-v2/session-scheduler/bootstrap-status`：legacy scheduler `running=true`、`thread_alive=true`、`last_run_at=2026-07-02T06:15:42.516340+00:00`（14:15:42 +08）、`session_count=0`、`enabled_portfolio_count=0`。
- `GET /api/v1/qmt/status`：`enabled=true`、`connected=true`、`mode=SIM`、`account_id=62266303`、`pid=164416`、`client_class=backend.infra.qmt_client.XtQuantQMTClient`。
- `GET /api/v1/monitor/miniqmt/status`：旧 monitor 入口返回 `enabled=false/connected=false`，与当前生产 QMT 入口 `/api/v1/qmt/status` 不是同一状态来源；本 RCA 以 `/api/v1/qmt/status` 为当前 QMT client 证据。

## DB 证据

只读 SQL 时间：`2026-07-02 14:20:58+08`，PostgreSQL timezone `Asia/Shanghai`。

### eligible binding 仍存在

`paper_v2.simulation_release_binding` 在 `2026-07-02` 仍有 4 个 active eligible rows，均 `SIM_VALIDATING` 且 `effective_from=effective_to=2026-07-02`：

| order | binding_id | backend | strategy_id | package_id | slot |
|---:|---|---|---|---|---|
| 1 | `simbind_de37b2342c8eee91` | `local_sim` | `paper_b26d2312d986441f8497f7484c05f0ec` | `pkg_378eb9c91e104c64935404e257e932ee` | - |
| 2 | `simbind_982a76fdbd824dad` | `local_sim` | `paper_e225bf8a68244c54b4cc25506dadad81` | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | - |
| 3 | `simbind_dcabd41bdbac1b1c` | `minqmt_sim` | `codex_final_ms_l16_20260603` | `pkg_378eb9c91e104c64935404e257e932ee` | `codex_final_ms_l16_20260603` |
| 4 | `simbind_06efa40c99da8bc9` | `minqmt_sim` | `codex_final_ms_l2_20260603` | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | `codex_final_ms_l2_20260603` |

代码筛选依据：`SimulationRuntimeRepository.list_simulation_release_bindings()` 用 `approval_state IN (...)`、`effective_from <= active_on`、`effective_to >= active_on`，排序 `ORDER BY created_at DESC, binding_id`（`backend/services/simulation_runtime/repository.py:177`-`backend/services/simulation_runtime/repository.py:228`）。

### daily_run 更新时间

`paper_v2.simulation_daily_run where trade_date='2026-07-02'` 当前 4 行，全部 `FAILED_RETRYABLE`、`selection_evidence_id=NULL`、`execution_plan_id=NULL`、`broker_called=false`、`submitted_intents=0`：

| binding_id | backend | last_updated | durable reason |
|---|---|---:|---|
| `simbind_dcabd41bdbac1b1c` | `minqmt_sim` | `2026-07-02 12:59:56.709544+08` | `REALTIME_QUOTE_STALE`，symbol `000048.SZ`，quote timestamp `2026-07-02T11:30:00` |
| `simbind_06efa40c99da8bc9` | `minqmt_sim` | `2026-07-02 12:59:57.886074+08` | `REALTIME_QUOTE_STALE`，symbol `002049.SZ`，quote timestamp `2026-07-02T11:30:00` |
| `simbind_de37b2342c8eee91` | `local_sim` | `2026-07-02 14:01:53.356935+08` | `REALTIME_QUOTE_TIMESTAMP_INVALID`，TDX raw timestamp `13990274` |
| `simbind_982a76fdbd824dad` | `local_sim` | `2026-07-02 14:01:58.488625+08` | `REALTIME_QUOTE_TIMESTAMP_INVALID`，TDX raw timestamp `13984048` |

这说明 MiniQMT 两个 daily_run 的 durable row 确实停在 12:59:56/12:59:57；LocalSim 在本轮取证时已看到 14:01:53/14:01:58 更新。两类失败均发生在 selection/plan 前，且没有 broker side effect。

## 日志证据

### 线程没有崩溃退出

`aistock.log` 中 `simulation-runtime-lifecycle-scheduler` 线程持续输出 DB audit：

- 12:50:27 起持续有 `thread="simulation-runtime-lifecycle-scheduler"` 的 `db_connection_audit`。
- 13:00:29、13:00:32、13:00:34 仍有同线程 DB audit。
- 14:12:48、14:13:49、14:14:49、14:15:50、14:16:49、14:17:50、14:18:49、14:19:49、14:20:50、14:21:53 仍有同线程 DB audit。
- 12:00-14:21 范围内统计到 `Simulation runtime scheduler run_once crashed` 数量为 0。

### 13:00:48 后每轮 tick 被 LiveInferencePreflightError 阻断

`aistock.log` 首次命中：

- `backend/logs/aistock.log:26645`，`2026-07-02 13:00:48 WARNING [aistock.simulation_runtime.scheduler] Simulation runtime scheduler tick failed: {'type': 'LiveInferencePreflightError', ... 'package_id': 'pkg_378eb9c91e104c64935404e257e932ee', ... 'reason_code': 'strategy_package_model_code_missing', ... 'missing_relative_paths': ['model.py'], ...}`

随后同类 warning 持续重复：

- `13:01:52`、`13:02:51`、...、`14:21:22`；本轮统计 86 条。
- 最新样本包括 `backend/logs/aistock.log:26835`（14:20:18）和 `backend/logs/aistock.log:26837`（14:21:22），仍是同一 `LiveInferencePreflightError` / `pkg_378...` / `strategy_package_model_code_missing`。

`errors.log` 在 2026-07-02 13:00-14:00 范围没有发现 `Simulation runtime scheduler run_once crashed` traceback；只有其它 unrelated traceback（例如 RA semantic MCP planner）。

## 13:01 后“不更新”的机制定性

### 排除项

- **不是 legacy scheduler 还活着但 simulation scheduler 死了。**legacy 和 simulation 是两套线程；legacy `bootstrap-status` 不能证明 simulation runtime tick 活着或死了。simulation runtime 自己的线程日志持续到 14:21+。
- **不是 active binding 全部不再 selected。**DB active binding 查询仍选中 4 个 binding，包括 L2/L16，approval/effective 窗口未排除。
- **不是已观察到 QMT client disconnected。**`/api/v1/qmt/status` 当前返回 connected=true；MiniQMT quote stale 作为 durable `REALTIME_QUOTE_STALE` 已写入 daily_run，但 13:00 后的 scheduler warning 是 model-code preflight，不是 QMT disconnect traceback。

### 直接机制

1. Background wrapper 每轮进入 `SimulationLifecycleScheduler.run_once()`。
2. inner scheduler 先按 created_at 排序处理 eligible binding。
3. 对 `DataUnavailableError` / `RuntimeConfigInvalidError`，inner 会 per-binding persist `FAILED_RETRYABLE` pre-run failure 并继续后续 binding。
4. 但 `selection_service.run_selection()` 中的 cold-start preflight 抛 `LiveInferencePreflightError`，它不是上述两个类型。
5. 该异常越过 inner per-binding catch，导致整个 inner `run_once()` abort。
6. Background wrapper catch 到异常，只把它写进 `last_result.errors` 和日志 warning；不会给当前 binding 写 durable `simulation_daily_run` failure，也不会继续处理后续 binding。
7. 因而 MiniQMT rows 停留在 12:59:56/12:59:57 的上一次 durable `REALTIME_QUOTE_STALE`，后续 tick 仍在跑，但被 `pkg_378...` 的 model-code preflight 阻断。

相关代码链：

- inner only catches two exception types: `backend/services/simulation_runtime/scheduler.py:2223`-`backend/services/simulation_runtime/scheduler.py:2248`。
- selection preflight raise path: `backend/services/simulation_runtime/scheduler.py:5163`-`backend/services/simulation_runtime/scheduler.py:5199` -> `backend/services/selection_center/service.py:487`-`backend/services/selection_center/service.py:525` -> `backend/services/strategy_package/live_inference.py:1333`-`backend/services/strategy_package/live_inference.py:1378`。
- background catches and records/logs but does not persist per-binding: `backend/services/simulation_runtime/scheduler.py:7014`-`backend/services/simulation_runtime/scheduler.py:7021`。

## 是否需要重启 backend 才能恢复

明确结论：**不需要，也不建议把 backend restart 当作本 RCA 的恢复动作。**

理由：

- simulation lifecycle 线程仍活着并继续 tick；不是 dead thread。
- 当前硬阻塞是 deterministic `LiveInferencePreflightError` / `strategy_package_model_code_missing`。backend restart 只会重新遇到同一 package preflight。
- MiniQMT client 当前 `/api/v1/qmt/status` 为 connected=true；没有证据显示 backend 持有旧 xtquant 句柄导致线程死亡。
- 如果后续确认 quote feed stale 需要恢复，则方向应是 MiniQMT/QMT quote feed 或 QMT client reconnect/reload 的受控操作；这仍不同于“必须 backend restart”。

分情形判断：

| 情形 | 本次证据 | 是否需 backend restart |
|---|---|---|
| 线程死/异常退出 | 未命中；线程日志持续、crash count 0 | 否 |
| 线程活但 binding 被 skip | active binding 未被 approval/effective 排除；不是 skip | 否 |
| 线程活但某 binding/package 非 per-binding 异常 abort | 命中；`LiveInferencePreflightError` 每分钟重复 | 不靠 restart，需清除阻塞或修代码 |
| QMT quote stale | MiniQMT row durable 记录到 12:59:56/57；当前 QMT status connected | 先修 quote/feed/reconnect 方向；restart backend 不是首选 |

## 是否 Bug / BUG 方向

### 主 Bug：per-binding 隔离与 durable failure 缺失

建议登记方向：`SimulationLifecycleScheduler should isolate and persist non-DataUnavailable per-binding failures`。

验收方向：

- `_run_binding()` 内或 `run_once()` per-binding loop 应捕获更完整的 domain/runtime exception（至少包括 `LiveInferencePreflightError` 或通用 `Exception` 的 fail-loud分类）。
- 对 selection/preflight 阶段失败也写 durable `simulation_daily_run` / scheduler audit，包含 `binding_id`、`package_id`、`reason_code`、`blocked_check`、`missing_relative_paths`。
- 一个 binding 失败不得阻断后续 eligible binding。
- Background `last_result.errors` 仍保留 loud warning，但不再是唯一证据。
- Status API 应暴露 underlying `running/thread_alive/last_run_at/last_result.errors`，避免只看 legacy bootstrap 或只看 DB updated_at 时误判“线程停”。

### QMT 连接/quote 自愈 hardening

当前证据没有证明 QMT reconnect/old xtquant handle 是 13:01 后不更新的主因，但存在独立 hardening 方向：

- `XtQuantQMTClient.status()` 会 probe/autoconnect（`backend/infra/qmt_client.py:699`-`backend/infra/qmt_client.py:780`），`get_positions()` / `get_orders()` 等也 probe。
- 但 `get_full_tick()` 只 `_ensure_xtquant()` 然后调用 `xtdata.get_full_tick(...)`（`backend/infra/qmt_client.py:1173`-`backend/infra/qmt_client.py:1182`），没有显式 `_probe_connection_locked()` / stale quote reconnect / client singleton rebuild。
- quote stale gate 会 fail loud 为 `REALTIME_QUOTE_STALE`（`backend/services/paper_trading_v2/market_data.py:696`-`backend/services/paper_trading_v2/market_data.py:755`），但目前主要落到 daily_run pre_run_failure；没有看到 scheduler 自愈 quote feed。

建议作为相关 BUG/hardening：`MiniQMT broker quote stale/connection invalid detection should be loud and self-healing without silently starving scheduler ticks`。注意：这是相关 hardening，不是本次 13:01 后 DB 不更新的直接定因。

## 对今天恢复运行的影响

在当前阻塞解除前：

- L2/L16 的 `simulation_daily_run` 仍无 `selection_evidence_id`、无 `execution_plan_id`、`broker_called=false`、`submitted_intents=0`，因此换包/清仓后的新 run 不会可靠创建/推进到 selection/plan/submit。
- 如果新 binding 的排序在阻塞 binding 之后，仍会被 `pkg_378...` preflight abort 阻断。
- 如果新 binding 排序在阻塞 binding 之前，可能先被处理，但只要阻塞 binding 仍 eligible，同一轮后续 binding 仍会被截断；不应把这当作稳定恢复。

最小恢复方向（只给方向，不实施）：

1. **先清除 scheduler abort blocker**：修复 `pkg_378eb9c91e104c64935404e257e932ee` 对应 runtime asset 中缺失的 `model.py` / model-code，或由战略 session 授权临时把触发该 preflight 的 binding/package 移出 eligible 集合（approval/effective 层面），让 tick 能继续处理后续 binding。
2. **再确认行情 gate**：MiniQMT L2/L16 仍有 `REALTIME_QUOTE_STALE` durable evidence，LocalSim 仍有 TDX timestamp invalid evidence；要恢复 plan/submit，需要 quote timestamp/freshness gate 变绿。
3. **不把 backend restart 作为最小动作**：除非后续战略 session 明确要求重建 process-level QMT singleton 或验证 backend live process state，否则本 RCA 不支持“必须重启 backend”。

## 与其它问题隔离

- **WSL/model-code 窗口**：本次反复 abort 的直接 error 就是 `strategy_package_model_code_missing`，因此它与另窗口修 model-code 相关；但“调度不更新”的平台缺陷是 scheduler 未 per-binding 隔离/持久化该错误，不等同于 WSL 本身导致线程停。
- **行情 stale**：MiniQMT 的 `REALTIME_QUOTE_STALE` 与 LocalSim 的 `REALTIME_QUOTE_TIMESTAMP_INVALID` 是 pre-run data gate，解释为什么对应 run 不能进入 selection/plan/submit；但 13:00 后每分钟 warning 的主签名是 model-code preflight，不是行情 stale。
- **BUG-562/565/567**：当前 4 个 daily_run 均在 selection/plan 前失败，`execution_plan_id=NULL`、`broker_called=false`、`submitted_intents=0`；没有进入 reconcile、post-close terminalization、binding refreeze、broker order side-effect 路径。因此本次“scheduler tick 被 abort”独立于 BUG-562/565/567 的订单/reconcile/refreeze问题域。

## 生产 gates

- `production_ddl_gate`: `noop`
- `production_backend_dependency_gate`: `noop`
- `production_frontend_dependency_gate`: `noop`
- 生产 runtime：未启停、未重启、未触碰 QMT。
- 生产 DB：只读 SELECT；未写入、未 DDL、未 DML。
- 券商订单：未发单、未撤单、未 operator/apply。
