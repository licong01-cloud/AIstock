# 2026-06-23 盘前模拟盘无值守只读检查

检查时间：2026-06-23 00:26-00:40 Asia/Shanghai

检查角色：盘前只读检查员

范围：AIstock unified simulation runtime 的 MiniQMT 与 LocalSim 无值守链路；同时确认 legacy Paper v2 session scheduler 状态。

只读声明：本次未启动、重启、停止任何服务；未写生产 DB/DDL；未执行受控 tick/submit/operator command。唯一写入为本 handoff 文档。

## 结论

- MiniQMT unified simulation runtime：GO，可在 2026-06-23 A 股开盘后无值守运行；需盘中关注 L16 capacity residual 指标，但该残差已被显式暴露，不再是隐藏成功。
- LocalSim L2：GO；2026-06-22 已有成功 run，绑定可滚动。
- LocalSim L16：NO-GO/WATCH；2026-06-22 存在 active binding，但没有生成 simulation_daily_run / execution_plan / selection evidence，且底层 Paper v2 portfolio 状态停在 2026-06-17。建议按 P1 新 bug 登记“LocalSim pre-run failure not durably persisted”。
- Legacy Paper v2 session auto-run：NO-GO；scheduler thread 正常，但 auto_run.enabled_portfolio_count=0，不会自动跑旧 session 路线。
- 后端修复生效判断：按进程启动时间晚于 main HEAD、cwd、health/openapi 与状态接口推断已加载最新 main；当前没有显式 commit endpoint，因此不做“运行时 commit 精确匹配”的绝对声明。

## 阶段 1：修复合并与运行时加载

### Git / main 状态

- main HEAD：`98ad58f4396dff3b8b5d1a9dd37d9a05ef3b8034`
- HEAD time：`2026-06-22T20:32:28+08:00`
- HEAD subject：`chore(issue): close-sync BUG-473 after merge (#1486)`
- `git status --short --branch`：`## main...origin/main`，检查前 clean。

### 11 个修复状态

| BUG | GitHub issue | 状态 | fix_commit | 备注 |
| --- | --- | --- | --- | --- |
| BUG-446 | #1384 | CLOSED / fixed | `eced67a75fa473b64737e51f76db3cf1fd972154` | MiniQMT EOD fresh reconcile |
| BUG-447 | #1385 | CLOSED / fixed | `bb0477c8c7241334b16c5a4211b7f1b89a1efa13` | qmt_strategy_ledger broker-authoritative |
| BUG-448 | #1386 | CLOSED / fixed | `e58e81afe7a5284dd0e3e10ed101d6e5a3d966fb` | run status SQL guard / terminal orders |
| BUG-470 | #1453 | CLOSED / fixed | `a96a48212b816ba3e9e80060839e4ae5b51bbc44` | MiniQMT order open/terminal 状态归一 |
| BUG-478 | #1467 | CLOSED / fixed | `b009bd9c714e5a94a3e32fb67fdcbacf76f81a1f` | capacity residual 顶层暴露 |
| BUG-462 | #1427 | CLOSED / fixed | `557eddad379edaffe259eae64c29f27c1c5e588a` | LocalSim cash ledger idempotency + transaction |
| BUG-463 | #1428 | CLOSED / fixed | `64291ba237c319acc0ee0b6fc9afbcfcfb3c5bdd` | LocalSim scheduler timezone / terminalizer |
| BUG-464 | #1429 | CLOSED / fixed | `a72b2f548351e300a40fbe35a4a2eaef3a08f315` | LocalSim live tick persistence |
| BUG-465 | #1430 | CLOSED / fixed | `84004919c1521399d9a97ab5e57b036def2db3fd` | LocalSim ledger Decimal / board lot |
| BUG-466 | #1431 | CLOSED / fixed | `6d1bf35dde17a94fb619b9da4fd7ef5f5508b000` | LocalSim TDX freshness / ST / limit gate |
| BUG-467 | #1432 | CLOSED / fixed | `4bdcb72fc21f88e50a2a0795f5a4951ab30121c7` | LocalSim turnover_rate_f fail-closed |

### DDL gate

- 本次检查未执行 DDL。
- BUG-462 生产 DDL 已只读验证：`paper_v2.cash_ledger` 存在唯一约束 `cash_ledger_run_fill_unique`，定义为 `UNIQUE (run_id, fill_id)`。
- 本次操作 gate：`production_ddl_gate=noop`；既有 BUG-462 DDL 状态：`applied_and_verified(read-only evidence)`。

### 后端进程 / 修复加载

- health：`GET /api/v1/health` 返回 `{"status":"ok","app":"Aistock Next Backend"}`。
- 监听：`0.0.0.0:8001`，PID `37908`，`python.exe`。
- cwd：`F:\Dev\AIstock`。
- cmdline：`uvicorn backend.main:app --host 0.0.0.0 --port 8001`。
- start time：`2026-06-22T22:19:15.453233`，晚于 main HEAD commit time `2026-06-22T20:32:28+08:00`。
- env：`ENABLE_SIMULATION_RUNTIME_SCHEDULER=true`、`SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT=true`、`SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE=DB_HISTORICAL`、`ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER=1`、`PAPER_V2_AUTO_RUN_ENABLED=true`、`ENABLE_PAPER_TRADING_V2_SCHEDULER=true`。
- commit env：`GIT_COMMIT` / `AISTOCK_GIT_COMMIT` / `COMMIT_SHA` 均未设置；没有发现显式 runtime commit endpoint。

判断：不要求用户立即重启后端；但该判断基于进程启动时间 + cwd + API 状态推断，不是运行时 commit endpoint 的精确证明。

## 阶段 2：无值守 readiness

### 交易日

`GET /api/v1/trading-calendar/status?as_of_date=2026-06-23`

- `is_trading_day=true`
- `previous_trading_day=2026-06-22`
- `next_trading_day=2026-06-24`
- source：`market.trading_calendar:file_cache`
- coverage：到 `2026-12-31`

### Unified simulation runtime scheduler

`GET /api/v1/simulation-runtime/scheduler/status`

- `autostart=true`
- `default_submit=true`
- context provider：`ProductionSimulationRunContextProvider`，`ready=true`
- `localsim_state_source=paper_v2_portfolio`
- `miniqmt_state_source=broker_authoritative_positions_with_strategy_slot_projection`
- `localsim_broker_enabled=true`
- `miniqmt_preview_enabled=true`
- `miniqmt_submit_enabled=true`
- data source：`DB_HISTORICAL`
- policy：LocalSim same-day `TDX_REALTIME`，MiniQMT `MINIQMT_REALTIME`
- `account_slot_persistence.enabled=true`
- `miniqmt_unified_binding_mode=account_group_slots`
- 窗口：08:50-09:10 readiness；09:10-09:20 selection；09:20-09:25 planning；09:25-15:00 submit；15:00-15:30 post_close_reconcile。

说明：代码中的 background scheduler status 包含 `running/thread_alive`，但 `backend/services/simulation_runtime/ops.py:47` 的只读 API 投影未直接暴露这两个字段；这里以 `autostart=true`、backend 进程环境和状态接口推断 unified scheduler 已启用。

### QMT / MiniQMT

`GET /api/v1/qmt/status`

- `enabled=true`
- `connected=true`
- `mode=SIM`
- `account_id=62266303`
- provider：`xtquant`
- userdata：`F:\QMT_SIM\userdata_mini`
- `last_error=null`
- PID：`37908`

`GET /api/v1/qmt/monitor/strategies`

- account：available_cash `24836648.12`，total_asset `29556985.61`，market_value `4718881.49`，frozen_cash `0.0`。
- strategies 非空，共 2 个：
  - `codex_final_ms_l2_20260`：orders_count 15，trades_count 30，symbols_count 15，market_value `373357.45`。
  - `codex_final_ms_l16_2026`：orders_count 3，trades_count 14，symbols_count 3，market_value `56953.0`。

`GET /api/v1/qmt/orders?cancelable_only=false`

- total orders：18。
- status counts：`{56: 18}`。
- open-like `{48,49,50,51,52,53,55}` count：0。
- 结论：当前无 open/partial/pending 委托残留；BUG-470 所关注的 partial-fill open-like 语义在当前券商状态下没有未终结订单触发风险。

### Simulation run 结果

`GET /api/v1/simulation-runtime/runs?trade_date=2026-06-23&limit=100`

- run_count 0。盘前 00:xx 时点早于当天 scheduler windows，符合预期。

`GET /api/v1/simulation-runtime/runs?trade_date=2026-06-22&limit=100`

- run_count 3。
- by_status：`SUCCEEDED:3`。
- by_broker_backend：`local_sim:1`，`minqmt_sim:2`。
- MiniQMT L2：`simrun_93e98d9afc6cbc67`，SUCCEEDED，submitted 15，failed 0。
- MiniQMT L16：`simrun_cbf014e6445d60b8`，SUCCEEDED with capacity residual；31 intents，submitted 3，failed/capacity residual 28；BUG-478 后顶层 summary 暴露 `succeeded_with_capacity_residual_count=1`、`capacity_residual_count=28`、`capacity_residual_failed_intents=28`。
- LocalSim L2：`simrun_08c84378ffbf2068`，SUCCEEDED，submitted 38。
- 缺口：没有 2026-06-22 LocalSim L16 run。

### Release bindings / roll-forward

只读 DB 查询显示 2026-06-22 有 4 个 active/recent binding：

| binding | backend | strategy_id | package_id | account/slot | 2026-06-22 run |
| --- | --- | --- | --- | --- | --- |
| `simbind_0788784efe004f51` | local_sim | `paper_b26d2312d986441f8497f7484c05f0ec` | `pkg_378eb9c91e104c64935404e257e932ee` | N/A | false |
| `simbind_ffac5f6f48957127` | local_sim | `paper_e225bf8a68244c54b4cc25506dadad81` | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | N/A | true |
| `simbind_f40c528cfd285e9c` | minqmt_sim | `codex_final_ms_l16_20260603` | `pkg_378eb9c91e104c64935404e257e932ee` | `ag_minqmt_62266303_sim` / `codex_final_ms_l16_20260603` | true |
| `simbind_10f9c3859451ba33` | minqmt_sim | `codex_final_ms_l2_20260603` | `pkg_a2f53f3f2f3e4095a910b939464c35e6` | `ag_minqmt_62266303_sim` / `codex_final_ms_l2_20260603` | true |

这些 binding 的 `effective_to=2026-06-22`，代码路径支持无人值守 roll-forward 到 2026-06-23（`backend/services/simulation_runtime/scheduler.py:2767`、`:2825`、`:2838`）。roll-forward 是 scheduler tick 写入行为，本次未手工触发。

### StrategyPackage readiness

检查 package：

- L16：`pkg_378eb9c91e104c64935404e257e932ee`
- L2：`pkg_a2f53f3f2f3e4095a910b939464c35e6`

接口：

- `/api/v1/strategy-governance/packages/{pkg}/selection-readiness`：两者 `ok=true`，`selection_candidate=true`，blockers empty，package_status `BACKTEST_APPROVED`。
- `/api/v1/strategy-governance/packages/{pkg}/paper-readiness`：两者 `ok=true`，`paper_simulation_allowed=true`，blockers empty。
- live strict governance warnings 存在，但响应明确 `does_not_block_paper_simulation=true`，不阻塞模拟盘。

### Legacy Paper v2 session scheduler

`GET /api/v1/paper-v2/session-scheduler/status`

- `running=true`
- `thread_alive=true`
- `auto_run.env_enabled=true`
- `auto_run.enabled_portfolio_count=0`
- `last_result.session_count=0`
- `last_error=null`

`GET /api/v1/paper-v2/session-scheduler/bootstrap-status`

- scheduler running/thread_alive true。
- `auto_run.enabled_portfolio_count=0`。
- `miniqmt_account_group_slots.enabled=true`。
- `unified_path_active=true`。

结论：legacy Paper v2 session route 不会自动跑；如果用户仍要求这条旧路线，需要用户另行启用/创建/recover auto-run portfolio/session。本次不执行启用动作。

## 阶段 3：对抗性复扫

复扫路径：选股 cutoff -> 起跑 -> 行情 -> 特征 -> 定额 -> 撮合 -> 落库 -> 收盘终结 -> 对账。

### 未发现新的 MiniQMT P0 run-blocking

- QMT 已连接 SIM，策略监控非空，账户 frozen_cash 为 0。
- 当前券商订单全部为终态 56，无 open-like 残留。
- 2026-06-22 MiniQMT L2/L16 均有 terminal SUCCEEDED run。
- L16 capacity residual 已通过 BUG-478 暴露为顶层残差计数，不会误报为完全 clean success。
- BUG-446/470 覆盖的 EOD fresh reconcile 与 open/terminal 语义在当前状态无新增反例。

### 新增 P1 候选：LocalSim pre-run failure not durably persisted

证据：

- 2026-06-22 active LocalSim L16 binding `simbind_0788784efe004f51` 存在，package 为 L16 `pkg_378eb9c91e104c64935404e257e932ee`。
- 同一日期无 `simulation_daily_run`，无 execution plan / selection evidence。
- backing Paper v2 portfolio `paper_b26d2312d986441f8497f7484c05f0ec` 状态 `READY`，但 positions/daily snapshots 最新 trade_date 均停在 `2026-06-17`。
- `SimulationLifecycleScheduler.run_once()` 在 `_run_binding()` 外层捕获 `DataUnavailableError` / `RuntimeConfigInvalidError` 后，只向 tick results 追加 transient FAILED，若失败发生在 run 创建前，DB/API 没有 durable run/error（`backend/services/simulation_runtime/scheduler.py:2016`-`2048`）。
- background scheduler 会记录 last_result，但 status API 只投影固定字段，未暴露 last_result（`backend/services/simulation_runtime/scheduler.py:6087`-`6095`，`backend/services/simulation_runtime/ops.py:47`-`82`）。

影响：LocalSim 某个 active binding 可在 selection/context load 阶段失败并从 DB/API 视角“消失”，导致无值守覆盖缺口。该问题不阻断 MiniQMT 今天运行，但阻断 LocalSim L16 无值守可信度。

建议：按 P1 登记并修复，目标是 pre-run 阶段失败也必须持久化到 `simulation_daily_run` 或专门 scheduler audit 表，并在 status/API 中显式可见；不得只留内存 tick result。

## 阶段 4：已知遗留失败影响判断

用户给出的两个 pre-existing 失败：

1. `test_order_service_preflight` 账户组现金超额。
2. `test_router_summary` V25 MiniQMT preview -> 400。

判断：不属于 2026-06-23 unified MiniQMT unattended submit 直接路径的盘前必修项，可盘后 triage。

理由：

- 无值守 MiniQMT 路径使用 `simulation_runtime` scheduler / orchestrator + qmt ledger / order service，不依赖旧 preview router 的成功响应。
- 账户组现金超额已在实际 2026-06-22 MiniQMT L16 中体现为 capacity residual，并通过 BUG-478 顶层暴露；这是 capacity/资金约束状态，不应隐藏为 clean success。
- `backend/tests/qmt_strategy_ledger/test_router_summary.py:382` 附近的旧 V25 preview fail-fast 与当前 shared execution plan preview 是不同路径；不阻断 scheduler submit。

## 运营动作清单

盘前必须/建议动作：

1. MiniQMT unified simulation runtime：无需我建议立即重启；开盘后重点观察 09:10 selection、09:20 planning、09:25 submit 窗口是否生成 2026-06-23 L2/L16 runs。
2. 若要求 LocalSim L16 也必须无值守：建议先登记并处理 P1 “pre-run failure not durably persisted”，或者今天人工盯盘确认是否生成 run；LocalSim L2 可继续观察。
3. 若要求 legacy Paper v2 session route：需要用户启用/创建/recover auto-run portfolio/session；当前 `enabled_portfolio_count=0`，不会自动执行。
4. 盘中监控 MiniQMT L16 capacity residual：若继续出现，应按容量/分仓资金约束处理，不视为隐藏成功。
5. 当前不要由 Codex 重启后端；若用户后续手动重启，建议重启后再次只读检查 `/api/v1/simulation-runtime/scheduler/status`、`/api/v1/qmt/status`、`/api/v1/qmt/orders`。

## Gate 汇总

- production_ddl_gate：本次检查 `noop`；BUG-462 既有 DDL 只读验证为已应用。
- production_backend_dependency_gate：本次检查 `noop`；未安装依赖、未重启服务。
- production_frontend_dependency_gate：`noop`；未触碰前端。
- 服务操作：未启动/停止/重启。
- DB 操作：只读查询；无写入、无 DDL。
