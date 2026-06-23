# 2026-06-23 后端重启后模拟盘只读复查

检查时间：2026-06-23 03:54-04:05 Asia/Shanghai

角色：盘前只读检查员

范围：后端重启后代码加载确认、unified simulation runtime、MiniQMT 无值守链路、LocalSim 当日 run 生成前置状态、legacy Paper v2 session scheduler 状态。

只读声明：本次未启动、重启、停止任何服务；未执行受控 scheduler tick、submit、operator command；未写生产 DB/DDL。唯一写入为本 handoff 文档。

## 结论

- 后端已加载最新 main：GO。后端 PID `69852` 启动于 `2026-06-23T03:52:08+08:00`，晚于当前 main/origin/main HEAD `f3bc1fba95abd1539ce9b5b5c50e78bdddf812c4` 的提交时间 `2026-06-23T03:48:04+08:00`，进程 cwd 为 `F:\Dev\AIstock`。
- MiniQMT unified simulation-runtime：盘前 GO / 09:10 观察。QMT connected=true、mode=SIM、账号 `62266303`、frozen_cash=0、broker cancelable orders=[]、DB open-like order ledger count=0。
- 2026-06-23 当日 runs 尚未生成：当前检查时段早于 09:10 selection window，符合预期；今天 binding 也尚未 roll-forward 落库，需 09:10 后确认自动创建 MiniQMT L2/L16 与 LocalSim L2 run。
- L16 pre-run failure reason_code：当前无 2026-06-23 run，因此尚无 reason_code。若 09:10 后 L16 pre-run 失败，BUG-484 已合入并重启生效，预期应在 durable `FAILED_RETRYABLE` run 的 `pre_run_failure.reason_code` 中可见。
- Legacy Paper v2 session scheduler：NO-GO if required。线程运行，但 `auto_run.enabled_portfolio_count=0`；两个 auto_run portfolio 均为 `FAILED`，不会走旧 session auto-run 路线。

## 修复在位与生效

### Git/main

- `HEAD=origin/main=f3bc1fba95abd1539ce9b5b5c50e78bdddf812c4`
- HEAD subject：`chore(issue): close-sync BUG-486 after merge (#1500)`
- HEAD commit time：`2026-06-23T03:48:04+08:00`
- `git status --short --branch`：main 与 origin/main 对齐，检查前无待提交代码变更。

### 已合入并关闭的相关 BUG

| BUG | Issue | 状态 | 说明 |
| --- | --- | --- | --- |
| BUG-446 | #1384 | fixed / CLOSED | MiniQMT EOD fresh reconcile |
| BUG-447 | #1385 | fixed / CLOSED | qmt_strategy_ledger broker-authoritative |
| BUG-448 | #1386 | fixed / CLOSED | simulation run 状态机守卫 |
| BUG-470 | #1453 | fixed / CLOSED | MiniQMT open/terminal order status 归一 |
| BUG-478 | #1467 | fixed / CLOSED | capacity residual 顶层观测性 |
| BUG-462 | #1427 | fixed / CLOSED | LocalSim cash ledger 幂等与事务地基 |
| BUG-463 | #1428 | fixed / CLOSED | LocalSim scheduler 时区、终结器、交易日门 |
| BUG-464 | #1429 | fixed / CLOSED | LocalSim live tick 持久化与 no-fill 语义 |
| BUG-465 | #1430 | fixed / CLOSED | LocalSim Decimal ledger 与 board-lot |
| BUG-466 | #1431 | fixed / CLOSED | LocalSim TDX quote freshness、ST、涨跌停门 |
| BUG-467 | #1432 | fixed / CLOSED | LocalSim turnover_rate_f fail-closed |
| BUG-484 | #1490 | fixed / CLOSED | pre-run binding failure durable audit |

### 进程证据

| 服务 | 端口 | PID | 启动时间 | cwd |
| --- | ---: | ---: | --- | --- |
| FastAPI backend | 8001 | 69852 | `2026-06-23T03:52:08+08:00` | `F:\Dev\AIstock` |
| TDX Go web | 19080 | 93428 | `2026-06-23T03:52:07+08:00` | `F:\Dev\AIstock\tdx-api-main\web` |
| Next.js frontend | 3000 | 161492 | `2026-06-23T03:52:08+08:00` | `F:\Dev\AIstock\frontend` |

后端 cmdline：`uvicorn backend.main:app --host 0.0.0.0 --port 8001`

后端关键 env：

- `ENABLE_SIMULATION_RUNTIME_SCHEDULER=true`
- `SIMULATION_RUNTIME_CONTEXT_PROVIDER=production`
- `ENABLE_SIMULATION_RUNTIME_PRODUCTION_PROVIDER=1`
- `SIMULATION_RUNTIME_SCHEDULER_DEFAULT_SUBMIT=true`
- `SIMULATION_RUNTIME_SCHEDULER_DATA_SOURCE=DB_HISTORICAL`
- `ENABLE_PAPER_TRADING_V2_SCHEDULER=true`

## 运行态 readiness

### 基础 API

- `/api/v1/health`：200，`status=ok`。
- `/api/v1/trading-calendar/status?as_of_date=2026-06-23`：`is_trading_day=true`，previous=`2026-06-22`，next=`2026-06-24`，source=`market.trading_calendar:file_cache`，warnings=[]。
- TDX Go `/api/search?keyword=000001`：200，返回 success。

注意：TradingCalendarStatusService 当前把 as_of_date 当作日历日判断；盘前查询 `latest_completed_trading_day=2026-06-23`，这不等价于盘后数据已同步。实际 DB `market.kline_daily_raw` 最大 `trade_date=2026-06-22`，符合“盘前只能依赖 TDX/miniQMT 实时数据，DB 日线盘后同步”的运行逻辑。

### Simulation runtime scheduler

`GET /api/v1/simulation-runtime/scheduler/status`：

- `autostart=true`
- `default_submit=true`
- `context_provider_mode=production`
- provider：`ProductionSimulationRunContextProvider`，`ready=true`
- `localsim_broker_enabled=true`
- `miniqmt_preview_enabled=true`
- `miniqmt_submit_enabled=true`
- `miniqmt_submit_default=false`
- data source policy：LocalSim same-day=`TDX_REALTIME`，MiniQMT=`MINIQMT_REALTIME`
- account slot persistence enabled，`miniqmt_unified_binding_mode=account_group_slots`

说明：该只读 API 的 ops projection 未直接暴露 `running/thread_alive/last_run_at`，但 `autostart=true` 来自 background scheduler status 的 running 值；结合后端 env 与进程状态，本次判定 unified scheduler 已启用。

### QMT/MiniQMT

`GET /api/v1/qmt/status`：

- `enabled=true`
- `connected=true`
- `mode=SIM`
- `account_id=62266303`
- provider=`xtquant`
- userdata=`F:\QMT_SIM\userdata_mini`
- `last_error=null`

`GET /api/v1/qmt/snapshot`：

- available_cash=`24836648.12`
- total_asset=`29556985.61`
- market_value=`4718881.49`
- frozen_cash=`0.0`
- position_count=54
- T+1 导致 `quantity>0 && can_sell=0` 的持仓数=10，属于交易日前一日买入后的可卖数量约束，不是 open order。

`GET /api/v1/qmt/orders?cancelable_only=true`：

- 返回 `[]`。

`GET /api/v1/qmt/orders`：

- 18 笔券商订单均为 `order_status=56` terminal filled。
- 未发现 48/49/50/51/52/53/55 open-like 或 partial 残留。

DB `qmt_strategy.order_ledger`：

- open-like `{48,49,50,51,52,53,55}` count=0。
- BUG-470 关注的 partial-fill 55 漏判 open 风险当前没有残留样本触发。

QMT strategy summary：

- `codex_final_ms_l16_20260603`：ENABLED，cash=`126.09`，frozen_cash=`0.0`，market_value=`1968844.95`，positions_count=53。
- `codex_final_ms_l2_20260603`：ENABLED，cash=`120324.42`，frozen_cash=`0.0`，market_value=`426219.44`，positions_count=8。
- `qmt_unmanaged_baseline_20260604`：ENABLED，market_value=`22001236.3963`。
- `unattributed_orders=0`，`unattributed_trades=0`。

## 当日 run/binding 状态

### 2026-06-23

- `/api/v1/simulation-runtime/runs?trade_date=2026-06-23&limit=200`：run_count=0。
- `/api/v1/simulation-runtime/runs?trade_date=2026-06-23&broker_backend=minqmt_sim`：run_count=0。
- `/api/v1/simulation-runtime/runs?trade_date=2026-06-23&broker_backend=local_sim`：run_count=0。
- DB `paper_v2.simulation_daily_run where trade_date='2026-06-23'`：0 行。
- DB `paper_v2.simulation_release_binding active_on 2026-06-23`：0 行。

解释：检查时间在 09:10 前，scheduler 还未进入 selection/planning/submit window；binding roll-forward 是 scheduler tick 的写入行为，本次只读复查没有手工触发。

### 2026-06-22 参考基线

昨日 unified runtime 有 3 条成功 run：

- MiniQMT L2：`simrun_93e98d9afc6cbc67`，strategy=`codex_final_ms_l2_20260603`，status=`SUCCEEDED`，15 个 managed orders 全部 broker_called=true。
- MiniQMT L16：`simrun_cbf014e6445d60b8`，strategy=`codex_final_ms_l16_20260603`，status=`SUCCEEDED`，但 `succeeded_with_capacity_residual=true`，capacity_residual_count=28，reason_code=`MINIQMT_SUCCEEDED_WITH_CAPACITY_RESIDUAL`。
- LocalSim L2：`simrun_08c84378ffbf2068`，status=`SUCCEEDED`。

昨日最新 bindings 都是单日 binding，`effective_from=effective_to=2026-06-22`：

- L16 MiniQMT：`simbind_f40c528cfd285e9c`，capital_allocation=`2000000.000000`，account_group=`ag_minqmt_62266303_sim`，slot=`codex_final_ms_l16_20260603`。
- L2 MiniQMT：`simbind_10f9c3859451ba33`，capital_allocation=`500000.000000`，account_group=`ag_minqmt_62266303_sim`，slot=`codex_final_ms_l2_20260603`。
- LocalSim L2/L16：`simbind_ffac5f6f48957127` / `simbind_0788784efe004f51`。

代码路径支持从最近历史 binding 自动 roll-forward：`backend/services/simulation_runtime/scheduler.py` 的 `_with_unattended_roll_forward_bindings()`、`_binding_can_roll_forward()`、`_roll_forward_unattended_binding()`。

## Legacy Paper v2 session scheduler

`GET /api/v1/paper-v2/session-scheduler/status` 与 bootstrap-status：

- `running=true`
- `thread_alive=true`
- `auto_run.env_enabled=true`
- `auto_run.enabled_portfolio_count=0`
- `session_count=0`
- `miniqmt_account_group_slots.enabled=true`
- `unified_path_active=true`

DB：

- `paper_1d9b1f03700f4810aef8351124c8ab6c`，portfolio=`miniqmt_qe_20260520_loop16_20260525`，broker=`minqmt_sim`，status=`FAILED`，auto_run_enabled=true。
- `paper_3bf764d1f95a44dd80e1852d2e87bef0`，portfolio=`aistock_localsim_auto_20260601_codex`，broker=`local_sim`，status=`FAILED`，auto_run_enabled=true。

结论：旧 Paper v2 session auto-run 不是今天可依赖的无值守路线；当前可依赖路线是 unified simulation runtime。

## 对抗性复扫

### MiniQMT

未发现新的 MiniQMT P0/P1 run-blocking 缺陷：

- QMT 连接正常，账号可读，frozen_cash=0。
- 券商侧无可撤/open-like 委托。
- DB order_ledger open-like count=0。
- qmt_strategy 最新 ERROR reconciliation issue 最大时间为 `2026-06-17 15:00:05+08:00`；BUG-470 close-sync 后 ERROR count=0。
- 2026-06-22 L2/L16 均到 terminal `SUCCEEDED`；L16 capacity residual 已通过 BUG-478 顶层暴露，不再是 clean success 误判。
- 旧 `qmt_strategy.order_batch` 中仍有历史 `PARTIAL/FAILED/PREFLIGHT_FAILED` 记录，但不是当日 active run，且 broker open-like count=0；不作为今日开盘阻断。

### LocalSim

无新增代码层面的 P0 阻断，但有 09:10 观察点：

- 2026-06-23 还没有 LocalSim binding/run，需由 scheduler roll-forward。
- 2026-06-22 LocalSim L2 成功，LocalSim L16 没有 run；BUG-484 已修复 pre-run 失败 durable audit，因此若今天 L16 或任一 LocalSim binding pre-run 失败，预期不再“消失式跳过”，应生成可查 `FAILED_RETRYABLE` + `pre_run_failure.reason_code`。
- `market.kline_daily_raw` 最大日期为 `2026-06-22`，盘前没有 `2026-06-23` DB 日线；LocalSim same-day policy 是 `TDX_REALTIME`，这是正确预期。
- `market.stock_st` 表存在，行数 327417；ST 数据源不是缺表状态。

## 09:10/09:20/09:25 观察清单

1. 09:10 后只读查 `/api/v1/simulation-runtime/runs?trade_date=2026-06-23&limit=200`，应看到 MiniQMT L2、MiniQMT L16、LocalSim L2 至少开始生成当日 run。
2. 若 L16 run status=`FAILED_RETRYABLE`，立即读取 run detail/DB `run_payload_json.pre_run_failure.reason_code`，这是 BUG-484 修复后的关键诊断字段。
3. 09:20 后确认 selection_evidence_id 与 execution_plan_id 开始出现。
4. 09:25 后确认 MiniQMT submit window 内 broker_called 与 qmt_order_id，且无新的 open-like order 残留。
5. 若 09:12 后仍然 run_count=0，优先怀疑 scheduler roll-forward/window execution 没有落库；不要手工启动服务，先登记/诊断。

## Go/No-Go

- Unified MiniQMT unattended：GO for market open observation。
- Unified LocalSim L2：GO for market open observation。
- LocalSim L16：WATCH；等待 BUG-484 durable audit 暴露真实 reason_code。
- Legacy Paper v2 session scheduler：NO-GO unless user explicitly re-enables/recover old FAILED portfolios。

## Production gates

- production_ddl_gate：noop。本次未执行 DDL；BUG-462 生产 DDL 已由 Tier2 执行，当前只读复查未变更。
- production_backend_dependency_gate：noop。本次未安装依赖、未启动/停止/重启服务。
- production_frontend_dependency_gate：noop。本次未改前端依赖。
- production DB：只读 SELECT，无写入。
- 服务操作：无启动、无停止、无重启。
