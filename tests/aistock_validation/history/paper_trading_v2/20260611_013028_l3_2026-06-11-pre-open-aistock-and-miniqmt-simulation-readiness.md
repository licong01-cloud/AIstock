# 2026-06-11 盘前 AIstock + MiniQMT 模拟盘准备验证

- Module: paper_trading_v2 / simulation_runtime / qmt_virtual_strategy_ledger
- Level: L3
- Date: 2026-06-11T01:30:28+08:00
- Git commit: 0f493210
- Operator: Codex / lc999
- Status: partial / pre-open blockers remain

## Scope

- 盘前复核后端重启后的生产 8001 状态、TDX 19080 状态、MiniQMT SIM 连接、统一 simulation runtime scheduler 状态。
- 复核 2026-06-11 的 AIstock LocalSim 与 MiniQMT 双策略包绑定、选股证据、执行计划、未提交订单状态。
- 仅执行只读 API/DB 查询和 MiniQMT managed order preview / preflight；未调用真实下单、撤单、重启、DDL。
- 上一阶段已在生产库创建 2026-06-11 的 4 个 simulation_release_binding / runtime release，本记录只复核其结果。

## Environment

- Backend: `http://127.0.0.1:8001`, `/api/v1/health` OK。
- TDX: `http://127.0.0.1:19080`, `/api/health` OK, `/api/server-status` connected=true。
- MiniQMT: `/api/v1/qmt/status` connected=true, mode=SIM, account_id=62266303, pid=122720。
- DB: PostgreSQL `aistock` read-only checks; no DDL。
- Git: `main...origin/main`, clean before validation record creation; HEAD `0f493210 chore(issue): close-sync BUG-210 after merge (#923)`。

## Matrix

| Case | Expected business result | Evidence | Result |
|---|---|---|---|
| Service health | Backend / TDX / MiniQMT all reachable after user restart | `/api/v1/health`, `/api/health`, `/api/server-status`, `/api/v1/qmt/status` | PASS |
| Scheduler state | Unified simulation runtime active, production provider ready, account_group_slots active | `/api/v1/simulation-runtime/scheduler/status` | PASS |
| 2026-06-11 bindings | LocalSim L16/L2 + MiniQMT L16/L2 all bound for today | DB `paper_v2.simulation_release_binding`, 4 active rows | PASS |
| Selection evidence | LocalSim and MiniQMT for same package produce identical alpha list | DB `selection.daily_selection_evidence`; L16 first15 identical, L2 all 8 identical; cutoff_date=2026-06-10 | PASS |
| Execution plans | 4 plans exist and have non-zero intents | `/api/v1/simulation-runtime/runs?trade_date=2026-06-11`; 4 runs status PLANNING_EXECUTION | PASS |
| No pre-open broker side effect | No 2026-06-11 paper orders/fills/events; MiniQMT cancelable orders 0 | DB `paper_v2.orders/fills/order_events/order_execution_state`; `/api/v1/qmt/orders?cancelable_only=true` | PASS |
| MiniQMT managed preflight L16 | SELL-first order sort works; no hard blocker before submit | Local local-script invoking `QmtManagedOrderService._batch_preflight` with fake broker positions | PARTIAL: 30 SELL + 2 BUY allowed, 28 BUY correctly deferred as SELL_PROCEEDS_REQUIRED |
| MiniQMT managed preflight L2 | Batch can submit without false success or cash overrun | Same batch preflight | FAIL/BLOCKER: 8 BUY hard-blocked by BATCH_INSUFFICIENT_CASH |
| Data readiness | Pre-open historical data uses 2026-06-10; same-day minute comes from TDX after open | DB `market.kline_daily_raw`, `market.kline_minute_raw`, TDX current probe | PASS for pre-open expectation; `stk_limit` 2026-06-11 pending until scheduled 09:10 |

## Key Evidence

### Runtime and plans

- 4 active 2026-06-11 runs: 2 `local_sim`, 2 `minqmt_sim`, all status `PLANNING_EXECUTION`.
- L16 package `pkg_378eb9c91e104c64935404e257e932ee`:
  - LocalSim evidence `dse_5512ee0f505f21a2`, MiniQMT evidence `dse_830e44adf88aa3d0`。
  - Both cutoff_date=`2026-06-10`, selected_count=50, first15 identical: `301273.SZ,301353.SZ,603989.SH,688448.SH,688479.SH,603303.SH,301097.SZ,688301.SH,301115.SZ,300976.SZ,301322.SZ,688368.SH,603373.SH,605018.SH,605128.SH`。
  - LocalSim plan intents=41 BUY; MiniQMT plan intents=60, BUY=30, SELL=30。
- L2 package `pkg_a2f53f3f2f3e4095a910b939464c35e6`:
  - LocalSim evidence `dse_5a943325ef17b34f`, MiniQMT evidence `dse_812e89926dabdf13`。
  - Both cutoff_date=`2026-06-10`, selected_count=8, selected list identical: `603678.SH,002484.SZ,600330.SH,002969.SZ,300959.SZ,301013.SZ,600584.SH,002995.SZ`。
  - LocalSim plan intents=8 BUY; MiniQMT plan intents=8 BUY, plus one trading-rule rejected decision for existing unsellable holding (`TPLUS1_AVAILABLE_ZERO`)。

### MiniQMT preflight

- Public execution-plan preview returned individual preflights all allowed for L16/L2, but this endpoint does not run batch aggregate cash validation.
- Direct batch preflight through `QmtManagedOrderService._batch_preflight` with fake broker positions confirmed actual submit semantics:
  - L16 `plan_4a25c15fe123d1b0`: requests=60, sell_count=30, buy_count=30, sell_before_buy_order=true, allowed=32, dependent_buy_deferred=28, hard_blocked=0, error_codes={`SELL_PROCEEDS_REQUIRED`: 28}。
  - L2 `plan_ed37761a0bba02f3`: requests=8, sell_count=0, buy_count=8, allowed=0, dependent_buy_deferred=0, hard_blocked=8, error_codes={`BATCH_INSUFFICIENT_CASH`: 8}。
- L2 blocker details: virtual strategy cash=447106.86, no same-batch sell proceeds, buy freeze total=450452.00; existing holding sell was rejected by T+1 availability, so the allocator produced buy intents that exceed immediately usable cash by about 3345.14。

### Data readiness

- `market.trading_calendar`: 2026-06-11 is trading day。
- `market.kline_daily_raw`: latest trade_date=2026-06-10, rows_0610=5526, rows_0611=0; expected before open/盘后同步模型。
- `market.kline_minute_raw`: latest trade_time=2026-06-10 15:00:00+08:00, rows_0610=1325762, rows_0611=0; expected before open, same-day minute must come from TDX。
- TDX `/api/kline-all/tdx?code=SZ000001&type=minute1`: latest bar currently 2026-06-10 15:00:00+08:00; expected before 09:30。
- `market.suspend_d`: 2026-06-11 rows=7, audit success。
- `market.stk_limit`: latest=2026-06-10, rows_0611=0; schedule has `stk_limit incremental` at 09:10 and must be rechecked before execution window。

## Commands

```powershell
# Health and runtime status
python - <<'PY'
# used urllib against /api/v1/health, /api/v1/simulation-runtime/scheduler/status,
# /api/v1/simulation-runtime/runs?trade_date=2026-06-11&limit=50,
# /api/v1/qmt/status, /api/v1/qmt/orders?cancelable_only=true
PY

# DB state checks
$env:TDX_DB_PASSWORD='***'
python - <<'PY'
# queried paper_v2.simulation_release_binding, paper_v2.simulation_daily_run,
# selection.daily_selection_evidence, paper_v2.execution_plan,
# market.trading_calendar/kline_daily_raw/kline_minute_raw/suspend_d/stk_limit
PY

# TDX smoke
python - <<'PY'
# called http://127.0.0.1:19080/api/health,
# http://127.0.0.1:19080/api/server-status,
# http://127.0.0.1:19080/api/kline-all/tdx?code=SZ000001&type=minute1
PY

# MiniQMT preview and batch preflight without broker submit
python - <<'PY'
# called /api/v1/qmt/virtual-strategies/execution-plans/{plan_id}/orders/preview
# then locally ran QmtManagedOrderService._batch_preflight with fake broker positions
PY
```

## Failures And Fixes

| Failure | Root cause | Fix | Rerun evidence |
|---|---|---|---|
| MiniQMT L2 hard preflight block | Existing holding sell is unavailable under T+1; generated 8 BUY intents require 450452.00 cash while virtual cash is 447106.86 | Not fixed in this run. Needs cash-fit / sizing adjustment or operator-approved strategy capital/cash change. Do not mark submit success if broker is not called. | Batch preflight reports 8 `BATCH_INSUFFICIENT_CASH`; no broker call made |
| `stk_limit` 2026-06-11 not loaded before 09:10 | Current project semantics: `stk_limit` incremental schedule is 09:10; DB has prior trade day before then | No action before scheduled window. Must recheck after 09:10; LocalSim same-day execution requires `stk_limit` rows. | Schedule row exists: `stk_limit incremental`, next_run_at=2026-06-11 09:10:00+08:00 |

## Result

- Final status: PARTIAL readiness.
- Ready: backend/TDX/MiniQMT connectivity, 4 simulation bindings, 4 selection evidences, 4 execution plans, alpha consistency between AIstock and MiniQMT for the same package, no pre-open orders/fills/cancelable orders.
- Blocking before unattended green validation: MiniQMT L2 batch preflight hard-fails due cash sizing after T+1 sell rejection.
- Time-gated check before execution: verify `stk_limit` 2026-06-11 loaded after 09:10; verify TDX same-day minute bars after open.
- Need production backend restart: no.
- Need frontend restart: no.
- production_ddl_gate: noop.
- production_frontend_dependency_gate: noop.
- production_backend_dependency_gate: noop.
