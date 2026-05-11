# Paper v2 数据采集缺口与补齐建议 (2026-05-10)

> 状态：审计 + 待办清单，仅文档，无代码改动。
> 输入：本目录 `paper_v2_data_capture_audit_20260510.md`（A1）+ 本人对 `backend/services/paper_trading_v2/` 与 `daemon/` 的代码读取。

## §1 与 A1 的关系

A1（`paper_v2_data_capture_audit_20260510.md`）按 schema → 代码方向逐字段审计了 `paper_v2.*` 21 张表，结论是 schema 内部基本健康（无死字段，always-written 占绝大多数）。但 A1 只能审计"schema 已经定义出来的列"。

A2 关注 schema **以外** 的两类捕获缺口：

1. **§2**：服务/Broker/算法代码里实际"发生了"但没有任何 `paper_v2.*` 行承载的运行时事件。
2. **§3**：数仓建模需要、但 schema 当前没有暴露出来的事实/维度（即将来 fact_paper_* 表必须有、目前 paper_v2 不存的列或表）。

每条缺口给出补齐方案 + 紧迫度（BLOCKING / DEFERRED / NICE-TO-HAVE）。

## §2 应记录但未记录的运行时事件

### 2.1 Daemon SQLite 旁路：9 类事件不入 PG

**最大缺口**。`backend/services/paper_trading_v2/daemon/event_log.py:1-98` 把 daemon-driven 模拟运行的事件落到 **worktree-local SQLite**（默认 `var/paper_v2_sim/daemon_events.db`，gitignored），不进 paper_v2 schema。

| 事件类型 | 当前位置 | 当前流向 | 补齐方案 | 紧迫度 |
|----------|----------|----------|----------|--------|
| `RUN_STARTED` | `daemon/sim_runner.py:118-119` | SQLite | live/replay 路径已用 `save_run_event` 写 PG（`live_session.py:161` 等）。daemon 路径需镜像调用，或用 PG 表 `paper_v2.daemon_event_log` | DEFERRED |
| `INTENT_CREATED` | `daemon/sim_runner.py:201-202` | SQLite | 同上：增 `paper_v2.daemon_event_log` 或扩 `paper_v2.run_events` 加 `event_seq` PK | DEFERRED |
| `ORDER_SUBMITTED` | `daemon/sim_runner.py:251-252` | SQLite | 同上 | DEFERRED |
| `FILL_RECEIVED` | `daemon/sim_runner.py:135-136` | SQLite | 与 `paper_v2.order_events FILL` 重复语义；daemon 路径需走 `repository.save_order_event` | DEFERRED |
| `ORDER_REJECTED` | `daemon/sim_runner.py:217-218` / `230-231` | SQLite + 抛 `BrokerRejectedError` | live 路径靠上层 catch 后 `save_error`，daemon 路径只到 SQLite。需让 daemon 也写 `paper_v2.errors` + `paper_v2.order_events(event_type='REJECTED', reason, metadata.cause_code)` | **BLOCKING**（DW 侧"被拒订单率"是核心 KPI） |
| `ORDER_CANCELLED` | `daemon/sim_runner.py` (DaemonEventType.ORDER_CANCELLED) | SQLite | 同 ORDER_REJECTED | DEFERRED |
| `POSITION_UPDATED` | `daemon/sim_runner.py:154-155` | SQLite | live 路径用 `save_intraday_snapshot` + `save_positions`；daemon 路径需镜像 | DEFERRED |
| `RUN_COMPLETED` | `daemon/sim_runner.py:167-168` | SQLite | live 路径靠 `update_run_status`；daemon 路径需镜像 | DEFERRED |
| `RUN_FAILED` | `daemon/sim_runner.py:178-179` | SQLite | 同上，错误也需写 `paper_v2.errors` | **BLOCKING**（失败原因 DW 必须可见） |

**整体结论**：daemon path 是 phase-2 演示路径（注释 `event_log.py:3-7` 显式说"production PG migration requires user approval"），与 live/replay 主线流路径并列。短期 DW 设计里要么忽略 daemon path、要么必须把这层 SQLite 升级到 PG。建议在 B1 ETL 设计文档里明确"daemon SQLite 不被 ETL"，避免双源 ground truth。

### 2.2 Broker 内部 in-memory 状态：被拒订单细节

`broker/localsim.py:236-269` / `277-307` 在订单被算法层（ExecutionAlgoError/RiskRuleError）或账本层（RiskRuleError/TradingCoreError）拒绝时：

- 在内存 `_records[handle_id]` 里存 `OrderHandleStatus(state="rejected", rejection_reason=..., last_event_at=...)`；
- 把 `intent_index[intent_id]` 指向这个 rejection handle；
- 抛 `BrokerRejectedError` with rich `context={"intent_id", "handle_id", "symbol", "side", "quantity", "cause", "cause_code"}`。

**当前流向**：

- live/replay path：`day_runner.py` / `live_session.py` catch `BrokerRejectedError` → 走 `save_error` 与 `save_run_event`（已 cite，audit doc §8 验证）。`paper_v2.errors.context` 与 `paper_v2.run_events.context` 收纳了 cause_code。✅
- daemon path：仅 SQLite（详 §2.1）。✗

| 事件 | 当前位置 | 当前流向 | 补齐方案 | 紧迫度 |
|------|----------|----------|----------|--------|
| 算法层 reject 详细原因 | `broker/localsim.py:266-268` | live: `paper_v2.errors.context.cause_code`；daemon: SQLite | 把 `cause` / `cause_code` 写进 `paper_v2.order_events.metadata`（事件类型 = `REJECTED_BY_ALGO`）而不仅仅 `errors` 表，方便 ETL JOIN order_id | NICE-TO-HAVE |
| 账本层 reject（如 cash 不足） | `broker/localsim.py:299-307` | 同上 | 同上，事件类型 = `REJECTED_BY_LEDGER` | NICE-TO-HAVE |
| 内存 `_records` 字典 | `broker/localsim.py:316-322` | 进程内存，不入库 | 不需要：所有用户可见状态都已透过 callback / OrderHandleStatus 落到 `orders` / `order_events` | no-op |

### 2.3 Phase / catchup 切换事件

`live_session.py:113-184` 的 CATCHUP_THEN_LIVE 模式已经在状态机切换点显式 emit `paper_v2.session_events`：`SESSION_CATCHUP_REPLAY_STARTED` / `SESSION_CATCHUP_REPLAY_SUCCEEDED` 等（已 cite）。

| 事件 | 当前位置 | 流向 | 补齐方案 | 紧迫度 |
|------|----------|------|----------|--------|
| HISTORICAL_REPLAY → CATCHING_UP → LIVE 阶段切换 | `live_session.py:152, 183` | `paper_v2.session_events`（已落库） | 已覆盖 | no-op |
| 历史 replay 失败导致 phase 回退 | 同上 | 同上 | 已覆盖 | no-op |
| 实时 LIVE 阶段 bar 流中断/恢复 | （未发现专用事件） | logger.* 也几乎不存在（Grep paper_trading_v2 内 `logger\.(info|warning|error)` 仅 scheduler.py 1 处） | 增 session-level 事件类型 `LIVE_BAR_STREAM_STALLED` / `LIVE_BAR_STREAM_RECOVERED`，写 `paper_v2.session_events`，含最近收到 bar_time 与中断时长 | NICE-TO-HAVE |

### 2.4 数据源失效 / 切换事件

`market_data.py` 是分钟数据接入层。Grep 只发现 `data_source` 在第 710 行作 source.value 字符串使用；未见 `failover` / `switch` 关键词，也无显式 logger 调用。

| 事件 | 现状 | 补齐方案 | 紧迫度 |
|------|------|----------|--------|
| TDX_REALTIME 拉取失败回退到 DB_HISTORICAL（如有） | 当前 schema 不允许同一 portfolio 跨 source 切换（D1 联合 CHECK 锁定 broker × source 组合），所以 fallback 不会发生 | 不需要补；属设计禁止 | no-op |
| MINIQMT_REALTIME 心跳丢失 | minqmt_sim broker 还未实装（D1 注释明确） | 实装时同步设计 `paper_v2.broker_event` 表（heartbeat / reconnect 事件） | DEFERRED |

### 2.5 算法状态机转换内部步骤

`order_execution_state.algo_state_json` 是 schemaless JSONB，承载算法（如 V24/V25/V26 minute_execution）的所有内部状态。

| 事件 | 现状 | 补齐方案 | 紧迫度 |
|------|------|----------|--------|
| 算法每个 minute bar 决策（吃单 / 等待 / 撤单） | 仅最终状态写 `algo_state_json`，每步决策不持久化 | UPSERT 路径已 OK（每 bar 一次 UPDATE）；如果 DW 需要"每 bar 决策序列"，应增 `paper_v2.algo_step` 表（order_id + bar_time + decision + reason） | NICE-TO-HAVE |
| parent intent → child orders 派生关系 | `orders.intent_id` 存 parent，但同一 intent 派生多 child order 时缺 parent_order_id 列 | 增 `orders.parent_order_id TEXT NULL`（自引用）；或在 metadata 里规范键 | DEFERRED |

### 2.6 Intent → fill 延迟、价格滑点

下面这两类指标在 broker / engine 层是**可计算的**（intent.timestamp、fill.trade_time 都有），但当前没有持久化。

| 事件 | 现状 | 补齐方案 | 紧迫度 |
|------|------|----------|--------|
| Intent 提交时刻 → 首个 fill 时刻 latency | broker 内有 OrderHandle.submitted_at；fills 表有 trade_time；但 intent 的提交时刻没单独存（intent_id 是 PK，无时间戳列） | 在 `paper_v2.orders.metadata` 加 `intent_submitted_at` 键，或增 `intent_submitted_at TIMESTAMPTZ` 列 | NICE-TO-HAVE |
| 意向价（algo target price）vs 实际 fill price 的滑点 | 当前完全不持久化 | 在 `paper_v2.fills` 加 `intended_price DOUBLE PRECISION NULL` 列；algo 在 emit fill 时填 | **BLOCKING**（DW 侧"执行质量"维度必备） |
| 模拟 fill price vs 真实分钟 bar 价格对照 | 仅 `bar_time` 存在；真实 bar 收/低/高/VWAP 在 fill 行不存 | 在 `paper_v2.fills.metadata` 规范键 `bar_open` / `bar_close` / `bar_vwap`；或新增 `paper_v2.fill_market_context` 表（fill_id PK, bar_open, bar_close, bar_high, bar_low, bar_vwap, suspend_d, limit_state） | **BLOCKING**（DW 侧"模拟 vs 真实"对照样本） |

## §3 数仓建模需要但 schema 未暴露的维度

视角：假设要建一张 `dw.fact_paper_fill`，每行一笔 fill；它需要哪些字段？

| 维度 | 数仓使用场景 | 补齐方案 | 紧迫度 |
|------|--------------|----------|--------|
| **broker_backend**（fill 发生时所属 broker） | 区分 local_sim vs minqmt_sim 的成交质量 | A1 §7：portfolio 已 covered；但 `paper_v2.fills` 不存 broker_backend，DW 必须 JOIN `paper_v2.run` → `paper_v2.portfolio` 间接拿。建议 ETL 时直接 JOIN 解决（不改 schema） | NICE-TO-HAVE |
| **意向价（intended_price）/ 滑点（slippage_bps）** | 评估 V2x 算法执行质量 | §2.6：增 `fills.intended_price` 列 + `metadata.slippage_bps` 计算键 | **BLOCKING** |
| **实际市场 bar 上下文（OHLC + VWAP）** | 模拟成交 vs 真实成交对照样本 | §2.6：新表 `paper_v2.fill_market_context` | **BLOCKING** |
| **suspend_d / limit_state 在成交时刻** | 排除停牌、涨跌停极端样本 | 同上：把 suspend_d / limit_up / limit_down 标志位写入 `fill_market_context` | **BLOCKING** |
| **市场 regime label**（Trend / MR / High-Vol etc.） | 按制度切片"执行质量" | DW 侧 ETL 时 JOIN regime 数据集生成；不改 paper_v2 schema | DEFERRED |
| **parent → child order 派生链** | 算法树状执行可见 | §2.5：`orders.parent_order_id` 自引用列 | DEFERRED |
| **算法层 sub-algo 序列**（V24 子算法、二级决策） | 评估二级决策路径分布 | §2.5：增 `paper_v2.algo_step` 表 | NICE-TO-HAVE |
| **fee 拆分**（commission / stamp_duty / transfer_fee） | 费用归因 | `cash_ledger.fee` 当前是合计；拆分需 metadata 规范键或新 fee 列 | DEFERRED |
| **Intent latency（intent.created_at → first fill）** | 延迟分布 | §2.6：fills 加列 / intent metadata 时间戳 | NICE-TO-HAVE |
| **Minute-data freshness at fill time**（数据流多新） | 质量监控 | 用 session_day.last_processed_bar_time + fill.bar_time 算差；不改 schema | NICE-TO-HAVE |
| **portfolio_status @ fill_time** | 历史诊断（fill 发生时组合是 RUNNING/PAUSED） | portfolio.status 当前只存"当前值"，需 audit 表查询；DW 可 JOIN `config_change_audit` 还原 | DEFERRED |
| **broker_event（heartbeat/reconnect）维度** | minqmt_sim 上线后必备 | §2.4：新表 `paper_v2.broker_event` | DEFERRED |

## §4 补齐方案分组汇总

### 4.1 BLOCKING（必须补，否则 ETL 不可行）

1. **`fills.intended_price`** + 滑点 metadata 键（§2.6 + §3）。无此字段 fact_paper_fill 缺执行质量主维。
2. **`paper_v2.fill_market_context`** 新表 / 或在 `fills.metadata` 规范化以下键：`bar_open` / `bar_high` / `bar_low` / `bar_close` / `bar_vwap` / `suspend_d` / `limit_state`（§2.6 + §3）。无此 fact 表无法做"模拟 vs 真实"对照。
3. **Daemon path 的 `ORDER_REJECTED` / `RUN_FAILED` 写 PG**（§2.1）。否则 daemon 跑出来的失败原因 DW 完全看不到（仅 worktree SQLite 可见）。
4. **明确 daemon SQLite 是否进 ETL 范围**（B1 文档输入，§2.1）。两种合规方案：(a) ETL 不消费 daemon SQLite；(b) 把 daemon path 升级写 PG（需 user-approved DB migration，按 handoff §6 P0 走）。

### 4.2 DEFERRED（等 DW schema 定型后再决定）

- §2.1 其余 daemon 事件类型（INTENT_CREATED / ORDER_SUBMITTED / FILL_RECEIVED / POSITION_UPDATED / RUN_STARTED / RUN_COMPLETED / ORDER_CANCELLED）入 PG。
- §2.4 / §2.5 minqmt_sim 心跳事件 / parent_order_id 自引用列。
- §3 fee 拆分、portfolio_status 时序。
- §3 market regime label（DW 侧 ETL 解决）。

### 4.3 NICE-TO-HAVE（锦上添花）

- §2.2 `paper_v2.order_events.metadata` 规范化 reject cause_code 键（重复 `errors.context`，但便于 join order_id）。
- §2.3 LIVE bar stream 中断/恢复事件。
- §2.5 `paper_v2.algo_step` 表（每 bar 决策序列）。
- §2.6 intent latency 持久化。
- §3 fill 行直接冗余 broker_backend（避免 JOIN）。

## §5 与 B1 ETL 设计的衔接点

B1 设计 ETL 模式 X 之前必须先决定的输入：

1. **daemon SQLite 是否进 ETL 范围**：选 (a) 排除 — 简化设计，但 DW 看不到 daemon 跑的样本；选 (b) 升级写 PG — 需用户批准 DB migration。建议 B1 文档显式记录这个抉择。**§4.1 的 #4。**
2. **fact_paper_fill 是否需要执行质量字段**：是 → 必须先补 `fills.intended_price` 与 `fill_market_context`（§4.1 #1, #2）。否 → 推迟到 v2 ETL 迭代。
3. **D1 broker_backend 字段是否在 fact 上冗余**：建议 fact_paper_fill 行级冗余 `broker_backend` 列以避免 3 跳 JOIN（fills → run → portfolio）。该字段已 covered（A1 §7.3），ETL 直接 JOIN 取即可。**§3 的 broker_backend 行**。
4. **portfolio_status 历史还原**：fact_paper_run 是否带 portfolio_status 当时值？若需，则 ETL 需读 `config_change_audit` 时序还原；若否，仅记录 run_status 即可。
5. **fee 是否拆分**：当前 `cash_ledger.fee` 是合计。若 DW 要按 commission/stamp_duty 切片，必须先补 §4.2 的拆分。

完成这些抉择后，B1 才能开始定义 fact 表 / 维表 / ETL 触发器。
