# Paper v2 → 数仓 ETL 接口设计草案

> 状态：设计草案。仅设计文档，不含 DDL，不含代码。等用户审过 + 数仓 schema 定型后再产出 PR。
> 配套文档：A1 (`paper_v2_data_capture_audit_20260510.md`) + A2 (`paper_v2_capture_gaps_20260510.md`) — 由 cross-test 同批次产出
> Created: 2026-05-10. Branch: `claude/paper-v2-vnpy-mvp-20260508`

---

## §1 背景与设计前提

**核心定位**：

- `paper_v2.*` 是**短期运行层**。用户已口头明确：模拟盘运行数据未来不长期持久化保存，会按窗口清理（窗口长度待定，见 §8 Q2）。
- 数仓（schema 名待定，见 §8 Q1）是**永久保存层**，承载策略实盘演进、风险归因、长周期回测对照所需的"已发生事实"。

**设计目标**：

1. 必须在 `paper_v2` 数据被清理之前，把"具备永久价值的事件"完整抽走；瞬时运行态可以丢弃。
2. **允许冗余**：在过渡期（数仓上线 → paper_v2 清理周期生效），同一条事件可同时存在于两侧；以数仓侧为最终历史真源（single source of truth for history）。
3. **零业务侵入**：本 ETL 不改 `paper_v2` 业务逻辑，不引入对 `backend/services/paper_trading_v2/` service 层的反向依赖；service 层不感知数仓存在。
4. **失败隔离**：ETL 任何一次失败、回放、回退都不能影响 paper_v2 关键路径（撮合 / 下单 / 当日 NAV 计算）。

**约束（来自 broker_backend 上下文）**：`paper_v2.portfolio.broker_backend ∈ {local_sim, minqmt_sim}`（`backend/db/init_trading_core_v2_schema.py:274`、`backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql:11`）。两类 broker 写入同一组 paper_v2 表，因此数仓事实表无需按 broker 分表，仅通过维度列区分。

---

## §2 ETL 模式选择（A / B / C 评估）

### 模式 A — 每日定时增量（cron + watermark）

- **机制**：调度器（airflow / cron / 已有的 backend scheduler）按固定窗口（建议 T+1 收盘后或每小时）对每张源表用 watermark 字段拉取新增/更新行，写入数仓 staging → 合并到正式表。
- **watermark 字段**：优先 `updated_at`（`paper_v2.run`、`paper_v2.trade_session`、`paper_v2.session_day`、`paper_v2.order_execution_state`、`paper_v2.portfolio` 均有：`backend/db/init_trading_core_v2_schema.py:281,397,417,438,487`）；append-only 表用领域时间字段（`fills.trade_time` 511、`daily_snapshots.snapshot_time` 559、`order_events.event_time` 496）。
- **延迟**：分钟～小时级，可调；满足"事后归档"语义。
- **实现成本**：低。无需触发器、无需逻辑复制。新增一个 ETL 进程 + 一张 watermark 状态表即可。
- **失败恢复**：天然支持。watermark 回拨即可幂等重跑（前提：目标表用源表自然键 upsert，§6 详述）。
- **耦合度**：极低。ETL 仅"读"paper_v2，paper_v2 完全不知道数仓存在。
- **适合场景**：T+1 归档、跑批分析、长期监控指标聚合。

### 模式 B — CDC（监听写入事件）

- **机制**：三种实现路径
  1. **PG logical replication**：开启 `wal_level=logical`，数仓侧用 publication/subscription 或 Debezium 接入；近实时（秒级）。
  2. **触发器 + 事件表**：每张关键源表加 `AFTER INSERT/UPDATE` trigger，写入 `paper_v2._cdc_events` 中转表，ETL 进程消费。
  3. **Outbox**：service 层在事务内同时写业务表 + outbox 表（介于 B 和 C 之间）。
- **延迟**：准实时（秒～分钟）。
- **实现成本**：高。logical replication 涉及 wal slot 运维（slot 不消费会导致 WAL 堆积撑爆磁盘）；trigger 方案侵入 schema、影响写入性能；Debezium 需独立服务。
- **失败恢复**：复杂。replication slot lag、trigger 表膨胀都需独立监控。
- **耦合度**：方案 1 零侵入；方案 2 中等侵入（schema 加 trigger）；方案 3 强侵入（service 写双路径）。
- **适合场景**：需要近实时仪表板 / 风控告警的场景。本期目标（永久归档）用不上这种实时性。

### 模式 C — 双写（service 层并写 dw_staging）

- **机制**：在 `repository.py` 写 fill / daily_snapshot 等关键路径处，事务结束后在同一连接或新事务里再写一份到数仓 staging 表。
- **延迟**：实时。
- **实现成本**：中。但每张目标表都要改 service 层。
- **耦合度**：**最高**。service 层必须知道数仓存在；新增数仓字段要同步改 service；任何数仓侧 schema 漂移都会反馈到 paper_v2 关键路径。
- **失败恢复**：**最差**。一致性风险：业务事务成功 + 数仓写失败时，要么吞错（破坏数据完整性，违反"No Silent Errors" 偏好）、要么回滚业务事务（让模拟盘下单失败，影响关键路径）。
- **性能影响**：每次 fill / snapshot 多一次写，分钟级 paper_v2 撮合路径会被放大。
- **适合场景**：极少数需要"业务和分析强一致"的场景。本期不适用。

### 矩阵打分

| 模式 | 实现成本 | 延迟 | 与 paper_v2 耦合 | 失败恢复 | 推荐度 |
|---|---|---|---|---|---|
| A — cron + watermark | 低 | 分钟～小时 | 极低 | 优秀（幂等回放） | ★★★★★ |
| B1 — logical replication | 高 | 秒级 | 极低 | 中（slot 运维） | ★★ |
| B2 — trigger + 中转表 | 中 | 秒级 | 中（schema 侵入） | 中 | ★★ |
| B3 — outbox | 中 | 秒级 | 高（service 改） | 中 | ★ |
| C — 双写 | 中 | 实时 | 极高 | 差（一致性） | ☆ |

### 推荐（engine-design）

**主推：模式 A（拉式 + watermark）**。
理由：(1) paper_v2 是短期运行层，目标只是"事件被清理前抽走"，无需实时；(2) 用户允许冗余，T+1 滞后完全可接受；(3) A 的零侵入特性满足"不影响 paper_v2 关键路径"的硬约束；(4) 失败重跑通过 watermark 回拨天然幂等，运维心智负担最小。

**补充**：若未来某类事件出现"必须秒级到达数仓"的需求（例如风控告警 / 合规审计），可对该单一表局部启用 outbox 补丁；不必整体切换模式。当前阶段不预先实现。

---

## §3 表映射草案（grain 必须明确）

下列映射覆盖"具备永久价值"的 paper_v2 表。源行号引用 `backend/db/init_trading_core_v2_schema.py`。

### 3.1 `paper_v2.run` → `dw.fact_paper_run`

- **源表**：line 369-381。`UNIQUE(portfolio_id, trade_date)`。
- **grain**：`portfolio_id × trade_date`（即一个组合一日一行）。
- **partition 键**：`trade_date`（按月或按季 partition；高频写入 + 长保留场景下推荐按月）。
- **watermark**：`COALESCE(completed_at, started_at)`，二者均 TIMESTAMPTZ；ETL 比较用 `started_at`（NOT NULL，line 376）+ 单独维护一个 "未完成 run" 重抽列表。
- **SCD 类型**：fact（每行不可变；status / completed_at / error_json 可被覆写时使用 upsert by `run_id`）。
- **保留**：永久。

### 3.2 `paper_v2.trade_session` → `dw.fact_paper_session`

- **源表**：line 383-401。
- **grain**：`session_id`（一次模拟会话一行；REPLAY_ONLY / LIVE_ONLY / CATCHUP_THEN_LIVE 三种 mode）。
- **partition 键**：`start_date`（按月）。
- **watermark**：`updated_at`（NOT NULL，line 397）。
- **SCD 类型**：fact，但 status / phase / completed_at 会被多次更新；用 upsert by `session_id`。
- **保留**：永久。

### 3.3 `paper_v2.fills` → `dw.fact_paper_fill`

- **源表**：line 503-515。append-only。
- **grain**：`fill_id`（每笔成交一行；一个 order 可拆多笔 fill）。
- **partition 键**：`DATE(trade_time)`（按月 partition）。
- **watermark**：`trade_time`（NOT NULL，line 511）。**注意**：source 缺 `updated_at` / `created_at` 字段，ETL 必须用 `trade_time` 作 watermark；A2 应记录这一点（建议补 `created_at`，否则乱序 trade_time 数据会丢）。
- **SCD 类型**：纯 fact（append-only，永不更新）。
- **保留**：永久；这是策略归因的最关键事实表。

### 3.4 `paper_v2.daily_snapshots` → `dw.fact_paper_daily`

- **源表**：line 550-562。`UNIQUE(portfolio_id, trade_date)`。
- **grain**：`portfolio_id × trade_date`。
- **partition 键**：`trade_date`（按月）。
- **watermark**：`snapshot_time`（NOT NULL，line 559）。**注意**：source 缺 `created_at`/`updated_at`，与 fills 同问题；A2 应标记。
- **SCD 类型**：fact；同一 (portfolio, trade_date) 在重跑时 upsert。
- **保留**：永久；NAV / 持仓数 / 现金的日级时序基础。

### 3.5 `paper_v2.order_execution_state` → `dw.fact_paper_order_lifecycle`

- **源表**：line 422-440。`UNIQUE(order_id)`。
- **grain**：`order_id × terminal_state`（建议 ETL 仅在 `status` 进入终态 {FILLED, CANCELLED, REJECTED, EXPIRED} 后再写数仓；运行中的中间 status 不抽）。
- **partition 键**：`trade_date`（按月）。
- **watermark**：`updated_at`（NOT NULL，line 438）。
- **SCD 类型**：fact；用 `order_id` upsert；终态后不再变更，符合 fact 语义。
- **保留**：永久。

### 3.6 `paper_v2.session_day` → `dw.fact_paper_session_day`

- **源表**：line 404-419。`UNIQUE(session_id, trade_date)`。
- **grain**：`session_id × trade_date`（一个 session 跨多个 trade_date 各一行；提供 session 内日级粒度的聚合与"哪天补哪天活"诊断）。
- **partition 键**：`trade_date`（按月）。
- **watermark**：`updated_at`（NOT NULL，line 417）。
- **SCD 类型**：fact，upsert by `session_day_id`。
- **保留**：永久。

### 3.7 `paper_v2.portfolio`（含 manifest_sha256 / fee_policy / risk_policy 快照） → `dw.dim_paper_portfolio_version`（SCD2）

- **源表**：line 265-287。`portfolio_id` PK。
- **grain**：`portfolio_id × version_no`（每次配置/策略变更产生一个新版本行，valid_from / valid_to 区间对齐）。
- **partition 键**：无（维度表通常不分区；体量小）。
- **watermark**：`updated_at`（NOT NULL，line 281） + `manifest_sha256` / `fee_policy` / `risk_policy` / `execution_policy` / `broker_backend` 任一字段变化即触发新版本。
- **SCD 类型**：**SCD2**。每个 portfolio 永久保留所有历史配置版本；事实表通过 `portfolio_id + trade_date` 关联到当时有效的版本行。
- **保留**：永久。这是事后归因"用了什么策略"的关键。

### 3.8 候选补充表（A1 揭示 paper_v2 还有哪些表的话）

A1 / A2 跑出后，若有以下源表被列入"长期价值"，本节将补对应映射：

- `paper_v2.cash_ledger`（line 518-532）→ 候选 `dw.fact_paper_cash_event`。grain = `cash_id`，append-only。watermark = `created_at`。
- `paper_v2.run_events`（line 565-572）+ `paper_v2.session_events`（line 461-468）→ 是否进数仓取决于"事件流回放"是否纳入归档目标（见 §8 Q4）。
- `paper_v2.intraday_snapshots`（line 443-457）→ 体量大、价值低（盘中分钟级 NAV），默认**不进数仓**；仅在用户特别要求"分钟级回放"时按需归档。
- `paper_v2.config_change_audit`（line 350-366）→ 候选 `dw.fact_paper_config_change`。grain = `audit_id`，append-only；与 §3.7 SCD2 互补：SCD2 给"配置当前是什么样"，audit 给"谁改的、为什么改"。

---

## §4 冗余策略（明确每类数据的 single source of truth）

### 4.1 仅 paper_v2，不进数仓（瞬时运行态）

- `paper_v2.order_execution_state.algo_state_json`（line 430）— 算法运行时中间状态，频繁变更、终态后无价值。
- `paper_v2.session_day.last_processed_bar_time` / `latest_available_bar_time`（line 414-415）— 实时心跳，重启即作废。
- `paper_v2.intraday_snapshots`（默认）— 体量大、价值低（见 §3.8）。
- `paper_v2.errors`（line 575-583）— 短期诊断；超过保留窗口可丢，不进数仓。

### 4.2 进数仓 + paper_v2 短期保留（终态事件 / 永久价值）

- `paper_v2.fills` → `dw.fact_paper_fill`（append-only，事实级真源）。
- `paper_v2.daily_snapshots` → `dw.fact_paper_daily`。
- `paper_v2.order_execution_state` 的**终态行** → `dw.fact_paper_order_lifecycle`。
- `paper_v2.run` / `trade_session` / `session_day` → 对应 fact 表。

**single source of truth (history)**：数仓侧。paper_v2 在保留窗口内可被查询，超窗后清理；任何"昨天 / 上周 / 上月 / 历史"查询应走数仓。

### 4.3 双侧记录（配置快照 / SCD2 维度）

- `paper_v2.portfolio`（manifest_sha256 / fee_policy / risk_policy / execution_policy / broker_backend / data_source）：
  - paper_v2 侧保留**当前活动版本**（PK 一行，由 service 持续覆写）。
  - 数仓 `dw.dim_paper_portfolio_version` 保留**所有历史版本**（SCD2）。
- `paper_v2.config_change_audit`（line 350-366）：append-only 已在 paper_v2 侧；ETL 同步抽到数仓后，paper_v2 侧可在保留窗口外清理。

**single source of truth (config now)**：paper_v2 侧。
**single source of truth (config history)**：数仓侧。
事实表的 `portfolio_id` 始终用 paper_v2 原始 ID（保持稳定，跨侧可 join），版本通过事实行 `trade_date` 在数仓 SCD2 区间内查找。

---

## §5 schema 占位（仅字段名 + 类型注释；不写真实 DDL）

> 本节为字段草案，DDL 在用户审过 + 数仓 schema 名定型 + 物理位置确定后再产出。

### 5.1 `dw.fact_paper_run`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 paper_v2 字段 |
|---|---|---|---|---|
| run_id | TEXT | Y | 主键 | `paper_v2.run.run_id` |
| portfolio_id | TEXT | Y | 关联维度 | `paper_v2.run.portfolio_id` |
| trade_date | DATE | Y | partition 键 | `paper_v2.run.trade_date` |
| status | TEXT | Y | 终态 status | `paper_v2.run.status` |
| data_source | TEXT | Y | TDX / DB / MINIQMT | `paper_v2.run.data_source` |
| broker_backend | VARCHAR(32) | Y | local_sim / minqmt_sim（join 自 dim） | 派生自 portfolio SCD2 当时版本 |
| runtime_config_sha256 | TEXT | N | 当时 runtime_config 哈希 | 派生自 `paper_v2.run.runtime_config` |
| started_at | TIMESTAMPTZ | Y | | `paper_v2.run.started_at` |
| completed_at | TIMESTAMPTZ | N | NULL = 异常未完成 | `paper_v2.run.completed_at` |
| error_code | TEXT | N | error_json 提取 | `paper_v2.run.error_json` |
| etl_loaded_at | TIMESTAMPTZ | Y | ETL 写入时间 | 派生 |
| etl_source_version | TEXT | Y | paper_v2 schema 版本 | 派生 |

### 5.2 `dw.fact_paper_session`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| session_id | TEXT | Y | 主键 | `trade_session.session_id` |
| portfolio_id | TEXT | Y | | `trade_session.portfolio_id` |
| mode | TEXT | Y | REPLAY/LIVE/CATCHUP | `trade_session.mode` |
| status | TEXT | Y | 终态 | `trade_session.status` |
| phase | TEXT | Y | | `trade_session.phase` |
| start_date | DATE | Y | | `trade_session.start_date` |
| end_date | DATE | N | NULL = 未结束 | `trade_session.end_date` |
| historical_data_source | TEXT | N | | line 391 |
| live_data_source | TEXT | N | | line 392 |
| created_by | TEXT | N | | line 395 |
| created_at | TIMESTAMPTZ | Y | | line 396 |
| started_at | TIMESTAMPTZ | N | | line 398 |
| completed_at | TIMESTAMPTZ | N | | line 399 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

### 5.3 `dw.fact_paper_fill`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| fill_id | TEXT | Y | 主键 | `fills.fill_id` |
| run_id | TEXT | Y | | `fills.run_id` |
| portfolio_id | TEXT | Y | join 自 run | 派生 |
| order_id | TEXT | Y | | `fills.order_id` |
| symbol | TEXT | Y | | `fills.symbol` |
| side | TEXT | Y | BUY/SELL | `fills.side` |
| quantity | INTEGER | Y | | `fills.quantity` |
| price | DOUBLE PRECISION | Y | | `fills.price` |
| trade_time | TIMESTAMPTZ | Y | partition 键派生源 | `fills.trade_time` |
| trade_date | DATE | Y | DATE(trade_time) | 派生 |
| bar_time | TIMESTAMPTZ | N | | `fills.bar_time` |
| reason | TEXT | Y | | `fills.reason` |
| broker_backend | VARCHAR(32) | Y | join 自 SCD2 | 派生 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

### 5.4 `dw.fact_paper_daily`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| portfolio_id | TEXT | Y | 复合 PK | `daily_snapshots.portfolio_id` |
| trade_date | DATE | Y | 复合 PK + partition | `daily_snapshots.trade_date` |
| run_id | TEXT | Y | | `daily_snapshots.run_id` |
| cash | DOUBLE PRECISION | Y | | `daily_snapshots.cash` |
| market_value | DOUBLE PRECISION | Y | | `daily_snapshots.market_value` |
| nav | DOUBLE PRECISION | Y | | `daily_snapshots.nav` |
| position_count | INTEGER | Y | | `daily_snapshots.position_count` |
| snapshot_time | TIMESTAMPTZ | Y | watermark | `daily_snapshots.snapshot_time` |
| portfolio_version_id | TEXT | Y | SCD2 版本指针 | 派生 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

### 5.5 `dw.fact_paper_order_lifecycle`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| execution_state_id | TEXT | Y | 主键 | `order_execution_state.execution_state_id` |
| order_id | TEXT | Y | unique | line 426 |
| run_id | TEXT | Y | | line 425 |
| session_id | TEXT | Y | | line 424 |
| symbol | TEXT | Y | | |
| trade_date | DATE | Y | partition | line 428 |
| algo_code | TEXT | Y | | line 429 |
| plan_sha256 | TEXT | N | | line 432 |
| filled_quantity | INTEGER | Y | 终态 | |
| remaining_quantity | INTEGER | Y | 终态 | |
| terminal_status | TEXT | Y | FILLED / CANCELLED / REJECTED / EXPIRED | line 436 |
| created_at | TIMESTAMPTZ | Y | | line 437 |
| updated_at | TIMESTAMPTZ | Y | watermark | line 438 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

### 5.6 `dw.fact_paper_session_day`（草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| session_day_id | TEXT | Y | 主键 | `session_day.session_day_id` |
| session_id | TEXT | Y | | line 406 |
| portfolio_id | TEXT | Y | | line 407 |
| trade_date | DATE | Y | partition | line 408 |
| run_id | TEXT | N | | line 409 |
| status | TEXT | Y | | line 410 |
| phase | TEXT | Y | | line 411 |
| data_source | TEXT | Y | | line 412 |
| expected_bar_count | INTEGER | N | | line 413 |
| created_at | TIMESTAMPTZ | Y | | line 416 |
| updated_at | TIMESTAMPTZ | Y | watermark | line 417 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

注：实时心跳字段 `latest_available_bar_time` / `last_processed_bar_time` 不抽（§4.1）。

### 5.7 `dw.dim_paper_portfolio_version`（SCD2 草案）

| 列名 | 类型 | NOT NULL | 注释 | 来源 |
|---|---|---|---|---|
| portfolio_version_id | TEXT | Y | 代理键（surrogate） | 派生（uuid） |
| portfolio_id | TEXT | Y | 业务键 | `portfolio.portfolio_id` |
| version_no | INTEGER | Y | 单调递增 | 派生 |
| portfolio_name | TEXT | Y | | line 267 |
| package_id | TEXT | Y | | line 268 |
| manifest_sha256 | TEXT | Y | | line 269 |
| frozen_manifest_json | JSONB | Y | | line 270 |
| initial_cash | NUMERIC(20,6) | Y | | line 271 |
| start_date | DATE | Y | | line 272 |
| data_source | TEXT | Y | | line 273 |
| broker_backend | VARCHAR(32) | Y | | line 274 |
| fee_policy | JSONB | Y | | line 276 |
| risk_policy | JSONB | Y | | line 277 |
| execution_policy | JSONB | Y | | line 278 |
| status | TEXT | Y | | line 279 |
| valid_from | TIMESTAMPTZ | Y | 此版本生效起点 | 派生（来自 portfolio.updated_at 或 config_change_audit） |
| valid_to | TIMESTAMPTZ | N | 此版本失效时点；NULL = 当前版本 | 派生 |
| is_current | BOOLEAN | Y | | 派生 |
| etl_loaded_at | TIMESTAMPTZ | Y | | 派生 |

> 本节为字段草案，DDL 在用户审过 + 数仓 schema 名定型 + 物理位置确定后再产出。

---

## §6 接口契约

### 6.1 拉式（Pull-based）

- ETL 进程归属**数仓侧**，由数仓侧调度器触发；paper_v2 service 层不主动调任何 ETL 函数 / 接口。
- 这一约定明确解耦：paper_v2 升级、停服、重启都不影响数仓 ETL（最多导致下个调度周期空跑或部分窗口数据延后）。

### 6.2 paper_v2 的承诺（contract from paper_v2 to DW）

paper_v2 仅承诺以下两点稳定，且变更必须走 deprecation flow（提前公告、保留兼容窗口）：

1. **表 schema 稳定**：§3 列出的 7 张源表的字段名 / 类型 / 主键 / unique 约束在不通知数仓侧的情况下不得删除或改语义。新增列允许（向前兼容）。
2. **watermark 字段齐全且 NOT NULL**：
   - `paper_v2.run.started_at`、`paper_v2.trade_session.updated_at`、`paper_v2.session_day.updated_at`、`paper_v2.order_execution_state.updated_at`、`paper_v2.portfolio.updated_at`、`paper_v2.fills.trade_time`、`paper_v2.daily_snapshots.snapshot_time` 必须 NOT NULL（已确认，引用见 §3）。
   - **A2 BLOCKING 项**：`paper_v2.fills` 缺 `created_at`、`paper_v2.daily_snapshots` 缺 `created_at` / `updated_at`、`paper_v2.positions`（line 534-547）缺 `created_at` / `updated_at`。这些缺口需在 ETL 上线前由 paper_v2 侧补齐（A2 文档列 BLOCKING 优先级）。

### 6.3 ETL 自身约定

- **幂等**：所有目标表用源自然键（fill_id / run_id / session_id / execution_state_id 等）作 upsert key；watermark 回拨重抽不会产生重复行。
- **失败重试**：ETL 工具（airflow / 自研 scheduler）负责；任何重试都不读写 paper_v2 业务表的非读路径。
- **失败隔离**：ETL 写错、回放、慢查询都仅影响数仓侧；不持有 paper_v2 长事务、不锁 paper_v2 表（pg_dump 风格的 REPEATABLE READ snapshot 也只在抽取阶段，且阻塞 < 数十秒）。
- **观测**：每次 ETL 跑产出一行运行 metadata（开始 / 结束 / 抽取行数 / watermark before/after / 错误），写入数仓自有的 `dw.etl_run_log`（草案，本期不展开）。

### 6.4 与 broker_backend 的关系

`broker_backend` 列在 paper_v2.portfolio 侧已是 immutable 设计（`backend/services/paper_trading_v2/` 设计 §3.6）。SCD2 版本切换的触发条件**不应**包含 broker_backend 变化（业务上不允许变；若变了说明数据被外部修改，应告警并保留两条版本）。

---

## §7 与 A1 / A2 的衔接

- **A1**（`docs/analysis/paper_v2_data_capture_audit_20260510.md`）列出当前 paper_v2 侧字段完整性、写入路径、覆盖率。本设计 §3 所有"源 → 目标"映射的 source 列基于 A1。A1 若发现 §3 / §3.8 之外的高价值字段，本设计将补充对应映射节。
- **A2**（`docs/analysis/paper_v2_capture_gaps_20260510.md`）列出当前缺口 + 优先级。本设计 §6.2 中"watermark 字段齐全"的承诺**依赖 A2 中标记为 BLOCKING 的缺口先被修复**。具体已知 BLOCKING：fills / daily_snapshots / positions 三张表缺规范 created_at / updated_at。

引用路径（相对仓库根）：
- `docs/analysis/paper_v2_data_capture_audit_20260510.md`
- `docs/analysis/paper_v2_capture_gaps_20260510.md`

---

## §8 未决问题清单（留给用户）

| # | 问题 | 设计影响 |
|---|---|---|
| Q1 | 数仓 schema 名定为？候选：`dw` / `paper_dw` / `warehouse_paper` / 与 `qe_archive` 同实例不同 schema | 影响 §3 / §5 所有目标表名前缀；影响 §6 跨 schema 访问授权设计 |
| Q2 | paper_v2 数据保留窗口：30 / 90 / 180 天 / 永久 | 决定 ETL 可允许的最大延迟和回放窗口；若窗口 ≤ 30 天，必须保证 ETL 不间断且具备 7 天内补抽能力 |
| Q3 | ETL 是否允许重跑（幂等）？watermark 失效（被外部回拨 / paper_v2 行被改写）场景如何处理？ | 影响 §6.3 幂等约定；若不允许重跑，需引入"已处理 ID 集"额外去重表 |
| Q4 | 异常订单 / 撤单 / 事件流：单独表 `fact_paper_order_event`（保留 `paper_v2.order_events` line 491-500、`paper_v2.run_events` line 565-572、`paper_v2.session_events` line 461-468 的全量事件流），还是仅用 §3.5 的终态行表 + 不再保留事件流？ | 决定 §3.8 是否升级为 §3 一等公民；决定数仓体量（事件流可比 fills 大 10×） |
| Q5 | regime label（牛 / 熊 / 震荡）：在 ETL 阶段补打（数仓 join 一张市场 regime 表）vs 在 paper_v2 阶段就打（fills.metadata 加字段） | 影响 §3.3 fills 表是否需要冗余列；影响 paper_v2 service 层是否需要改 |
| Q6 | SCD2 vs SCD1（portfolio 维度）：本设计 §3.7 默认 SCD2；若用户接受"每次配置变更产生新 portfolio_id"则可降级为 SCD1 | 影响 §5.7 表 8 列 vs 18 列；影响事实表是否需要 portfolio_version_id 列 |
| Q7 | 数仓物理位置：同 PG 实例不同 schema（最简）vs 独立 PG 实例（隔离强）vs 列存（ClickHouse / DuckDB / Iceberg，长期分析友好） | 决定 §6.1 调度器 / 网络 / 备份策略；列存方案下 §5 类型映射要重做（NUMERIC 不变，TIMESTAMPTZ → DateTime64）|
| Q8 | 与 `qe_archive.*`（line 26+）的关联键设计：paper_v2.portfolio_id 是 paper_v2 内部 ID，是否需要单独维护一张 `dw.bridge_paper_to_qe`（portfolio_id ↔ qe_archive.run_id ↔ logical_experiment_id）？ | 决定能否做"实盘组合 ↔ 实验回测"对照分析；若不建桥，长期归因受限 |

每个 Q 已在表中标"设计影响"。

---

## §9 关联文档

- `docs/analysis/paper_v2_data_capture_audit_20260510.md`（A1，cross-test 同批次产出）
- `docs/analysis/paper_v2_capture_gaps_20260510.md`（A2，cross-test 同批次产出）
- `docs/architecture/strategy_engine_design_20260508.md`（broker_backend 上下文）
- `docs/architecture/broker_backend_switch_flow_20260509.md`（broker 切换流程）
- `backend/db/init_trading_core_v2_schema.py`（paper_v2 DDL 真源）
- `backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql`（D1 broker_backend 列）
- `backend/db/init_qe_archive_schema.py`（qe_archive 命名风格参考）
