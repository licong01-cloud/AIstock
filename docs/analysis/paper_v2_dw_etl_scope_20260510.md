# Paper v2 → 数仓 ETL 优先级清单 (2026-05-10)

> 状态：scope 文档；基于 A1（21 表审计）+ A2（gaps）+ B1（ETL 设计草案）。
> 仅文档；不改代码 / schema / migration。
> Created: 2026-05-10. Branch: `claude/paper-v2-vnpy-mvp-20260508`

## §0 重要发现：qe_archive 现有 schema 与 paper_v2 在语义维度上不重叠

在写 scope 之前，先确认了 `qe_archive.*` 现有表的真实语义（cite `backend/db/init_qe_archive_schema.py`）：

- `qe_archive.run` (line 35) — **研究侧 backtest run**：PK=`run_id`，UNIQUE 业务键 `(logical_experiment_id, attempt_no)`，含 `model_catalog_id` / `factor_set_hash` / `score_total` 等 **回测/研究** 维度。**不是** paper trading 的 run。
- `qe_archive.run_metric` (line 276) / `run_curve` (line 300) / `run_factor` (line 318) / `run_factor_importance` (line 346) — 回测产出（IC / NDCG / 净值曲线 / 因子贡献），与 `paper_v2.daily_snapshots` / `fills` 不同语义。
- `qe_archive.run_position` (line 472) / `run_order` (line 492) / `run_trade` (line 512) — **研究回测的虚拟下单 / 持仓**，行级带 `weight` / `target_weight` / `score` 等回测专用字段；`paper_v2.fills` / `positions` / `orders` 是真实模拟盘事实。
- `qe_archive.run_execution_event` (line 535) — 研究侧 event；`paper_v2.run_events` / `session_events` / `order_events` 是 paper 模拟盘事件。
- 命名约定：`qe_archive.<schema_version|run|run_*>` 全 `run_` 前缀，全部以 `run_id REFERENCES qe_archive.run(run_id)` 为锚（line 35→ 92, 116, 148, 192, 231, 278, 302, 320, 348, 376, 397, 421, 456, 474, 494, 514, 537, 553, 580, 596, 660, 683 等）。所有 fact 表都挂在 `qe_archive.run` 这个研究 run 实体上。

**核心含义**：

1. **不能** 简单沿用 `qe_archive.run_*` 命名给 paper_v2 的 fact 表 —— 会与现有 `qe_archive.run`（研究 run）混淆 PK 语义。`qe_archive.run_position` 已经存在且是研究数据，再起一张 `qe_archive.run_position` for paper 不可行。
2. paper_v2 的事实表与 `qe_archive.run` 在概念上 **不应** 用 `run_id` 外键挂钩（paper run ≠ research run）。如果未来需要做"实盘组合 vs 研究回测"对照（B1 §8 Q8 提到的 bridge），需要单独 bridge 表，不能直接 FK。
3. B1 §8 Q1 候选 schema 名 `dw` / `paper_dw` / `warehouse_paper`：从 qe_archive 命名风格看，更适合 **新起一个 schema**（如 `paper_dw` 或 `qe_archive` 的兄弟 `paper_archive`），而不是把 paper_v2 fact 塞进 `qe_archive` 的 run-centric 模型里。详见 §5。

---

## §1 21 表 × 优先级矩阵

主表，每行一张 paper_v2 表。引用源 DDL 行号 = `backend/db/init_trading_core_v2_schema.py:LINE`。

| # | 表 | 行数预估 | ETL 优先级 | 数据 grain | 业务价值 | 与 qe_archive 关系 | 建议目标表名 | 理由 |
|---|----|----------|------------|-----------|---------|-------------------|--------------|------|
| 1 | `portfolio` (line 265) | 低 | **FIRST** | portfolio 级（SCD2） | HIGH | 独立新表（与 `qe_archive.run` 不同实体） | `paper_dw.dim_paper_portfolio_version` | 配置/费率/broker_backend 历史归因；事实表必须 join 到当时有效版本 |
| 2 | `run` (line 369) | 中 | **FIRST** | portfolio_id × trade_date | HIGH | **不可** 用 `qe_archive.run` 名（命名冲突 + 语义不同） | `paper_dw.fact_paper_run` | 每日运行事实表；status/error 终态；模拟盘成败基础 |
| 3 | `trade_session` (line 383) | 低 | **FIRST** | session_id | HIGH | 独立新表 | `paper_dw.fact_paper_session` | 跨日 session（REPLAY/LIVE/CATCHUP）边界 |
| 4 | `session_day` (line 404) | 中 | **FIRST** | session_id × trade_date | HIGH | 独立新表 | `paper_dw.fact_paper_session_day` | session 内日级粒度；bar 进度诊断 |
| 5 | `order_execution_state` (line 422) | 中 | **FIRST** | order_id (终态) | HIGH | 独立新表（与 `qe_archive.run_order` 语义不同） | `paper_dw.fact_paper_order_lifecycle` | 单笔 order 算法状态机终态；filled / cancelled / rejected |
| 6 | `fills` (line 503) | **高** | **FIRST** | fill_id (append-only) | HIGH | 独立新表（与 `qe_archive.run_trade` 语义不同：研究虚拟 vs 模拟真实） | `paper_dw.fact_paper_fill` | 策略归因最关键事实表；append-only |
| 7 | `positions` (line 534) | 中 | **FIRST** | run_id × symbol | HIGH | 独立新表（与 `qe_archive.run_position` 不同：研究持仓 vs 模拟实持） | `paper_dw.fact_paper_position` | 每 run 收盘持仓快照（save_positions DELETE+INSERT 重写） |
| 8 | `daily_snapshots` (line 550) | 中 | **FIRST** | portfolio_id × trade_date | HIGH | 独立新表 | `paper_dw.fact_paper_daily` | NAV / cash / market_value 日级时序 |
| 9 | `cash_ledger` (line 518) | 高 | **FIRST** | cash_id (append-only) | HIGH | 独立新表 | `paper_dw.fact_paper_cash_event` | 现金流水真源；与 fills 互证 |
| 10 | `execution_policy_activation` (line 289) | 低 | DEFER | portfolio_id × trade_date | MEDIUM | 配置变更类，可挂 dim_portfolio_version | `paper_dw.fact_paper_exec_policy_activation`（等触发） | 每日 policy 激活；现阶段 SCD2 portfolio 已能还原 |
| 11 | `runtime_profile` (line 306) | 低 | DEFER | profile_id | MEDIUM | 与 dim_portfolio_version 部分重叠 | `paper_dw.dim_runtime_profile`（等触发） | profile 主表；当前一个 portfolio 一个活跃 profile，价值有限 |
| 12 | `runtime_profile_version` (line 319) | 中 | DEFER | profile_version_id | MEDIUM | SCD2 候选 | `paper_dw.dim_runtime_profile_version`（等触发） | 包含 config_json + sha256；与 dim_portfolio_version 字段层重叠 |
| 13 | `runtime_config_activation` (line 336) | 低 | DEFER | activation_id | MEDIUM | 同 #10 | `paper_dw.fact_paper_runtime_config_activation`（等触发） | 与 #10 类似；SUPERSEDED 状态机 |
| 14 | `config_change_audit` (line 350) | 中 | DEFER | audit_id (append-only) | MEDIUM | 与 SCD2 互补 | `paper_dw.fact_paper_config_change`（等触发） | 谁/为什么改；首批不要，先靠 SCD2 valid_from/to 还原 |
| 15 | `intraday_snapshots` (line 443) | **极高** | **EXCLUDE** | snapshot_id (盘中分钟级) | LOW | — | — | 体量爆炸（每分钟一条 × portfolio）；快过期；DW 价值低（A1 §3.11 + B1 §3.8 已建议不进数仓） |
| 16 | `session_events` (line 460) | 中 | DEFER | event_id (append-only) | MEDIUM | 与 `qe_archive.run_execution_event` 名近但 grain 不同 | `paper_dw.fact_paper_session_event`（等触发） | session 状态切换流水；首批不要，大部分用 SCD2 + run.status 可还原 |
| 17 | `orders` (line 471) | 中 | **EXCLUDE** | order_id (UPSERT) | LOW | 与 #5 重复 | — | 与 `order_execution_state` 同 order_id；`#5` 终态行已含核心字段（filled_qty / status / avg_fill_price）。orders 是中间状态 + intent 索引，DW 用 #5 + #6 即可 |
| 18 | `order_events` (line 491) | 高 | DEFER | event_id (append-only) | MEDIUM | — | `paper_dw.fact_paper_order_event`（等触发） | 全量 order 生命周期事件；fills 已是关键终态。是否进 DW 取决于 B1 §8 Q4 |
| 19 | `run_events` (line 565) | 中 | DEFER | event_seq (append-only) | MEDIUM | — | `paper_dw.fact_paper_run_event`（等触发） | run 级事件流水；首批可由 fact_paper_run.status / error_code 替代 |
| 20 | `errors` (line 575) | 低 | DEFER | error_id (append-only) | MEDIUM | — | `paper_dw.fact_paper_error`（如升级） | A2 标记 daemon path REJECTED/RUN_FAILED 是 BLOCKING，但是 `errors` 表当前覆盖 live/replay 已 OK；DW 优先级取决于 daemon path 是否进 ETL（B1 §8 Q4 / A2 §4.1#4） |
| 21 | `reset_audit` (line 586) | 低 | DEFER | audit_id (append-only) | LOW | — | `paper_dw.fact_paper_reset_audit`（如升级） | reset 操作罕见；首批不需要；运维诊断用 paper_v2 直查即可 |

**总分**：FIRST=9，DEFER=10，EXCLUDE=2。

注：与 caller prompt 建议的 ~8 张 FIRST 不同，本文档把 `cash_ledger` 和 `positions` 也纳入首批（共 9 张），理由见 §2.x。

---

## §2 首批（FIRST）表清单 + ETL grain 详化

### 2.1 portfolio → `paper_dw.dim_paper_portfolio_version` (SCD2)

- **源**：`paper_v2.portfolio` (line 265-287)，PK = `portfolio_id`。
- **grain**：`portfolio_id × version_no`（每次配置/费率/policy 变更产生新版本行；valid_from / valid_to 区间对齐）。
- **watermark**：`updated_at` (line 281, NOT NULL DEFAULT NOW())。**OK**（B1 §6.2）。
- **SCD 类型**：**SCD2**。
- **触发新版本的字段**：`manifest_sha256` / `frozen_manifest_json` / `fee_policy` / `risk_policy` / `execution_policy` / `data_source` 任一变化；**不包括** `broker_backend`（A1 §7 / B1 §6.4 设计为 immutable，若变即异常）。
- **与 qe_archive 关联键**：`portfolio_id` 是 paper_v2 内部 ID，**不映射** `qe_archive.run.run_id`（不同实体）。如未来需要"模拟组合 ↔ 研究 backtest 对照"，需独立 bridge 表（B1 §8 Q8）。

### 2.2 run → `paper_dw.fact_paper_run`

- **源**：`paper_v2.run` (line 369-381)，UNIQUE (portfolio_id, trade_date)。
- **grain**：`portfolio_id × trade_date`（每天每组合一行）。
- **watermark**：`started_at` (NOT NULL, line 376) — INSERT 时即定。`completed_at` (line 377, nullable) 用于"未完成 run"重抽列表。**A2 BLOCKING gap**：`run` 表没有 `updated_at`（只有 `started_at` / `completed_at`），用 `started_at` 作 watermark 即可。
- **SCD 类型**：fact，upsert by `run_id`。
- **关联键**：`portfolio_id` → `paper_dw.dim_paper_portfolio_version`（按 trade_date join 到当时有效版本，valid_from <= trade_date < valid_to）。

### 2.3 trade_session → `paper_dw.fact_paper_session`

- **源**：`paper_v2.trade_session` (line 383-401)。
- **grain**：`session_id`（一次 session 一行；mode=REPLAY_ONLY/LIVE_ONLY/CATCHUP_THEN_LIVE）。
- **watermark**：`updated_at` (NOT NULL, line 397)。**OK**。
- **SCD 类型**：fact，upsert by `session_id`；status / phase / completed_at 多次 UPDATE。
- **关联键**：`portfolio_id` → SCD2；`session_id` 作主键，被 `session_day` / `order_execution_state` / `intraday_snapshots` / `session_events` 引用。

### 2.4 session_day → `paper_dw.fact_paper_session_day`

- **源**：`paper_v2.session_day` (line 404-419)，UNIQUE (session_id, trade_date)。
- **grain**：`session_id × trade_date`。
- **watermark**：`updated_at` (NOT NULL, line 417)。**OK**。
- **SCD 类型**：fact，upsert by `session_day_id`。**注意**：reset 路径会把 `run_id` 清 NULL（A1 §5 / `repository.py:1643`），ETL 必须保留 NULL 而不是丢行。
- **不抽** `latest_available_bar_time` / `last_processed_bar_time`（B1 §4.1 实时心跳）。

### 2.5 order_execution_state → `paper_dw.fact_paper_order_lifecycle`

- **源**：`paper_v2.order_execution_state` (line 422-440)，UNIQUE (order_id)。
- **grain**：`order_id`（建议仅在 `status` ∈ {FILLED, CANCELLED, REJECTED, EXPIRED} 终态后写入；运行中 status 不抽，B1 §3.5）。
- **watermark**：`updated_at` (NOT NULL, line 438)。**OK**。
- **SCD 类型**：fact，upsert by `execution_state_id`。
- **关联键**：`run_id` → fact_paper_run；`session_id` → fact_paper_session。

### 2.6 fills → `paper_dw.fact_paper_fill`

- **源**：`paper_v2.fills` (line 503-515)，append-only。
- **grain**：`fill_id`（每笔成交一行；一个 order 可拆多笔）。
- **watermark**：`trade_time` (NOT NULL, line 511)。**A2 BLOCKING（B1 §6.2 / A2 §3）**：表缺 `created_at` / `updated_at`，必须靠 `trade_time` 作 watermark。如果 caller 写入时 trade_time 乱序（极端情况）会丢行。**建议在 ETL 上线前先补 `created_at` 列（A2 §4.1）**。
- **SCD 类型**：纯 fact，永不更新。
- **A2 BLOCKING #1 #2 #3 必须先补**：`intended_price` 列（滑点 KPI） + `fill_market_context` 表 / metadata 规范键（OHLC/VWAP/suspend_d/limit_state）+ daemon path REJECTED 写 PG。否则 fact_paper_fill 不能完整建模。
- **关联键**：`run_id` → fact_paper_run；`order_id` → fact_paper_order_lifecycle；`portfolio_id` 派生自 run；`broker_backend` 派生自 SCD2 portfolio version（B1 §3.3 / §6.4）。

### 2.7 positions → `paper_dw.fact_paper_position`

- **源**：`paper_v2.positions` (line 534-547)。
- **grain**：`run_id × symbol`（每 run 收盘持仓；service 用 DELETE+INSERT 重写，A1 §3.17）。
- **watermark**：**A2 BLOCKING（B1 §6.2 / A2）** — 表缺 `created_at` / `updated_at`，必须靠 `run_id` 关联到 `run.started_at`。或在 ETL 上线前补 `updated_at`（A2 §4.1）。
- **SCD 类型**：fact，upsert by (run_id, symbol)；旧 run 的行随 reset 路径删除。
- **首批纳入理由**：A1 评级 HIGH（持仓还原是策略归因核心）；caller prompt 建议 ~8 张未列 positions，本 scope 决定 **加入** 因为没有持仓快照无法做"日终持仓 vs daily_snapshots"互证。

### 2.8 daily_snapshots → `paper_dw.fact_paper_daily`

- **源**：`paper_v2.daily_snapshots` (line 550-562)，UNIQUE (portfolio_id, trade_date)。
- **grain**：`portfolio_id × trade_date`。
- **watermark**：`snapshot_time` (NOT NULL, line 559)。**A2 BLOCKING**：缺 `created_at` / `updated_at`，建议补齐（A2 §4.1）。
- **SCD 类型**：fact，upsert by (portfolio_id, trade_date)。
- **关键警告（A1 §6）**：`metadata.position_count` 当 caller 不传 metadata 时退化为 0；**ETL 不应直接信任该列**，应用 `positions` GROUP BY count 重新算（这就是为什么 §2.7 必须纳入首批）。
- **关联键**：`portfolio_version_id` → SCD2 by trade_date。

### 2.9 cash_ledger → `paper_dw.fact_paper_cash_event`

- **源**：`paper_v2.cash_ledger` (line 518-532)，append-only。
- **grain**：`cash_id`（每条现金流水一行）。
- **watermark**：`created_at` (NOT NULL DEFAULT NOW(), line 531)。**OK**（B1 §3.8 已识别）。
- **SCD 类型**：纯 fact，永不更新。
- **首批纳入理由**：A1 评级 HIGH；fee 拆分 + cash 走向是 NAV 重算的真源，与 fills 互证。caller prompt 建议 ~8 张未列 cash_ledger，本 scope 决定 **加入** 因为无 cash_ledger 时 fact_paper_daily.cash 只是单点快照，无法解释"为什么 cash 这么多"。
- **关联键**：`run_id` → fact_paper_run；`fill_id`（nullable，A1 §5）→ fact_paper_fill。

---

## §3 第二批（DEFER）表清单 + 触发条件

### 3.1 execution_policy_activation (#10)
- **不在首批**：当前 portfolio 一个活跃 policy；SCD2 portfolio_version 已能还原"今天用了哪个 policy_sha256"。
- **触发条件**：当用户开始多 policy 灰度（同 portfolio 跨日切 policy）或需要"policy 变更频率/原因"分析。

### 3.2 runtime_profile (#11)
- **不在首批**：profile 主表的核心字段（current_version_id / status）冗余在 #12 runtime_profile_version；首批不需要重复抽。
- **触发条件**：当用户需要 profile 级 KPI（一个 profile 关联多少 portfolio）。

### 3.3 runtime_profile_version (#12)
- **不在首批**：`config_json` + `config_sha256` 与 SCD2 portfolio_version 在 fee_policy/risk_policy/execution_policy 上字段层重叠；首批用 SCD2 即可。
- **触发条件**：当用户需要 profile 跨 portfolio 共享分析（同一 profile_version 在多少 portfolio 激活过）。

### 3.4 runtime_config_activation (#13)
- **不在首批**：与 #10 类似；SUPERSEDED 状态机变更频率低。
- **触发条件**：runtime config 变更频率上升 + 需要"哪天换 profile"时序。

### 3.5 config_change_audit (#14)
- **不在首批**：SCD2 portfolio_version 的 valid_from/to + reason 已能回答"配置何时变、为什么变"。
- **触发条件**：当 SCD2 不足以 attribution 配置变更（如需要 before/after JSON diff、who/request_id）。

### 3.6 session_events (#16)
- **不在首批**：session 状态切换可由 fact_paper_session.status / phase + fact_paper_session_day 还原；首批不需 event 流。
- **触发条件**：B1 §8 Q4 用户决定要保留事件流回放 / SLO 监控落地（如 LIVE_BAR_STREAM_STALLED 事件类）。

### 3.7 order_events (#18)
- **不在首批**：fills（成交终态）+ fact_paper_order_lifecycle（生命周期终态）已覆盖核心 KPI。order_events 给的是细粒度生命周期（CREATED → SUBMITTED → PARTIAL → FILLED）。
- **触发条件**：B1 §8 Q4；当用户需要"intent 提交 → 首笔 fill"延迟 / 撤单率分析（A2 §3 latency 维度）。

### 3.8 run_events (#19)
- **不在首批**：fact_paper_run.status / error_code / completed_at 覆盖核心。
- **触发条件**：当用户需要 run 内细粒度事件（如分钟级 RUN_HEARTBEAT）。

### 3.9 errors (#20)
- **不在首批**：fact_paper_run.error_code 已派生自 `error_json`；首批可不抽。
- **触发条件**：A2 §4.1#3 — daemon path REJECTED/RUN_FAILED 改写 PG 后，errors 表行数会陡增并具备 DW 价值。届时升级为 fact_paper_error。

### 3.10 reset_audit (#21)
- **不在首批**：reset 操作罕见（按 portfolio 级，年频次个位数）；运维查 paper_v2 直查更快。
- **触发条件**：reset 频率上升 + 需要"reset 前后 NAV 异动"分析。

---

## §4 不进数仓（EXCLUDE）表清单 + 理由

### 4.1 intraday_snapshots (#15)
- **理由**：盘中分钟级权益快照（cash/mv/nav/positions JSON），体量爆炸（每分钟一条 × 活跃 portfolio）；A1 §3.11 + B1 §3.8 + B1 §4.1 已明确 DW 价值低 / 体量大。
- **替代查询路径**：盘中查询直接走 paper_v2 短期保留窗口；事后归因用 fact_paper_daily（日级 NAV）即可，不需要分钟级。
- **EXCLUDE 例外条件**：若用户特别要求"分钟级 NAV 回放"（B1 §8 Q4），可按需归档；本 scope 默认排除。

### 4.2 orders (#17)
- **理由**：`paper_v2.orders` 与 `paper_v2.order_execution_state` 在 (order_id) 上 1:1（A1 §3.13 + 3.10），关键字段（filled_quantity / status / avg_fill_price）在 #5 终态行已覆盖；orders 多出来的字段（intent_id / parent_intent / metadata）信息量低。
- **替代查询路径**：fact_paper_order_lifecycle（terminal_status, filled_qty）+ fact_paper_fill（每笔成交）即可；intent_id 可在 ETL 时从 paper_v2.orders join 一次冗余进 fact_paper_order_lifecycle，但不需要独立 fact 表。
- **EXCLUDE 例外条件**：如未来 intent → multi-order 派生关系（A2 §2.5 parent_order_id）落库后，可能需要独立 dim_paper_intent 表；当前不存在。

---

## §5 与 qe_archive 集成建议

### 5.1 命名约定

**推荐**：使用 **新 schema** `paper_dw`（或 `paper_archive`），**不要** 把 paper_v2 fact 表放进 `qe_archive`。

理由（基于 §0 调研）：

1. **PK 语义冲突**：`qe_archive.run` 已是研究 backtest run（PK=run_id, 业务键=logical_experiment_id）。paper_v2 的 run（PK=run_id, 业务键 portfolio_id × trade_date）虽然字段名同为 `run_id`，但实体含义完全不同。混入同 schema 会导致 DW 用户分不清"哪个 run"。
2. **FK 约束语义**：`qe_archive.*` 几乎所有表都 `REFERENCES qe_archive.run(run_id) ON DELETE CASCADE`（line 92, 116, 148, 192, 231, 278, 302, 320, 348, 376, 397, 421, 456, 474, 494, 514, 537, 553, 580）；paper_v2 的 fact 表如果挂进同 schema，FK 关系会变得混乱。
3. **命名前缀冲突**：`qe_archive.run_position` / `run_order` / `run_trade` 已存在并是研究侧虚拟数据；如果再起 `qe_archive.run_paper_*` 或 `qe_archive.paper_v2_*` 命名怪异且与 `run_*` 命名风格不一致。
4. **schema 隔离便于权限管理**：研究 / 模拟盘的访问权限可能不同（modelers vs ops），分 schema 易于 GRANT。

**目标命名约定（建议）**：

```
paper_dw.dim_paper_portfolio_version     -- SCD2 维度
paper_dw.fact_paper_run
paper_dw.fact_paper_session
paper_dw.fact_paper_session_day
paper_dw.fact_paper_order_lifecycle
paper_dw.fact_paper_fill
paper_dw.fact_paper_position
paper_dw.fact_paper_daily
paper_dw.fact_paper_cash_event
paper_dw.etl_run_log                      -- ETL 自身观测（B1 §6.3）
```

DEFER 表升级时遵循同样前缀：`paper_dw.fact_paper_<x>`。

### 5.2 关联键设计

- `portfolio_id`（paper_v2 内部 TEXT ID）作为 fact 表所有跨表 join 的主桥；不需要外键到 `qe_archive.*`。
- 如未来需要"实盘组合 ↔ 研究 backtest 对照"（B1 §8 Q8），起独立 bridge 表：

  ```
  paper_dw.bridge_portfolio_to_qe_run
    portfolio_id TEXT
    qe_logical_experiment_id TEXT  -- 不指 qe_archive.run.run_id（attempt 会变），指 logical id
    relation_type TEXT  -- 'derived_from' / 'compared_to' / 'replicates'
    valid_from TIMESTAMPTZ
    valid_to TIMESTAMPTZ
  ```

  关联到 logical id（`qe_archive.run.logical_experiment_id`，line 37）而不是 `run_id`，因为 `run_id` 受 attempt_no 影响；logical id 跨 attempt 稳定。

### 5.3 schema 隔离 vs 单 schema

| 选项 | 优点 | 缺点 |
|------|------|------|
| **新 schema `paper_dw`**（推荐） | PK / FK / 命名冲突最小；权限管理独立；ETL 目标边界清晰；qe_archive 升级不影响 paper 一侧 | 跨 schema query 写起来略繁琐（`paper_dw.fact_paper_fill` JOIN `qe_archive.run_*`） |
| 同 schema `qe_archive` | 一站式 query；schema_version 共用 | 命名冲突（§0 + §5.1）；FK 语义混乱；权限粒度粗 |
| 独立 PG 实例 / Iceberg | 物理隔离；列存对长保留分析友好 | 运维成本高；ETL 网络/备份策略翻倍 |

**推荐**：**新 schema 同 PG 实例**，与 qe_archive 平级。B1 §8 Q1 / Q7 一并由用户拍板。

---

## §6 与 B1 设计的差异 / 补充

### 6.1 B1 已覆盖（§3.1-3.6 + §3.7 SCD2）的 7 张
B1 § 3 给的 7 张映射：run / trade_session / fills / daily_snapshots / order_execution_state / session_day / portfolio。本文档全部沿用其 grain / watermark / SCD 设计，不做修改。

### 6.2 本文档对 B1 §3.8 候选补充的明确决策

B1 §3.8 列了 4 张候选（cash_ledger / run_events+session_events / intraday_snapshots / config_change_audit），未拍板。本 scope 给定决策：

| B1 候选 | 本 scope 决策 |
|---------|--------------|
| `cash_ledger` | **FIRST**（§2.9）— 不再候选，纳入首批 |
| `intraday_snapshots` | **EXCLUDE**（§4.1）— 与 B1 §3.8 / §4.1 一致 |
| `run_events` / `session_events` | **DEFER**（§3.6 / §3.8）— 触发条件待 B1 §8 Q4 |
| `config_change_audit` | **DEFER**（§3.5）— SCD2 portfolio_version 优先 |

### 6.3 本文档新增 B1 漏掉的表

B1 §3 + §3.8 未覆盖以下表，本 scope 全部赋了 DEFER/EXCLUDE：

- **EXCLUDE**：`orders`（§4.2，与 order_execution_state 重叠）
- **DEFER**：`positions`（§2.7，本 scope 决定升级到 FIRST）、`execution_policy_activation`、`runtime_profile`、`runtime_profile_version`、`runtime_config_activation`、`order_events`、`errors`、`reset_audit`

### 6.4 与 B1 grain 表述的差异 / 修正

无 grain 出入。B1 §3 设计在本 scope 全部沿用。

### 6.5 SCD2 触发字段：补充 B1

B1 §3.7 列了 manifest_sha256 / fee_policy / risk_policy / execution_policy / broker_backend 任一变化触发新版本。本 scope §2.1 **修正**：`broker_backend` **不应**作为 SCD2 触发字段（B1 §6.4 已明确 immutable，若变即异常 + 告警，不该作"正常版本切换"）。本 scope 触发字段列表去掉 `broker_backend`，加上 `data_source` 和 `frozen_manifest_json`。

---

## §7 待用户拍板的开放问题

不重复 B1 §8 已列的 Q1-Q8。本 scope 新增：

| # | 问题 | 影响 |
|---|------|------|
| S1 | **首批 9 张 vs 7 张**：本 scope 在 B1 7 张基础上加了 `positions` 和 `cash_ledger`，是否同意？或先上 7 张，positions/cash_ledger 第二批？ | 影响 ETL 上线时间窗 + fact_paper_daily.position_count 是否可信（A1 §6 警告） |
| S2 | **首批是否拆 phase 1a / 1b**：phase 1a = portfolio SCD2 + run + daily_snapshots + fills（最小可观测 NAV / 成交闭环）；phase 1b = session/session_day/order_lifecycle/positions/cash_ledger（深度归因）。还是一次全上？ | 影响 ETL 调度复杂度 + 用户首日就能看到什么报表 |
| S3 | **DEFER 表的进数仓时间窗口**：本 scope 列了 10 张 DEFER 表的触发条件，是否有总体期限（如半年内必上 / 不限期）？ | 影响 paper_v2 数据保留窗口（B1 §8 Q2）—— 如果半年内必上，paper_v2 保留窗口 ≥ 半年；否则可缩短 |
| S4 | **EXCLUDE 表是否需要 paper_v2 内更长保留**：`intraday_snapshots` 体量大但短期诊断有用，是否在 paper_v2 内单独短保留（如 14 天）后清理？ | 影响 paper_v2 自身清理策略 |
| S5 | **schema 命名**：`paper_dw` vs `paper_archive` vs 其它。本 scope 推荐 `paper_dw`（与 §5.3 选项 A 一致）。 | 影响 §3 / §5 所有目标表名前缀；与 B1 §8 Q1 合并决策 |
| S6 | **bridge_portfolio_to_qe_run 是否需要从一开始就建**：本 scope §5.2 提到的 bridge 表，是首批就建（即使空表）还是等真正需要对照分析时再建？ | 影响 fact 表是否需预留 `qe_logical_experiment_id` 列 |

---

## §8 关联文档

- **A1** commit `e8c41ef` — `docs/analysis/paper_v2_data_capture_audit_20260510.md`
- **A2** commit `d50d3c5` — `docs/analysis/paper_v2_capture_gaps_20260510.md`
- **B1** commit `dbafb0d` — `docs/architecture/paper_v2_to_dw_etl_design_20260510.md`
- qe_archive 现有表 DDL — `backend/db/init_qe_archive_schema.py:25-700`（关键表行号见 §0）
- paper_v2 schema DDL — `backend/db/init_trading_core_v2_schema.py:265-590`（21 张表）
- D1 broker_backend migration — `backend/db/add_paper_v2_portfolio_broker_backend_20260509.sql`
