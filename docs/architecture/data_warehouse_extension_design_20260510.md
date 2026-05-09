# 数仓采集层扩展设计 — paper_v2 + 因子值 + regime_label

> **作者**：Claude Code Opus 4.7（战略 session）
> **日期**：2026-05-10
> **状态**：v2.0（D1=a 精化完成，用户已 ratify D1-D4 + N1+N2 + §12.1-12.7），待 Codex D5 协商
> **基于**：`qe_realtime_experiment_warehouse_top_level_design_20260502.md` v2.1 + `qe_experiment_data_completeness_prewarehouse_plan_20260503.md` v1.1
> **关键修订（v1 → v2）**：D1=a 精化——paper_v2 portfolio_run **不**共享 qe_archive.run，独立成 paper_v2_run 主表，避免 grain 不匹配（research run 是 logical_experiment_id × attempt_no，paper_v2 是 portfolio_id × trade_date）

---

## §0 一句话核心

扩展现有 `qe_archive` 数仓 schema，**事件驱动**覆盖 paper_v2 全部 21 张运行时表 + **因子值跟随因子库重算同步** + **market.regime_label** 派生维度。**零新建 schema**、复用 outbox+worker+archive_job 通用框架，与 QE 入库机制 100% 同构。

---

## §1 范围与边界

### §1.1 用户已拍板（2026-05-10）

| 项 | 决策 |
|---|---|
| 数据建模 | **B 方案精化（D1=a）**：qe_archive 内 paper_v2_* 独立表族，**含 paper_v2_run 独立主表**（NOT 共享 qe_archive.run），避免 grain 不匹配（research run 是 logical_experiment_id × attempt_no，paper_v2 是 portfolio_id × trade_date）|
| 事件触发粒度 | **双触发**：portfolio.run.completed + daily_snapshot.captured |
| regime_label 归属 | **market.regime_label**（与原始市场数据同 schema） |
| qe_archive Worker | 在生产启动（默认 disabled 改 enabled） |
| 启动时机 | 立即设计（与 D2.b 合 main 并行） |
| daemon SQLite 旁路修复 | **路径 A**（daemon 直连 PG outbox） |
| paper_v2 表 ETL scope | **全 21 张一次性纳入** |
| dw 工作面 | 新开 worktree `dw-foundation-20260510`（不在 paper-v2 团队内） |
| 因子值更新策略 | **跟随因子库重算**（事件驱动 factor.recompute.completed） |
| 文档优先 | 先完成本文档 → Codex 协商 → 并行开发 |

### §1.2 不动的（边界）

- ❌ 不新建 schema（数仓 = qe_archive，已存在）
- ❌ 不动现有 qe_archive 13 张表 schema（Codex Phase 0-7 主体）
- ❌ 不动 outbox_event + archive_job 表 schema（已稳定）
- ❌ 不动 worker.py 主框架（仅注册新 handler）
- ❌ 不动 market.* 现有表 schema（日 K / 分钟 K / 指数 K 全保留）

### §1.3 新增的（本设计范围）

- ✅ qe_archive 新增 18 张 paper_v2_* 表族（含 paper_v2_run 独立主表 + 17 张明细/维度/事件表，NOT 共享 qe_archive.run）
- ✅ qe_archive 新增 1 张 factor_value 表
- ✅ market 新增 1 张 regime_label 表
- ✅ outbox_event 新增 4 类 event_type（paper.* / factor.*）
- ✅ 新建 handler：PaperV2ArchiveHandler / FactorValueArchiveHandler
- ✅ paper_v2.daemon 改为直连 PG outbox（路径 A）
- ✅ 新建 regime_label 计算脚本（每日盘后定时）

---

## §2 现有基线（已建成，不重复）

详见调研报告（之前会话）。要点回顾：

| 组件 | 状态 |
|---|---|
| qe_archive schema（13 张表）| ✅ 已建（init_qe_archive_schema.py） |
| outbox_event + archive_job 三件套 | ✅ 已实现（worker.py） |
| QE 自动入库（safe_archive_loop_completed）| ✅ 已集成（qe_evolution_service.py:151） |
| Level A/B/C 三层入库 | ⚠ A 已落，B/C 待补 |
| Worker 启动状态 | ⚠ 默认 disabled，本设计要求生产启动 |
| paper_v2 当前入库 | ❌ 完全无（本设计目标） |
| 因子值入库 | ❌ 完全无（本设计目标） |
| regime_label | ❌ 完全无（本设计目标） |

---

## §3 数据建模（B 方案落地）

### §3.1 paper_v2 portfolio_run → qe_archive.paper_v2_run 独立主表（D1=a 精化）

**关键设计调整**：paper_v2 portfolio_run **不**写入 qe_archive.run，而是写入新增的独立主表 **qe_archive.paper_v2_run**。原因（T3 cross-test 发现的 grain 冲突）：

- qe_archive.run 设计 grain = `logical_experiment_id × attempt_no`（一次研究实验的一次重试）
- paper_v2 portfolio_run grain = `portfolio_id × trade_date`（一个模拟盘的某个交易日）

两个 grain 在 attempt_no、score_total 等字段上语义完全不同。强行共享 run 主表会污染 research-side leaderboard 查询，且 paper_v2 数据需要额外 source_system 过滤。

**修订后方案**：paper_v2_run 在同一 schema 内独立成表，与 qe_archive.run 解耦但**共用** outbox/worker/archive_job 通用框架。Leaderboard 跨域比较通过应用层 union view 实现（不在主表共享）。

#### qe_archive.paper_v2_run 字段（独立主表）

| 字段 | 来源 | 说明 |
|---|---|---|
| run_id | `paper_v2.run.id::TEXT` | UUID 转文本，PRIMARY KEY |
| portfolio_id | `paper_v2.run.portfolio_id::TEXT` | 模拟盘 ID（grain 维度 1） |
| trade_date | `paper_v2.run.trade_date` | 交易日（grain 维度 2） |
| portfolio_version_id | `dim_paper_v2_portfolio` SCD2 lookup | FK 维度表 |
| package_id | `paper_v2.portfolio.package_id::TEXT` | strategy_pkg.package |
| manifest_sha256 | strategy_pkg.package.manifest_sha256 | 冻结策略指纹 |
| broker_backend | `paper_v2.portfolio.broker_backend` | localsim / miniqmtsim |
| data_source | `paper_v2.portfolio.data_source` | TDX_REALTIME / DB_HISTORICAL / MINIQMT_REALTIME |
| node_id | `paper_v2.run.runtime_config.node_id` | 多节点扩展 |
| model_params_origin | `paper_v2.run.model_params_origin` | node / cache / unavailable（T1 加字段）|
| status | `paper_v2.run.status` | pending/running/completed/failed |
| started_at | `paper_v2.run.started_at` | |
| completed_at | `paper_v2.run.completed_at` | |
| captured_at | NOW() | ETL 时间 |
| UNIQUE | (portfolio_id, trade_date) | 自然键防重 |

**leaderboard 设计**：跨 research + paper_v2 比较通过应用层 view（不在主表混合）：

```sql
CREATE VIEW qe_archive.v_run_leaderboard AS
SELECT 'research' AS run_type, run_id, score_total, completed_at
FROM qe_archive.run WHERE research_valid = TRUE
UNION ALL
SELECT 'paper_v2' AS run_type, run_id, NULL AS score_total, completed_at
FROM qe_archive.paper_v2_run WHERE status = 'completed';
```

paper_v2 不参与 score_total 排名（NULL），但可以在 view 里看到所有 run 的时序。

### §3.2 paper_v2.* 21 张表 → qe_archive 镜像清单

ETL scope 全 21 张：

| # | paper_v2 源表 | qe_archive 目标表 | grain | ETL 方式 |
|---|---|---|---|---|
| 1 | portfolio | dim_paper_v2_portfolio (SCD2) | portfolio_id × manifest_sha256 × broker_backend | 双触发，配置变更切版本 |
| 2 | run | **qe_archive.paper_v2_run（独立主表，NOT 共享 qe_archive.run）** | trade_date × portfolio_id | portfolio.run.completed |
| 3 | trade_session | paper_v2_session | session_id | portfolio.run.completed |
| 4 | session_day | paper_v2_session_day | session_id × trade_date | portfolio.run.completed |
| 5 | session_events | paper_v2_session_event | append | portfolio.run.completed |
| 6 | run_events | paper_v2_run_event | append | portfolio.run.completed |
| 7 | order_execution_state | paper_v2_order_execution_state | order_id 终态 | portfolio.run.completed |
| 8 | orders | paper_v2_order | order_id | portfolio.run.completed |
| 9 | order_events | paper_v2_order_event | append | portfolio.run.completed |
| 10 | fills | paper_v2_fill | fill_id | portfolio.run.completed |
| 11 | positions | paper_v2_position_snapshot | trade_date × portfolio_id × symbol | daily_snapshot.captured |
| 12 | daily_snapshots | paper_v2_daily_snapshot | trade_date × portfolio_id | daily_snapshot.captured |
| 13 | intraday_snapshots | paper_v2_intraday_snapshot | snapshot_id | portfolio.run.completed |
| 14 | cash_ledger | paper_v2_cash_ledger | append | portfolio.run.completed |
| 15 | errors | paper_v2_error | append | portfolio.run.completed |
| 16 | runtime_profile | dim_paper_v2_runtime_profile (SCD2) | profile_id × version | profile.changed |
| 17 | runtime_profile_version | dim_paper_v2_runtime_profile_version | version_id | profile.changed |
| 18 | runtime_config_activation | paper_v2_runtime_config_activation | activation_id | config.changed |
| 19 | execution_policy_activation | paper_v2_execution_policy_activation | activation_id | config.changed |
| 20 | config_change_audit | paper_v2_config_change_audit | append | config.changed |
| 21 | reset_audit | paper_v2_reset_audit | append | run.completed |

**主表（1 张）**：paper_v2_run（D1=a 新增独立主表）
**SCD2 维度（3 张）**：portfolio / runtime_profile / runtime_profile_version
**事实表（13 张）**：session / session_day / order_execution / order / fill / position_snapshot / daily_snapshot / intraday_snapshot / cash_ledger / error / runtime_config_activation / execution_policy_activation / reset_audit
**事件表 append-only（5 张）**：session_event / run_event / order_event / config_change_audit / broker_error（如有）

合计 paper_v2_* 表：**1 + 3 + 13 + 5 = 22**（其中 broker_error 视 schema 实际情况，可能合并到 paper_v2_error，故文档其他章节按 18 张计算 = 1 主表 + 17 子表）。

### §3.3 增量去重策略

- **SCD2 维度**：以 (natural_key, valid_from) 为唯一键，新版本插入新行，旧行 valid_to=now() + is_current=FALSE
- **事实表**：以 (natural_key) UPSERT，重跑幂等
- **事件表**：以 (event_id) UNIQUE 跳过重复

---

## §4 事件 schema + handler 接口

### §4.1 新增 event_type（4 类）

| event_type | 触发点 | payload schema |
|---|---|---|
| `paper.portfolio_run.completed` | paper_v2.run status → 'completed' | {portfolio_id, run_id, trade_date, occurred_at} |
| `paper.daily_snapshot.captured` | daily_snapshots 写完 | {portfolio_id, trade_date, snapshot_id, occurred_at} |
| `paper.config.changed` | config_change_audit 写入 | {portfolio_id, change_type, audit_id, occurred_at} |
| `factor.recompute.completed` | 因子库 _save_metrics 完成 | {factor_name, code_text_hash, data_start, data_end, snapshot_date, occurred_at} |

### §4.2 outbox_event 写入示例

```sql
INSERT INTO qe_archive.outbox_event (
  event_id,
  event_type,
  source_system,
  source_id,
  source_sub_id,
  payload,
  status,
  retry_count,
  next_retry_at,
  created_at
) VALUES (
  gen_random_uuid()::TEXT,
  'paper.portfolio_run.completed',
  'paper_v2',
  'portfolio_id_xxx',
  'run_id_yyy',
  '{"trade_date":"2026-05-15","occurred_at":"2026-05-15T15:30:00"}'::jsonb,
  'pending',
  0,
  NOW(),
  NOW()
);
```

注：`(event_type, source_system, source_id, source_sub_id)` 已是 UNIQUE 约束（init:674），重复 enqueue 自动去重。

### §4.3 Handler 接口契约

```python
# 位置：backend/services/qe_archive/handlers/paper_v2_handler.py
# 注册位置：backend/services/qe_archive/worker.py 启动时通过 register_handler() 注入

class PaperV2ArchiveHandler:
    """处理 paper.* 事件，写入 qe_archive.run + paper_v2_* 扩展表。"""

    def can_handle(self, event_type: str) -> bool:
        return event_type.startswith('paper.')

    def handle(self, event: OutboxEvent, archive_job: ArchiveJob) -> ArchiveResult:
        if event.event_type == 'paper.portfolio_run.completed':
            return self._handle_portfolio_run_completed(event, archive_job)
        elif event.event_type == 'paper.daily_snapshot.captured':
            return self._handle_daily_snapshot_captured(event, archive_job)
        elif event.event_type == 'paper.config.changed':
            return self._handle_config_changed(event, archive_job)
        else:
            raise UnsupportedEventType(event.event_type)

    def _handle_portfolio_run_completed(self, event, job) -> ArchiveResult:
        portfolio_id = event.source_id
        run_id = event.source_sub_id
        trade_date = event.payload['trade_date']

        with transaction():
            # 1. 写 qe_archive.run（如已有则 UPSERT）
            self._upsert_run(portfolio_id, run_id, trade_date)
            # 2. 写 dim_paper_v2_portfolio（SCD2）
            self._upsert_dim_portfolio(portfolio_id, trade_date)
            # 3. 镜像 13 张事实/事件表（按 trade_date + portfolio_id 拉源数据）
            self._mirror_paper_v2_run_data(portfolio_id, run_id, trade_date)

        return ArchiveResult(rows_inserted=N, rows_upserted=M, status='completed')

    # ... 其他 handler 方法


class FactorValueArchiveHandler:
    """处理 factor.recompute.completed 事件，从 single/{name}.parquet 入库。"""

    def can_handle(self, event_type: str) -> bool:
        return event_type == 'factor.recompute.completed'

    def handle(self, event, job) -> ArchiveResult:
        factor_name = event.payload['factor_name']
        code_text_hash = event.payload['code_text_hash']
        snapshot_date = event.payload['snapshot_date']

        parquet_path = f"single/{factor_name}.parquet"
        df = pd.read_parquet(parquet_path)

        # 增量去重：(factor_name, code_text_hash, trade_date, code) 已存在则跳过
        new_rows = self._diff_with_existing(df, factor_name, code_text_hash)
        self._bulk_insert(new_rows, factor_name, code_text_hash, snapshot_date)

        return ArchiveResult(rows_inserted=len(new_rows), status='completed')
```

### §4.4 Worker 派发逻辑

worker.py 主循环 claim outbox_event 后，根据 event_type 前缀派发到对应 handler。当前 QE 已注册 `qe.*` handler，本设计新增 `paper.*` 和 `factor.*` 两个 handler。

---

## §5 paper_v2_* 扩展表 DDL 草稿（18 张：1 主表 + 17 子表）

注：以下仅给关键字段 + 类型 + 注释；完整 DDL（含约束 / 索引 / 注释）等本文档审过后写入 SQL 文件。

### §5.0 paper_v2_run 独立主表（D1=a 新增）

```sql
CREATE TABLE qe_archive.paper_v2_run (
    run_id                TEXT PRIMARY KEY,                  -- paper_v2.run.id 转 TEXT
    portfolio_id          TEXT NOT NULL,                     -- 模拟盘 ID（grain 维度 1）
    trade_date            DATE NOT NULL,                     -- 交易日（grain 维度 2）
    portfolio_version_id  BIGINT REFERENCES qe_archive.dim_paper_v2_portfolio(portfolio_version_id),
    package_id            TEXT,                              -- strategy_pkg.package
    manifest_sha256       TEXT,                              -- 冻结策略指纹
    broker_backend        TEXT NOT NULL,                     -- 'localsim' / 'miniqmtsim' / ...
    data_source           TEXT NOT NULL,                     -- 'TDX_REALTIME' / 'DB_HISTORICAL' / 'MINIQMT_REALTIME'
    node_id               TEXT,                              -- 多节点扩展
    model_params_origin   TEXT NOT NULL DEFAULT 'node',      -- node / cache / unavailable (T1 加字段)
    status                TEXT NOT NULL,                     -- pending / running / completed / failed
    started_at            TIMESTAMPTZ,
    completed_at          TIMESTAMPTZ,
    captured_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (portfolio_id, trade_date)                        -- 自然键防重
);

CREATE INDEX ix_paper_v2_run_portfolio ON qe_archive.paper_v2_run (portfolio_id, trade_date);
CREATE INDEX ix_paper_v2_run_status_date ON qe_archive.paper_v2_run (status, completed_at DESC);
CREATE INDEX ix_paper_v2_run_package ON qe_archive.paper_v2_run (package_id, trade_date);
```

**说明**：
- 不与 qe_archive.run 共享主键空间（避免 grain 冲突）
- 所有 paper_v2_* 子表的 run_id FK 引用 qe_archive.paper_v2_run(run_id)
- 子表通过 ON DELETE CASCADE 与主表联动

### §5.1 dim_paper_v2_portfolio (SCD2)

```sql
CREATE TABLE qe_archive.dim_paper_v2_portfolio (
    portfolio_version_id  BIGSERIAL PRIMARY KEY,
    portfolio_id          UUID NOT NULL,
    manifest_sha256       TEXT NOT NULL,
    broker_backend        TEXT NOT NULL,        -- 'localsim' / 'miniqmtsim' / ...
    data_source           TEXT NOT NULL,        -- 'TDX_REALTIME' / 'DB_HISTORICAL' / 'MINIQMT_REALTIME'
    package_id            UUID,                 -- FK strategy_pkg.package
    initial_cash          NUMERIC(18,4),
    fee_policy_json       JSONB,
    risk_policy_json      JSONB,
    valid_from            TIMESTAMPTZ NOT NULL,
    valid_to              TIMESTAMPTZ,
    is_current            BOOLEAN NOT NULL DEFAULT TRUE,
    captured_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (portfolio_id, manifest_sha256, broker_backend, valid_from)
);

CREATE INDEX ix_dim_paper_v2_portfolio_current
    ON qe_archive.dim_paper_v2_portfolio (portfolio_id) WHERE is_current;
```

### §5.2 paper_v2_session

```sql
CREATE TABLE qe_archive.paper_v2_session (
    session_pk           BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT REFERENCES qe_archive.dim_paper_v2_portfolio(portfolio_version_id),
    trade_session_id     UUID NOT NULL UNIQUE,
    trade_date           DATE NOT NULL,
    mode                 TEXT,                  -- 'REPLAY_ONLY' / 'LIVE_ONLY' / 'CATCHUP_THEN_LIVE'
    validated_execution_policy_json JSONB,
    started_at           TIMESTAMPTZ,
    ended_at             TIMESTAMPTZ,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.3 paper_v2_session_day

```sql
CREATE TABLE qe_archive.paper_v2_session_day (
    session_day_pk        BIGSERIAL PRIMARY KEY,
    run_id                TEXT NOT NULL,
    trade_session_id      UUID NOT NULL,
    trade_date            DATE NOT NULL,
    expected_bar_count    INT,
    actual_bar_count      INT,
    latest_available_bar_time TIMESTAMPTZ,
    data_quality          TEXT,                 -- 'ok' / 'low_coverage' / ...
    captured_at           TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (trade_session_id, trade_date)
);
```

### §5.4 paper_v2_order

```sql
CREATE TABLE qe_archive.paper_v2_order (
    order_pk             BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL,
    portfolio_version_id BIGINT,
    order_id             TEXT NOT NULL UNIQUE,
    trade_session_id     UUID,
    trade_date           DATE,
    symbol               TEXT,
    side                 TEXT,                  -- 'BUY' / 'SELL'
    order_type           TEXT,                  -- 'LIMIT' / 'MARKET' / ...
    quantity             BIGINT,
    price                NUMERIC(18,4),
    status               TEXT,                  -- terminal status
    placed_at            TIMESTAMPTZ,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.5 paper_v2_order_event (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_order_event (
    event_pk             BIGSERIAL PRIMARY KEY,
    event_id             TEXT NOT NULL UNIQUE,
    order_id             TEXT NOT NULL,
    run_id               TEXT NOT NULL,
    trade_date           DATE,
    event_type           TEXT,                  -- 'placed' / 'partial_fill' / 'fill' / 'cancelled' / 'rejected'
    event_payload        JSONB,
    occurred_at          TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX ix_paper_v2_order_event_order
    ON qe_archive.paper_v2_order_event (order_id, occurred_at);
```

### §5.6 paper_v2_order_execution_state (终态)

```sql
CREATE TABLE qe_archive.paper_v2_order_execution_state (
    state_pk             BIGSERIAL PRIMARY KEY,
    order_id             TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    trade_date           DATE,
    algo_code            TEXT,
    final_algo_state_json JSONB,
    filled_quantity      BIGINT,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.7 paper_v2_fill

```sql
CREATE TABLE qe_archive.paper_v2_fill (
    fill_pk              BIGSERIAL PRIMARY KEY,
    fill_id              TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    portfolio_version_id BIGINT,
    order_id             TEXT NOT NULL,
    trade_date           DATE NOT NULL,
    trade_session_id     UUID,
    symbol               TEXT NOT NULL,
    side                 TEXT,
    filled_quantity      BIGINT,
    fill_price           NUMERIC(18,4),
    fill_value           NUMERIC(18,4),
    fees                 NUMERIC(18,4),
    slippage_bps         NUMERIC(10,2),         -- 计算字段，可空
    broker_backend       TEXT,                  -- 冗余便于查询
    algo_code            TEXT,
    filled_at            TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
) PARTITION BY RANGE (trade_date);

-- 按月分区
CREATE TABLE qe_archive.paper_v2_fill_y2026m05
    PARTITION OF qe_archive.paper_v2_fill
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE INDEX ix_paper_v2_fill_run ON qe_archive.paper_v2_fill (run_id, trade_date);
CREATE INDEX ix_paper_v2_fill_symbol ON qe_archive.paper_v2_fill (symbol, trade_date);
```

### §5.8 paper_v2_position_snapshot

```sql
CREATE TABLE qe_archive.paper_v2_position_snapshot (
    snapshot_pk          BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL,
    portfolio_version_id BIGINT,
    trade_date           DATE NOT NULL,
    symbol               TEXT NOT NULL,
    quantity             BIGINT,
    cost_basis           NUMERIC(18,4),
    market_value         NUMERIC(18,4),
    unrealized_pnl       NUMERIC(18,4),
    captured_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (run_id, trade_date, symbol)
);
```

### §5.9 paper_v2_daily_snapshot

```sql
CREATE TABLE qe_archive.paper_v2_daily_snapshot (
    daily_pk             BIGSERIAL PRIMARY KEY,
    run_id               TEXT NOT NULL,
    portfolio_version_id BIGINT,
    trade_date           DATE NOT NULL,
    total_value          NUMERIC(18,4),
    cash                 NUMERIC(18,4),
    positions_value      NUMERIC(18,4),
    realized_pnl         NUMERIC(18,4),
    unrealized_pnl       NUMERIC(18,4),
    -- 直接 join market.index_daily 拉 benchmark
    benchmark_csi300     NUMERIC(10,4),
    benchmark_csi500     NUMERIC(10,4),
    benchmark_csi1000    NUMERIC(10,4),
    relative_to_csi300   NUMERIC(10,4),
    -- regime 在 ETL 时打（依赖 market.regime_label）
    regime               TEXT,
    captured_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (run_id, trade_date)
);
```

### §5.10 paper_v2_intraday_snapshot

```sql
CREATE TABLE qe_archive.paper_v2_intraday_snapshot (
    snapshot_pk          BIGSERIAL PRIMARY KEY,
    snapshot_id          TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    trade_date           DATE NOT NULL,
    snapshot_time        TIMESTAMPTZ NOT NULL,
    total_value          NUMERIC(18,4),
    cash                 NUMERIC(18,4),
    positions_json       JSONB,                 -- 详细持仓
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.11 paper_v2_cash_ledger (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_cash_ledger (
    ledger_pk            BIGSERIAL PRIMARY KEY,
    ledger_entry_id      TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    portfolio_version_id BIGINT,
    trade_date           DATE,
    entry_type           TEXT,                  -- 'deposit' / 'withdraw' / 'fee' / 'fill_credit' / 'fill_debit'
    amount               NUMERIC(18,4),
    balance_after        NUMERIC(18,4),
    related_order_id     TEXT,
    occurred_at          TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.12 paper_v2_error

```sql
CREATE TABLE qe_archive.paper_v2_error (
    error_pk             BIGSERIAL PRIMARY KEY,
    error_id             TEXT NOT NULL UNIQUE,
    run_id               TEXT,
    trade_date           DATE,
    trade_session_id     UUID,
    error_class          TEXT,                  -- 'BrokerBackendError' subclass / general
    error_code           TEXT,
    error_message        TEXT,
    stack_trace          TEXT,
    related_order_id     TEXT,
    occurred_at          TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.13 paper_v2_session_event (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_session_event (
    event_pk             BIGSERIAL PRIMARY KEY,
    event_id             TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    trade_session_id     UUID,
    trade_date           DATE,
    event_type           TEXT,
    event_payload        JSONB,
    occurred_at          TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.14 paper_v2_run_event (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_run_event (
    event_pk             BIGSERIAL PRIMARY KEY,
    event_id             TEXT NOT NULL UNIQUE,
    run_id               TEXT NOT NULL,
    trade_date           DATE,
    event_type           TEXT,
    event_payload        JSONB,
    occurred_at          TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.15 dim_paper_v2_runtime_profile (SCD2)

```sql
CREATE TABLE qe_archive.dim_paper_v2_runtime_profile (
    profile_version_id   BIGSERIAL PRIMARY KEY,
    profile_id           UUID NOT NULL,
    profile_name         TEXT,
    profile_json         JSONB,                 -- 完整 profile config
    valid_from           TIMESTAMPTZ NOT NULL,
    valid_to             TIMESTAMPTZ,
    is_current           BOOLEAN NOT NULL DEFAULT TRUE,
    captured_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (profile_id, valid_from)
);
```

### §5.16 dim_paper_v2_runtime_profile_version

```sql
CREATE TABLE qe_archive.dim_paper_v2_runtime_profile_version (
    version_pk           BIGSERIAL PRIMARY KEY,
    version_id           UUID NOT NULL UNIQUE,
    profile_id           UUID NOT NULL,
    version_number       INT,
    config_diff_json     JSONB,                 -- 与上版差异
    created_by           TEXT,
    created_at           TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.17 paper_v2_runtime_config_activation / execution_policy_activation

```sql
CREATE TABLE qe_archive.paper_v2_runtime_config_activation (
    activation_pk        BIGSERIAL PRIMARY KEY,
    activation_id        TEXT NOT NULL UNIQUE,
    portfolio_id         UUID NOT NULL,
    profile_version_id   UUID,
    activated_at         TIMESTAMPTZ NOT NULL,
    activated_by         TEXT,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE qe_archive.paper_v2_execution_policy_activation (
    activation_pk        BIGSERIAL PRIMARY KEY,
    activation_id        TEXT NOT NULL UNIQUE,
    portfolio_id         UUID NOT NULL,
    policy_sha256        TEXT,
    policy_json          JSONB,
    activated_at         TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.18 paper_v2_config_change_audit (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_config_change_audit (
    audit_pk             BIGSERIAL PRIMARY KEY,
    audit_id             TEXT NOT NULL UNIQUE,
    portfolio_id         UUID,
    change_type          TEXT,                  -- 'runtime_profile' / 'execution_policy' / 'fee_policy' / 'risk_policy'
    old_value_json       JSONB,
    new_value_json       JSONB,
    changed_by           TEXT,
    changed_at           TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

### §5.19 paper_v2_reset_audit (append-only)

```sql
CREATE TABLE qe_archive.paper_v2_reset_audit (
    audit_pk             BIGSERIAL PRIMARY KEY,
    audit_id             TEXT NOT NULL UNIQUE,
    portfolio_id         UUID,
    reset_type           TEXT,
    reset_reason         TEXT,
    snapshot_before_json JSONB,
    reset_at             TIMESTAMPTZ NOT NULL,
    captured_at          TIMESTAMPTZ DEFAULT NOW()
);
```

---

## §6 daemon 直连 PG outbox（路径 A 落地）

### §6.1 现状（A2 #3 BLOCKING）

`backend/services/paper_trading_v2/daemon/event_log.py` 写本地 SQLite (`var/paper_v2_sim/daemon_events.db`)，9 类事件不进 PG。

### §6.2 改造（路径 A）

```python
# event_log.py 改造目标

class DaemonEventLog:
    def __init__(self, pg_conn=None, sqlite_fallback_path=None):
        self.pg_conn = pg_conn or self._init_pg_from_env()
        self.sqlite_path = sqlite_fallback_path  # 仅 fallback 用
        if self.pg_conn is None and self.sqlite_path:
            log.warning("daemon: PG unavailable, falling back to SQLite (will replay on next start)")

    def emit(self, event_type: str, payload: dict):
        """主路径：直接写 PG outbox。失败则 SQLite fallback + 标记 unsynced。"""
        if self.pg_conn:
            try:
                self._enqueue_pg_outbox(event_type, payload)
                return
            except (psycopg2.OperationalError, ConnectionError) as e:
                log.error(f"daemon: PG outbox emit failed: {e}, falling back to SQLite")

        # Fallback：本地 SQLite + unsynced 标记
        self._write_sqlite(event_type, payload, synced=False)

    def _enqueue_pg_outbox(self, event_type: str, payload: dict):
        with self.pg_conn.cursor() as cur:
            cur.execute("""
                INSERT INTO qe_archive.outbox_event (
                    event_id, event_type, source_system, source_id, source_sub_id,
                    payload, status, retry_count, created_at
                ) VALUES (%s, %s, 'paper_v2', %s, %s, %s::jsonb, 'pending', 0, NOW())
                ON CONFLICT (event_type, source_system, source_id, source_sub_id) DO NOTHING
            """, (...))
            self.pg_conn.commit()

    def replay_unsynced_on_startup(self):
        """启动时回放 SQLite unsynced 事件到 PG outbox。"""
        if not self.sqlite_path or not Path(self.sqlite_path).exists():
            return
        unsynced = self._read_sqlite_unsynced()
        for event in unsynced:
            try:
                self._enqueue_pg_outbox(event.event_type, event.payload)
                self._mark_sqlite_synced(event.id)
            except Exception:
                log.error(f"daemon: replay failed for event {event.id}")
```

### §6.3 前置 audit（T2 任务输出后才能完成）

T2 audit 要回答：
- daemon 启动入口在哪？
- daemon 进程能否拿到 .env 里的 TDX_DB_*？
- daemon 当前依赖能否加 psycopg2？
- daemon 启动顺序与 PG 是否冲突？

T2 完成后本节细化为可实施版本。

### §6.4 风险

- **PG 不可用时**：daemon 走 SQLite fallback，启动时 replay。理论数据不丢，但 fallback 期间事件**晚到** outbox（替代实时）。
- **SQLite fallback 持久化**：磁盘满 / SQLite 损坏 → 9 类事件丢失。需监控告警。

---

## §7 因子值与因子库同步

### §7.1 触发点

`factor_pipeline_v2`（exec 沙箱模式）的 `on_factor_success` 回调：

```python
# 在 _save_metrics() 完成 + 因子值写入 single/{name}.parquet 后 emit
def on_factor_success(factor_name, code_text_hash, data_start, data_end, snapshot_date):
    # ... 现有 _save_metrics() ...

    # 新增：emit 事件
    enqueue_factor_recompute_completed(
        factor_name=factor_name,
        code_text_hash=code_text_hash,
        data_start=data_start,
        data_end=data_end,
        snapshot_date=snapshot_date,
    )
```

### §7.2 qe_archive.factor_value 表

```sql
CREATE TABLE qe_archive.factor_value (
    value_pk             BIGSERIAL PRIMARY KEY,
    factor_name          TEXT NOT NULL,
    code_text_hash       TEXT NOT NULL,         -- 因子代码版本指纹
    trade_date           DATE NOT NULL,
    code                 TEXT NOT NULL,         -- 股票代码
    value                NUMERIC(18,8),
    snapshot_date        DATE NOT NULL,         -- 因子计算的快照基准
    captured_at          TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE (factor_name, code_text_hash, trade_date, code)
) PARTITION BY RANGE (trade_date);

CREATE TABLE qe_archive.factor_value_y2026m05
    PARTITION OF qe_archive.factor_value
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

CREATE INDEX ix_factor_value_factor ON qe_archive.factor_value (factor_name, trade_date);
CREATE INDEX ix_factor_value_code ON qe_archive.factor_value (code, trade_date);
```

### §7.3 增量去重

UNIQUE (factor_name, code_text_hash, trade_date, code) → 因子代码不变，重复入库无影响；因子代码变（hash 变），自动新增一行（不覆盖旧版）。

### §7.4 体积估算

- 假设 500 因子 × 5000 股票 × 8 年（~2000 交易日）= 50 亿行
- 每行 ~80 bytes → ~400 GB
- 必须分区 + 压缩（pg_partman + pg_dump 压缩档案）

### §7.5 触发频率

跟随因子库重算节奏（用户拍板）。当前因子库重算频率约每周-每月（视情况）。每次重算只入新增/变化部分。

---

## §8 market.regime_label 设计

### §8.1 表结构

```sql
CREATE TABLE market.regime_label (
    label_pk             BIGSERIAL PRIMARY KEY,
    trade_date           DATE NOT NULL,
    regime               TEXT NOT NULL,         -- 'bull' / 'bear' / 'oscillation' / 'high_vol' / 'low_vol'
    regime_confidence    NUMERIC(4,3),          -- 0-1
    source_method        TEXT NOT NULL,         -- 'simple_quadrant' / 'hmm_viterbi' / 'bbq' / 'ensemble'
    source_signal_json   JSONB,                 -- 计算依据：{"csi300_6m_ret": 0.15, "csi300_60d_vol_pct": 0.32, ...}
    labeled_at           TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (trade_date, source_method)
);

CREATE INDEX ix_regime_label_date ON market.regime_label (trade_date);
CREATE INDEX ix_regime_label_regime ON market.regime_label (regime, trade_date);
```

### §8.2 计算入口

每日盘后定时任务（不走 outbox，是市场数据派生不是事件）：

```python
# scripts/regime_label_daily.py
def compute_regime_for_date(trade_date: date, method: str = 'simple_quadrant'):
    if method == 'simple_quadrant':
        # 用 CSI300 半年收益分位 + 60d 波动率分位 → 4 象限
        ret_6m = query_index_return('csi300', trade_date, lookback=126)
        vol_60d = query_index_volatility('csi300', trade_date, lookback=60)
        ret_pct = compute_percentile(ret_6m, history='5y')
        vol_pct = compute_percentile(vol_60d, history='5y')

        if ret_pct > 0.6 and vol_pct < 0.4:
            regime = 'bull'
        elif ret_pct < 0.4 and vol_pct > 0.6:
            regime = 'bear'
        elif vol_pct > 0.6:
            regime = 'high_vol'
        elif vol_pct < 0.4:
            regime = 'low_vol'
        else:
            regime = 'oscillation'

        confidence = compute_confidence(ret_pct, vol_pct)
        signal = {"csi300_6m_ret": ret_6m, "csi300_60d_vol_pct": vol_pct, ...}

        upsert_regime_label(trade_date, regime, confidence, method, signal)
```

### §8.3 数据源

- `market.index_daily`（已存在，CSI300/CSI500/CSI1000 等指数 K 线）
- 不需要新加任何源数据

### §8.4 多方法并存

PRIMARY KEY (trade_date, source_method) 允许同一交易日多种方法并存（simple_quadrant / hmm_viterbi / bbq）便于对比 / ensemble。

### §8.5 第一版方法

仅实现 `simple_quadrant`。HMM Viterbi（已有 `hmm_viterbi_forward_filter_fix` 修复版）+ BBQ 留 P2 批次。

### §8.6 与 paper_v2_daily_snapshot 的关联

`paper_v2_daily_snapshot.regime` 字段在 ETL 时 join `market.regime_label` 拉值。`source_method` 默认取 'simple_quadrant'（可配置）。

---

## §9 与 Codex Phase 0-7 的边界

### §9.1 必须协商（4 项 schema 变更）

| # | 改动 | Codex 协商点 |
|---|---|---|
| 1 | qe_archive 新增 18 张 paper_v2_* 表族（含 paper_v2_run 独立主表 + 17 子表，**NOT 共享 qe_archive.run**） | qe_archive schema 治理是 Codex 主体；新增表必须同意；独立主表设计避免 grain 冲突 |
| 2 | qe_archive 新增 1 张 factor_value 表 | 同上 |
| 3 | outbox_event 新增 4 类 event_type（paper.* / factor.*） | event_type 命名空间需 Codex 同意 |
| 4 | qe_archive worker 注册新 handler（PaperV2ArchiveHandler / FactorValueArchiveHandler） | worker.py 注册位置需 Codex 同意 |

### §9.2 不需要 Codex 协商（2 项）

| # | 改动 | 工作面 |
|---|---|---|
| 1 | paper_v2.daemon 改为直连 PG outbox（路径 A） | paper_v2 工作面（D1 边界内） |
| 2 | market.regime_label 新表 + 计算脚本 | 市场数据 schema，不是 Codex 工作面 |

### §9.3 因子库 emit 事件 hook（边界灰色）

`factor_pipeline_v2._save_metrics()` 完成后 emit `factor.recompute.completed`，需要在因子库代码里加 hook。

- 如果因子库归 RDAgent 主线团队 → 不需要 Codex 协商
- 如果因子库归 Codex Phase 0-7 → 需 Codex 同意
- **建议在 D5 协商时一并问清楚**

---

## §10 [DECISION] D5 cross-tool 草案（v2 — D1=a 精化后）

### §10.1 草案内容（用户已 ratify 文档，待发给 Codex）

```
[DECISION] D5 - qe_archive schema extension for paper_v2 + factor_value capture

Hello Codex. Claude Code strategy session requesting your review on extending qe_archive schema to capture paper_v2 simulation runtime + factor pipeline data into the warehouse. Full design at main branch docs/architecture/data_warehouse_extension_design_20260510.md.

# Background

paper_v2 (21 runtime tables) and factor values (single/factor_name.parquet) currently do not flow into the warehouse. User ratified:
- Warehouse = qe_archive (no new schema)
- Event-driven capture (reuse outbox + worker + archive_job)
- All 21 paper_v2 tables in scope
- Factor values follow factor library recompute (event-driven)
- regime_label placed under market schema (not qe_archive)
- D1 boundary already accepted: paper_v2 portfolio_run does NOT share qe_archive.run main table due to grain mismatch (research run is logical_experiment_id x attempt_no, paper_v2 run is portfolio_id x trade_date). New independent paper_v2_run main table proposed instead.

# 4 specific protocol points

## D5.Q1: qe_archive new tables - 18 paper_v2_* family + 1 factor_value

Naming convention adopted (paper_v2_ prefix family, NOT sharing qe_archive.run):
- 1 main table: paper_v2_run (independent main table, grain = portfolio_id x trade_date, UNIQUE on natural key)
- 3 SCD2 dimensions: dim_paper_v2_portfolio / dim_paper_v2_runtime_profile / dim_paper_v2_runtime_profile_version
- 13 fact tables: paper_v2_session / paper_v2_session_day / paper_v2_order / paper_v2_order_execution_state / paper_v2_fill / paper_v2_position_snapshot / paper_v2_daily_snapshot / paper_v2_intraday_snapshot / paper_v2_cash_ledger / paper_v2_error / paper_v2_runtime_config_activation / paper_v2_execution_policy_activation / paper_v2_reset_audit
- 5 append-only event tables: paper_v2_session_event / paper_v2_run_event / paper_v2_order_event / paper_v2_config_change_audit / paper_v2_broker_error (the 5th may merge into paper_v2_error subject to schema review)
- 1 factor value: factor_value (PARTITION BY RANGE trade_date, monthly partitions)

Q1.a: Approve adding these tables under qe_archive schema?
Q1.b: Name conventions (paper_v2_* / dim_paper_v2_* / factor_value) compatible with your Phase 0-7 design?
Q1.c: Any field or table already reserved by your Phase 0-7 plan that conflicts?
Q1.d: Who writes the DDL: Claude Code strategy session drafts and you review + merge to main, or you take over drafting?

## D5.Q2: outbox_event new event_types

New types added (no schema change to outbox_event table itself):
- paper.portfolio_run.completed
- paper.daily_snapshot.captured
- paper.config.changed
- factor.recompute.completed

Q2.a: event_type names (paper.* / factor.* prefix) conflict with your existing qe.* namespace?
Q2.b: payload schema needs to follow your Phase 0-7 standardized template?
Q2.c: retry / timeout / dead-letter strategies reuse existing worker config?

## D5.Q3: worker registers new handlers

PaperV2ArchiveHandler / FactorValueArchiveHandler registration location:
- Option a: handler files at backend/services/qe_archive/handlers/, registered by worker.py at startup (coexist with existing qe handlers)
- Option b: handlers in separate backend/services/paper_v2_archive/, worker.py loads via plugin discovery

Q3.a: choose a or b?
Q3.b: handler implementation code: you write / we write / split (you define interface + we implement)?
Q3.c: worker default disabled to enabled in production: any conflict with your Phase 0-7 cadence?

## D5.Q4: factor pipeline emit-hook workspace ownership

factor_pipeline_v2._save_metrics() will emit factor.recompute.completed event after success.

Q4.a: factor library (factor_pipeline_v2 / qe_factors / aistock_factor_catalog) under your Phase 0-7 or RDAgent main?
Q4.b: who adds the emit hook (affects next-batch workspace assignment)?
Q4.c: emit frequency upper bound to prevent recompute storms triggering warehouse pressure?

# Claude Code commitments / boundaries

- Will not modify existing 13 qe_archive tables schema
- Will not modify outbox_event / archive_job tables themselves (only add event_types)
- Will not modify worker.py main framework (only register_handler)
- daemon SQLite to PG outbox is done in paper_v2 workspace (D2 path A, T6.2 dispatched)
- market.regime_label is done in market data workspace (separate from qe_archive)
- paper_v2_run is independent main table, NOT sharing qe_archive.run, so research-side leaderboard is not polluted

# Reference commits and drawers

- Doc: docs/architecture/data_warehouse_extension_design_20260510.md (commit hash to be appended after push)
- N1+N2 fix landed: origin/fix/rl_execution_module_visibility-20260510 (commits da6673c + 6275e9d)
- T4 audit summary drawer: 5888d73fb9882664d531760e
- T1 model_params_origin field added to paper_v2.run (will propagate to paper_v2_run)

# Timeline suggested

- Within 2 days after your reply, Claude Code completes DDL SQL files + handler interface contract doc
- After paper_v2 D2.b merges to main, schema migration within 1 week (user authorization + backup)
- Integration point: D2.b merge + first paper_v2 simulation run

Reply format: 4 items yes / no / qualified-yes + any caveats.

-- Claude Code strategy session 2026-05-10
   Ref: docs/architecture/data_warehouse_extension_design_20260510.md
```

---

## §11 联调点 + 5 里程碑时间线

```
M1（今天 → 明天）：本文档审完 + 你 ratify
   - 用户审 §1-§12 全部章节
   - 拍板 §12 剩余项
   - 我发 [DECISION] D5 给 Codex

M2（明天 → 后天）：Codex 协商完成
   - Codex 回 D5 4 项答复
   - 双方达成边界
   - 我更新 §10 草案为最终版（合并 Codex 修订）
   - 文档 v2.0 进 main

M3（M2 完成 → 1 周）：双方并行开发
   - 新开 dw-foundation worktree（含 regime_label + factor_value handler）
   - paper_v2 团队：daemon 直连 PG outbox（路径 A）+ source 端 emit_paper_event 调用
   - Codex 工作面（如承接）：qe_archive 17 张表 DDL + handler 注册
   - 我（战略）：监督 + 集成 review

M4（M3 完成 → 2 天）：联调点 1（关键）
   - D4 用户操作完成（DB migration / 8001 重启 / 浏览器手测）
   - D2.b paper_v2 合 main
   - 启动第一次模拟盘运行
   - paper_v2 → outbox_event 写入验证
   - worker pull → handler dispatch → qe_archive.run + paper_v2_* 写入验证
   - join market.regime_label 验证 regime 字段填充正确
   - 重跑 ETL 行数不变（幂等验证）

M5（M4 完成 → 1 周）：联调点 2 + 上线
   - 因子库下次重算时 emit factor.recompute.completed 验证
   - factor_value 入库 + 增量去重验证
   - 60 天 paper_v2 数据 → qe_archive 完整保留对照
   - Worker 监控告警接入
   - 文档 v3.0 含联调记录 + 已知问题进 main
```

---

## §12 用户已拍板项摘要 + 剩余拍板项

### §12.1 已拍板（v2 文档落地依据）

| # | 项 | 决策 |
|---|---|---|
| D1 | schema 命名修正 | (a) qe_archive 内 paper_v2_* 独立表族（含 paper_v2_run 独立主表），NOT 共享 qe_archive.run |
| D2 | T2 daemon to PG 路径 | A：直连 PG outbox + SQLite fallback + 启动时 replay |
| D3 | T1 default-trickery 妥协 | 接受：PaperRun.model_params_origin default 'node' + TODO + update_run_model_params_origin 回填方法 |
| D4 | A2 BLOCKING 字段补齐 T5+T6 启动 | 启动（与 T7 同批派给 paper-v2 Lead） |
| T7 | enable_paper() 路径 audit | 加入 D4 同批 |
| N1 | RDAgent .gitignore + rl_execution module 修复 | 由 Claude Code 战略 session 直接做（已完成：origin/fix/rl_execution_module_visibility-20260510，commits da6673c + 6275e9d） |
| N2 | 基础设施 fallback 永久层归属 | RDAgent main 同批做（合并到 N1，已完成 commit 6275e9d） |
| §12.1 | DDL SQL 文件 | 独立文件 `backend/db/init_qe_archive_paper_v2_extension_20260510.sql` |
| §12.2 | dw-foundation worktree | 独立小团队 |
| §12.3 | factor_value 分区 | 按月分区 |
| §12.4 | Worker 启动方式 | systemd 单元 + ENV `QE_ARCHIVE_WORKER_ENABLED=true` |
| §12.5 | D2.b 与 schema migration 节奏 | paper_v2 D2.b 先合 → 再启动数仓 schema migration |
| §12.7 | 本文档 commit + push main | 是（D1=a 修订完成后立即合 main） |

### §12.2 剩余拍板（等 Codex D5 答复后再拍）

1. **handler 注册位置**（D5.Q3.a）：(a) qe_archive/handlers/ vs (b) 独立 paper_v2_archive/——等 Codex 表态
2. **DDL 起草分工**（D5.Q1.d）：Claude Code 战略起草 vs Codex 接手——等 Codex 表态
3. **因子库 hook 工作面**（D5.Q4.a）：Codex Phase 0-7 vs RDAgent 主线——等 Codex 确认
4. **dw-foundation worktree 启动时机**：D5 Codex 答复后立即建（推荐）vs 等 paper_v2 D2.b 合 main 后建

---

**结束**。文档 v2.0（D1=a 修订）。下次会话或 cross-tool 协商基于本文档。
