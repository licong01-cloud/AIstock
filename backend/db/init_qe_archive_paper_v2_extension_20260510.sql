-- T12: qe_archive paper_v2_* extension + factor_value DDL
-- Per data_warehouse_extension_design_20260510.md §5 + §3.2.3 (commit de26e5a)
-- Status: DRAFT — awaiting Codex review per D5 Q1.d (drawer 9cd6d6bb...)
--
-- Scope:
--   1 main table          : paper_v2_run                              (§5.0)
--   3 SCD2 dimensions     : dim_paper_v2_portfolio                    (§5.1)
--                            dim_paper_v2_runtime_profile             (§5.15)
--                            dim_paper_v2_runtime_profile_version     (§5.16)
--   9 fact tables         : paper_v2_session                          (§5.2)
--                            paper_v2_session_day                     (§5.3)
--                            paper_v2_order                           (§5.4)
--                            paper_v2_order_execution_state           (§5.6)
--                            paper_v2_fill                            (§5.7, partitioned)
--                            paper_v2_position_snapshot               (§5.8)
--                            paper_v2_daily_snapshot                  (§5.9)
--                            paper_v2_intraday_snapshot               (§5.10)
--                            paper_v2_cash_ledger                     (§5.11)
--   3 config/audit facts  : paper_v2_runtime_config_activation        (§5.17)
--                            paper_v2_execution_policy_activation     (§5.17)
--                            paper_v2_reset_audit                     (§5.19)
--   4 event tables        : paper_v2_order_event                      (§5.5)
--                            paper_v2_session_event                   (§5.13)
--                            paper_v2_run_event                       (§5.14)
--                            paper_v2_config_change_audit             (§5.18)
--   1 error table         : paper_v2_error                            (§5.12, includes BrokerBackendError per §3.2.2)
--   1 factor value        : factor_value                              (§7.2, partitioned)
--
-- Total new tables: 22 (1 + 3 + 9 + 3 + 4 + 1 + 1 = 22)
-- Note on count vs §3.2.1 "20 paper_v2_* + 1 factor_value": this file emits the union of
-- every table enumerated in design §5 plus factor_value. The design §3.2.1 tally appears to
-- omit paper_v2_error from its sub-totals while still requiring its DDL in §5.12. Codex review
-- should reconcile: keep paper_v2_error as fact-style append (current) vs. classify as event.
-- paper_v2_broker_error is NOT created (per §3.2.2 — merged into paper_v2_error.error_class).
--
-- Boundaries:
--   * No BEGIN/COMMIT — migration script wraps in transaction at apply time.
--   * No DROP — idempotent CREATE IF NOT EXISTS.
--   * No data writes, no SELECT, no schema changes outside qe_archive.
--   * Existing 13 qe_archive.* tables (run / outbox_event / archive_job / ...) untouched.
--   * Not yet applied to any database.
--
-- Naming: paper_v2_* / dim_paper_v2_* prefix family (per Codex D5 Q1.b ack).
-- All FK to paper_v2_run use ON DELETE CASCADE so a run rollback cascades cleanly.

CREATE SCHEMA IF NOT EXISTS qe_archive;

-- ============================================================================
-- §5.1  dim_paper_v2_portfolio (SCD2 dimension)
--       Created BEFORE paper_v2_run because paper_v2_run.portfolio_version_id
--       references this table.
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.dim_paper_v2_portfolio (
    portfolio_version_id BIGSERIAL PRIMARY KEY,
    portfolio_id         TEXT         NOT NULL,
    manifest_sha256      TEXT         NOT NULL,
    broker_backend       TEXT         NOT NULL,
    data_source          TEXT         NOT NULL,
    package_id           TEXT,
    initial_cash         NUMERIC(18,4),
    fee_policy_json      JSONB        NOT NULL DEFAULT '{}'::jsonb,
    risk_policy_json     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    valid_from           TIMESTAMPTZ  NOT NULL,
    valid_to             TIMESTAMPTZ,
    is_current           BOOLEAN      NOT NULL DEFAULT TRUE,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_paper_v2_portfolio_natural
        UNIQUE (portfolio_id, manifest_sha256, broker_backend, valid_from)
);

COMMENT ON TABLE qe_archive.dim_paper_v2_portfolio IS
    'paper_v2 portfolio 维度 SCD2 表。自然键 = (portfolio_id, manifest_sha256, broker_backend)，'
    'broker / manifest 切换 → 新版本行；valid_to + is_current 维护历史窗口。'
    '由 paper.portfolio_run.completed + paper.config.changed 双触发更新。';

COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.portfolio_version_id IS
    'SCD2 surrogate key，paper_v2_run.portfolio_version_id 等子表 FK 指向此列。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.portfolio_id IS
    '业务自然键（来自 paper_v2.portfolio.id）。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.manifest_sha256 IS
    '冻结策略包 manifest 指纹。Manifest 变化即视为新版本（即使 portfolio 名不变）。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.broker_backend IS
    'localsim / miniqmtsim / qmt_real ... broker 切换需要新建版本以正确归因 fill 行为。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.data_source IS
    'TDX_REALTIME / DB_HISTORICAL / MINIQMT_REALTIME，影响 fill_market_context 解释。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_portfolio.is_current IS
    'TRUE = 当前生效版本；同一 portfolio_id 仅允许一行 is_current=TRUE（由 ETL 维护）。';

CREATE INDEX IF NOT EXISTS ix_dim_paper_v2_portfolio_current
    ON qe_archive.dim_paper_v2_portfolio (portfolio_id)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS ix_dim_paper_v2_portfolio_valid
    ON qe_archive.dim_paper_v2_portfolio (portfolio_id, valid_from DESC);


-- ============================================================================
-- §5.0  paper_v2_run (independent main table — D1=a, NOT sharing qe_archive.run)
--       grain = portfolio_id × trade_date
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_run (
    run_id               TEXT         PRIMARY KEY,
    portfolio_id         TEXT         NOT NULL,
    trade_date           DATE         NOT NULL,
    portfolio_version_id BIGINT       REFERENCES qe_archive.dim_paper_v2_portfolio(portfolio_version_id),
    package_id           TEXT,
    manifest_sha256      TEXT,
    broker_backend       TEXT         NOT NULL,
    data_source          TEXT         NOT NULL,
    node_id              TEXT,
    model_params_origin  TEXT         NOT NULL DEFAULT 'node',
    status               TEXT         NOT NULL,
    started_at           TIMESTAMPTZ,
    completed_at         TIMESTAMPTZ,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_paper_v2_run_natural UNIQUE (portfolio_id, trade_date),
    CONSTRAINT ck_paper_v2_run_status CHECK (
        -- Source paper_v2.run.status uses uppercase enum (probed values: SUCCEEDED, FAILED).
        -- Forward-compat list per Codex review fix round 2: keep PENDING + RUNNING for in-flight rows,
        -- INTERRUPTED for daemon-killed runs that may surface in future emit pipelines.
        status IN ('PENDING','RUNNING','SUCCEEDED','FAILED','INTERRUPTED')
    ),
    CONSTRAINT ck_paper_v2_run_origin CHECK (
        model_params_origin IN ('node','cache','unavailable')
    )
);

COMMENT ON TABLE qe_archive.paper_v2_run IS
    'paper_v2 模拟盘运行主表。D1=a 决议: 与 qe_archive.run 解耦，'
    'grain = portfolio_id × trade_date（与 research run 的 logical_experiment_id × attempt_no 不同），'
    '避免 leaderboard 字段（attempt_no / score_total）语义污染。'
    '所有 paper_v2_* 子表的 run_id FK 指向此表，ON DELETE CASCADE。';

COMMENT ON COLUMN qe_archive.paper_v2_run.run_id IS
    'paper_v2.run.id::TEXT。UUID 转文本以便统一 outbox source_sub_id 类型。';
COMMENT ON COLUMN qe_archive.paper_v2_run.portfolio_id IS
    '模拟盘 ID（grain 维度 1）。';
COMMENT ON COLUMN qe_archive.paper_v2_run.trade_date IS
    '交易日（grain 维度 2）。';
COMMENT ON COLUMN qe_archive.paper_v2_run.portfolio_version_id IS
    'SCD2 维度 FK，指向 dim_paper_v2_portfolio 当前生效版本。';
COMMENT ON COLUMN qe_archive.paper_v2_run.manifest_sha256 IS
    '冻结策略指纹冗余（也存在 dim 表）。便于无 join 直接审计。';
COMMENT ON COLUMN qe_archive.paper_v2_run.broker_backend IS
    'localsim / miniqmtsim / ... broker backend 类型。';
COMMENT ON COLUMN qe_archive.paper_v2_run.data_source IS
    'TDX_REALTIME / DB_HISTORICAL / MINIQMT_REALTIME 数据源类型。';
COMMENT ON COLUMN qe_archive.paper_v2_run.model_params_origin IS
    'T1 字段: node = 节点直拉模型参数 / cache = 命中缓存 / unavailable = 模型参数取不到。';
COMMENT ON COLUMN qe_archive.paper_v2_run.captured_at IS
    'ETL 时间戳。源 paper_v2.run.created_at / completed_at 单独保留。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_run_portfolio_date
    ON qe_archive.paper_v2_run (portfolio_id, trade_date DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_run_status_completed
    ON qe_archive.paper_v2_run (status, completed_at DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_run_package
    ON qe_archive.paper_v2_run (package_id, trade_date DESC)
    WHERE package_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_paper_v2_run_node
    ON qe_archive.paper_v2_run (node_id, trade_date DESC)
    WHERE node_id IS NOT NULL;


-- ============================================================================
-- §5.15  dim_paper_v2_runtime_profile (SCD2 dimension)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.dim_paper_v2_runtime_profile (
    profile_version_id BIGSERIAL PRIMARY KEY,
    profile_id         TEXT         NOT NULL,
    profile_name       TEXT,
    profile_json       JSONB        NOT NULL DEFAULT '{}'::jsonb,
    valid_from         TIMESTAMPTZ  NOT NULL,
    valid_to           TIMESTAMPTZ,
    is_current         BOOLEAN      NOT NULL DEFAULT TRUE,
    captured_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_paper_v2_runtime_profile_natural
        UNIQUE (profile_id, valid_from)
);

COMMENT ON TABLE qe_archive.dim_paper_v2_runtime_profile IS
    'paper_v2 runtime_profile SCD2 维度。自然键 = (profile_id, valid_from)。'
    'profile_json 存完整 profile 配置；profile.changed 事件触发新版本。';

COMMENT ON COLUMN qe_archive.dim_paper_v2_runtime_profile.profile_version_id IS
    'SCD2 surrogate key。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_runtime_profile.profile_id IS
    '业务自然键（来自 paper_v2.runtime_profile.id）。';

CREATE INDEX IF NOT EXISTS ix_dim_paper_v2_runtime_profile_current
    ON qe_archive.dim_paper_v2_runtime_profile (profile_id)
    WHERE is_current;

CREATE INDEX IF NOT EXISTS ix_dim_paper_v2_runtime_profile_valid
    ON qe_archive.dim_paper_v2_runtime_profile (profile_id, valid_from DESC);


-- ============================================================================
-- §5.16  dim_paper_v2_runtime_profile_version (immutable version log)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.dim_paper_v2_runtime_profile_version (
    version_pk        BIGSERIAL PRIMARY KEY,
    version_id        TEXT         NOT NULL UNIQUE,
    profile_id        TEXT         NOT NULL,
    version_number    INTEGER      NOT NULL,
    config_diff_json  JSONB        NOT NULL DEFAULT '{}'::jsonb,
    created_by        TEXT,
    created_at        TIMESTAMPTZ  NOT NULL,
    captured_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_dim_paper_v2_profile_version_seq
        UNIQUE (profile_id, version_number)
);

COMMENT ON TABLE qe_archive.dim_paper_v2_runtime_profile_version IS
    'paper_v2 runtime_profile 版本号变更日志（immutable，append-only）。'
    '与 dim_paper_v2_runtime_profile 不同: 后者是 SCD2 当前/历史快照表，'
    '此表保留每次版本号递增及 diff，便于审计回溯。';

COMMENT ON COLUMN qe_archive.dim_paper_v2_runtime_profile_version.version_id IS
    'paper_v2.runtime_profile_version.id（外部唯一）。';
COMMENT ON COLUMN qe_archive.dim_paper_v2_runtime_profile_version.config_diff_json IS
    '本版与上一版的字段差异（用于审计 / 回滚 / 理解变更意图）。';

CREATE INDEX IF NOT EXISTS ix_dim_paper_v2_profile_version_profile
    ON qe_archive.dim_paper_v2_runtime_profile_version (profile_id, version_number DESC);


-- ============================================================================
-- §5.2  paper_v2_session
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_session (
    session_pk           BIGSERIAL PRIMARY KEY,
    run_id               TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT       REFERENCES qe_archive.dim_paper_v2_portfolio(portfolio_version_id),
    trade_session_id     TEXT         NOT NULL UNIQUE,
    trade_date           DATE         NOT NULL,
    mode                 TEXT,
    validated_execution_policy_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    started_at           TIMESTAMPTZ,
    ended_at             TIMESTAMPTZ,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_paper_v2_session_mode CHECK (
        mode IS NULL OR mode IN ('REPLAY_ONLY','LIVE_ONLY','CATCHUP_THEN_LIVE')
    )
);

COMMENT ON TABLE qe_archive.paper_v2_session IS
    'paper_v2 trade_session 镜像。grain = trade_session_id（自然键 UNIQUE）。'
    '一个 paper_v2_run 通常对应 1 个 trade_session（CATCHUP_THEN_LIVE 模式可能拆 2 段）。';

COMMENT ON COLUMN qe_archive.paper_v2_session.mode IS
    'REPLAY_ONLY = 回放历史 / LIVE_ONLY = 仅实时 / CATCHUP_THEN_LIVE = 先回放后接实时。';
COMMENT ON COLUMN qe_archive.paper_v2_session.validated_execution_policy_json IS
    '本 session 启动时校验通过的 execution policy 全文，便于复现 fill 行为。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_session_run
    ON qe_archive.paper_v2_session (run_id);
CREATE INDEX IF NOT EXISTS ix_paper_v2_session_date
    ON qe_archive.paper_v2_session (trade_date DESC);


-- ============================================================================
-- §5.3  paper_v2_session_day
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_session_day (
    session_day_pk            BIGSERIAL PRIMARY KEY,
    run_id                    TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_session_id          TEXT         NOT NULL,
    trade_date                DATE         NOT NULL,
    expected_bar_count        INTEGER,
    actual_bar_count          INTEGER,
    latest_available_bar_time TIMESTAMPTZ,
    data_quality              TEXT,
    captured_at               TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_paper_v2_session_day_natural UNIQUE (trade_session_id, trade_date),
    CONSTRAINT ck_paper_v2_session_day_quality CHECK (
        data_quality IS NULL OR data_quality IN ('ok','low_coverage','partial','missing')
    )
);

COMMENT ON TABLE qe_archive.paper_v2_session_day IS
    'session × trade_date 行情覆盖摘要。expected vs actual bar_count 用于侦测数据缺口。';

COMMENT ON COLUMN qe_archive.paper_v2_session_day.expected_bar_count IS
    '该交易日预期分钟 bar 数（A 股通常 240）。';
COMMENT ON COLUMN qe_archive.paper_v2_session_day.actual_bar_count IS
    '实际收到的分钟 bar 数。actual < expected → data_quality = low_coverage / partial。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_session_day_run
    ON qe_archive.paper_v2_session_day (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_session_day_quality
    ON qe_archive.paper_v2_session_day (data_quality, trade_date)
    WHERE data_quality IS NOT NULL AND data_quality <> 'ok';


-- ============================================================================
-- §5.4  paper_v2_order
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_order (
    order_pk             BIGSERIAL PRIMARY KEY,
    run_id               TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT       REFERENCES qe_archive.dim_paper_v2_portfolio(portfolio_version_id),
    order_id             TEXT         NOT NULL UNIQUE,
    trade_session_id     TEXT,
    trade_date           DATE         NOT NULL,
    symbol               TEXT         NOT NULL,
    side                 TEXT         NOT NULL,
    order_type           TEXT         NOT NULL,
    quantity             BIGINT       NOT NULL,
    price                NUMERIC(18,4),
    status               TEXT         NOT NULL,
    placed_at            TIMESTAMPTZ,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_paper_v2_order_side CHECK (side IN ('BUY','SELL')),
    CONSTRAINT ck_paper_v2_order_type CHECK (
        order_type IN ('LIMIT','MARKET','STOP','STOP_LIMIT')
    ),
    -- NEW per Codex review fix round 2: ADD CHECK on order.status using source enum
    -- (probed values: FILLED, PARTIALLY_FILLED, SUBMITTED) plus forward-compat tail.
    CONSTRAINT ck_paper_v2_order_status CHECK (
        status IN ('SUBMITTED','PARTIALLY_FILLED','FILLED','CANCELLED','REJECTED','EXPIRED')
    )
);

COMMENT ON TABLE qe_archive.paper_v2_order IS
    'paper_v2 orders 镜像。order_id 自然键 UNIQUE，重复入库幂等。'
    '终态字段 status / quantity / price 反映该 order 的最终镜像（中间事件落 paper_v2_order_event）。';

COMMENT ON COLUMN qe_archive.paper_v2_order.order_type IS
    'LIMIT / MARKET / STOP / STOP_LIMIT。当前生产几乎全 MARKET（参 §5.7 fill.intended_price 注释）。';
COMMENT ON COLUMN qe_archive.paper_v2_order.price IS
    'LIMIT 订单的指定价；MARKET 订单为 NULL（结构性 NULL，不是缺失数据）。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_order_run_date
    ON qe_archive.paper_v2_order (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_order_symbol
    ON qe_archive.paper_v2_order (symbol, trade_date DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_order_session
    ON qe_archive.paper_v2_order (trade_session_id)
    WHERE trade_session_id IS NOT NULL;


-- ============================================================================
-- §5.5  paper_v2_order_event (append-only event)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_order_event (
    event_pk      BIGSERIAL PRIMARY KEY,
    event_id      TEXT         NOT NULL UNIQUE,
    order_id      TEXT         NOT NULL,
    run_id        TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_date    DATE,
    event_type    TEXT         NOT NULL,
    event_payload JSONB        NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   TIMESTAMPTZ  NOT NULL,
    captured_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT ck_paper_v2_order_event_type CHECK (
        -- Source paper_v2.order_events.event_type uses uppercase enum (probed values:
        -- NO_FILL, PARTIALLY_FILLED, FILLED). Keep SUBMITTED/CANCELLED/REJECTED for
        -- forward-compat with order lifecycle events that have not surfaced yet on prod.
        event_type IN (
            'SUBMITTED','PARTIALLY_FILLED','FILLED','NO_FILL','CANCELLED','REJECTED'
        )
    )
);

COMMENT ON TABLE qe_archive.paper_v2_order_event IS
    'paper_v2 order_events 镜像（append-only）。event_id 自然键 UNIQUE。'
    'order 全生命周期事件序列；终态镜像见 paper_v2_order / paper_v2_order_execution_state。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_order_event_order
    ON qe_archive.paper_v2_order_event (order_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_paper_v2_order_event_run_date
    ON qe_archive.paper_v2_order_event (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_order_event_type
    ON qe_archive.paper_v2_order_event (event_type, occurred_at);


-- ============================================================================
-- §5.6  paper_v2_order_execution_state (terminal state)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_order_execution_state (
    state_pk              BIGSERIAL PRIMARY KEY,
    order_id              TEXT         NOT NULL UNIQUE,
    run_id                TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_date            DATE,
    algo_code             TEXT,
    final_algo_state_json JSONB        NOT NULL DEFAULT '{}'::jsonb,
    filled_quantity       BIGINT,
    captured_at           TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_order_execution_state IS
    '执行算法终态镜像（每 order 一行）。algo_code 标识使用的执行算法版本（V24 / V25 / V26 / TWAP / VWAP ...）。'
    'final_algo_state_json 持久化算法终态用于回放/调试。';

COMMENT ON COLUMN qe_archive.paper_v2_order_execution_state.algo_code IS
    '执行算法标识符（如 V24_B1 / TWAP_5MIN）。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_order_execution_state_run_date
    ON qe_archive.paper_v2_order_execution_state (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_order_execution_state_algo
    ON qe_archive.paper_v2_order_execution_state (algo_code, trade_date DESC)
    WHERE algo_code IS NOT NULL;


-- ============================================================================
-- §5.7  paper_v2_fill (PARTITIONED BY RANGE trade_date)
--       Partition example for 2026-05; production deploys pg_partman.
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_fill (
    fill_pk              BIGSERIAL,
    fill_id              TEXT         NOT NULL,
    run_id               TEXT         NOT NULL,
    portfolio_version_id BIGINT,
    order_id             TEXT         NOT NULL,
    trade_date           DATE         NOT NULL,
    trade_session_id     TEXT,
    symbol               TEXT         NOT NULL,
    side                 TEXT,
    filled_quantity      BIGINT,
    fill_price           NUMERIC(18,4),
    fill_value           NUMERIC(18,4),
    fees                 NUMERIC(18,4),
    slippage_bps         NUMERIC(10,2),
    broker_backend       TEXT,
    algo_code            TEXT,
    intended_price       NUMERIC(18,4),
    fill_market_context  JSONB,
    filled_at            TIMESTAMPTZ  NOT NULL,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (fill_pk, trade_date),
    CONSTRAINT uq_paper_v2_fill_natural UNIQUE (fill_id, trade_date),
    CONSTRAINT ck_paper_v2_fill_side CHECK (side IS NULL OR side IN ('BUY','SELL'))
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE qe_archive.paper_v2_fill IS
    'paper_v2 fills 镜像，按 trade_date 月分区。fill_id 自然键 UNIQUE（含 trade_date 以满足分区表约束要求）。'
    '体量预估：日均 ~10K 行 → 月分区 ~250K 行；按月清理/归档便于成本管理。'
    '生产部署用 pg_partman 自动管理分区；本文件仅给一个月示例。';

COMMENT ON COLUMN qe_archive.paper_v2_fill.intended_price IS
    'T6.1 修正：当前生产几乎全 MARKET 订单 → intended_price 为结构性 NULL（first-class signal，'
    '表示 "该 fill 来自 MARKET 订单"）。未来支持 LIMIT 后该列才会有值。';
COMMENT ON COLUMN qe_archive.paper_v2_fill.slippage_bps IS
    '仅当 intended_price IS NOT NULL 时计算 (fill_price - intended_price) / intended_price * 10000；'
    '否则 NULL（MARKET 订单无 reference 价）。';
COMMENT ON COLUMN qe_archive.paper_v2_fill.fill_market_context IS
    'T6.1 揭露的真实 key 集（与早期文档假设的 bid/ask/spread 不同）：'
    '{stock_id, trade_date, data_source, prev_close, limit_up, limit_down, suspend_status, '
    'full_day_open/close/volume/high/low, generated_at, [day_features_*] (V25)}。'
    '由 backend/services/paper_trading_v2/market_data.py:692 _build_market_context 产出。';
COMMENT ON COLUMN qe_archive.paper_v2_fill.broker_backend IS
    '冗余 broker 信息（也存在 paper_v2_run），便于 fill 维度无 join 直接审计。';

-- 注: PARTITION 分区下 FK ON DELETE CASCADE 由父 paper_v2_run 触发；
-- 分区表本身不设跨表 FK（PG 限制：partitioned table 不支持指向其他分区表的 FK）。
-- run_id 一致性在 ETL 层保证 + 应用层 JOIN 验证。

-- 月分区示例（2026-05）。生产由 pg_partman 自动管理。
CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_fill_y2026m05
    PARTITION OF qe_archive.paper_v2_fill
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

-- DEFAULT 兜底分区 per Codex review fix round 2 (P1.5):
--   Batch A imported 8243 fills spanning multiple months; without a DEFAULT partition
--   any INSERT outside 2026-05-01..2026-06-01 fails with 'no partition for row'.
--   Production rollout will replace this with pg_partman-managed monthly partitions
--   covering the full historical range; until then DEFAULT prevents apply / smoke failures.
CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_fill_default
    PARTITION OF qe_archive.paper_v2_fill DEFAULT;

CREATE INDEX IF NOT EXISTS ix_paper_v2_fill_run_date
    ON qe_archive.paper_v2_fill (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_fill_symbol_date
    ON qe_archive.paper_v2_fill (symbol, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_fill_order
    ON qe_archive.paper_v2_fill (order_id);
CREATE INDEX IF NOT EXISTS ix_paper_v2_fill_algo
    ON qe_archive.paper_v2_fill (algo_code, trade_date DESC)
    WHERE algo_code IS NOT NULL;


-- ============================================================================
-- §5.8  paper_v2_position_snapshot (daily, per symbol)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_position_snapshot (
    snapshot_pk          BIGSERIAL PRIMARY KEY,
    run_id               TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT,
    trade_date           DATE         NOT NULL,
    symbol               TEXT         NOT NULL,
    quantity             BIGINT,
    cost_basis           NUMERIC(18,4),
    market_value         NUMERIC(18,4),
    unrealized_pnl       NUMERIC(18,4),
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_paper_v2_position_snapshot_natural
        UNIQUE (run_id, trade_date, symbol)
);

COMMENT ON TABLE qe_archive.paper_v2_position_snapshot IS
    '每日收盘持仓快照（每 run × trade_date × symbol 一行）。'
    '由 paper.daily_snapshot.captured 事件触发入库。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_position_snapshot_date_symbol
    ON qe_archive.paper_v2_position_snapshot (trade_date, symbol);
CREATE INDEX IF NOT EXISTS ix_paper_v2_position_snapshot_symbol
    ON qe_archive.paper_v2_position_snapshot (symbol, trade_date DESC);


-- ============================================================================
-- §5.9  paper_v2_daily_snapshot (per portfolio per day)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_daily_snapshot (
    daily_pk             BIGSERIAL PRIMARY KEY,
    run_id               TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT,
    trade_date           DATE         NOT NULL,
    total_value          NUMERIC(18,4),
    cash                 NUMERIC(18,4),
    positions_value      NUMERIC(18,4),
    realized_pnl         NUMERIC(18,4),
    unrealized_pnl       NUMERIC(18,4),
    benchmark_csi300     NUMERIC(10,4),
    benchmark_csi500     NUMERIC(10,4),
    benchmark_csi1000    NUMERIC(10,4),
    relative_to_csi300   NUMERIC(10,4),
    regime               TEXT,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_paper_v2_daily_snapshot_natural UNIQUE (run_id, trade_date)
);

COMMENT ON TABLE qe_archive.paper_v2_daily_snapshot IS
    '每日收盘组合级快照。ETL 时 join market.index_daily 拉 benchmark，'
    'join market.regime_label (source_method=simple_quadrant 默认) 拉 regime。';

COMMENT ON COLUMN qe_archive.paper_v2_daily_snapshot.regime IS
    '该交易日市场 regime 标签（bull/bear/oscillation/high_vol/low_vol），'
    '取自 market.regime_label。NULL 表示该日尚未生成标签。';
COMMENT ON COLUMN qe_archive.paper_v2_daily_snapshot.relative_to_csi300 IS
    '当日组合收益率 - CSI300 收益率（超额）。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_daily_snapshot_date
    ON qe_archive.paper_v2_daily_snapshot (trade_date DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_daily_snapshot_run
    ON qe_archive.paper_v2_daily_snapshot (run_id);
CREATE INDEX IF NOT EXISTS ix_paper_v2_daily_snapshot_regime
    ON qe_archive.paper_v2_daily_snapshot (regime, trade_date DESC)
    WHERE regime IS NOT NULL;


-- ============================================================================
-- §5.10  paper_v2_intraday_snapshot
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_intraday_snapshot (
    snapshot_pk     BIGSERIAL PRIMARY KEY,
    snapshot_id     TEXT         NOT NULL UNIQUE,
    run_id          TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_date      DATE         NOT NULL,
    snapshot_time   TIMESTAMPTZ  NOT NULL,
    total_value     NUMERIC(18,4),
    cash            NUMERIC(18,4),
    positions_json  JSONB        NOT NULL DEFAULT '{}'::jsonb,
    captured_at     TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_intraday_snapshot IS
    '盘中快照（snapshot_id UNIQUE）。positions_json 含详细持仓。'
    '体量较大（每 run × 数十分钟节点），需注意磁盘消耗；'
    '后续可考虑按月分区或冷热分层。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_intraday_snapshot_run_date
    ON qe_archive.paper_v2_intraday_snapshot (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_intraday_snapshot_time
    ON qe_archive.paper_v2_intraday_snapshot (trade_date, snapshot_time);


-- ============================================================================
-- §5.11  paper_v2_cash_ledger (append-only)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_cash_ledger (
    ledger_pk            BIGSERIAL PRIMARY KEY,
    ledger_entry_id      TEXT         NOT NULL UNIQUE,
    run_id               TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    portfolio_version_id BIGINT,
    trade_date           DATE,
    entry_type           TEXT         NOT NULL,
    amount               NUMERIC(18,4) NOT NULL,
    balance_after        NUMERIC(18,4),
    related_order_id     TEXT,
    occurred_at          TIMESTAMPTZ  NOT NULL,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    -- entry_type CHECK REMOVED in Codex review fix round 2: source paper_v2.cash_ledger
    -- has no entry_type column (probed source schema: cash_id, run_id, portfolio_id, fill_id,
    -- trade_date, symbol, side, notional, fee, cash_delta, cash_after, created_at).
    -- Handler derives entry_type at archive time from (side, notional, fee, cash_delta).
    -- Downstream invariant enforced at handler, not at SQL CHECK, until source introduces
    -- a stable enum field. The entry_type column itself remains declared above as plain TEXT NOT NULL.
);

COMMENT ON TABLE qe_archive.paper_v2_cash_ledger IS
    '现金账本流水（append-only）。balance_after 冗余记录入账后余额便于审计；'
    '若 ETL 顺序有 race 不保证全局单调，仅作 best-effort 参考。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_cash_ledger_run_time
    ON qe_archive.paper_v2_cash_ledger (run_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_paper_v2_cash_ledger_entry_type
    ON qe_archive.paper_v2_cash_ledger (entry_type, occurred_at);
CREATE INDEX IF NOT EXISTS ix_paper_v2_cash_ledger_order
    ON qe_archive.paper_v2_cash_ledger (related_order_id)
    WHERE related_order_id IS NOT NULL;


-- ============================================================================
-- §5.12  paper_v2_error (含 BrokerBackendError 子类，per §3.2.2)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_error (
    error_pk         BIGSERIAL PRIMARY KEY,
    error_id         TEXT         NOT NULL UNIQUE,
    run_id           TEXT         REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_date       DATE,
    trade_session_id TEXT,
    error_class      TEXT         NOT NULL,
    error_code       TEXT,
    error_message    TEXT,
    stack_trace      TEXT,
    related_order_id TEXT,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    captured_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_error IS
    'paper_v2 errors 镜像（append-only）。统一捕获 BrokerBackendError 子类 + StrategyPackageError + 一般异常，'
    '不再单独建 paper_v2_broker_error（per design §3.2.2）。'
    'run_id 可空：daemon-level 异常可能没有关联 run。';

COMMENT ON COLUMN qe_archive.paper_v2_error.error_class IS
    'Python 异常类全名前缀，例如 BrokerBackendError / BrokerBackendOrderRejected / '
    'StrategyPackageError / GenericError。'
    '查询 broker 异常: WHERE error_class LIKE ''BrokerBackend%''。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_error_class_time
    ON qe_archive.paper_v2_error (error_class, occurred_at DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_error_run
    ON qe_archive.paper_v2_error (run_id, occurred_at)
    WHERE run_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_paper_v2_error_order
    ON qe_archive.paper_v2_error (related_order_id)
    WHERE related_order_id IS NOT NULL;


-- ============================================================================
-- §5.13  paper_v2_session_event (append-only)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_session_event (
    event_pk         BIGSERIAL PRIMARY KEY,
    event_id         TEXT         NOT NULL UNIQUE,
    run_id           TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_session_id TEXT,
    trade_date       DATE,
    event_type       TEXT         NOT NULL,
    event_payload    JSONB        NOT NULL DEFAULT '{}'::jsonb,
    occurred_at      TIMESTAMPTZ  NOT NULL,
    captured_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_session_event IS
    'paper_v2 session_events 镜像（append-only）。涵盖 session 启动/关闭/降级/错误等高层事件。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_session_event_session
    ON qe_archive.paper_v2_session_event (trade_session_id, occurred_at)
    WHERE trade_session_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_paper_v2_session_event_run_date
    ON qe_archive.paper_v2_session_event (run_id, trade_date);
CREATE INDEX IF NOT EXISTS ix_paper_v2_session_event_type
    ON qe_archive.paper_v2_session_event (event_type, occurred_at);


-- ============================================================================
-- §5.14  paper_v2_run_event (append-only)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_run_event (
    event_pk      BIGSERIAL PRIMARY KEY,
    event_id      TEXT         NOT NULL UNIQUE,
    run_id        TEXT         NOT NULL REFERENCES qe_archive.paper_v2_run(run_id) ON DELETE CASCADE,
    trade_date    DATE,
    event_type    TEXT         NOT NULL,
    event_payload JSONB        NOT NULL DEFAULT '{}'::jsonb,
    occurred_at   TIMESTAMPTZ  NOT NULL,
    captured_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_run_event IS
    'paper_v2 run_events 镜像（append-only）。涵盖 run 启停/状态变更/重试等高层事件。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_run_event_run_time
    ON qe_archive.paper_v2_run_event (run_id, occurred_at);
CREATE INDEX IF NOT EXISTS ix_paper_v2_run_event_type
    ON qe_archive.paper_v2_run_event (event_type, occurred_at);


-- ============================================================================
-- §5.17  paper_v2_runtime_config_activation
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_runtime_config_activation (
    activation_pk      BIGSERIAL PRIMARY KEY,
    activation_id      TEXT         NOT NULL UNIQUE,
    portfolio_id       TEXT         NOT NULL,
    profile_version_id TEXT,
    activated_at       TIMESTAMPTZ  NOT NULL,
    activated_by       TEXT,
    captured_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_runtime_config_activation IS
    'runtime_profile 切换激活记录（每次 portfolio 启用某 profile_version 落一行）。'
    'profile_version_id 是源端 paper_v2.runtime_profile_version.id（不是 archive surrogate），'
    '便于反查源数据。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_runtime_config_activation_portfolio
    ON qe_archive.paper_v2_runtime_config_activation (portfolio_id, activated_at DESC);


-- ============================================================================
-- §5.17  paper_v2_execution_policy_activation
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_execution_policy_activation (
    activation_pk BIGSERIAL PRIMARY KEY,
    activation_id TEXT         NOT NULL UNIQUE,
    portfolio_id  TEXT         NOT NULL,
    policy_sha256 TEXT,
    policy_json   JSONB        NOT NULL DEFAULT '{}'::jsonb,
    activated_at  TIMESTAMPTZ  NOT NULL,
    captured_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

COMMENT ON TABLE qe_archive.paper_v2_execution_policy_activation IS
    'execution_policy 激活记录。policy_sha256 是策略全文指纹（去重 + 审计）。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_execution_policy_activation_portfolio
    ON qe_archive.paper_v2_execution_policy_activation (portfolio_id, activated_at DESC);
CREATE INDEX IF NOT EXISTS ix_paper_v2_execution_policy_activation_sha
    ON qe_archive.paper_v2_execution_policy_activation (policy_sha256)
    WHERE policy_sha256 IS NOT NULL;


-- ============================================================================
-- §5.18  paper_v2_config_change_audit (append-only)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_config_change_audit (
    audit_pk       BIGSERIAL PRIMARY KEY,
    audit_id       TEXT         NOT NULL UNIQUE,
    portfolio_id   TEXT,
    change_type    TEXT         NOT NULL,
    old_value_json JSONB,
    new_value_json JSONB,
    changed_by     TEXT,
    changed_at     TIMESTAMPTZ  NOT NULL,
    captured_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    -- change_type CHECK rewritten per Codex review fix round 2: source
    -- paper_v2.config_change_audit.change_type uses ACTION enum (probed: CREATE, ACTIVATE),
    -- NOT subject-type as the column name suggests. Per brief decision (archive directly
    -- mirrors source, no transform layer), CHECK enforces the actual source enum.
    -- Forward-compat: DEACTIVATE, MODIFY for future paper_v2 source extensions.
    -- Downstream queries that need subject-type must inspect old_value_json/new_value_json
    -- payload (which carries the affected config subject).
    CONSTRAINT ck_paper_v2_config_change_audit_action CHECK (
        change_type IN ('CREATE','ACTIVATE','DEACTIVATE','MODIFY')
    )
);

COMMENT ON TABLE qe_archive.paper_v2_config_change_audit IS
    '配置变更审计（append-only）。一行 = 一次 portfolio 配置变更，old/new 全字段 JSONB 留档。'
    'paper.config.changed 事件触发入库。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_config_change_audit_portfolio
    ON qe_archive.paper_v2_config_change_audit (portfolio_id, changed_at DESC)
    WHERE portfolio_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_paper_v2_config_change_audit_type
    ON qe_archive.paper_v2_config_change_audit (change_type, changed_at DESC);


-- ============================================================================
-- §5.19  paper_v2_reset_audit (append-only)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.paper_v2_reset_audit (
    audit_pk             BIGSERIAL PRIMARY KEY,
    audit_id             TEXT         NOT NULL UNIQUE,
    portfolio_id         TEXT,
    reset_type           TEXT         NOT NULL,
    reset_reason         TEXT,
    snapshot_before_json JSONB,
    reset_at             TIMESTAMPTZ  NOT NULL,
    captured_at          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
    -- reset_type CHECK REMOVED in Codex review fix round 2: source paper_v2.reset_audit
    -- has no reset_type column at all (probed source schema: audit_id, portfolio_id, rerun_policy,
    -- start_date, end_date, confirm_text, deleted_counts, status, context, created_at).
    -- The archive models reset_type / reset_reason / snapshot_before_json as ETL-derived fields;
    -- broader source-vs-archive reconciliation deferred to a separate design pass. Until then,
    -- reset_type stays as the plain TEXT NOT NULL column declared above, populated by the handler
    -- from a deterministic function of (rerun_policy, deleted_counts).
);

COMMENT ON TABLE qe_archive.paper_v2_reset_audit IS
    'portfolio 重置审计。snapshot_before_json 留档重置前状态便于事后追溯。';

CREATE INDEX IF NOT EXISTS ix_paper_v2_reset_audit_portfolio
    ON qe_archive.paper_v2_reset_audit (portfolio_id, reset_at DESC)
    WHERE portfolio_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS ix_paper_v2_reset_audit_type
    ON qe_archive.paper_v2_reset_audit (reset_type, reset_at DESC);


-- ============================================================================
-- §7.2  factor_value (PARTITIONED BY RANGE trade_date)
-- ============================================================================

CREATE TABLE IF NOT EXISTS qe_archive.factor_value (
    value_pk       BIGSERIAL,
    factor_name    TEXT         NOT NULL,
    code_text_hash TEXT         NOT NULL,
    trade_date     DATE         NOT NULL,
    code           TEXT         NOT NULL,
    value          NUMERIC(18,8),
    snapshot_date  DATE         NOT NULL,
    captured_at    TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    PRIMARY KEY (value_pk, trade_date),
    CONSTRAINT uq_factor_value_natural
        UNIQUE (factor_name, code_text_hash, trade_date, code)
) PARTITION BY RANGE (trade_date);

COMMENT ON TABLE qe_archive.factor_value IS
    '因子值仓库，按 trade_date 月分区。'
    '自然键 = (factor_name, code_text_hash, trade_date, code)：因子代码不变重复入库无影响；'
    '因子代码变（hash 变）→ 自动新增一行（不覆盖旧版，保留多版本对比能力）。'
    '体量预估：500 因子 × 5000 股票 × 8 年 ≈ 50 亿行，~400 GB。'
    '生产部署用 pg_partman 自动管理分区 + 老分区压缩归档。'
    '由 factor.recompute.completed 事件触发（factor_pipeline_v2._save_metrics() 完成后 emit）。';

COMMENT ON COLUMN qe_archive.factor_value.factor_name IS
    '因子标识（业务名）。';
COMMENT ON COLUMN qe_archive.factor_value.code_text_hash IS
    '因子代码版本指纹（SHA256 或同等）。代码 diff → 新 hash → 多版本并存。';
COMMENT ON COLUMN qe_archive.factor_value.trade_date IS
    '因子值对应的交易日（分区键）。';
COMMENT ON COLUMN qe_archive.factor_value.code IS
    '股票代码（如 000001.SZ）。';
COMMENT ON COLUMN qe_archive.factor_value.snapshot_date IS
    '因子计算所用的快照基准日（PIT cutoff）。可能与 trade_date 不同（如 PIT 基本面因子）。';

-- 月分区示例（2026-05）。生产由 pg_partman 自动管理。
CREATE TABLE IF NOT EXISTS qe_archive.factor_value_y2026m05
    PARTITION OF qe_archive.factor_value
    FOR VALUES FROM ('2026-05-01') TO ('2026-06-01');

-- DEFAULT 兜底分区 per Codex review fix round 2 (P1.5):
--   factor_value source data spans 2018+ (per design §7.4 ~50B rows over 8 years);
--   without a DEFAULT partition, any INSERT outside the example month fails.
--   Production rollout will use pg_partman-managed monthly partitions over the full range.
CREATE TABLE IF NOT EXISTS qe_archive.factor_value_default
    PARTITION OF qe_archive.factor_value DEFAULT;

CREATE INDEX IF NOT EXISTS ix_factor_value_factor_date
    ON qe_archive.factor_value (factor_name, trade_date);
CREATE INDEX IF NOT EXISTS ix_factor_value_code_date
    ON qe_archive.factor_value (code, trade_date);
CREATE INDEX IF NOT EXISTS ix_factor_value_factor_hash
    ON qe_archive.factor_value (factor_name, code_text_hash);


-- ============================================================================
-- End of T12 DDL draft.
-- Pending review by Codex (D5 Q1.d). Migration script wrapping + apply pending
-- separate user authorization (no DB writes from this file alone).
-- ============================================================================
