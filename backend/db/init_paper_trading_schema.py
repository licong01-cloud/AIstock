"""初始化实盘演练模块数据库表（paper_trading schema）.

本脚本创建以下表：
- paper_trading.portfolio_config — 模拟盘实例配置
- paper_trading.trade_signals — T-1 信号 → T 执行
- paper_trading.daily_snapshot — 每日净值快照
- paper_trading.positions — 持仓明细
- paper_trading.trades — 交易记录（费用拆分）
- paper_trading.stock_pnl_summary — 个股盈亏汇总
- paper_trading.training_jobs — 训练任务记录
- paper_trading.factor_attribution — 因子贡献归因
- model_live_track — 模型实盘追踪（public schema）
- factor_live_summary / model_live_summary — 视图

与设计文档 `reports/实盘演练模块设计_v4.md` §6.1 对应。
"""
from __future__ import annotations

from typing import List

from .pg_pool import get_conn

DDL: List[str] = [
    # ── Schema ──
    "CREATE SCHEMA IF NOT EXISTS paper_trading",

    # ── 1. portfolio_config ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.portfolio_config (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_name      TEXT NOT NULL,
        signal_source       TEXT NOT NULL,
        signal_source_id    TEXT NOT NULL,
        signal_loop_id      INTEGER,
        model_source        TEXT DEFAULT 'original',
        training_job_id     TEXT,
        initial_capital     NUMERIC(14,2) NOT NULL DEFAULT 1000000,
        max_positions       INTEGER NOT NULL DEFAULT 20,
        max_position_pct    NUMERIC(5,4) NOT NULL DEFAULT 0.10,
        trade_freq          TEXT NOT NULL DEFAULT 'daily',
        max_turnover_pct    NUMERIC(5,4) DEFAULT 0.30,
        fee_config          JSONB NOT NULL DEFAULT '{
            "default_fees": {
                "commission_rate": 0.0003,
                "stamp_tax_rate": 0.0005,
                "transfer_fee_rate": 0.00002,
                "slippage": 0.001,
                "min_commission": 5
            },
            "custom_fees": {}
        }'::jsonb,
        benchmark           TEXT DEFAULT '000300.SH',
        auto_run            BOOLEAN DEFAULT TRUE,
        enable_factor_attribution BOOLEAN DEFAULT TRUE,
        enable_live_ic      BOOLEAN DEFAULT TRUE,
        enable_intraday     BOOLEAN DEFAULT FALSE,
        intraday_exec_mode  TEXT DEFAULT 'replay',
        intraday_strategy   TEXT DEFAULT 'CLOSE_PRICE',
        intraday_config     JSONB DEFAULT '{}'::jsonb,
        intraday_data_source TEXT DEFAULT 'auto',
        intraday_freq       TEXT DEFAULT '5m',
        status              TEXT DEFAULT 'created',
        start_date          DATE,
        factor_list         JSONB,
        model_catalog_id    BIGINT,
        asset_bundle_id     TEXT,
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        updated_at          TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── 2. trade_signals ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.trade_signals (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        signal_date         DATE NOT NULL,
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        side                TEXT NOT NULL,
        target_quantity     INTEGER NOT NULL,
        target_weight       NUMERIC(8,6),
        score               NUMERIC(10,6),
        status              TEXT DEFAULT 'pending',
        skip_reason         TEXT,
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(portfolio_id, trade_date, symbol)
    )
    """,

    # ── 3. daily_snapshot ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.daily_snapshot (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        trade_date          DATE NOT NULL,
        total_value         NUMERIC(14,2),
        cash                NUMERIC(14,2),
        stock_value         NUMERIC(14,2),
        daily_pnl           NUMERIC(14,2),
        daily_return        NUMERIC(10,6),
        cumulative_return   NUMERIC(10,6),
        max_drawdown        NUMERIC(10,6),
        current_drawdown    NUMERIC(10,6),
        benchmark_return    NUMERIC(10,6),
        benchmark_cumulative NUMERIC(10,6),
        position_count      INTEGER,
        turnover            NUMERIC(10,6),
        buy_count           INTEGER DEFAULT 0,
        sell_count          INTEGER DEFAULT 0,
        buy_amount          NUMERIC(14,2) DEFAULT 0,
        sell_amount         NUMERIC(14,2) DEFAULT 0,
        total_commission    NUMERIC(10,4) DEFAULT 0,
        total_stamp_tax     NUMERIC(10,4) DEFAULT 0,
        total_transfer_fee  NUMERIC(10,4) DEFAULT 0,
        total_slippage      NUMERIC(10,4) DEFAULT 0,
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(portfolio_id, trade_date)
    )
    """,

    # ── 4. positions ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.positions (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        symbol_name         TEXT,
        industry            TEXT,
        quantity            INTEGER NOT NULL,
        avg_cost            NUMERIC(10,4),
        close_price         NUMERIC(10,4),
        market_value        NUMERIC(14,2),
        weight              NUMERIC(8,6),
        unrealized_pnl      NUMERIC(14,2),
        unrealized_pnl_pct  NUMERIC(10,6),
        realized_pnl        NUMERIC(14,2) DEFAULT 0,
        entry_date          DATE,
        holding_days        INTEGER,
        score               NUMERIC(10,6),
        UNIQUE(portfolio_id, trade_date, symbol)
    )
    """,

    # ── 5. trades ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.trades (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        trade_date          DATE NOT NULL,
        symbol              TEXT NOT NULL,
        symbol_name         TEXT,
        side                TEXT NOT NULL,
        quantity            INTEGER NOT NULL,
        price               NUMERIC(10,4) NOT NULL,
        amount              NUMERIC(14,2),
        commission          NUMERIC(10,4),
        stamp_tax           NUMERIC(10,4),
        transfer_fee        NUMERIC(10,4),
        slippage_cost       NUMERIC(10,4),
        total_cost          NUMERIC(10,4),
        net_amount          NUMERIC(14,2),
        realized_pnl        NUMERIC(14,2),
        holding_days        INTEGER,
        avg_cost_at_trade   NUMERIC(10,4),
        reason              TEXT,
        signal_id           BIGINT,
        exec_algo           TEXT DEFAULT 'CLOSE_PRICE',
        exec_bars           INTEGER DEFAULT 1,
        exec_time           TIMESTAMPTZ,
        created_at          TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── 6. stock_pnl_summary ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.stock_pnl_summary (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        symbol              TEXT NOT NULL,
        symbol_name         TEXT,
        buy_count           INTEGER DEFAULT 0,
        sell_count          INTEGER DEFAULT 0,
        total_realized_pnl  NUMERIC(14,2) DEFAULT 0,
        total_unrealized_pnl NUMERIC(14,2) DEFAULT 0,
        total_pnl           NUMERIC(14,2) DEFAULT 0,
        total_pnl_pct       NUMERIC(10,6),
        win_count           INTEGER DEFAULT 0,
        loss_count          INTEGER DEFAULT 0,
        avg_holding_days    NUMERIC(8,2),
        first_trade_date    DATE,
        last_trade_date     DATE,
        is_holding          BOOLEAN DEFAULT FALSE,
        updated_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(portfolio_id, symbol)
    )
    """,

    # ── 7. training_jobs ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.training_jobs (
        id                  BIGSERIAL PRIMARY KEY,
        job_id              TEXT NOT NULL UNIQUE,
        signal_source       TEXT NOT NULL,
        signal_source_id    TEXT NOT NULL,
        signal_loop_id      INTEGER,
        train_start         DATE NOT NULL,
        train_end           DATE NOT NULL,
        valid_start         DATE NOT NULL,
        valid_end           DATE NOT NULL,
        n_epochs            INTEGER,
        batch_size          INTEGER,
        lr                  DOUBLE PRECISION,
        early_stop          INTEGER,
        source_config_path  TEXT,
        workspace_path      TEXT,
        status              TEXT DEFAULT 'pending',
        best_epoch          INTEGER,
        best_valid_loss     DOUBLE PRECISION,
        valid_ic            DOUBLE PRECISION,
        valid_icir          DOUBLE PRECISION,
        model_pkl_path      TEXT,
        source_model_catalog_id BIGINT,
        started_at          TIMESTAMPTZ,
        completed_at        TIMESTAMPTZ,
        error_message       TEXT,
        created_at          TIMESTAMPTZ DEFAULT NOW()
    )
    """,

    # ── 8. factor_attribution ──
    """
    CREATE TABLE IF NOT EXISTS paper_trading.factor_attribution (
        id                  BIGSERIAL PRIMARY KEY,
        portfolio_id        BIGINT NOT NULL REFERENCES paper_trading.portfolio_config(id),
        trade_date          DATE NOT NULL,
        factor_prefix       TEXT NOT NULL,
        factor_count        INTEGER,
        contribution_pct    NUMERIC(10,6),
        contribution_amount NUMERIC(14,2),
        avg_factor_ic       NUMERIC(10,6),
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(portfolio_id, trade_date, factor_prefix)
    )
    """,

    # ── 9. model_live_track (public schema) ──
    """
    CREATE TABLE IF NOT EXISTS model_live_track (
        id                  BIGSERIAL PRIMARY KEY,
        model_catalog_id    BIGINT NOT NULL REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT,
        portfolio_id        BIGINT NOT NULL,
        trade_date          DATE NOT NULL,
        daily_return        NUMERIC(10,6),
        daily_sharpe        NUMERIC(10,6),
        rolling_20d_return  NUMERIC(10,6),
        rolling_20d_sharpe  NUMERIC(10,6),
        rolling_60d_return  NUMERIC(10,6),
        rolling_60d_sharpe  NUMERIC(10,6),
        max_drawdown_20d    NUMERIC(10,6),
        prediction_accuracy NUMERIC(10,6),
        top20_hit_rate      NUMERIC(10,6),
        created_at          TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE(model_catalog_id, portfolio_id, trade_date)
    )
    """,

    # ── Indexes ──
    """
    CREATE INDEX IF NOT EXISTS idx_pt_signals_portfolio_trade
    ON paper_trading.trade_signals(portfolio_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_signals_status
    ON paper_trading.trade_signals(status) WHERE status = 'pending'
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_snapshot_portfolio_date
    ON paper_trading.daily_snapshot(portfolio_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_positions_portfolio_date
    ON paper_trading.positions(portfolio_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_trades_portfolio_date
    ON paper_trading.trades(portfolio_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_trades_symbol
    ON paper_trading.trades(portfolio_id, symbol)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_stock_pnl_portfolio
    ON paper_trading.stock_pnl_summary(portfolio_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_training_jobs_status
    ON paper_trading.training_jobs(status)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_pt_factor_attr_portfolio_date
    ON paper_trading.factor_attribution(portfolio_id, trade_date)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_mlt_model_date
    ON model_live_track(model_catalog_id, trade_date)
    """,
]

# ── Views（需要 CREATE OR REPLACE，单独处理） ──
VIEWS: List[str] = [
    # factor_live_summary: 近 60 日滚动因子汇总
    """
    CREATE OR REPLACE VIEW factor_live_summary AS
    SELECT
        flt.factor_catalog_id,
        fc.factor_name,
        fc.source,
        AVG(flt.daily_ic) AS live_ic_mean,
        STDDEV(flt.daily_ic) AS live_ic_std,
        AVG(flt.daily_ic) / NULLIF(STDDEV(flt.daily_ic), 0) AS live_icir,
        AVG(COALESCE(flt.signal_hit_rate, 0)) AS live_hit_rate,
        AVG(flt.rolling_60d_ic) AS live_rolling_60d_ic,
        COUNT(*) AS n_trading_days,
        MAX(flt.trade_date) AS last_trade_date,
        CASE
            WHEN COUNT(*) < 10 THEN 'testing'
            WHEN AVG(flt.daily_ic) > 0.01 THEN 'validated'
            WHEN AVG(flt.daily_ic) > -0.005 THEN 'neutral'
            ELSE 'degraded'
        END AS validation_status
    FROM factor_live_track flt
    JOIN aistock_factor_catalog fc ON fc.id = flt.factor_catalog_id
    WHERE flt.trade_date >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY flt.factor_catalog_id, fc.factor_name, fc.source
    """,

    # model_live_summary: 近 60 日滚动模型汇总
    """
    CREATE OR REPLACE VIEW model_live_summary AS
    SELECT
        mlt.model_catalog_id,
        AVG(mlt.daily_return) * 252 AS live_annualized_return,
        STDDEV(mlt.daily_return) * SQRT(252) AS live_volatility,
        AVG(mlt.daily_return) / NULLIF(STDDEV(mlt.daily_return), 0) * SQRT(252) AS live_sharpe,
        MIN(mlt.max_drawdown_20d) AS live_worst_drawdown,
        AVG(mlt.prediction_accuracy) AS live_prediction_accuracy,
        AVG(mlt.top20_hit_rate) AS live_top20_hit_rate,
        COUNT(*) AS n_trading_days,
        MAX(mlt.trade_date) AS last_trade_date,
        CASE
            WHEN COUNT(*) < 10 THEN 'testing'
            WHEN AVG(mlt.daily_return) > 0 AND AVG(mlt.prediction_accuracy) > 0.52 THEN 'validated'
            WHEN AVG(mlt.daily_return) > -0.0005 THEN 'neutral'
            ELSE 'degraded'
        END AS validation_status
    FROM model_live_track mlt
    WHERE mlt.trade_date >= CURRENT_DATE - INTERVAL '60 days'
    GROUP BY mlt.model_catalog_id
    """,
]


MIGRATIONS: List[str] = [
    # trades 表新增 exec_algo / exec_bars 列（兼容已有数据库）
    """
    DO $$
    BEGIN
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'paper_trading' AND table_name = 'trades' AND column_name = 'exec_algo'
        ) THEN
            ALTER TABLE paper_trading.trades ADD COLUMN exec_algo TEXT DEFAULT 'CLOSE_PRICE';
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'paper_trading' AND table_name = 'trades' AND column_name = 'exec_bars'
        ) THEN
            ALTER TABLE paper_trading.trades ADD COLUMN exec_bars INTEGER DEFAULT 1;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'paper_trading' AND table_name = 'portfolio_config' AND column_name = 'enable_intraday'
        ) THEN
            ALTER TABLE paper_trading.portfolio_config ADD COLUMN enable_intraday BOOLEAN DEFAULT FALSE;
        END IF;
        IF NOT EXISTS (
            SELECT 1 FROM information_schema.columns
            WHERE table_schema = 'paper_trading' AND table_name = 'portfolio_config' AND column_name = 'intraday_exec_mode'
        ) THEN
            ALTER TABLE paper_trading.portfolio_config ADD COLUMN intraday_exec_mode TEXT DEFAULT 'replay';
        END IF;
    END $$
    """,
    # portfolio_config.intraday_strategy 默认值统一为 CLOSE_PRICE（仅在存在旧值时更新）
    """
    UPDATE paper_trading.portfolio_config
    SET intraday_strategy = 'CLOSE_PRICE'
    WHERE intraday_strategy = 'close_price'
    """,
]


def _schema_exists() -> bool:
    """检查 paper_trading schema 核心表是否已存在."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT COUNT(*) FROM information_schema.tables
                WHERE table_schema = 'paper_trading' AND table_name = 'portfolio_config'
                """
            )
            return cur.fetchone()[0] > 0


def _migrations_needed() -> bool:
    """检查是否有未完成的迁移."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    (SELECT COUNT(*) FROM information_schema.columns
                     WHERE table_schema='paper_trading' AND table_name='trades' AND column_name='exec_algo') +
                    (SELECT COUNT(*) FROM information_schema.columns
                     WHERE table_schema='paper_trading' AND table_name='portfolio_config' AND column_name='enable_intraday') +
                    (SELECT COUNT(*) FROM information_schema.columns
                     WHERE table_schema='paper_trading' AND table_name='portfolio_config' AND column_name='intraday_exec_mode')
                """
            )
            return cur.fetchone()[0] < 3


def init_paper_trading_schema() -> None:
    """幂等地创建实盘演练相关表、索引和视图.

    如果核心表已存在且无待执行迁移则直接跳过，避免每次启动重复执行。
    """
    already_exists = _schema_exists()
    needs_migration = _migrations_needed() if already_exists else False

    if already_exists and not needs_migration:
        return

    with get_conn() as conn:
        with conn.cursor() as cur:
            if not already_exists:
                for sql in DDL:
                    cur.execute(sql)
                for sql in VIEWS:
                    cur.execute(sql)
                print("实盘演练数据库表创建完成（paper_trading schema）")
            if needs_migration:
                for sql in MIGRATIONS:
                    cur.execute(sql)
                print("实盘演练数据库迁移完成")
        conn.commit()


if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv

    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    init_paper_trading_schema()
