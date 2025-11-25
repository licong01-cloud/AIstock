"""初始化量化模型元数据相关表（新程序专用，不修改旧 init_app_schema）.

本脚本创建以下表，全部放在 app schema 下：
- app.model_config
- app.model_train_run
- app.model_inference_run
- app.quant_unified_signal

与设计文档 `docs/quant_analyst_design.md` 第 5/8 章对应。
"""
from __future__ import annotations

from typing import List

from .pg_pool import get_conn


DDL: List[str] = [
    # 确保 app schema 存在
    "CREATE SCHEMA IF NOT EXISTS app",
    # model_config
    """
    CREATE TABLE IF NOT EXISTS app.model_config (
        id                  BIGSERIAL PRIMARY KEY,
        model_name          TEXT NOT NULL,
        description         TEXT,
        config_json         JSONB NOT NULL,
        enabled             BOOLEAN NOT NULL DEFAULT TRUE,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # model_train_run
    """
    CREATE TABLE IF NOT EXISTS app.model_train_run (
        id                  BIGSERIAL PRIMARY KEY,
        model_name          TEXT NOT NULL,
        config_snapshot     JSONB NOT NULL,
        status              TEXT NOT NULL,
        start_time          TIMESTAMPTZ NOT NULL,
        end_time            TIMESTAMPTZ,
        duration_seconds    DOUBLE PRECISION,
        symbols_covered_count INTEGER,
        time_range_start    TIMESTAMPTZ,
        time_range_end      TIMESTAMPTZ,
        data_granularity    TEXT,
        metrics_json        JSONB,
        log_path            TEXT
    )
    """,
    # model_inference_run
    """
    CREATE TABLE IF NOT EXISTS app.model_inference_run (
        id                  BIGSERIAL PRIMARY KEY,
        model_name          TEXT NOT NULL,
        schedule_name       TEXT,
        config_snapshot     JSONB NOT NULL,
        status              TEXT NOT NULL,
        start_time          TIMESTAMPTZ NOT NULL,
        end_time            TIMESTAMPTZ,
        duration_seconds    DOUBLE PRECISION,
        symbols_covered     INTEGER,
        time_of_data        TIMESTAMPTZ,
        metrics_json        JSONB
    )
    """,
    # quant_unified_signal
    """
    CREATE TABLE IF NOT EXISTS app.quant_unified_signal (
        id                  BIGSERIAL PRIMARY KEY,
        symbol              TEXT NOT NULL,
        as_of_time          TIMESTAMPTZ NOT NULL,
        frequency           TEXT NOT NULL,
        horizon             TEXT NOT NULL,
        direction           TEXT,
        prob_up             DOUBLE PRECISION,
        prob_down           DOUBLE PRECISION,
        prob_flat           DOUBLE PRECISION,
        confidence          DOUBLE PRECISION,
        expected_return     NUMERIC(12,6),
        expected_volatility NUMERIC(12,6),
        risk_score          DOUBLE PRECISION,
        regime              TEXT,
        liquidity_label     TEXT,
        microstructure_label TEXT,
        anomaly_flags       JSONB,
        suggested_position_delta NUMERIC(8,4),
        suggested_t0_action TEXT,
        model_votes         JSONB,
        ensemble_method     TEXT,
        data_coverage       JSONB,
        model_versions      JSONB,
        quality_flags       JSONB,
        created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # model_universe_config
    """
    CREATE TABLE IF NOT EXISTS app.model_universe_config (
        id              BIGSERIAL PRIMARY KEY,
        universe_name   TEXT NOT NULL UNIQUE,
        description     TEXT,
        config_json     JSONB NOT NULL,
        enabled         BOOLEAN NOT NULL DEFAULT TRUE,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # stock_static_features
    """
    CREATE TABLE IF NOT EXISTS app.stock_static_features (
        id                BIGSERIAL PRIMARY KEY,
        ts_code           TEXT NOT NULL,
        as_of_date        DATE NOT NULL,
        industry          TEXT,
        sub_industry      TEXT,
        size_bucket       TEXT,
        volatility_bucket TEXT,
        liquidity_bucket  TEXT,
        extra_json        JSONB,
        created_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at        TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (ts_code, as_of_date)
    )
    """,
    # model_schedule: 控制模型训练/推理调度计划，风格与 market.ingestion_schedules 类似
    """
    CREATE TABLE IF NOT EXISTS app.model_schedule (
        id              BIGSERIAL PRIMARY KEY,
        model_name      TEXT NOT NULL,
        schedule_name   TEXT NOT NULL,
        task_type       TEXT NOT NULL CHECK (task_type IN ('train','inference')),
        frequency       TEXT NOT NULL,
        enabled         BOOLEAN NOT NULL DEFAULT TRUE,
        config_json     JSONB NOT NULL,
        last_run_at     TIMESTAMPTZ,
        next_run_at     TIMESTAMPTZ,
        last_status     TEXT,
        last_error      TEXT,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    # 可选唯一约束和索引
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_quant_unified_signal_symbol_time
    ON app.quant_unified_signal (symbol, as_of_time, frequency, horizon)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_train_run_model_time
    ON app.model_train_run (model_name, start_time DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_inference_run_model_time
    ON app.model_inference_run (model_name, start_time DESC)
    """,
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_model_schedule_name
    ON app.model_schedule (model_name, schedule_name, task_type)
    """,
]


def init_quant_schema() -> None:
    """执行所有 DDL 语句，幂等地创建模型相关表和索引."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    init_quant_schema()
