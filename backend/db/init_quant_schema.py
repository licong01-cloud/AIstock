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
    """
    CREATE TABLE IF NOT EXISTS aistock_loop_catalog (
        id                   BIGSERIAL PRIMARY KEY,
        catalog_version      TEXT NOT NULL,
        generated_at_utc     TEXT NOT NULL,
        catalog_source       TEXT NOT NULL,
        task_run_id          TEXT NOT NULL,
        loop_id              INTEGER NOT NULL,
        workspace_id         TEXT NOT NULL,
        asset_bundle_id      TEXT,
        is_solidified        BOOLEAN DEFAULT FALSE,
        sync_status          TEXT DEFAULT 'pending',
        manifest_schema_version INTEGER,
        manifest_primary_workspace_id TEXT,
        manifest_factor_entry_relpath TEXT,
        manifest_model_weight_relpath TEXT,
        manifest_config_relpath TEXT,
        source_workspace_path TEXT,
        log_dir              TEXT,
        log_uri              TEXT,
        scenario             TEXT,
        step_name            TEXT,
        action               TEXT,
        status               TEXT,
        has_result           BOOLEAN,
        strategy_id          TEXT,
        factor_names         JSONB,
        metrics              JSONB,
        annualized_return    DOUBLE PRECISION,
        max_drawdown         DOUBLE PRECISION,
        sharpe               DOUBLE PRECISION,
        ic                   DOUBLE PRECISION,
        ic_ir                DOUBLE PRECISION,
        win_rate             DOUBLE PRECISION,
        decision             BOOLEAN,
        summary_execution    TEXT,
        summary_value_feedback TEXT,
        summary_shape_feedback TEXT,
        code_critic          JSONB,
        limitations          JSONB,
        path_factor_meta     TEXT,
        path_factor_perf     TEXT,
        path_feedback        TEXT,
        path_ret_curve       TEXT,
        path_dd_curve        TEXT,
        path_strategy_meta   TEXT,
        path_model_meta      TEXT,
        log_dir              TEXT,
        workspace_role       TEXT,
        path_mlruns          TEXT,
        path_model_files     JSONB,
        materialization_status TEXT,
        materialization_error TEXT,
        materialization_updated_at_utc TEXT,
        raw_payload          JSONB,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (task_run_id, loop_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS aistock_factor_catalog (
        id                   BIGSERIAL PRIMARY KEY,
        factor_name          TEXT NOT NULL,
        catalog_version      TEXT NOT NULL,
        generated_at_utc     TEXT NOT NULL,
        catalog_source       TEXT NOT NULL,
        expression           TEXT,
        source               TEXT NOT NULL,
        region               TEXT,
        tags                 JSONB,
        description_cn       TEXT,
        formula_hint         TEXT,
        variables            JSONB,
        freq                 TEXT,
        align                TEXT,
        nan_policy           TEXT,
        created_at_utc       TEXT,
        experiment_id        TEXT,
        impl_module          TEXT,
        impl_func            TEXT,
        impl_version         TEXT,
        performance_metrics  JSONB,
        best_performance     TEXT,
        best_performance_sharpe DOUBLE PRECISION,
        best_performance_ann_ret DOUBLE PRECISION,
        interface_info       JSONB,
        raw_payload          JSONB,
        asset_bundle_id      TEXT,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (factor_name, source)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS aistock_model_catalog (
        id                   BIGSERIAL PRIMARY KEY,
        catalog_version      TEXT NOT NULL,
        generated_at_utc     TEXT NOT NULL,
        catalog_source       TEXT NOT NULL,
        model_id             TEXT NOT NULL UNIQUE,
        task_run_id          TEXT NOT NULL,
        loop_id              INTEGER NOT NULL,
        workspace_id         TEXT NOT NULL,
        workspace_path       TEXT NOT NULL,
        log_dir              TEXT,
        model_type           TEXT,
        model_config         JSONB,
        dataset_config       JSONB,
        feature_schema       JSONB,
        flattened_feature_list JSONB,
        model_artifacts      JSONB,
        asset_bundle_id      TEXT,
        raw_payload          JSONB,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (task_run_id, loop_id, workspace_id)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS aistock_strategy_catalog (
        id                   BIGSERIAL PRIMARY KEY,
        strategy_id          TEXT NOT NULL UNIQUE,
        catalog_version      TEXT NOT NULL,
        generated_at_utc     TEXT NOT NULL,
        catalog_source       TEXT NOT NULL,
        scenario             TEXT,
        step_name            TEXT,
        action               TEXT,
        example_task_run_id  TEXT,
        example_loop_id      INTEGER,
        example_workspace_id TEXT,
        example_workspace_path TEXT,
        template_files       JSONB,
        data_config          JSONB,
        dataset_config       JSONB,
        portfolio_config     JSONB,
        backtest_config      JSONB,
        model_config         JSONB,
        feature_list         JSONB,
        market               TEXT,
        instruments          JSONB,
        freq                 TEXT,
        python_implementation JSONB,
        in_selection_center  BOOLEAN DEFAULT FALSE,
        raw_payload          JSONB,
        created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW()
    );
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
    # app.sync_meta 补齐
    """
    CREATE TABLE IF NOT EXISTS app.sync_meta (
        key         TEXT PRIMARY KEY,
        value       TEXT,
        updated_at  TIMESTAMPTZ DEFAULT NOW()
    );
    """,
    "INSERT INTO app.sync_meta (key, value) VALUES ('rdagent_last_sync_time', '2000-01-01T00:00:00Z') ON CONFLICT DO NOTHING;"
]


def init_quant_schema() -> None:
    """执行所有 DDL 语句，幂等地创建模型相关表和索引."""

    with get_conn() as conn:
        with conn.cursor() as cur:
            # 1. 执行基础 DDL
            for sql in DDL:
                cur.execute(sql)
            
            # 2. 补齐现有表的缺失字段 (针对已存在的表进行升级)
            def add_column_if_not_exists(table, column, col_type):
                cur.execute(f"SELECT column_name FROM information_schema.columns WHERE table_name='{table}' AND column_name='{column}';")
                if not cur.fetchone():
                    cur.execute(f"ALTER TABLE {table} ADD COLUMN {column} {col_type};")
                    print(f"[db][upgrade] Added column {column} to {table}")

            # aistock_loop_catalog 补齐
            add_column_if_not_exists("aistock_loop_catalog", "asset_bundle_id", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "is_solidified", "BOOLEAN DEFAULT FALSE")
            add_column_if_not_exists("aistock_loop_catalog", "sync_status", "TEXT DEFAULT 'pending'")
            add_column_if_not_exists("aistock_loop_catalog", "manifest_schema_version", "INTEGER")
            add_column_if_not_exists("aistock_loop_catalog", "manifest_primary_workspace_id", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "manifest_factor_entry_relpath", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "manifest_model_weight_relpath", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "manifest_config_relpath", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "source_workspace_path", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "log_dir", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "log_uri", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "display_name", "TEXT")

            # aistock_strategy_catalog 补齐
            add_column_if_not_exists("aistock_strategy_catalog", "in_selection_center", "BOOLEAN DEFAULT FALSE")
            add_column_if_not_exists("aistock_strategy_catalog", "display_name", "TEXT")

            # aistock_model_catalog 补齐
            add_column_if_not_exists("aistock_model_catalog", "asset_bundle_id", "TEXT")
            add_column_if_not_exists("aistock_model_catalog", "display_name", "TEXT")


if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv
    # 寻找 .env 文件（假设在 backend 的上级目录）
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    
    init_quant_schema()
