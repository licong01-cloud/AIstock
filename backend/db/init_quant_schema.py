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
    "INSERT INTO app.sync_meta (key, value) VALUES ('rdagent_last_sync_time', '2000-01-01T00:00:00Z') ON CONFLICT DO NOTHING;",
    # aistock_factor_metrics: 单因子17项指标（支持多窗口、多时间段历史记录）
    """
    CREATE TABLE IF NOT EXISTS aistock_factor_metrics (
        id                       BIGSERIAL PRIMARY KEY,
        factor_name              TEXT NOT NULL,
        calculated_at            TIMESTAMPTZ NOT NULL,
        data_start               DATE NOT NULL,
        data_end                 DATE NOT NULL,
        eval_window              TEXT NOT NULL,
        return_horizon           TEXT NOT NULL DEFAULT 'T2T1',
        universe                 TEXT NOT NULL DEFAULT 'all',
        ic_mean                  DOUBLE PRECISION,
        ic_std                   DOUBLE PRECISION,
        rank_ic_mean             DOUBLE PRECISION,
        rank_ic_std              DOUBLE PRECISION,
        icir                     DOUBLE PRECISION,
        rank_icir                DOUBLE PRECISION,
        icir_annualized          DOUBLE PRECISION,
        rank_icir_annualized     DOUBLE PRECISION,
        ic_positive_ratio        DOUBLE PRECISION,
        top_annual_return        DOUBLE PRECISION,
        top_excess_annual_return DOUBLE PRECISION,
        top_sharpe               DOUBLE PRECISION,
        top_max_drawdown         DOUBLE PRECISION,
        top_excess_sharpe        DOUBLE PRECISION,
        benchmark_annual_return  DOUBLE PRECISION,
        group_return_monotonicity DOUBLE PRECISION,
        turnover                 DOUBLE PRECISION,
        ic_decay_half_life       DOUBLE PRECISION,
        ic_csz_mean              DOUBLE PRECISION,
        rank_ic_1d               DOUBLE PRECISION,
        rank_ic_5d               DOUBLE PRECISION,
        rank_ic_10d              DOUBLE PRECISION,
        rank_ic_20d              DOUBLE PRECISION,
        coverage                 DOUBLE PRECISION,
        coverage_numerator       BIGINT,
        coverage_denominator     BIGINT,
        coverage_semantics       TEXT,
        universe_rule_version    TEXT,
        universe_fingerprint_sha256 TEXT,
        index_policy             TEXT,
        eligible_sample_count    BIGINT,
        suspended_excluded_count BIGINT,
        st_pit_excluded_count    BIGINT,
        n_trading_days           INTEGER,
        source_task_id           TEXT,
        factor_catalog_id        BIGINT,
        calc_engine              TEXT NOT NULL DEFAULT 'rdagent',
        calc_batch_id            TEXT,
        snapshot_date            DATE,
        created_at               TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (factor_name, eval_window, data_start, data_end, snapshot_date)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_factor_metrics_name
    ON aistock_factor_metrics (factor_name);
    """,
    # ── 月频 IC 衰退趋势表 ──
    """
    CREATE TABLE IF NOT EXISTS aistock_factor_monthly_ic (
        id              BIGSERIAL PRIMARY KEY,
        factor_name     TEXT NOT NULL,
        month_end       TEXT NOT NULL,
        snapshot_date   DATE NOT NULL,
        ic_mean         DOUBLE PRECISION,
        rank_ic_mean    DOUBLE PRECISION,
        ic_std          DOUBLE PRECISION,
        ic_ewma_6m      DOUBLE PRECISION,
        n_days          INTEGER,
        created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (factor_name, month_end, snapshot_date)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_monthly_ic_factor
    ON aistock_factor_monthly_ic(factor_name);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_factor_metrics_task
    ON aistock_factor_metrics (source_task_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_factor_metrics_snapshot
    ON aistock_factor_metrics (factor_name, snapshot_date);
    """,
    # aistock_factor_metrics 表和字段中文注释
    "COMMENT ON TABLE aistock_factor_metrics IS '单因子独立评估指标表，每个因子在不同评估窗口下的17项量化指标，支持历史多次计算记录';",
    "COMMENT ON COLUMN aistock_factor_metrics.factor_name IS '因子名称，与 aistock_factor_catalog.factor_name 对应';",
    "COMMENT ON COLUMN aistock_factor_metrics.calculated_at IS '指标计算时间(UTC)，同一因子可多次计算以追踪衰退';",
    "COMMENT ON COLUMN aistock_factor_metrics.data_start IS '计算所用数据的起始日期';",
    "COMMENT ON COLUMN aistock_factor_metrics.data_end IS '计算所用数据的截止日期';",
    "COMMENT ON COLUMN aistock_factor_metrics.eval_window IS '评估窗口: full=全量, out_sample=样本外(2024-07-01起), recent_6m=近6月, recent_3m=近3月';",
    "COMMENT ON COLUMN aistock_factor_metrics.return_horizon IS '收益率计算方式，T2T1表示Ref($close,-2)/Ref($close,-1)-1';",
    "COMMENT ON COLUMN aistock_factor_metrics.universe IS '股票池范围，all=全市场';",
    "COMMENT ON COLUMN aistock_factor_metrics.ic_mean IS 'IC均值：因子值与未来收益的Pearson相关系数的日均值';",
    "COMMENT ON COLUMN aistock_factor_metrics.ic_std IS 'IC标准差：IC序列的波动率';",
    "COMMENT ON COLUMN aistock_factor_metrics.rank_ic_mean IS 'Rank IC均值：因子排名与收益排名的Spearman相关系数日均值';",
    "COMMENT ON COLUMN aistock_factor_metrics.rank_ic_std IS 'Rank IC标准差：Rank IC序列的波动率';",
    "COMMENT ON COLUMN aistock_factor_metrics.icir IS 'ICIR：IC均值/IC标准差，衡量IC的稳定性';",
    "COMMENT ON COLUMN aistock_factor_metrics.rank_icir IS 'Rank ICIR：Rank IC均值/Rank IC标准差';",
    "COMMENT ON COLUMN aistock_factor_metrics.ic_positive_ratio IS 'IC胜率：IC>0的交易日占比';",
    # 注意: top_annual_return 等纯多头列的 COMMENT 在迁移完成后执行（见 POST_MIGRATION_COMMENTS）
    "COMMENT ON COLUMN aistock_factor_metrics.group_return_monotonicity IS '分组收益单调性：[-1,1]，1表示因子值越高收益越高，完美单调';",
    "COMMENT ON COLUMN aistock_factor_metrics.turnover IS '因子换手率：相邻交易日因子排名变化程度，越低越稳定';",
    "COMMENT ON COLUMN aistock_factor_metrics.ic_decay_half_life IS 'IC衰减半衰期(天)：IC随预测周期衰减到一半所需天数';",
    "COMMENT ON COLUMN aistock_factor_metrics.coverage IS '因子覆盖率：有效因子值占总样本的比例';",
    "COMMENT ON COLUMN aistock_factor_metrics.n_trading_days IS '评估窗口内的交易日数量';",
    "COMMENT ON COLUMN aistock_factor_metrics.source_task_id IS '来源RD-Agent任务ID';",
    "COMMENT ON COLUMN aistock_factor_metrics.calc_engine IS '计算引擎: rdagent=RD-Agent侧计算, aistock_local=AIstock本地计算';",
    "COMMENT ON COLUMN aistock_factor_metrics.snapshot_date IS '数据快照日期，标识本次计算使用的数据截止时间点，用于追踪因子IC衰变趋势';",
    # aistock_factor_calc_log: 因子计算日志表，记录每个因子×窗口的计算状态
    """
    CREATE TABLE IF NOT EXISTS aistock_factor_calc_log (
        id               BIGSERIAL PRIMARY KEY,
        calc_batch_id    TEXT NOT NULL,
        source_task_id   TEXT,
        factor_name      TEXT NOT NULL,
        eval_window      TEXT NOT NULL,
        status           TEXT NOT NULL,
        error_message    TEXT,
        n_trading_days   INTEGER,
        required_days    INTEGER,
        data_start       DATE,
        data_end         DATE,
        data_source      TEXT NOT NULL DEFAULT 'parquet',
        calc_engine      TEXT NOT NULL DEFAULT 'rdagent',
        calculated_at    TIMESTAMPTZ NOT NULL,
        factor_catalog_id BIGINT,
        created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        UNIQUE (calc_batch_id, factor_name, eval_window)
    );
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_calc_log_batch
    ON aistock_factor_calc_log (calc_batch_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_calc_log_task
    ON aistock_factor_calc_log (source_task_id);
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_calc_log_factor
    ON aistock_factor_calc_log (factor_name);
    """,
    "COMMENT ON TABLE aistock_factor_calc_log IS '因子计算日志表，记录每个因子在每个评估窗口下的计算状态（成功/跳过/失败），支持历史多次计算追溯';",
    "COMMENT ON COLUMN aistock_factor_calc_log.id IS '自增主键';",
    "COMMENT ON COLUMN aistock_factor_calc_log.calc_batch_id IS '计算批次ID(UUID v4)，同一次计算请求中所有因子×窗口记录共享同一个ID，关联aistock_factor_metrics表';",
    "COMMENT ON COLUMN aistock_factor_calc_log.source_task_id IS '来源RD-Agent任务ID，与aistock_factor_catalog.source_task_id对应';",
    "COMMENT ON COLUMN aistock_factor_calc_log.factor_name IS '因子名称，与aistock_factor_catalog.factor_name对应';",
    "COMMENT ON COLUMN aistock_factor_calc_log.eval_window IS '评估窗口: full=全量数据, out_sample=样本外(2024-07-01起), recent_6m=近6月(126交易日), recent_3m=近3月(63交易日)';",
    "COMMENT ON COLUMN aistock_factor_calc_log.status IS '计算状态: ok=计算成功, skipped=跳过(数据不足等), error=计算失败(异常)';",
    "COMMENT ON COLUMN aistock_factor_calc_log.error_message IS '错误或跳过原因的详细描述，status为ok时为NULL';",
    "COMMENT ON COLUMN aistock_factor_calc_log.n_trading_days IS '该窗口内实际可用的交易日数量，即使跳过或失败也记录';",
    "COMMENT ON COLUMN aistock_factor_calc_log.required_days IS '该评估窗口所需的最少交易日数: full=0, out_sample=0, recent_6m=126, recent_3m=63';",
    "COMMENT ON COLUMN aistock_factor_calc_log.data_start IS '计算所用数据的起始日期';",
    "COMMENT ON COLUMN aistock_factor_calc_log.data_end IS '计算所用数据的截止日期';",
    "COMMENT ON COLUMN aistock_factor_calc_log.data_source IS '数据来源: parquet=离线parquet文件, realtime=实时行情数据, merged=离线+实时合并数据';",
    "COMMENT ON COLUMN aistock_factor_calc_log.calc_engine IS '计算引擎标识: rdagent=RD-Agent侧计算引擎';",
    "COMMENT ON COLUMN aistock_factor_calc_log.calculated_at IS '计算执行时间(UTC)，记录引擎实际执行计算的时刻';",
    "COMMENT ON COLUMN aistock_factor_calc_log.created_at IS '记录创建时间，数据库写入时自动生成';",
    # ── 通用模型训练管理表 ──────────────────────────────────────────
    # model_train_configs: 通用模型训练超参配置版本（通过 model_type 区分模型类型）
    """
    CREATE TABLE IF NOT EXISTS model_train_configs (
        config_id        TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
        model_type       TEXT NOT NULL,
        display_name     TEXT NOT NULL,
        config_json      JSONB NOT NULL,
        cron_expression  TEXT,
        cron_enabled     BOOLEAN DEFAULT FALSE,
        created_at       TIMESTAMPTZ DEFAULT NOW()
    )
    """,
    # model_train_snapshots: 时间版本快照（同一超参版本下不同训练日期的模型产出）
    """
    CREATE TABLE IF NOT EXISTS model_train_snapshots (
        snapshot_id      TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
        config_id        TEXT NOT NULL REFERENCES model_train_configs(config_id) ON DELETE RESTRICT,
        trained_at       TIMESTAMPTZ DEFAULT NOW(),
        model_path       TEXT NOT NULL,
        sector_count     INTEGER DEFAULT 0,
        status           TEXT DEFAULT 'pending',
        metrics_json     JSONB
    )
    """,
    # model_train_jobs: 训练任务执行跟踪
    """
    CREATE TABLE IF NOT EXISTS model_train_jobs (
        job_id           TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
        config_id        TEXT NOT NULL REFERENCES model_train_configs(config_id) ON DELETE RESTRICT,
        snapshot_id      TEXT REFERENCES model_train_snapshots(snapshot_id),
        status           TEXT DEFAULT 'pending',
        started_at       TIMESTAMPTZ,
        completed_at     TIMESTAMPTZ,
        error_message    TEXT
    )
    """,
    # model_train_daily_coefficient_jobs: HMM daily as-of coefficient generation audit jobs
    """
    CREATE TABLE IF NOT EXISTS model_train_daily_coefficient_jobs (
        job_id                TEXT PRIMARY KEY DEFAULT gen_random_uuid()::TEXT,
        snapshot_id           TEXT NOT NULL REFERENCES model_train_snapshots(snapshot_id) ON DELETE RESTRICT,
        config_id             TEXT NOT NULL REFERENCES model_train_configs(config_id) ON DELETE RESTRICT,
        signal_preset         TEXT NOT NULL,
        as_of_trade_date      DATE NOT NULL,
        effective_trade_date  DATE NOT NULL,
        generation_mode       TEXT NOT NULL,
        status                TEXT NOT NULL DEFAULT 'PENDING',
        result_status         TEXT,
        requested_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        started_at            TIMESTAMPTZ,
        completed_at          TIMESTAMPTZ,
        input_data_max_dates  JSONB NOT NULL DEFAULT '{}'::jsonb,
        output_path           TEXT NOT NULL,
        artifact_sha256       TEXT,
        plan_json             JSONB NOT NULL,
        result_json           JSONB,
        error_message         TEXT,
        error_context         JSONB
    )
    """,
    # (model_type, display_name) 联合唯一索引
    """
    CREATE UNIQUE INDEX IF NOT EXISTS uq_model_train_configs_type_name
    ON model_train_configs (model_type, display_name)
    """,
    # config_id 外键索引
    """
    CREATE INDEX IF NOT EXISTS idx_model_train_snapshots_config_id
    ON model_train_snapshots (config_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_train_jobs_config_id
    ON model_train_jobs (config_id)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_train_daily_coeff_jobs_snapshot
    ON model_train_daily_coefficient_jobs (snapshot_id, requested_at DESC)
    """,
    """
    CREATE INDEX IF NOT EXISTS idx_model_train_daily_coeff_jobs_config
    ON model_train_daily_coefficient_jobs (config_id, requested_at DESC)
    """,
]

# 纯多头列的 COMMENT（必须在列迁移完成后执行）
POST_MIGRATION_COMMENTS: List[str] = [
    "COMMENT ON COLUMN aistock_factor_metrics.top_annual_return IS '多头组年化收益：因子值最高的20%股票组合年化收益率';",
    "COMMENT ON COLUMN aistock_factor_metrics.top_excess_annual_return IS '多头超额年化收益：多头组年化收益 - 全市场等权基准年化收益';",
    "COMMENT ON COLUMN aistock_factor_metrics.top_sharpe IS '多头组夏普比：多头组合的年化夏普比率';",
    "COMMENT ON COLUMN aistock_factor_metrics.top_max_drawdown IS '多头组最大回撤：多头组合累计收益的最大回撤幅度(负值)';",
    "COMMENT ON COLUMN aistock_factor_metrics.top_excess_sharpe IS '多头超额夏普比：多头超额收益序列的年化夏普比率';",
    "COMMENT ON COLUMN aistock_factor_metrics.benchmark_annual_return IS '基准年化收益：全市场等权组合的年化收益率';",
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

            # Phase 3: 实验输出增强 — 新增诊断数据字段
            add_column_if_not_exists("aistock_loop_catalog", "enhanced_metrics", "JSONB")
            add_column_if_not_exists("aistock_loop_catalog", "ic_series", "JSONB")
            add_column_if_not_exists("aistock_loop_catalog", "return_curve", "JSONB")
            add_column_if_not_exists("aistock_loop_catalog", "training_diagnostics", "JSONB")
            add_column_if_not_exists("aistock_loop_catalog", "hypothesis_dimension", "TEXT")
            add_column_if_not_exists("aistock_loop_catalog", "hypothesis_summary", "TEXT")

            # aistock_strategy_catalog 补齐
            add_column_if_not_exists("aistock_strategy_catalog", "in_selection_center", "BOOLEAN DEFAULT FALSE")
            add_column_if_not_exists("aistock_strategy_catalog", "display_name", "TEXT")

            # aistock_model_catalog 补齐
            add_column_if_not_exists("aistock_model_catalog", "asset_bundle_id", "TEXT")
            add_column_if_not_exists("aistock_model_catalog", "display_name", "TEXT")

            # aistock_model_catalog: 训练诊断列 (C1)
            add_column_if_not_exists("aistock_model_catalog", "best_epoch", "INTEGER")
            add_column_if_not_exists("aistock_model_catalog", "total_epochs", "INTEGER")
            add_column_if_not_exists("aistock_model_catalog", "convergence_ratio", "DOUBLE PRECISION")
            add_column_if_not_exists("aistock_model_catalog", "overfit_ratio", "DOUBLE PRECISION")
            add_column_if_not_exists("aistock_model_catalog", "training_failed", "BOOLEAN DEFAULT FALSE")
            add_column_if_not_exists("aistock_model_catalog", "train_loss_final", "DOUBLE PRECISION")
            add_column_if_not_exists("aistock_model_catalog", "val_loss_final", "DOUBLE PRECISION")
            add_column_if_not_exists("aistock_model_catalog", "training_curves", "JSONB")

            # aistock_model_catalog: LLM 分析结果列 (E1)
            add_column_if_not_exists("aistock_model_catalog", "analysis_profile", "JSONB")
            add_column_if_not_exists("aistock_model_catalog", "model_grade", "VARCHAR(2)")
            add_column_if_not_exists("aistock_model_catalog", "grade_reason", "TEXT")
            add_column_if_not_exists("aistock_model_catalog", "training_quality_score", "DOUBLE PRECISION")

            # aistock_factor_metrics / aistock_factor_calc_log: catalog_id 关联列
            add_column_if_not_exists("aistock_factor_metrics", "factor_catalog_id", "BIGINT")
            add_column_if_not_exists("aistock_factor_calc_log", "factor_catalog_id", "BIGINT")

            # aistock_factor_metrics: 多空→纯多头列名迁移
            def rename_column_if_exists(table, old_col, new_col):
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name='{table}' AND column_name='{old_col}';"
                )
                if cur.fetchone():
                    cur.execute(
                        f"ALTER TABLE {table} RENAME COLUMN {old_col} TO {new_col};"
                    )
                    print(f"[db][upgrade] Renamed {table}.{old_col} -> {new_col}")

            def drop_column_if_exists(table, col):
                cur.execute(
                    "SELECT column_name FROM information_schema.columns "
                    f"WHERE table_name='{table}' AND column_name='{col}';"
                )
                if cur.fetchone():
                    cur.execute(f"ALTER TABLE {table} DROP COLUMN {col};")
                    print(f"[db][upgrade] Dropped {table}.{col}")

            # 旧列重命名为新列
            rename_column_if_exists(
                "aistock_factor_metrics",
                "long_short_annual_return", "top_annual_return",
            )
            rename_column_if_exists(
                "aistock_factor_metrics",
                "long_short_sharpe", "top_sharpe",
            )
            rename_column_if_exists(
                "aistock_factor_metrics",
                "long_short_max_drawdown", "top_max_drawdown",
            )
            # 旧列删除（纯多头体系不再需要）
            drop_column_if_exists(
                "aistock_factor_metrics", "long_short_volatility",
            )
            drop_column_if_exists(
                "aistock_factor_metrics", "top_group_return",
            )
            drop_column_if_exists(
                "aistock_factor_metrics", "bottom_group_return",
            )
            # 新增列（旧表可能没有）
            add_column_if_not_exists(
                "aistock_factor_metrics",
                "top_excess_annual_return", "DOUBLE PRECISION",
            )
            add_column_if_not_exists(
                "aistock_factor_metrics",
                "top_excess_sharpe", "DOUBLE PRECISION",
            )
            add_column_if_not_exists(
                "aistock_factor_metrics",
                "benchmark_annual_return", "DOUBLE PRECISION",
            )

            # 因子可用性管理: is_available 字段（软删除）
            add_column_if_not_exists("aistock_factor_catalog", "is_available", "BOOLEAN NOT NULL DEFAULT TRUE")

            # factor-calc-log: 新增 calc_batch_id 关联计算日志表
            add_column_if_not_exists(
                "aistock_factor_metrics",
                "calc_batch_id", "TEXT",
            )
            try:
                cur.execute(
                    "COMMENT ON COLUMN aistock_factor_metrics.calc_batch_id IS "
                    "'计算批次ID，关联aistock_factor_calc_log表的calc_batch_id，用于追溯本条指标属于哪次计算';"
                )
            except Exception:
                pass  # 列可能尚不存在

            # 清空旧的多空指标数据（列名迁移后数值语义不对，需重新计算）
            cur.execute("""
                DELETE FROM aistock_factor_metrics
                WHERE top_excess_annual_return IS NULL
                  AND top_excess_sharpe IS NULL
                  AND benchmark_annual_return IS NULL
            """)
            deleted = cur.rowcount
            if deleted > 0:
                print(f"[db][upgrade] Cleared {deleted} stale long-short metrics rows")

            # 3. 迁移后的 COMMENT（新列名必须在 rename 之后才能注释）
            for sql in POST_MIGRATION_COMMENTS:
                try:
                    cur.execute(sql)
                except Exception:
                    pass  # 列可能尚不存在（全新建表时由 DDL 创建）


if __name__ == "__main__":
    from pathlib import Path
    from dotenv import load_dotenv
    # 寻找 .env 文件（假设在 backend 的上级目录）
    env_path = Path(__file__).resolve().parents[2] / ".env"
    load_dotenv(env_path, override=True)
    
    init_quant_schema()
