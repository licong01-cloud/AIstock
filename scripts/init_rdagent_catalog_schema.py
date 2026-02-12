"""初始化 RD-Agent Phase2 因子/策略/loop Catalog 相关表.

本脚本创建以下表（位于 public schema）：
- aistock_factor_catalog
- aistock_strategy_catalog
- aistock_loop_catalog

使用 .env 中的 TDX_DB_* 环境变量连接 PostgreSQL：
- TDX_DB_HOST
- TDX_DB_PORT
- TDX_DB_USER
- TDX_DB_PASSWORD
- TDX_DB_NAME

运行方式（在项目根目录 F:\\Dev\\AIstock 下）：

    python -m scripts.init_rdagent_catalog_schema

脚本为每个字段添加 COMMENT，便于后续文档化与运维查看。
"""

from __future__ import annotations

import os
from typing import List

import psycopg2
from psycopg2.extensions import connection as PGConnection
from dotenv import load_dotenv


load_dotenv(override=True)


def _db_cfg() -> dict:
    """从环境变量构造数据库连接配置.

    与 backend.db.pg_pool._db_cfg 使用同一套 TDX_DB_* 约定，避免重复配置。
    """

    return {
        "host": os.getenv("TDX_DB_HOST", "127.0.0.1"),
        "port": int(os.getenv("TDX_DB_PORT", "5432")),
        "user": os.getenv("TDX_DB_USER", "postgres"),
        "password": os.getenv("TDX_DB_PASSWORD", ""),
        "dbname": os.getenv("TDX_DB_NAME", "aistock"),
        "application_name": "AIstock-rdagent-catalog-init",
    }


DDL_STATEMENTS: List[str] = [
    # 1) 因子总表
    """
    CREATE TABLE IF NOT EXISTS aistock_factor_catalog (
        factor_name        TEXT NOT NULL,
        source             TEXT NOT NULL,
        catalog_version    TEXT NOT NULL,
        generated_at_utc   TIMESTAMPTZ NOT NULL,
        catalog_source     TEXT NOT NULL,
        expression         TEXT,
        region             TEXT,
        tags               JSONB,
        description_cn     TEXT,
        formula_hint       TEXT,
        variables          JSONB,
        freq               TEXT,
        align              TEXT,
        nan_policy         TEXT,
        created_at_utc     TIMESTAMPTZ,
        experiment_id      TEXT,
        impl_module        TEXT,
        impl_func          TEXT,
        impl_version       TEXT,
        performance_metrics JSONB,
        first_sota_task_id TEXT,
        is_sota_factor    BOOLEAN,
        best_performance   TEXT,
        best_performance_sharpe DOUBLE PRECISION,
        best_performance_ann_ret DOUBLE PRECISION,
        interface_info     JSONB,
        asset_bundle_id    TEXT,
        raw_payload        JSONB,
        PRIMARY KEY (factor_name, source)
    )
    """,
    # 迁移逻辑: 如果主键只是 factor_name, 则变更为 (factor_name, source)
    """
    DO $$ 
    BEGIN 
        IF EXISTS (
            SELECT 1 FROM information_schema.table_constraints 
            WHERE table_name='aistock_factor_catalog' AND constraint_type='PRIMARY KEY'
        ) THEN
            -- 检查当前主键列数，如果只有一列则认为是旧的
            IF (
                SELECT count(*) FROM information_schema.key_column_usage 
                WHERE table_name='aistock_factor_catalog' 
                AND constraint_name=(
                    SELECT constraint_name FROM information_schema.table_constraints 
                    WHERE table_name='aistock_factor_catalog' AND constraint_type='PRIMARY KEY'
                )
            ) = 1 THEN
                ALTER TABLE aistock_factor_catalog DROP CONSTRAINT aistock_factor_catalog_pkey;
                ALTER TABLE aistock_factor_catalog ADD PRIMARY KEY (factor_name, source);
            END IF;
        END IF;
    END $$;
    """,
    "CREATE INDEX IF NOT EXISTS idx_aistock_factor_catalog_tags_gin ON aistock_factor_catalog USING GIN(tags);",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS description_cn TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS formula_hint TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS variables JSONB;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS freq TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS align TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS nan_policy TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS created_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS experiment_id TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS impl_module TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS impl_func TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS impl_version TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS performance_metrics JSONB;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS first_sota_task_id TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS is_sota_factor BOOLEAN;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS best_performance TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS best_performance_sharpe DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS best_performance_ann_ret DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS interface_info JSONB;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS asset_bundle_id TEXT;",
    # 因子表列注释
    "COMMENT ON TABLE aistock_factor_catalog IS 'AIstock 因子字典总表, 对应 RD-Agent 导出的 factor_catalog.json';",
    "COMMENT ON COLUMN aistock_factor_catalog.factor_name IS '因子名称, 对应 JSON 中 factors[].name, 主键';",
    "COMMENT ON COLUMN aistock_factor_catalog.catalog_version IS 'catalog 版本号, 对应 JSON 顶层 version';",
    "COMMENT ON COLUMN aistock_factor_catalog.generated_at_utc IS 'catalog 生成时间 (UTC), 对应顶层 generated_at_utc';",
    "COMMENT ON COLUMN aistock_factor_catalog.catalog_source IS 'catalog 来源标识, 例如 rdagent_tools';",
    "COMMENT ON COLUMN aistock_factor_catalog.expression IS '因子表达式文本, 对应 factors[].expression 或 Alpha 表达式';",
    "COMMENT ON COLUMN aistock_factor_catalog.source IS '因子来源类型, 如 qlib_alpha158 或 rdagent_generated 等';",
    "COMMENT ON COLUMN aistock_factor_catalog.region IS '因子适用区域, 如 cn';",
    "COMMENT ON COLUMN aistock_factor_catalog.tags IS '因子标签数组 (JSONB), 对应 factors[].tags';",
    "COMMENT ON COLUMN aistock_factor_catalog.description_cn IS '因子中文描述, 对应 factors[].description_cn';",
    "COMMENT ON COLUMN aistock_factor_catalog.formula_hint IS '因子计算公式提示/表达式说明, 对应 factors[].formula_hint';",
    "COMMENT ON COLUMN aistock_factor_catalog.variables IS '因子变量与参数字典 (JSONB), 对应 factors[].variables';",
    "COMMENT ON COLUMN aistock_factor_catalog.freq IS '因子计算频率, 如 1d, 对应 factors[].freq';",
    "COMMENT ON COLUMN aistock_factor_catalog.align IS '时间对齐规则, 如 close, 对应 factors[].align';",
    "COMMENT ON COLUMN aistock_factor_catalog.nan_policy IS '缺失值处理策略, 对应 factors[].nan_policy';",
    "COMMENT ON COLUMN aistock_factor_catalog.created_at_utc IS '因子自身创建时间 (UTC), 对应 factors[].created_at_utc';",
    "COMMENT ON COLUMN aistock_factor_catalog.experiment_id IS '产生该因子的实验 ID, 通常为 task_run_id/loop_id 组合, 对应 factors[].experiment_id';",
    "COMMENT ON COLUMN aistock_factor_catalog.impl_module IS '因子实现所在的 Python 模块, 对应 factors[].impl_module';",
    "COMMENT ON COLUMN aistock_factor_catalog.impl_func IS '因子实现函数名, 对应 factors[].impl_func';",
    "COMMENT ON COLUMN aistock_factor_catalog.impl_version IS '因子实现版本号, 对应 factors[].impl_version';",
    "COMMENT ON COLUMN aistock_factor_catalog.performance_metrics IS '因子回测绩效指标 (JSONB), 包含各窗口的 IC/收益等, 对应 factor_perf.json 中的数据';",
    "COMMENT ON COLUMN aistock_factor_catalog.first_sota_task_id IS '非 alpha 因子的首次进入 SOTA 的 task_id（以首次入选为准，后续再次入选不覆盖），用于去重与追溯';",
    "COMMENT ON COLUMN aistock_factor_catalog.is_sota_factor IS '是否为 SOTA 因子（至少一次进入 SOTA 则为 true）';",
    "COMMENT ON COLUMN aistock_factor_catalog.best_performance IS '因子历史最佳表现摘要 (如 Sharpe/年化收益), 对应 factors[].best_performance';",
    "COMMENT ON COLUMN aistock_factor_catalog.best_performance_sharpe IS '因子历史最佳 Sharpe 指标, 对应 factors[].best_performance_sharpe';",
    "COMMENT ON COLUMN aistock_factor_catalog.best_performance_ann_ret IS '因子历史最佳年化收益率, 对应 factors[].best_performance_ann_ret';",
    "COMMENT ON COLUMN aistock_factor_catalog.interface_info IS '因子接口信息 (JSONB), 包含 standard_wrapper 等, 对应 interface_info 字段 (REQ-FACTOR-P3-001)';",
    "COMMENT ON COLUMN aistock_factor_catalog.asset_bundle_id IS '因子所属资产包 ID (若由 RD-Agent 资产包导出)';",
    "COMMENT ON COLUMN aistock_factor_catalog.raw_payload IS '因子条目的原始 JSON 负载 (去除已结构化字段后的其余字段), 便于扩展';",
    # 2) 策略目录表
    """
    CREATE TABLE IF NOT EXISTS aistock_strategy_catalog (
        strategy_id            TEXT PRIMARY KEY,
        display_name           TEXT,
        catalog_version        TEXT NOT NULL,
        generated_at_utc       TIMESTAMPTZ NOT NULL,
        catalog_source         TEXT NOT NULL,
        scenario               TEXT,
        step_name              TEXT,
        action                 TEXT,
        example_task_run_id    TEXT,
        example_loop_id        INTEGER,
        example_workspace_id   TEXT,
        example_workspace_path TEXT,
        template_files         JSONB,
        data_config            JSONB,
        dataset_config         JSONB,
        portfolio_config       JSONB,
        backtest_config        JSONB,
        model_config           JSONB,
        feature_list           JSONB,
        market                 TEXT,
        instruments            JSONB,
        freq                   TEXT
    )
    """,
    # 策略表增加字段支持
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS display_name TEXT;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS feature_list JSONB;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS market TEXT;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS instruments JSONB;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS freq TEXT;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS python_implementation JSONB;",
    # 策略表列注释
    "COMMENT ON TABLE aistock_strategy_catalog IS 'RD-Agent 策略模板目录, 对应 strategy_catalog.json';",
    "COMMENT ON COLUMN aistock_strategy_catalog.strategy_id IS '策略模板 ID, 对应 strategies[].strategy_id, 主键';",
    "COMMENT ON COLUMN aistock_strategy_catalog.display_name IS '策略展示名称 (如 strategy-<short_id>), 便于前端展示';",
    "COMMENT ON COLUMN aistock_strategy_catalog.catalog_version IS 'catalog 版本号, 对应 JSON 顶层 version';",
    "COMMENT ON COLUMN aistock_strategy_catalog.generated_at_utc IS 'catalog 生成时间 (UTC), 对应 generated_at_utc';",
    "COMMENT ON COLUMN aistock_strategy_catalog.catalog_source IS 'catalog 来源标识, 例如 rdagent_tools';",
    "COMMENT ON COLUMN aistock_strategy_catalog.scenario IS '策略场景标识, 对应 strategies[].scenario, 可为空';",
    "COMMENT ON COLUMN aistock_strategy_catalog.step_name IS '策略所在步骤名称, 例如 train/backtest, 对应 strategies[].step_name';",
    "COMMENT ON COLUMN aistock_strategy_catalog.action IS '策略动作类型, 例如 finetune/backtest, 对应 strategies[].action';",
    "COMMENT ON COLUMN aistock_strategy_catalog.example_task_run_id IS '示例 workspace 所属 task_run_id, 对应 workspace_example.task_run_id';",
    "COMMENT ON COLUMN aistock_strategy_catalog.example_loop_id IS '示例 workspace 所属 loop_id, 对应 workspace_example.loop_id';",
    "COMMENT ON COLUMN aistock_strategy_catalog.example_workspace_id IS '示例 workspace 的 workspace_id, 对应 workspace_example.workspace_id';",
    "COMMENT ON COLUMN aistock_strategy_catalog.example_workspace_path IS '示例 workspace 的绝对路径, 对应 workspace_example.workspace_path';",
    "COMMENT ON COLUMN aistock_strategy_catalog.template_files IS '策略模板文件相对路径列表 (JSONB), 对应 strategies[].template_files';",
    "COMMENT ON COLUMN aistock_strategy_catalog.data_config IS 'Qlib data_config 原始配置 (JSONB), 对应 strategies[].data_config';",
    "COMMENT ON COLUMN aistock_strategy_catalog.dataset_config IS 'Qlib dataset_config 原始配置 (JSONB), 对应 strategies[].dataset_config';",
    "COMMENT ON COLUMN aistock_strategy_catalog.portfolio_config IS '组合/投资组合相关配置 (JSONB), 对应 strategies[].portfolio_config';",
    "COMMENT ON COLUMN aistock_strategy_catalog.backtest_config IS '回测相关配置 (JSONB), 对应 strategies[].backtest_config';",
    "COMMENT ON COLUMN aistock_strategy_catalog.model_config IS '模型相关配置 (JSONB), 对应 strategies[].model_config';",
    "COMMENT ON COLUMN aistock_strategy_catalog.feature_list IS '特征列表 (JSONB), 对应 strategies[].feature_list';",
    "COMMENT ON COLUMN aistock_strategy_catalog.market IS '适用市场标识, 对应 strategies[].market';",
    "COMMENT ON COLUMN aistock_strategy_catalog.instruments IS '标的池配置 (JSONB), 对应 strategies[].instruments';",
    "COMMENT ON COLUMN aistock_strategy_catalog.freq IS '执行频率, 对应 strategies[].freq';",
    "COMMENT ON COLUMN aistock_strategy_catalog.python_implementation IS '策略 Python 实现配置 (JSONB), 包含 module 和 func, 对应 python_implementation (REQ-STRATEGY-P3-001)';",
    # 策略表索引
    "CREATE INDEX IF NOT EXISTS idx_aistock_strategy_catalog_step_name ON aistock_strategy_catalog(step_name);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_strategy_catalog_action ON aistock_strategy_catalog(action);",
    # Task 选股新增：策略表补充 task 关联字段（不影响现有逻辑，仅用于 task 详情追溯）
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS task_id TEXT;",
    "ALTER TABLE aistock_strategy_catalog ADD COLUMN IF NOT EXISTS task_dir TEXT;",
    "COMMENT ON COLUMN aistock_strategy_catalog.task_id IS '关联的 task_id（log 目录名）；用于 task 级展示与追溯，不要求唯一';",
    "COMMENT ON COLUMN aistock_strategy_catalog.task_dir IS '该策略在 AIstock 侧落盘的 task 目录路径（可空）；用于快速定位 task 资产';",
    # 3) loop / 回测目录表
    """
    CREATE TABLE IF NOT EXISTS aistock_loop_catalog (
        id                       BIGSERIAL PRIMARY KEY,
        catalog_version          TEXT NOT NULL,
        generated_at_utc         TIMESTAMPTZ NOT NULL,
        catalog_source           TEXT NOT NULL,
        task_run_id              TEXT NOT NULL,
        loop_id                  INTEGER NOT NULL,
        display_name             TEXT,
        workspace_id             TEXT,
        workspace_path           TEXT,
        asset_bundle_id          TEXT,
        is_solidified            BOOLEAN,
        sync_status              TEXT,
        scenario                 TEXT,
        step_name                TEXT,
        action                   TEXT,
        status                   TEXT,
        has_result               BOOLEAN,
        strategy_id              TEXT,
        factor_names             JSONB,
        metrics                  JSONB,
        annualized_return        DOUBLE PRECISION,
        max_drawdown             DOUBLE PRECISION,
        sharpe                   DOUBLE PRECISION,
        ic                       DOUBLE PRECISION,
        ic_ir                    DOUBLE PRECISION,
        win_rate                 DOUBLE PRECISION,
        decision                 TEXT,
        summary_execution        TEXT,
        summary_value_feedback   TEXT,
        summary_shape_feedback   TEXT,
        path_factor_meta         TEXT,
        path_factor_perf         TEXT,
        path_feedback            TEXT,
        path_ret_curve           TEXT,
        path_dd_curve            TEXT,
        log_dir                  TEXT,
        workspace_role           TEXT,
        path_mlruns              TEXT,
        path_model_files         TEXT,
        materialization_status   TEXT,
        materialization_error    TEXT,
        materialization_updated_at_utc TIMESTAMPTZ,
        raw_payload              JSONB,
        CONSTRAINT aistock_loop_catalog_task_run_id_loop_id_key UNIQUE (task_run_id, loop_id)
    )
    """,

    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS annualized_return DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS sharpe DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS ic DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS ic_ir DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS win_rate DOUBLE PRECISION;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS id BIGSERIAL;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS scenario TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS step_name TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS action TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS status TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS has_result BOOLEAN;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS strategy_id TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS factor_names JSONB;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS metrics JSONB;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS decision TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS summary_execution TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS summary_value_feedback TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS summary_shape_feedback TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_factor_meta TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_factor_perf TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_feedback TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_ret_curve TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_dd_curve TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS display_name TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS workspace_path TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS log_dir TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS workspace_role TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_mlruns TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_model_files TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_strategy_meta TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS path_model_meta TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS asset_bundle_id TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS is_solidified BOOLEAN;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS sync_status TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS materialization_status TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS materialization_error TEXT;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS materialization_updated_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS code_critic JSONB;",
    "ALTER TABLE aistock_loop_catalog ADD COLUMN IF NOT EXISTS limitations JSONB;",
    # loop 表列注释
    "COMMENT ON TABLE aistock_loop_catalog IS 'RD-Agent 回测/loop 结果目录, 对应 loop_catalog.json';",
    "COMMENT ON COLUMN aistock_loop_catalog.id IS '自增主键, 便于内部引用';",
    "COMMENT ON COLUMN aistock_loop_catalog.catalog_version IS 'catalog 版本号, 对应 JSON 顶层 version';",
    "COMMENT ON COLUMN aistock_loop_catalog.generated_at_utc IS 'catalog 生成时间 (UTC), 对应 generated_at_utc';",
    "COMMENT ON COLUMN aistock_loop_catalog.catalog_source IS 'catalog 来源标识, 例如 rdagent_tools';",
    "COMMENT ON COLUMN aistock_loop_catalog.task_run_id IS '任务运行 ID, 对应 loops[].task_run_id, 与 loop_id 共同唯一标识一条 loop 记录';",
    "COMMENT ON COLUMN aistock_loop_catalog.loop_id IS 'loop 序号, 对应 loops[].loop_id, 与 task_run_id 共同唯一';",
    "COMMENT ON COLUMN aistock_loop_catalog.display_name IS 'loop 展示名称 (如 task-short-id-loopX), 便于前端展示';",
    "COMMENT ON COLUMN aistock_loop_catalog.workspace_id IS '对应的 workspace_id, 用于关联 RD-Agent workspace 目录';",
    "COMMENT ON COLUMN aistock_loop_catalog.workspace_path IS 'workspace 绝对路径, 对应 loops[].workspace_path';",
    "COMMENT ON COLUMN aistock_loop_catalog.scenario IS '场景标识, 对应 loops[].scenario, 可为空';",
    "COMMENT ON COLUMN aistock_loop_catalog.step_name IS '步骤名称, 例如 train/backtest, 对应 loops[].step_name';",
    "COMMENT ON COLUMN aistock_loop_catalog.action IS '动作类型, 例如 backtest/finetune, 对应 loops[].action';",
    "COMMENT ON COLUMN aistock_loop_catalog.status IS 'loop 状态, 例如 success/failed 等, 对应 loops[].status';",
    "COMMENT ON COLUMN aistock_loop_catalog.has_result IS '是否存在可用结果, 对应 loops[].has_result';",
    "COMMENT ON COLUMN aistock_loop_catalog.materialization_status IS '物化状态: pending, running, done, failed';",
    "COMMENT ON COLUMN aistock_loop_catalog.materialization_error IS '物化失败错误摘要';",
    "COMMENT ON COLUMN aistock_loop_catalog.materialization_updated_at_utc IS '物化状态最后更新时间 (UTC)';",
    "COMMENT ON COLUMN aistock_loop_catalog.strategy_id IS '关联的策略模板 ID, 对应 loops[].strategy_id, 可与 aistock_strategy_catalog.strategy_id 建立逻辑关联';",
    "COMMENT ON COLUMN aistock_loop_catalog.factor_names IS '本次 loop 参与的因子名称列表 (JSONB), 对应 loops[].factor_names';",
    "COMMENT ON COLUMN aistock_loop_catalog.metrics IS '本次 loop 的核心指标字典 (JSONB), 对应 loops[].metrics (如年化收益/信息比等)';",
    "COMMENT ON COLUMN aistock_loop_catalog.annualized_return IS '本次 loop 的年化收益率, 对应 loops[].annualized_return 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.max_drawdown IS '本次 loop 的最大回撤, 对应 loops[].max_drawdown 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.sharpe IS '本次 loop 的 Sharpe 比率, 对应 loops[].sharpe 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.ic IS '本次 loop 的信息系数 (IC), 对应 loops[].ic 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.ic_ir IS '本次 loop 的 IC 信息比率 (IC_IR), 对应 loops[].ic_ir 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.win_rate IS '本次 loop 的胜率指标, 对应 loops[].win_rate 或 metrics 中同名字段';",
    "COMMENT ON COLUMN aistock_loop_catalog.decision IS '本次 loop 的人工或系统决策结果, 例如 BUY/SKIP 等, 对应 loops[].decision';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_execution IS '执行侧总结文本, 对应 loops[].summary_texts.execution';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_value_feedback IS '收益/价值反馈文本, 对应 loops[].summary_texts.value_feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_shape_feedback IS '曲线形态/风险反馈文本, 对应 loops[].summary_texts.shape_feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.code_critic IS '代码审阅反馈 (JSONB), 对应 loops[].summary_texts.code_critic';",
    "COMMENT ON COLUMN aistock_loop_catalog.limitations IS '局限性说明 (JSONB), 对应 loops[].summary_texts.limitations';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_factor_meta IS 'factor_meta.json 相对路径, 对应 loops[].paths.factor_meta';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_factor_perf IS 'factor_perf.json 相对路径, 对应 loops[].paths.factor_perf';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_feedback IS 'feedback.json 相对路径, 对应 loops[].paths.feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_ret_curve IS '收益曲线文件相对路径, 对应 loops[].paths.ret_curve';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_dd_curve IS '回撤曲线文件相对路径, 对应 loops[].paths.dd_curve';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_strategy_meta IS 'strategy_meta.json 相对路径 (Phase 3 补录产物)';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_model_meta IS 'model_meta.json 相对路径 (Phase 3 补录产物)';",
    "COMMENT ON COLUMN aistock_loop_catalog.asset_bundle_id IS '资产包 ID, 对应 loops[].asset_bundle_id';",
    "COMMENT ON COLUMN aistock_loop_catalog.is_solidified IS '是否已固化(物化)的 loop 结果';",
    "COMMENT ON COLUMN aistock_loop_catalog.sync_status IS 'RD-Agent 侧同步状态 (pending/success/failed 等)';",
    # loop 表索引
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'aistock_loop_catalog_task_run_id_loop_id_key') THEN ALTER TABLE aistock_loop_catalog ADD CONSTRAINT aistock_loop_catalog_task_run_id_loop_id_key UNIQUE (task_run_id, loop_id); END IF; END $$;",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_task_run_loop ON aistock_loop_catalog(task_run_id, loop_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_strategy_id ON aistock_loop_catalog(strategy_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_status ON aistock_loop_catalog(status);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_step_name ON aistock_loop_catalog(step_name);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_action ON aistock_loop_catalog(action);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_task_run_id ON aistock_loop_catalog(task_run_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_loop_id ON aistock_loop_catalog(loop_id);",

    # 4) task 表：以 task 为单位管理资产同步、manifest 与加入选股
    """
    CREATE TABLE IF NOT EXISTS aistock_task_catalog (
        task_id                      TEXT PRIMARY KEY,
        log_dir                       TEXT,
        task_run_id                   TEXT,
        created_at_utc                TIMESTAMPTZ,
        updated_at_utc                TIMESTAMPTZ,
        task_dir                      TEXT,
        manifest_path                 TEXT,
        manifest_sha1                 TEXT,
        manifest_schema_version       INTEGER,
        sync_status                   TEXT,
        sync_error                    TEXT,
        sync_diagnostics              JSONB,
        sessions_count                INTEGER,
        loops_count                   INTEGER,
        sota_factors_count            INTEGER,
        sota_models_count             INTEGER,
        last_sota_factor_workspace_id TEXT,
        candidate_model_workspace_id  TEXT,
        candidate_model_run_id        TEXT,
        is_enabled_for_selection      BOOLEAN DEFAULT FALSE,
        enabled_for_selection_at_utc  TIMESTAMPTZ,
        enabled_for_selection_by      TEXT
    )
    """,
    # task 表可能由 _ensure_task_catalog_table() 先行创建，字段不全，需补齐
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS log_dir TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS task_run_id TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS created_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS updated_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS task_dir TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS manifest_path TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS manifest_sha1 TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS manifest_schema_version INTEGER;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sync_status TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sync_error TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sync_diagnostics JSONB;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sessions_count INTEGER;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS loops_count INTEGER;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sota_factors_count INTEGER;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS sota_models_count INTEGER;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS last_sota_factor_workspace_id TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS candidate_model_workspace_id TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS candidate_model_run_id TEXT;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS is_enabled_for_selection BOOLEAN DEFAULT FALSE;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS enabled_for_selection_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_task_catalog ADD COLUMN IF NOT EXISTS enabled_for_selection_by TEXT;",
    "CREATE INDEX IF NOT EXISTS idx_aistock_task_catalog_updated_at_utc ON aistock_task_catalog(updated_at_utc);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_task_catalog_sync_status ON aistock_task_catalog(sync_status);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_task_catalog_is_enabled_for_selection ON aistock_task_catalog(is_enabled_for_selection);",

    # 4) model / 模型目录表
    """
    CREATE TABLE IF NOT EXISTS aistock_model_catalog (
        model_id           TEXT PRIMARY KEY,
        catalog_version    TEXT NOT NULL,
        generated_at_utc   TIMESTAMPTZ NOT NULL,
        catalog_source     TEXT NOT NULL,
        task_run_id        TEXT NOT NULL,
        loop_id            INTEGER NOT NULL,
        workspace_id       TEXT NOT NULL,
        workspace_path     TEXT,
        log_dir            TEXT,
        display_name       TEXT,
        model_type         TEXT,
        model_config       JSONB,
        dataset_config     JSONB,
        feature_schema     JSONB,
        flattened_feature_list JSONB,
        "window"           INTEGER,
        freq               TEXT,
        model_artifacts    JSONB,
        asset_bundle_id    TEXT,
        raw_payload        JSONB,
        CONSTRAINT aistock_model_catalog_task_run_loop_workspace_key UNIQUE (task_run_id, loop_id, workspace_id)
    )
    """,
    # 迁移逻辑: 补齐缺失字段
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS workspace_path TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS log_dir TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS display_name TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS model_type TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS model_config JSONB;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS dataset_config JSONB;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS feature_schema JSONB;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS flattened_feature_list JSONB;",
    'ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS "window" INTEGER;',
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS freq TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS model_artifacts JSONB;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS asset_bundle_id TEXT;",
    "ALTER TABLE aistock_model_catalog ADD COLUMN IF NOT EXISTS raw_payload JSONB;",
    # 迁移逻辑: 如果主键是 BIGSERIAL (id), 则变更为 model_id
    """
    DO $$ 
    BEGIN 
        IF EXISTS (
            SELECT 1 FROM information_schema.columns 
            WHERE table_name='aistock_model_catalog' AND column_name='id'
        ) THEN
            -- 确保所有行都有 model_id，如果没有则用现有字段构造一个
            UPDATE aistock_model_catalog SET model_id = task_run_id || '_' || loop_id || '_' || workspace_id WHERE model_id IS NULL;
            
            -- 删除旧主键和列
            ALTER TABLE aistock_model_catalog DROP CONSTRAINT IF EXISTS aistock_model_catalog_pkey CASCADE;
            ALTER TABLE aistock_model_catalog DROP COLUMN IF EXISTS id;
            
            -- 设置 model_id 为新主键
            ALTER TABLE aistock_model_catalog ALTER COLUMN model_id SET NOT NULL;
            ALTER TABLE aistock_model_catalog ADD PRIMARY KEY (model_id);
        END IF;
    END $$;
    """,
    "COMMENT ON TABLE aistock_model_catalog IS 'RD-Agent 模型目录, 对应 model_catalog.json';",
    "COMMENT ON COLUMN aistock_model_catalog.model_id IS '模型实例唯一 ID, 对应 models[].model_id';",
    "COMMENT ON COLUMN aistock_model_catalog.task_run_id IS '任务运行 ID, 对应 models[].task_run_id';",
    "COMMENT ON COLUMN aistock_model_catalog.loop_id IS 'loop 序号, 对应 models[].loop_id';",
    "COMMENT ON COLUMN aistock_model_catalog.workspace_id IS 'workspace_id, 对应 models[].workspace_id';",
    "COMMENT ON COLUMN aistock_model_catalog.workspace_path IS 'workspace 绝对路径, 对应 models[].workspace_path';",
    "COMMENT ON COLUMN aistock_model_catalog.display_name IS '模型展示名称 (task-short-id-loopX-model-*)';",
    "COMMENT ON COLUMN aistock_model_catalog.model_config IS '模型配置 JSON, 对应 models[].model_config';",
    "COMMENT ON COLUMN aistock_model_catalog.feature_schema IS '特征 Schema 定义 JSON, 对应 models[].feature_schema';",
    "COMMENT ON COLUMN aistock_model_catalog.flattened_feature_list IS '展平后的特征名称列表 (JSONB), 对应 models[].flattened_feature_list';",
    "COMMENT ON COLUMN aistock_model_catalog.window IS '模型训练窗口大小, 对应 models[].window';",
    "COMMENT ON COLUMN aistock_model_catalog.freq IS '模型预测频率, 对应 models[].freq';",
    "COMMENT ON COLUMN aistock_model_catalog.model_artifacts IS '模型产物路径字典, 对应 models[].model_artifacts';",
    "COMMENT ON COLUMN aistock_model_catalog.asset_bundle_id IS '模型所属资产包 ID (若由 RD-Agent 资产包导出)';",
    "COMMENT ON COLUMN aistock_model_catalog.raw_payload IS '原始 JSON 负载（去除已结构化字段后的其余字段）';",
    "DO $$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'aistock_model_catalog_task_run_loop_workspace_key') THEN ALTER TABLE aistock_model_catalog ADD CONSTRAINT aistock_model_catalog_task_run_loop_workspace_key UNIQUE (task_run_id, loop_id, workspace_id); END IF; END $$;",
    "CREATE INDEX IF NOT EXISTS idx_aistock_model_catalog_task_run_loop ON aistock_model_catalog(task_run_id, loop_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_model_catalog_workspace_id ON aistock_model_catalog(workspace_id);",

    # 5) alpha158 因子库元信息表
    """
    CREATE TABLE IF NOT EXISTS aistock_alpha158_meta (
        factor_name        TEXT PRIMARY KEY,
        lib_version        TEXT NOT NULL,
        generated_at_utc   TIMESTAMPTZ,
        expression         TEXT,
        description_cn     TEXT,
        variables          JSONB,
        freq               TEXT,
        align              TEXT,
        nan_policy         TEXT,
        impl_module        TEXT,
        impl_func          TEXT,
        impl_version       TEXT,
        raw_payload        JSONB
    )
    """,
    "COMMENT ON TABLE aistock_alpha158_meta IS 'Alpha158 因子库元信息表, 对应 RD-Agent /alpha158/meta 输出';",
    "COMMENT ON COLUMN aistock_alpha158_meta.factor_name IS '因子名称, 对应 meta[].name, 主键';",
    "COMMENT ON COLUMN aistock_alpha158_meta.lib_version IS 'rd_factors_lib 版本号, 对应 alpha158_meta.version 或同义字段';",
    "COMMENT ON COLUMN aistock_alpha158_meta.generated_at_utc IS '元信息生成时间 (UTC)';",
    "COMMENT ON COLUMN aistock_alpha158_meta.expression IS '因子表达式文本, 对应 meta[].expression';",
    "COMMENT ON COLUMN aistock_alpha158_meta.description_cn IS '因子中文描述, 对应 meta[].description_cn';",
    "COMMENT ON COLUMN aistock_alpha158_meta.variables IS '因子变量/参数定义 (JSONB), 对应 meta[].variables';",
    "COMMENT ON COLUMN aistock_alpha158_meta.freq IS '因子计算频率, 对应 meta[].freq';",
    "COMMENT ON COLUMN aistock_alpha158_meta.align IS '时间对齐规则, 对应 meta[].align';",
    "COMMENT ON COLUMN aistock_alpha158_meta.nan_policy IS '缺失值处理策略, 对应 meta[].nan_policy';",
    "COMMENT ON COLUMN aistock_alpha158_meta.impl_module IS '因子实现所在模块, 对应 meta[].impl_module';",
    "COMMENT ON COLUMN aistock_alpha158_meta.impl_func IS '因子实现函数名, 对应 meta[].impl_func';",
    "COMMENT ON COLUMN aistock_alpha158_meta.impl_version IS '因子实现版本号, 对应 meta[].impl_version';",
    "COMMENT ON COLUMN aistock_alpha158_meta.raw_payload IS '原始 JSON 负载（去除已结构化字段后的其余字段）';",
    "CREATE INDEX IF NOT EXISTS idx_aistock_alpha158_meta_lib_version ON aistock_alpha158_meta(lib_version);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_alpha158_meta_freq ON aistock_alpha158_meta(freq);",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS generated_at_utc TIMESTAMPTZ;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS expression TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS description_cn TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS variables JSONB;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS freq TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS align TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS nan_policy TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS impl_module TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS impl_func TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS impl_version TEXT;",
    "ALTER TABLE aistock_alpha158_meta ADD COLUMN IF NOT EXISTS raw_payload JSONB;",

    # 6) aistock_factor_catalog 扩展字段 — SOTA 因子入库所需
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_task_id TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_code_relpath TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_code_origin TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_loop_tag TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS source_index INTEGER;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS ic DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS icir DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS max_drawdown DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS annualized_return DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS sharpe DOUBLE PRECISION;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS dedup_hash TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS dedup_group_id TEXT;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS is_dedup_primary BOOLEAN DEFAULT TRUE;",
    "ALTER TABLE aistock_factor_catalog ADD COLUMN IF NOT EXISTS code_text TEXT;",
    "CREATE INDEX IF NOT EXISTS idx_factor_catalog_dedup_hash ON aistock_factor_catalog(dedup_hash);",
    "CREATE INDEX IF NOT EXISTS idx_factor_catalog_dedup_group ON aistock_factor_catalog(dedup_group_id);",
    "CREATE INDEX IF NOT EXISTS idx_factor_catalog_source_task ON aistock_factor_catalog(source_task_id);",
    "CREATE INDEX IF NOT EXISTS idx_factor_catalog_is_sota ON aistock_factor_catalog(is_sota_factor);",
    "COMMENT ON COLUMN aistock_factor_catalog.source_task_id IS '来源 Task ID（RD-Agent log 目录名）';",
    "COMMENT ON COLUMN aistock_factor_catalog.source_code_relpath IS '因子代码在 AIstock 侧 task 目录下的相对路径';",
    "COMMENT ON COLUMN aistock_factor_catalog.source_code_origin IS 'RD-Agent 侧原始 asset key';",
    "COMMENT ON COLUMN aistock_factor_catalog.source_loop_tag IS '因子首次出现的 loop_id';",
    "COMMENT ON COLUMN aistock_factor_catalog.source_index IS '因子在 SOTA 列表中的序号';",
    "COMMENT ON COLUMN aistock_factor_catalog.ic IS '因子 IC 值（从 loop 回测指标提取）';",
    "COMMENT ON COLUMN aistock_factor_catalog.icir IS '因子 ICIR 值';",
    "COMMENT ON COLUMN aistock_factor_catalog.max_drawdown IS '因子所在 loop 最大回撤';",
    "COMMENT ON COLUMN aistock_factor_catalog.annualized_return IS '因子所在 loop 年化收益率';",
    "COMMENT ON COLUMN aistock_factor_catalog.sharpe IS '因子所在 loop 信息比率/Sharpe';",
    "COMMENT ON COLUMN aistock_factor_catalog.dedup_hash IS '因子核心代码归一化后的 SHA256 哈希，用于去重';",
    "COMMENT ON COLUMN aistock_factor_catalog.dedup_group_id IS '去重分组 ID，相同逻辑因子共享同一 group';",
    "COMMENT ON COLUMN aistock_factor_catalog.is_dedup_primary IS '是否为去重组内的主因子';",
    "COMMENT ON COLUMN aistock_factor_catalog.code_text IS '因子核心计算代码文本（用于去重比较和前端展示）';",
]


def init_rdagent_catalog_schema(conn: PGConnection | None = None) -> None:
    """执行所有 DDL 与 COMMENT 语句, 幂等地创建 Catalog 相关表和索引."""

    close_conn = False
    if conn is None:
        cfg = _db_cfg()
        conn = psycopg2.connect(**cfg)
        close_conn = True

    try:
        with conn.cursor() as cur:
            for sql in DDL_STATEMENTS:
                cur.execute(sql)
        conn.commit()
    finally:
        if close_conn and conn is not None:
            conn.close()

    print("RD-Agent Phase2 catalog tables ensured (aistock_factor_catalog / aistock_strategy_catalog / aistock_loop_catalog / aistock_model_catalog)")


if __name__ == "__main__":
    init_rdagent_catalog_schema()
