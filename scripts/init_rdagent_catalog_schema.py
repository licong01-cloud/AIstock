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

运行方式（在项目根目录 F:\Dev\AIstock 下）：

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
        factor_name        TEXT PRIMARY KEY,
        catalog_version    TEXT NOT NULL,
        generated_at_utc   TIMESTAMPTZ NOT NULL,
        catalog_source     TEXT NOT NULL,
        expression         TEXT,
        source             TEXT,
        region             TEXT,
        tags               JSONB,
        raw_payload        JSONB
    )
    """,
    # 因子表列注释
    "COMMENT ON TABLE aistock_factor_catalog IS 'AIstock 因子字典总表, 对应 RD-Agent 导出的 factor_catalog.json';",
    "COMMENT ON COLUMN aistock_factor_catalog.factor_name IS '因子名称, 对应 JSON 中 factors[].name, 主键';",
    "COMMENT ON COLUMN aistock_factor_catalog.catalog_version IS 'catalog 版本号, 对应 JSON 顶层 version';",
    "COMMENT ON COLUMN aistock_factor_catalog.generated_at_utc IS 'catalog 生成时间 (UTC), 对应 generated_at_utc';",
    "COMMENT ON COLUMN aistock_factor_catalog.catalog_source IS 'catalog 来源标识, 例如 rdagent_tools';",
    "COMMENT ON COLUMN aistock_factor_catalog.expression IS '因子表达式文本, 对应 factors[].expression';",
    "COMMENT ON COLUMN aistock_factor_catalog.source IS '因子来源类型, 如 qlib_alpha158 或 rdagent';",
    "COMMENT ON COLUMN aistock_factor_catalog.region IS '因子适用区域, 如 cn';",
    "COMMENT ON COLUMN aistock_factor_catalog.tags IS '因子标签数组 (JSONB), 对应 factors[].tags';",
    "COMMENT ON COLUMN aistock_factor_catalog.raw_payload IS '因子条目的原始 JSON 负载 (去除 name 后的其余字段), 便于扩展';",
    # 因子表索引
    "CREATE INDEX IF NOT EXISTS idx_aistock_factor_catalog_source ON aistock_factor_catalog(source);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_factor_catalog_region ON aistock_factor_catalog(region);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_factor_catalog_tags_gin ON aistock_factor_catalog USING GIN(tags);",
    # 2) 策略目录表
    """
    CREATE TABLE IF NOT EXISTS aistock_strategy_catalog (
        strategy_id            TEXT PRIMARY KEY,
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
        model_config           JSONB
    )
    """,
    # 策略表列注释
    "COMMENT ON TABLE aistock_strategy_catalog IS 'RD-Agent 策略模板目录, 对应 strategy_catalog.json';",
    "COMMENT ON COLUMN aistock_strategy_catalog.strategy_id IS '策略模板 ID, 对应 strategies[].strategy_id, 主键';",
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
    # 策略表索引
    "CREATE INDEX IF NOT EXISTS idx_aistock_strategy_catalog_step_name ON aistock_strategy_catalog(step_name);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_strategy_catalog_action ON aistock_strategy_catalog(action);",
    # 3) loop / 回测目录表
    """
    CREATE TABLE IF NOT EXISTS aistock_loop_catalog (
        id                       BIGSERIAL PRIMARY KEY,
        catalog_version          TEXT NOT NULL,
        generated_at_utc         TIMESTAMPTZ NOT NULL,
        catalog_source           TEXT NOT NULL,
        task_run_id              TEXT NOT NULL,
        loop_id                  INTEGER NOT NULL,
        workspace_id             TEXT,
        scenario                 TEXT,
        step_name                TEXT,
        action                   TEXT,
        status                   TEXT,
        has_result               BOOLEAN,
        strategy_id              TEXT,
        factor_names             JSONB,
        metrics                  JSONB,
        decision                 TEXT,
        summary_execution        TEXT,
        summary_value_feedback   TEXT,
        summary_shape_feedback   TEXT,
        path_factor_meta         TEXT,
        path_factor_perf         TEXT,
        path_feedback            TEXT,
        path_ret_curve           TEXT,
        path_dd_curve            TEXT,
        UNIQUE (task_run_id, loop_id)
    )
    """,
    # loop 表列注释
    "COMMENT ON TABLE aistock_loop_catalog IS 'RD-Agent 回测/loop 结果目录, 对应 loop_catalog.json';",
    "COMMENT ON COLUMN aistock_loop_catalog.id IS '自增主键, 便于内部引用';",
    "COMMENT ON COLUMN aistock_loop_catalog.catalog_version IS 'catalog 版本号, 对应 JSON 顶层 version';",
    "COMMENT ON COLUMN aistock_loop_catalog.generated_at_utc IS 'catalog 生成时间 (UTC), 对应 generated_at_utc';",
    "COMMENT ON COLUMN aistock_loop_catalog.catalog_source IS 'catalog 来源标识, 例如 rdagent_tools';",
    "COMMENT ON COLUMN aistock_loop_catalog.task_run_id IS '任务运行 ID, 对应 loops[].task_run_id, 与 loop_id 共同唯一标识一条 loop 记录';",
    "COMMENT ON COLUMN aistock_loop_catalog.loop_id IS 'loop 序号, 对应 loops[].loop_id, 与 task_run_id 共同唯一';",
    "COMMENT ON COLUMN aistock_loop_catalog.workspace_id IS '对应的 workspace_id, 用于关联 RD-Agent workspace 目录';",
    "COMMENT ON COLUMN aistock_loop_catalog.scenario IS '场景标识, 对应 loops[].scenario, 可为空';",
    "COMMENT ON COLUMN aistock_loop_catalog.step_name IS '步骤名称, 例如 train/backtest, 对应 loops[].step_name';",
    "COMMENT ON COLUMN aistock_loop_catalog.action IS '动作类型, 例如 backtest/finetune, 对应 loops[].action';",
    "COMMENT ON COLUMN aistock_loop_catalog.status IS 'loop 状态, 例如 success/failed 等, 对应 loops[].status';",
    "COMMENT ON COLUMN aistock_loop_catalog.has_result IS '是否存在可用结果, 对应 loops[].has_result';",
    "COMMENT ON COLUMN aistock_loop_catalog.strategy_id IS '关联的策略模板 ID, 对应 loops[].strategy_id, 可与 aistock_strategy_catalog.strategy_id 建立逻辑关联';",
    "COMMENT ON COLUMN aistock_loop_catalog.factor_names IS '本次 loop 参与的因子名称列表 (JSONB), 对应 loops[].factor_names';",
    "COMMENT ON COLUMN aistock_loop_catalog.metrics IS '本次 loop 的核心指标字典 (JSONB), 对应 loops[].metrics (如年化收益/信息比等)';",
    "COMMENT ON COLUMN aistock_loop_catalog.decision IS '本次 loop 的人工或系统决策结果, 例如 BUY/SKIP 等, 对应 loops[].decision';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_execution IS '执行侧总结文本, 对应 loops[].summary_texts.execution';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_value_feedback IS '收益/价值反馈文本, 对应 loops[].summary_texts.value_feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.summary_shape_feedback IS '曲线形态/风险反馈文本, 对应 loops[].summary_texts.shape_feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_factor_meta IS 'factor_meta.json 相对路径, 对应 loops[].paths.factor_meta';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_factor_perf IS 'factor_perf.json 相对路径, 对应 loops[].paths.factor_perf';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_feedback IS 'feedback.json 相对路径, 对应 loops[].paths.feedback';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_ret_curve IS '收益曲线文件相对路径, 对应 loops[].paths.ret_curve';",
    "COMMENT ON COLUMN aistock_loop_catalog.path_dd_curve IS '回撤曲线文件相对路径, 对应 loops[].paths.dd_curve';",
    # loop 表索引
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_task_run_loop ON aistock_loop_catalog(task_run_id, loop_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_strategy_id ON aistock_loop_catalog(strategy_id);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_status ON aistock_loop_catalog(status);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_step_name ON aistock_loop_catalog(step_name);",
    "CREATE INDEX IF NOT EXISTS idx_aistock_loop_catalog_action ON aistock_loop_catalog(action);",
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

    print("RD-Agent Phase2 catalog tables ensured (aistock_factor_catalog / aistock_strategy_catalog / aistock_loop_catalog)")


if __name__ == "__main__":
    init_rdagent_catalog_schema()
