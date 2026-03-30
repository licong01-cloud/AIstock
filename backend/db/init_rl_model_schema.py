"""初始化 RL 执行模型版本管理表 DDL.

二维版本管理:
- dev_version: 开发版本 (模型架构/超参/特征修改)
- roll_tag: 滚动标签 (训练数据时间窗口)
- version_tag: 完整标签 = dev_version + '-' + roll_tag
"""
from __future__ import annotations

from typing import List
from .pg_pool import get_conn


DDL: List[str] = [
    "CREATE SCHEMA IF NOT EXISTS app",
    """
    CREATE TABLE IF NOT EXISTS app.rl_model_versions (
        id                    BIGSERIAL PRIMARY KEY,

        -- 二维版本标识
        dev_version           TEXT NOT NULL,
        roll_tag              TEXT NOT NULL DEFAULT 'init',
        version_tag           TEXT GENERATED ALWAYS AS (dev_version || '-' || roll_tag) STORED,

        -- 开发版本描述
        dev_description       TEXT,
        parent_dev            TEXT,

        -- 模型文件
        policy_path           TEXT NOT NULL,

        -- 训练信息
        train_type            TEXT NOT NULL DEFAULT 'initial',
        train_start           DATE,
        train_end             DATE,
        purge_end             DATE,
        train_epochs          INT,
        train_duration_sec    INT,
        train_config          JSONB,

        -- 模型架构摘要
        state_dim             INT,
        action_dim            INT,
        network_arch          TEXT,

        -- 评估指标
        eval_oracle_gap_bps   FLOAT,
        eval_pa_bps           FLOAT,
        eval_ffr              FLOAT,
        eval_vs_twap_bps      FLOAT,
        eval_urgency_cost_bps FLOAT,
        eval_details          JSONB,

        -- 状态
        status                TEXT NOT NULL DEFAULT 'draft',
        created_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        activated_at          TIMESTAMPTZ,

        UNIQUE(dev_version, roll_tag)
    );
    """,
    "CREATE INDEX IF NOT EXISTS idx_rl_models_dev ON app.rl_model_versions(dev_version);",
    "CREATE INDEX IF NOT EXISTS idx_rl_models_status ON app.rl_model_versions(status);",
    "CREATE INDEX IF NOT EXISTS idx_rl_models_train_end ON app.rl_model_versions(train_end);",
]


def init_rl_model_schema() -> None:
    with get_conn() as conn:
        with conn.cursor() as cur:
            for sql in DDL:
                cur.execute(sql)


if __name__ == "__main__":
    from .pg_pool import init_db_pool
    init_db_pool()
    init_rl_model_schema()
    print("rl_model_versions table created.")
