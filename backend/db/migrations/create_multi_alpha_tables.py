#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Multi-Alpha 架构 DB Migration
- qe_factor_classification: 新增 4 个分类维度 (data_source_group, ts_info_density, update_freq, linearity)
- qe_experiments: 新增 alpha_mode + multi_alpha_config
- qe_multi_alpha_groups: 新建（每个 Alpha 组的因子/模型/结果）
- qe_meta_model_weights: 新建（Meta-Model 权重历史）

手动执行: python -m backend.db.migrations.create_multi_alpha_tables
"""

import os
import psycopg2

os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
os.environ.setdefault("TDX_DB_PORT", "5432")
os.environ.setdefault("TDX_DB_NAME", "aistock")
os.environ.setdefault("TDX_DB_USER", "postgres")
os.environ.setdefault("TDX_DB_PASSWORD", os.environ.get("DB_PASSWORD", os.environ.get("TDX_DB_PASSWORD", "")))


def run_migration():
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_HOST"],
        port=int(os.environ["TDX_DB_PORT"]),
        database=os.environ["TDX_DB_NAME"],
        user=os.environ["TDX_DB_USER"],
        password=os.environ["TDX_DB_PASSWORD"],
    )
    conn.autocommit = True
    cur = conn.cursor()

    print("=== Multi-Alpha DB Migration ===\n")

    # ── 1. qe_factor_classification: 新增 4 个分类维度 ──────────────
    classification_columns = {
        "data_source_group": "TEXT",        # price_volume / money_flow / fundamental / valuation / chip / sector / cross_dataset
        "ts_info_density": "TEXT",          # high / medium / low
        "update_freq": "TEXT",              # daily / weekly / quarterly
        "linearity": "TEXT",                # linear / nonlinear
    }

    for col_name, col_type in classification_columns.items():
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'qe_factor_classification' AND column_name = %s
        """, (col_name,))
        if cur.fetchone():
            print(f"  [SKIP] qe_factor_classification.{col_name} already exists")
        else:
            cur.execute(f"ALTER TABLE qe_factor_classification ADD COLUMN {col_name} {col_type}")
            print(f"  [ADD]  qe_factor_classification.{col_name} {col_type}")

    # 索引: data_source_group 是多 Alpha 分组的主要维度
    cur.execute("""
        SELECT indexname FROM pg_indexes
        WHERE tablename = 'qe_factor_classification' AND indexname = 'idx_cls_data_source_group'
    """)
    if not cur.fetchone():
        cur.execute("CREATE INDEX idx_cls_data_source_group ON qe_factor_classification(data_source_group)")
        print("  [ADD]  index idx_cls_data_source_group")
    else:
        print("  [SKIP] index idx_cls_data_source_group already exists")

    print()

    # ── 2. qe_experiments: 新增 alpha_mode + multi_alpha_config ────
    exp_columns = {
        "alpha_mode": ("TEXT", "DEFAULT 'single'"),
        "multi_alpha_config": ("JSONB", ""),
    }

    for col_name, (col_type, default) in exp_columns.items():
        cur.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'qe_experiments' AND column_name = %s
        """, (col_name,))
        if cur.fetchone():
            print(f"  [SKIP] qe_experiments.{col_name} already exists")
        else:
            ddl = f"ALTER TABLE qe_experiments ADD COLUMN {col_name} {col_type} {default}".strip()
            cur.execute(ddl)
            print(f"  [ADD]  qe_experiments.{col_name} {col_type} {default}")

    print()

    # ── 3. qe_multi_alpha_groups: Alpha 组结果表 ───────────────────
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'qe_multi_alpha_groups'
        )
    """)
    if cur.fetchone()[0]:
        print("  [SKIP] qe_multi_alpha_groups table already exists")
    else:
        cur.execute("""
            CREATE TABLE qe_multi_alpha_groups (
                id SERIAL PRIMARY KEY,
                parent_experiment_id TEXT NOT NULL,
                group_name TEXT NOT NULL,
                factor_names JSONB NOT NULL,
                model_id TEXT NOT NULL,
                dataset_type TEXT DEFAULT 'DatasetH',
                model_params JSONB,
                compute_resource TEXT DEFAULT 'cpu',
                assigned_node_id TEXT,
                qe_loop_id TEXT,
                prediction_path TEXT,
                group_ic DOUBLE PRECISION,
                group_icir DOUBLE PRECISION,
                group_sharpe DOUBLE PRECISION,
                meta_weight DOUBLE PRECISION,
                status TEXT DEFAULT 'pending',
                error_message TEXT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                completed_at TIMESTAMPTZ,
                CONSTRAINT uq_multi_alpha_group UNIQUE (parent_experiment_id, group_name)
            )
        """)
        cur.execute("""
            CREATE INDEX idx_malpha_groups_parent
            ON qe_multi_alpha_groups(parent_experiment_id)
        """)
        print("  [CREATE] qe_multi_alpha_groups + index")

    print()

    # ── 4. qe_meta_model_weights: Meta-Model 权重历史 ──────────────
    cur.execute("""
        SELECT EXISTS (
            SELECT FROM information_schema.tables
            WHERE table_name = 'qe_meta_model_weights'
        )
    """)
    if cur.fetchone()[0]:
        print("  [SKIP] qe_meta_model_weights table already exists")
    else:
        cur.execute("""
            CREATE TABLE qe_meta_model_weights (
                id SERIAL PRIMARY KEY,
                experiment_id TEXT NOT NULL,
                as_of_date DATE NOT NULL,
                method TEXT NOT NULL,
                weights JSONB NOT NULL,
                combined_ic DOUBLE PRECISION,
                lookback_window INT,
                created_at TIMESTAMPTZ DEFAULT NOW(),
                CONSTRAINT uq_meta_weights UNIQUE (experiment_id, as_of_date, method)
            )
        """)
        cur.execute("""
            CREATE INDEX idx_meta_weights_exp
            ON qe_meta_model_weights(experiment_id)
        """)
        print("  [CREATE] qe_meta_model_weights + index")

    print()

    # ── 验证 ───────────────────────────────────────────────────────
    print("=== Verification ===\n")

    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'qe_factor_classification'
          AND column_name IN ('data_source_group', 'ts_info_density', 'update_freq', 'linearity')
        ORDER BY column_name
    """)
    cols = [r[0] for r in cur.fetchall()]
    print(f"  qe_factor_classification new columns: {cols}")

    cur.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_name = 'qe_experiments'
          AND column_name IN ('alpha_mode', 'multi_alpha_config')
        ORDER BY column_name
    """)
    cols = [r[0] for r in cur.fetchall()]
    print(f"  qe_experiments new columns: {cols}")

    for tbl in ("qe_multi_alpha_groups", "qe_meta_model_weights"):
        cur.execute("SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = %s)", (tbl,))
        print(f"  {tbl}: {'EXISTS' if cur.fetchone()[0] else 'MISSING'}")

    cur.close()
    conn.close()
    print("\n=== Migration complete ===")


if __name__ == "__main__":
    run_migration()
