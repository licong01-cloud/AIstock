#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Multi-Alpha v1.1 模型复用 DB Migration
- qe_multi_alpha_groups: 新增 model_source_experiment_id, model_source_group_name, reuse_mode
- qe_experiments: 新增 parent_multi_alpha_id (v2.0 血统追踪)
- qe_group_prediction_correlations: 新建（缓存组间预测相关性，v2.0 诊断用）

手动执行: python -m backend.db.migrations.add_multi_alpha_reuse_columns
"""

import os
import psycopg2

os.environ.setdefault("TDX_DB_HOST", "127.0.0.1")
os.environ.setdefault("TDX_DB_PORT", "5432")
os.environ.setdefault("TDX_DB_NAME", "aistock")
os.environ.setdefault("TDX_DB_USER", "postgres")
# DB_PASSWORD 或 TDX_DB_PASSWORD 必须在环境中预设，禁止空密码兜底
if "TDX_DB_PASSWORD" not in os.environ:
    if "DB_PASSWORD" in os.environ:
        os.environ["TDX_DB_PASSWORD"] = os.environ["DB_PASSWORD"]
    else:
        raise RuntimeError("必须设置环境变量 TDX_DB_PASSWORD 或 DB_PASSWORD")


def run_migration():
    conn = psycopg2.connect(
        host=os.environ["TDX_DB_HOST"],
        port=int(os.environ["TDX_DB_PORT"]),
        database=os.environ["TDX_DB_NAME"],
        user=os.environ["TDX_DB_USER"],
        password=os.environ["TDX_DB_PASSWORD"],
    )
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # ── qe_multi_alpha_groups: 模型复用字段 ─────────────────────
        for col, col_type, default in [
            ("model_source_experiment_id", "TEXT", None),
            ("model_source_group_name", "TEXT", None),
            ("reuse_mode", "TEXT", "'retrain'"),
        ]:
            cur.execute(
                f"""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'qe_multi_alpha_groups' AND column_name = '{col}'
                    ) THEN
                        ALTER TABLE qe_multi_alpha_groups
                        ADD COLUMN {col} {col_type}{f' DEFAULT {default}' if default else ''};
                    END IF;
                END $$;
                """
            )
            print(f"  [OK] qe_multi_alpha_groups.{col}")

        # ── qe_experiments: 血统追踪字段 ─────────────────────────────
        cur.execute(
            """
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'qe_experiments' AND column_name = 'parent_multi_alpha_id'
                ) THEN
                    ALTER TABLE qe_experiments
                    ADD COLUMN parent_multi_alpha_id TEXT;
                END IF;
            END $$;
            """
        )
        print("  [OK] qe_experiments.parent_multi_alpha_id")

        # ── qe_group_prediction_correlations: 组间预测相关性缓存 ─────
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS qe_group_prediction_correlations (
                id              SERIAL PRIMARY KEY,
                experiment_id   TEXT NOT NULL,
                group_a         TEXT NOT NULL,
                group_b         TEXT NOT NULL,
                correlation     DOUBLE PRECISION NOT NULL,
                computed_at     TIMESTAMPTZ DEFAULT NOW(),
                UNIQUE (experiment_id, group_a, group_b)
            );
            CREATE INDEX IF NOT EXISTS idx_group_pred_corr_exp
                ON qe_group_prediction_correlations (experiment_id);
            """
        )
        print("  [OK] qe_group_prediction_correlations table")

        conn.commit()
        print("\n Migration completed successfully.")
    except Exception as e:
        conn.rollback()
        print(f"\n Migration failed: {e}")
        raise
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    run_migration()
