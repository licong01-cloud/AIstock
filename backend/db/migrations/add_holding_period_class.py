"""为 qe_factor_classification 表添加 holding_period_class 列。

新增列：
- holding_period_class: 基于 aistock_factor_metrics.ic_decay_half_life 的持仓周期分类
  取值: 'short' (<8d), 'medium' (8-25d), 'long' (>25d), 'unknown' (NULL)

用途: 双管道策略架构的因子分池维度，详见:
  F:\\Dev\\RD-Agent-main\\reports\\dual_pipeline_strategy_design_20260410.md

执行方式：
  cd F:/Dev/AIstock
  python -m backend.db.migrations.add_holding_period_class
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from backend.db.pg_pool import get_conn

COLUMNS = [
    ("holding_period_class", "TEXT"),
]


def migrate():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for col_name, col_type in COLUMNS:
                cur.execute(f"""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'qe_factor_classification'
                      AND column_name = '{col_name}'
                """)
                if cur.fetchone():
                    print(f"  列 {col_name} 已存在，跳过")
                else:
                    cur.execute(
                        f"ALTER TABLE qe_factor_classification "
                        f"ADD COLUMN {col_name} {col_type}"
                    )
                    print(f"  已添加列 {col_name} {col_type}")

            # 添加列注释
            cur.execute("""
                COMMENT ON COLUMN qe_factor_classification.holding_period_class
                IS '持仓周期分类，基于IC半衰期: short(<8d), medium(8-25d), long(>25d), unknown(NULL)'
            """)
            print("  已添加列注释")

    print("迁移完成。")


if __name__ == "__main__":
    migrate()
