"""为 aistock_factor_metrics 表添加多持有期IC和辅助IC列。

新增列：
- ic_csz_mean: 截面Z-Score版Pearson IC（辅助指标）
- rank_ic_1d/5d/10d/20d: 多持有期Rank IC

执行方式：
  cd F:/dev/AIstock
  python -m backend.db.migrations.add_multi_period_ic_columns
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from backend.db.pg_pool import get_conn

COLUMNS = [
    ("ic_csz_mean", "DOUBLE PRECISION"),
    ("rank_ic_1d", "DOUBLE PRECISION"),
    ("rank_ic_5d", "DOUBLE PRECISION"),
    ("rank_ic_10d", "DOUBLE PRECISION"),
    ("rank_ic_20d", "DOUBLE PRECISION"),
]


def migrate():
    with get_conn() as conn:
        with conn.cursor() as cur:
            for col_name, col_type in COLUMNS:
                cur.execute(f"""
                    SELECT 1 FROM information_schema.columns
                    WHERE table_name = 'aistock_factor_metrics'
                      AND column_name = '{col_name}'
                """)
                if cur.fetchone():
                    print(f"  列 {col_name} 已存在，跳过")
                else:
                    cur.execute(f"ALTER TABLE aistock_factor_metrics ADD COLUMN {col_name} {col_type}")
                    print(f"  已添加列 {col_name} {col_type}")

    print("迁移完成。")


if __name__ == "__main__":
    migrate()
