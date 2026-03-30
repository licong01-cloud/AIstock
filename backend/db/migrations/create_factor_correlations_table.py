"""创建 aistock_factor_correlations 表用于存储因子间相关性。

执行方式：
  cd F:/Dev/AIstock
  python -m backend.db.migrations.create_factor_correlations_table
"""
from __future__ import annotations

import sys, os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from dotenv import load_dotenv
load_dotenv()

from backend.db.pg_pool import get_conn


def migrate():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # 检查表是否存在
            cur.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM information_schema.tables
                    WHERE table_name = 'aistock_factor_correlations'
                )
            """)
            if cur.fetchone()[0]:
                print("  表 aistock_factor_correlations 已存在，跳过")
                return

            # 创建表
            cur.execute("""
                CREATE TABLE aistock_factor_correlations (
                    id SERIAL PRIMARY KEY,
                    factor_a VARCHAR(100) NOT NULL,
                    factor_b VARCHAR(100) NOT NULL,
                    correlation DOUBLE PRECISION NOT NULL,
                    method VARCHAR(20) DEFAULT 'pearson',
                    calculated_at TIMESTAMP DEFAULT NOW(),
                    UNIQUE(factor_a, factor_b)
                )
            """)

            # 创建索引
            cur.execute("""
                CREATE INDEX idx_factor_correlations_a ON aistock_factor_correlations(factor_a)
            """)
            cur.execute("""
                CREATE INDEX idx_factor_correlations_b ON aistock_factor_correlations(factor_b)
            """)

            print("  已创建表 aistock_factor_correlations")

    print("迁移完成。")


if __name__ == "__main__":
    migrate()
