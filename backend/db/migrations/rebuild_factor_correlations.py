"""
重建 qe_factor_correlations 表：
  - 移除 TEXT 列 (factor_a, factor_b)
  - 改用 catalog_id 主键 (factor_a_id, factor_b_id)
  - CHECK (factor_a_id < factor_b_id) 保证每对因子只存一次
  - UNIQUE (factor_a_id, factor_b_id) 替代旧三元组唯一约束
  - ON DELETE CASCADE 级联删除
  - 阈值改为 0，全量存储
  - catalog 表新增 correlation_computed_at + correlation_pair_count

执行: python backend/db/migrations/rebuild_factor_correlations.py
"""

import sys
from pathlib import Path

backend_path = Path(__file__).resolve().parent.parent.parent
if str(backend_path) not in sys.path:
    sys.path.insert(0, str(backend_path))

from dotenv import load_dotenv
env_path = backend_path.parent / ".env"
load_dotenv(env_path, override=True)

from db.pg_pool import get_conn


def migrate():
    with get_conn() as conn:
        with conn.cursor() as cur:
            # ── Step 1: catalog 表新增两列 ──
            print("[1/6] Adding correlation tracking columns to aistock_factor_catalog...")
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='aistock_factor_catalog' AND column_name='correlation_computed_at'
            """)
            if not cur.fetchone():
                cur.execute("""
                    ALTER TABLE aistock_factor_catalog
                    ADD COLUMN correlation_computed_at TIMESTAMPTZ DEFAULT NULL
                """)
                print("  -> Added correlation_computed_at")
            else:
                print("  -> correlation_computed_at already exists")

            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='aistock_factor_catalog' AND column_name='correlation_pair_count'
            """)
            if not cur.fetchone():
                cur.execute("""
                    ALTER TABLE aistock_factor_catalog
                    ADD COLUMN correlation_pair_count INTEGER DEFAULT 0
                """)
                print("  -> Added correlation_pair_count")
            else:
                print("  -> correlation_pair_count already exists")

            # ── Step 2: 备份旧表数据 ──
            print("[2/6] Checking existing data in qe_factor_correlations...")
            cur.execute("SELECT COUNT(*) FROM qe_factor_correlations")
            old_count = cur.fetchone()[0]
            print(f"  -> {old_count} existing records")

            # ── Step 3: 创建新表 ──
            print("[3/6] Creating new table qe_factor_correlations_v2...")
            cur.execute("DROP TABLE IF EXISTS qe_factor_correlations_v2 CASCADE")
            cur.execute("""
                CREATE TABLE qe_factor_correlations_v2 (
                    id                SERIAL PRIMARY KEY,
                    factor_a_id       BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                    factor_b_id       BIGINT NOT NULL REFERENCES aistock_factor_catalog(id) ON DELETE CASCADE,
                    correlation       DOUBLE PRECISION NOT NULL,
                    method            TEXT NOT NULL DEFAULT 'spearman_ewma',
                    as_of_date        DATE NOT NULL DEFAULT CURRENT_DATE,
                    data_window_days  INTEGER NOT NULL DEFAULT 252,
                    computed_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    CONSTRAINT chk_factor_order CHECK (factor_a_id < factor_b_id),
                    CONSTRAINT uq_factor_pair   UNIQUE (factor_a_id, factor_b_id)
                )
            """)
            cur.execute("CREATE INDEX idx_corr_a ON qe_factor_correlations_v2 (factor_a_id)")
            cur.execute("CREATE INDEX idx_corr_b ON qe_factor_correlations_v2 (factor_b_id)")
            cur.execute("""
                CREATE INDEX idx_corr_high ON qe_factor_correlations_v2 (correlation)
                WHERE ABS(correlation) > 0.5
            """)
            print("  -> Table created with CHECK + UNIQUE + indexes")

            # ── Step 4: 迁移旧数据到新表 ──
            if old_count > 0:
                print(f"[4/6] Migrating {old_count} records to new table...")
                cur.execute("""
                    INSERT INTO qe_factor_correlations_v2
                        (factor_a_id, factor_b_id, correlation, method, as_of_date, computed_at)
                    SELECT
                        LEAST(factor_a_catalog_id, factor_b_catalog_id),
                        GREATEST(factor_a_catalog_id, factor_b_catalog_id),
                        correlation,
                        COALESCE(method, 'spearman_ewma'),
                        COALESCE(
                            CASE
                                WHEN data_period ~ '\\d{4}-\\d{2}-\\d{2}'
                                THEN (regexp_match(data_period, '(\\d{4}-\\d{2}-\\d{2})'))[1]::DATE
                                ELSE CURRENT_DATE
                            END,
                            CURRENT_DATE
                        ),
                        COALESCE(computed_at, NOW())
                    FROM qe_factor_correlations
                    WHERE factor_a_catalog_id IS NOT NULL
                      AND factor_b_catalog_id IS NOT NULL
                      AND factor_a_catalog_id != factor_b_catalog_id
                    ON CONFLICT (factor_a_id, factor_b_id) DO UPDATE SET
                        correlation = EXCLUDED.correlation,
                        computed_at = EXCLUDED.computed_at
                """)
                migrated = cur.rowcount
                print(f"  -> Migrated {migrated} records")
            else:
                print("[4/6] No data to migrate, skipping.")

            # ── Step 5: 重命名表（原子切换） ──
            print("[5/6] Swapping tables...")
            cur.execute("ALTER TABLE IF EXISTS qe_factor_correlations RENAME TO qe_factor_correlations_backup")
            cur.execute("ALTER TABLE qe_factor_correlations_v2 RENAME TO qe_factor_correlations")
            # 同时重命名约束和索引（PostgreSQL 自动跟随表名，但索引需要手动）
            cur.execute("ALTER INDEX IF EXISTS idx_corr_a RENAME TO idx_corr_factor_a")
            cur.execute("ALTER INDEX IF EXISTS idx_corr_b RENAME TO idx_corr_factor_b")
            cur.execute("ALTER INDEX IF EXISTS idx_corr_high RENAME TO idx_corr_high_abs")
            print("  -> Old table -> _backup, new table -> qe_factor_correlations")

            # ── Step 6: 更新 catalog 相关性统计 ──
            print("[6/6] Updating catalog correlation stats...")
            cur.execute("""
                UPDATE aistock_factor_catalog c SET
                    correlation_computed_at = sub.max_computed,
                    correlation_pair_count = sub.pair_count
                FROM (
                    SELECT factor_id, MAX(computed_at) AS max_computed, COUNT(*) AS pair_count
                    FROM (
                        SELECT factor_a_id AS factor_id, computed_at FROM qe_factor_correlations
                        UNION ALL
                        SELECT factor_b_id AS factor_id, computed_at FROM qe_factor_correlations
                    ) t
                    GROUP BY factor_id
                ) sub
                WHERE c.id = sub.factor_id
            """)
            updated = cur.rowcount
            print(f"  -> Updated {updated} catalog records")

            conn.commit()

            # ── 验证 ──
            cur.execute("SELECT COUNT(*) FROM qe_factor_correlations")
            final_count = cur.fetchone()[0]
            cur.execute("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name='qe_factor_correlations'
                ORDER BY ordinal_position
            """)
            columns = [row[0] for row in cur.fetchall()]

            print("\n=== Migration Complete ===")
            print(f"Records in new table: {final_count}")
            print(f"Columns: {', '.join(columns)}")
            print("Backup table: qe_factor_correlations_backup (can be dropped later)")


if __name__ == "__main__":
    migrate()
