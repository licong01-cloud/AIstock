"""
迁移脚本：为因子和模型相关表添加 catalog_id 关联列

目标：
  - 因子侧 7 张表添加 factor_catalog_id BIGINT → FK aistock_factor_catalog(id)
  - 模型侧 4 张表添加 model_catalog_id BIGINT → FK aistock_model_catalog(id)
  - 回填历史数据
  - 设置 NOT NULL + FK ON DELETE RESTRICT

执行步骤：
  1. 加列（NULLABLE）
  2. 回填 id
  3. 检查 NULL 残留
  4. ALTER COLUMN SET NOT NULL
  5. ADD FOREIGN KEY ON DELETE RESTRICT
  6. 建索引
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", ".."))

from backend.db.pg_pool import get_conn


# ============================================================
# Phase 0: 为 catalog 基础表添加 id BIGINT 代理键
# （表原有主键不变，id 作为 FK 引用目标）
# ============================================================
ENSURE_CATALOG_ID_SQL = [
    # ---- aistock_factor_catalog ----
    "CREATE SEQUENCE IF NOT EXISTS aistock_factor_catalog_id_seq;",
    """DO $$ BEGIN
       IF NOT EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_name='aistock_factor_catalog' AND column_name='id'
       ) THEN
         ALTER TABLE aistock_factor_catalog
           ADD COLUMN id BIGINT DEFAULT nextval('aistock_factor_catalog_id_seq');
         ALTER TABLE aistock_factor_catalog ALTER COLUMN id SET NOT NULL;
         ALTER TABLE aistock_factor_catalog ADD CONSTRAINT uq_factor_catalog_id UNIQUE (id);
         ALTER SEQUENCE aistock_factor_catalog_id_seq OWNED BY aistock_factor_catalog.id;
       END IF;
    END $$;""",
    # ---- aistock_model_catalog ----
    "CREATE SEQUENCE IF NOT EXISTS aistock_model_catalog_id_seq;",
    """DO $$ BEGIN
       IF NOT EXISTS (
         SELECT 1 FROM information_schema.columns
         WHERE table_name='aistock_model_catalog' AND column_name='id'
       ) THEN
         ALTER TABLE aistock_model_catalog
           ADD COLUMN id BIGINT DEFAULT nextval('aistock_model_catalog_id_seq');
         ALTER TABLE aistock_model_catalog ALTER COLUMN id SET NOT NULL;
         ALTER TABLE aistock_model_catalog ADD CONSTRAINT uq_model_catalog_id UNIQUE (id);
         ALTER SEQUENCE aistock_model_catalog_id_seq OWNED BY aistock_model_catalog.id;
       END IF;
    END $$;""",
]


# ============================================================
# Phase 1: 加列（NULLABLE，便于回填）
# ============================================================
ADD_COLUMNS_SQL = [
    # ---- 因子侧 ----
    "ALTER TABLE qe_loop_factor_records ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    "ALTER TABLE qe_loop_factor_records ADD COLUMN IF NOT EXISTS factor_source TEXT;",
    "ALTER TABLE qe_factor_classification ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    "ALTER TABLE qe_factor_correlations ADD COLUMN IF NOT EXISTS factor_a_catalog_id BIGINT;",
    "ALTER TABLE qe_factor_correlations ADD COLUMN IF NOT EXISTS factor_b_catalog_id BIGINT;",
    "ALTER TABLE qe_factor_experiment_metrics ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    "ALTER TABLE factor_live_track ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    "ALTER TABLE aistock_factor_metrics ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    "ALTER TABLE aistock_factor_calc_log ADD COLUMN IF NOT EXISTS factor_catalog_id BIGINT;",
    # ---- 模型侧 ----
    "ALTER TABLE qe_loop_model_records ADD COLUMN IF NOT EXISTS model_catalog_id BIGINT;",
    "ALTER TABLE qe_experiments ADD COLUMN IF NOT EXISTS model_catalog_id BIGINT;",
    "ALTER TABLE qe_loop_factor_records ADD COLUMN IF NOT EXISTS model_catalog_id BIGINT;",
    "ALTER TABLE qe_factor_experiment_metrics ADD COLUMN IF NOT EXISTS model_catalog_id BIGINT;",
]


# ============================================================
# Phase 2: 回填因子 catalog_id
# ============================================================
BACKFILL_FACTOR_SQL = [
    # qe_factor_classification — 有 factor_source，精确匹配
    """UPDATE qe_factor_classification c
       SET factor_catalog_id = fc.id
       FROM aistock_factor_catalog fc
       WHERE c.factor_name = fc.factor_name
         AND c.factor_source = fc.source
         AND c.factor_catalog_id IS NULL;""",

    # qe_factor_experiment_metrics — 有 factor_source，精确匹配
    """UPDATE qe_factor_experiment_metrics m
       SET factor_catalog_id = fc.id
       FROM aistock_factor_catalog fc
       WHERE m.factor_name = fc.factor_name
         AND m.factor_source = fc.source
         AND m.factor_catalog_id IS NULL;""",

    # qe_loop_factor_records — 只有 factor_name，需要推断 source
    # 优先匹配 rdagent_task_sync，其次任意 source
    """UPDATE qe_loop_factor_records r
       SET factor_catalog_id = fc.id,
           factor_source = fc.source
       FROM aistock_factor_catalog fc
       WHERE r.factor_name = fc.factor_name
         AND fc.source = 'rdagent_task_sync'
         AND r.factor_catalog_id IS NULL;""",

    # 对剩余未匹配的，尝试任意 source（取 id 最小的）
    """UPDATE qe_loop_factor_records r
       SET factor_catalog_id = sub.id,
           factor_source = sub.source
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name, source
           FROM aistock_factor_catalog
           ORDER BY factor_name, id
       ) sub
       WHERE r.factor_name = sub.factor_name
         AND r.factor_catalog_id IS NULL;""",

    # qe_factor_correlations — factor_a / factor_b
    """UPDATE qe_factor_correlations c
       SET factor_a_catalog_id = fa.id
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name
           FROM aistock_factor_catalog ORDER BY factor_name, id
       ) fa
       WHERE c.factor_a = fa.factor_name
         AND c.factor_a_catalog_id IS NULL;""",

    """UPDATE qe_factor_correlations c
       SET factor_b_catalog_id = fb.id
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name
           FROM aistock_factor_catalog ORDER BY factor_name, id
       ) fb
       WHERE c.factor_b = fb.factor_name
         AND c.factor_b_catalog_id IS NULL;""",

    # factor_live_track
    """UPDATE factor_live_track t
       SET factor_catalog_id = fc.id
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name
           FROM aistock_factor_catalog ORDER BY factor_name, id
       ) fc
       WHERE t.factor_name = fc.factor_name
         AND t.factor_catalog_id IS NULL;""",

    # aistock_factor_metrics
    """UPDATE aistock_factor_metrics m
       SET factor_catalog_id = fc.id
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name
           FROM aistock_factor_catalog ORDER BY factor_name, id
       ) fc
       WHERE m.factor_name = fc.factor_name
         AND m.factor_catalog_id IS NULL;""",

    # aistock_factor_calc_log
    """UPDATE aistock_factor_calc_log l
       SET factor_catalog_id = fc.id
       FROM (
           SELECT DISTINCT ON (factor_name) id, factor_name
           FROM aistock_factor_catalog ORDER BY factor_name, id
       ) fc
       WHERE l.factor_name = fc.factor_name
         AND l.factor_catalog_id IS NULL;""",
]


# ============================================================
# Phase 2b: 回填模型 catalog_id
# ============================================================
BACKFILL_MODEL_SQL = [
    # qe_loop_model_records — 通过 QE task 的 source_task_id 匹配
    """UPDATE qe_loop_model_records r
       SET model_catalog_id = mc.id
       FROM qe_evolution_tasks et
       JOIN aistock_model_catalog mc
         ON mc.task_run_id = et.source_task_id
       WHERE r.task_id = et.task_id
         AND r.model_catalog_id IS NULL;""",

    # qe_experiments — 通过 qe_task_id 匹配
    """UPDATE qe_experiments e
       SET model_catalog_id = mc.id
       FROM qe_evolution_tasks et
       JOIN aistock_model_catalog mc
         ON mc.task_run_id = et.source_task_id
       WHERE e.qe_task_id = et.task_id
         AND e.model_catalog_id IS NULL;""",

    # qe_loop_factor_records — model_catalog_id 从同 loop 的 model_records 获取
    """UPDATE qe_loop_factor_records fr
       SET model_catalog_id = mr.model_catalog_id
       FROM qe_loop_model_records mr
       WHERE fr.loop_id = mr.loop_id
         AND fr.model_catalog_id IS NULL
         AND mr.model_catalog_id IS NOT NULL;""",

    # qe_factor_experiment_metrics — 通过 experiment_id 关联
    """UPDATE qe_factor_experiment_metrics m
       SET model_catalog_id = e.model_catalog_id
       FROM qe_experiments e
       WHERE m.experiment_id = e.experiment_id
         AND m.model_catalog_id IS NULL
         AND e.model_catalog_id IS NOT NULL;""",
]


# ============================================================
# Phase 3: 检查 NULL 残留
# ============================================================
CHECK_NULL_SQL = {
    "qe_loop_factor_records.factor_catalog_id":
        "SELECT COUNT(*) FROM qe_loop_factor_records WHERE factor_catalog_id IS NULL;",
    "qe_factor_classification.factor_catalog_id":
        "SELECT COUNT(*) FROM qe_factor_classification WHERE factor_catalog_id IS NULL;",
    "qe_factor_correlations.factor_a_catalog_id":
        "SELECT COUNT(*) FROM qe_factor_correlations WHERE factor_a_catalog_id IS NULL;",
    "qe_factor_correlations.factor_b_catalog_id":
        "SELECT COUNT(*) FROM qe_factor_correlations WHERE factor_b_catalog_id IS NULL;",
    "qe_factor_experiment_metrics.factor_catalog_id":
        "SELECT COUNT(*) FROM qe_factor_experiment_metrics WHERE factor_catalog_id IS NULL;",
    "factor_live_track.factor_catalog_id":
        "SELECT COUNT(*) FROM factor_live_track WHERE factor_catalog_id IS NULL;",
    "aistock_factor_metrics.factor_catalog_id":
        "SELECT COUNT(*) FROM aistock_factor_metrics WHERE factor_catalog_id IS NULL;",
    "aistock_factor_calc_log.factor_catalog_id":
        "SELECT COUNT(*) FROM aistock_factor_calc_log WHERE factor_catalog_id IS NULL;",
    "qe_loop_model_records.model_catalog_id":
        "SELECT COUNT(*) FROM qe_loop_model_records WHERE model_catalog_id IS NULL;",
    "qe_experiments.model_catalog_id":
        "SELECT COUNT(*) FROM qe_experiments WHERE model_catalog_id IS NULL;",
    "qe_loop_factor_records.model_catalog_id":
        "SELECT COUNT(*) FROM qe_loop_factor_records WHERE model_catalog_id IS NULL;",
    "qe_factor_experiment_metrics.model_catalog_id":
        "SELECT COUNT(*) FROM qe_factor_experiment_metrics WHERE model_catalog_id IS NULL;",
}


# ============================================================
# Phase 4: 删除无法关联的孤儿记录
# ============================================================
CLEANUP_ORPHAN_SQL = [
    "DELETE FROM qe_loop_factor_records WHERE factor_catalog_id IS NULL;",
    "DELETE FROM qe_factor_classification WHERE factor_catalog_id IS NULL;",
    "DELETE FROM qe_factor_correlations WHERE factor_a_catalog_id IS NULL OR factor_b_catalog_id IS NULL;",
    "DELETE FROM qe_factor_experiment_metrics WHERE factor_catalog_id IS NULL;",
    "DELETE FROM factor_live_track WHERE factor_catalog_id IS NULL;",
    "DELETE FROM aistock_factor_metrics WHERE factor_catalog_id IS NULL;",
    "DELETE FROM aistock_factor_calc_log WHERE factor_catalog_id IS NULL;",
    "DELETE FROM qe_loop_model_records WHERE model_catalog_id IS NULL;",
    # qe_experiments 和 qe_loop_factor_records 的 model_catalog_id 暂不删除
    # 因为早期实验可能没有对应 catalog 模型
]


# ============================================================
# Phase 5: 设 NOT NULL 约束
# ============================================================
SET_NOT_NULL_SQL = [
    "ALTER TABLE qe_loop_factor_records ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE qe_loop_factor_records ALTER COLUMN factor_source SET NOT NULL;",
    "ALTER TABLE qe_factor_classification ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE qe_factor_correlations ALTER COLUMN factor_a_catalog_id SET NOT NULL;",
    "ALTER TABLE qe_factor_correlations ALTER COLUMN factor_b_catalog_id SET NOT NULL;",
    "ALTER TABLE qe_factor_experiment_metrics ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE factor_live_track ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE aistock_factor_metrics ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE aistock_factor_calc_log ALTER COLUMN factor_catalog_id SET NOT NULL;",
    "ALTER TABLE qe_loop_model_records ALTER COLUMN model_catalog_id SET NOT NULL;",
]


# ============================================================
# Phase 6: 添加 FK 约束 + 索引
# ============================================================
ADD_FK_SQL = [
    # 因子 FK
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qlfr_factor_catalog') THEN
         ALTER TABLE qe_loop_factor_records
           ADD CONSTRAINT fk_qlfr_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qfc_factor_catalog') THEN
         ALTER TABLE qe_factor_classification
           ADD CONSTRAINT fk_qfc_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qfcorr_factor_a_catalog') THEN
         ALTER TABLE qe_factor_correlations
           ADD CONSTRAINT fk_qfcorr_factor_a_catalog
           FOREIGN KEY (factor_a_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qfcorr_factor_b_catalog') THEN
         ALTER TABLE qe_factor_correlations
           ADD CONSTRAINT fk_qfcorr_factor_b_catalog
           FOREIGN KEY (factor_b_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qfem_factor_catalog') THEN
         ALTER TABLE qe_factor_experiment_metrics
           ADD CONSTRAINT fk_qfem_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_flt_factor_catalog') THEN
         ALTER TABLE factor_live_track
           ADD CONSTRAINT fk_flt_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_afm_factor_catalog') THEN
         ALTER TABLE aistock_factor_metrics
           ADD CONSTRAINT fk_afm_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_afcl_factor_catalog') THEN
         ALTER TABLE aistock_factor_calc_log
           ADD CONSTRAINT fk_afcl_factor_catalog
           FOREIGN KEY (factor_catalog_id) REFERENCES aistock_factor_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",

    # 模型 FK
    """DO $$ BEGIN
       IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'fk_qlmr_model_catalog') THEN
         ALTER TABLE qe_loop_model_records
           ADD CONSTRAINT fk_qlmr_model_catalog
           FOREIGN KEY (model_catalog_id) REFERENCES aistock_model_catalog(id) ON DELETE RESTRICT;
       END IF;
    END $$;""",
]

ADD_INDEX_SQL = [
    "CREATE INDEX IF NOT EXISTS idx_qlfr_factor_catalog_id ON qe_loop_factor_records(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qfc_factor_catalog_id ON qe_factor_classification(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qfcorr_factor_a_catalog_id ON qe_factor_correlations(factor_a_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qfcorr_factor_b_catalog_id ON qe_factor_correlations(factor_b_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qfem_factor_catalog_id ON qe_factor_experiment_metrics(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_flt_factor_catalog_id ON factor_live_track(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_afm_factor_catalog_id ON aistock_factor_metrics(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_afcl_factor_catalog_id ON aistock_factor_calc_log(factor_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qlmr_model_catalog_id ON qe_loop_model_records(model_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qexp_model_catalog_id ON qe_experiments(model_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qlfr_model_catalog_id ON qe_loop_factor_records(model_catalog_id);",
    "CREATE INDEX IF NOT EXISTS idx_qfem_model_catalog_id ON qe_factor_experiment_metrics(model_catalog_id);",
]


def run_migration(dry_run: bool = False):
    """执行完整迁移。"""
    print("=" * 60)
    print("迁移：为因子/模型相关表添加 catalog_id 关联列")
    print("=" * 60)

    with get_conn() as conn:
        conn.autocommit = False  # 需要事务控制
        with conn.cursor() as cur:
            # Phase 0: 确保 catalog 表有 id 代理键
            print("\n[Phase 0] Ensure catalog tables have id column...")
            for sql in ENSURE_CATALOG_ID_SQL:
                cur.execute(sql)
            # 验证
            cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE id IS NOT NULL")
            fc_count = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM aistock_model_catalog WHERE id IS NOT NULL")
            mc_count = cur.fetchone()[0]
            print(f"  aistock_factor_catalog: {fc_count} rows with id")
            print(f"  aistock_model_catalog: {mc_count} rows with id")

            # Phase 1: 加列（IF NOT EXISTS 幂等安全，dry-run 也执行）
            print("\n[Phase 1] 添加列...")
            for sql in ADD_COLUMNS_SQL:
                col_info = sql.split("ADD COLUMN IF NOT EXISTS ")[1].split(";")[0] if "ADD COLUMN" in sql else sql[:60]
                table = sql.split("ALTER TABLE ")[1].split(" ")[0]
                print(f"  + {table}.{col_info.strip()}")
                cur.execute(sql)

            # Phase 2: 回填因子
            print("\n[Phase 2a] 回填因子 factor_catalog_id...")
            for sql in BACKFILL_FACTOR_SQL:
                table = sql.split("UPDATE ")[1].split(" ")[0].split("\n")[0]
                if not dry_run:
                    cur.execute(sql)
                    print(f"  → {table}: {cur.rowcount} 行已更新")
                else:
                    print(f"  → {table}: (dry-run)")

            # Phase 2b: 回填模型
            print("\n[Phase 2b] 回填模型 model_catalog_id...")
            for sql in BACKFILL_MODEL_SQL:
                table = sql.split("UPDATE ")[1].split(" ")[0].split("\n")[0]
                if not dry_run:
                    cur.execute(sql)
                    print(f"  → {table}: {cur.rowcount} 行已更新")
                else:
                    print(f"  → {table}: (dry-run)")

            # Phase 3: 检查 NULL
            print("\n[Phase 3] 检查 NULL 残留...")
            null_counts = {}
            for label, sql in CHECK_NULL_SQL.items():
                cur.execute(sql)
                count = cur.fetchone()[0]
                null_counts[label] = count
                status = "OK" if count == 0 else f"WARNING: {count} 条 NULL"
                print(f"  {label}: {status}")

            has_nulls_factor = any(
                v > 0 for k, v in null_counts.items()
                if "factor_catalog_id" in k and "model" not in k.split(".")[0]
            )
            has_nulls_model_core = null_counts.get("qe_loop_model_records.model_catalog_id", 0) > 0

            # Phase 4: 清理孤儿
            if has_nulls_factor or has_nulls_model_core:
                print("\n[Phase 4] 清理无法关联的孤儿记录...")
                for sql in CLEANUP_ORPHAN_SQL:
                    table = sql.split("FROM ")[1].split(" ")[0]
                    if not dry_run:
                        cur.execute(sql)
                        if cur.rowcount > 0:
                            print(f"  → {table}: 删除 {cur.rowcount} 条孤儿记录")
                    else:
                        print(f"  → {table}: (dry-run)")

            # Phase 5: NOT NULL 约束
            print("\n[Phase 5] 设置 NOT NULL 约束...")
            for sql in SET_NOT_NULL_SQL:
                col = sql.split("ALTER COLUMN ")[1].split(" ")[0]
                table = sql.split("ALTER TABLE ")[1].split(" ")[0]
                print(f"  + {table}.{col} SET NOT NULL")
                if not dry_run:
                    cur.execute(sql)

            # Phase 6: FK + 索引
            print("\n[Phase 6] 添加 FK 约束...")
            for sql in ADD_FK_SQL:
                fk_name = sql.split("conname = '")[1].split("'")[0] if "conname" in sql else "?"
                print(f"  + FK: {fk_name}")
                if not dry_run:
                    cur.execute(sql)

            print("\n[Phase 6b] 创建索引...")
            for sql in ADD_INDEX_SQL:
                idx_name = sql.split("INDEX IF NOT EXISTS ")[1].split(" ")[0]
                print(f"  + Index: {idx_name}")
                if not dry_run:
                    cur.execute(sql)

            if not dry_run:
                conn.commit()
                print("\n[DONE] Migration complete. All changes committed.")
            else:
                conn.rollback()
                print("\n[DONE] Dry-run complete. All changes rolled back.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="添加 catalog_id 关联列迁移脚本")
    parser.add_argument("--dry-run", action="store_true", help="仅检查，不实际执行")
    args = parser.parse_args()
    run_migration(dry_run=args.dry_run)
