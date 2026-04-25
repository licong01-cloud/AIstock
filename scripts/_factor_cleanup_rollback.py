"""回滚 v2_cleanup_20260423_110718 批次.

动作:
  1. is_available=true, 清空 disable_reason/batch_id/at  (409 行)
  2. rehab_candidate=false                                (82 行)
  3. 清空 qe_factor_classification.cluster_* 字段 (为下一轮新聚类让位)
"""
import os
import sys
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

BATCH_ID = "v2_cleanup_20260423_110718"

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

EXECUTE = "--execute" in sys.argv
print(f"模式: {'EXECUTE' if EXECUTE else 'DRY-RUN'}")
print(f"批次: {BATCH_ID}\n")

with conn.cursor() as cur:
    # 预览受影响行数
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE disable_batch_id=%s", (BATCH_ID,))
    n_disable = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE rehab_candidate=true")
    n_rehab = cur.fetchone()[0]
    cur.execute("""
        SELECT COUNT(*) FROM qe_factor_classification
        WHERE cluster_id IS NOT NULL OR cluster_role IS NOT NULL
    """)
    n_cluster = cur.fetchone()[0]
    print(f"将回滚: disable={n_disable}  rehab={n_rehab}  cluster_* 非空={n_cluster}")

if not EXECUTE:
    print("\n[DRY-RUN] 未执行. 加 --execute 真实回滚.")
    conn.close()
    sys.exit(0)

with conn.cursor() as cur:
    cur.execute("""
        UPDATE aistock_factor_catalog
        SET is_available=true, disable_reason=NULL, disable_batch_id=NULL, disable_at=NULL
        WHERE disable_batch_id=%s
    """, (BATCH_ID,))
    print(f"  解封: {cur.rowcount}")

    cur.execute("UPDATE aistock_factor_catalog SET rehab_candidate=false WHERE rehab_candidate=true")
    print(f"  清 rehab: {cur.rowcount}")

    cur.execute("""
        UPDATE qe_factor_classification
        SET cluster_id=NULL, cluster_role=NULL, cluster_size=NULL,
            intra_cluster_max_corr=NULL, representative_score=NULL
        WHERE cluster_id IS NOT NULL OR cluster_role IS NOT NULL
    """)
    print(f"  清 cluster_*: {cur.rowcount}")

    conn.commit()
    print("\n[OK] 回滚提交.")

with conn.cursor() as cur:
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=true")
    print(f"\n当前 enabled: {cur.fetchone()[0]}")
    cur.execute("SELECT COUNT(*) FROM aistock_factor_catalog WHERE is_available=false")
    print(f"当前 disabled: {cur.fetchone()[0]}")

conn.close()
