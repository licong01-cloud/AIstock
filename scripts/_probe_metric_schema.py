"""查 aistock_factor_metrics 和 qe_factor_experiment_metrics 的 schema + 对 id=472 的数据."""
import os
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

for tbl in ["aistock_factor_metrics", "qe_factor_experiment_metrics"]:
    with conn.cursor() as cur:
        cur.execute(f"""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name='{tbl}' ORDER BY ordinal_position
        """)
        print(f"\n=== {tbl} columns ===")
        cols = cur.fetchall()
        for c, t in cols:
            print(f"  {c}: {t}")

        # 样本 1 行
        cur.execute(f"SELECT COUNT(*) FROM {tbl}")
        n = cur.fetchone()[0]
        print(f"\n  总行数: {n}")

# 查 chip_concentration_price_position (id=472) 在两表的记录
for tbl, id_col in [("aistock_factor_metrics", "factor_catalog_id"), ("qe_factor_experiment_metrics", "factor_catalog_id")]:
    with conn.cursor() as cur:
        try:
            cur.execute(f"SELECT * FROM {tbl} WHERE {id_col}=472 LIMIT 3")
            rows = cur.fetchall()
            col_names = [d[0] for d in cur.description]
            print(f"\n=== {tbl} WHERE {id_col}=472 ({len(rows)} rows) ===")
            for row in rows:
                for k, v in zip(col_names, row):
                    if v is not None:
                        print(f"  {k}: {v}")
                print("  ---")
        except Exception as e:
            print(f"  err: {e}")
            conn.rollback()

conn.close()
