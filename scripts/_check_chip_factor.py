"""查 chip_concentration_price_position 为什么没被 Rule B 命中."""
import os
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor() as cur:
    cur.execute("""
        SELECT a.id, a.factor_name, a.is_available,
               a.disable_reason, a.disable_batch_id,
               r.official_grade, r.official_score,
               c.ic_value, c.ic_sign_consistency_12m, c.ts_info_density,
               c.cross_horizon_consistency, c.cluster_id, c.cluster_role
        FROM aistock_factor_catalog a
        LEFT JOIN (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings ORDER BY factor_catalog_id, graded_at DESC
        ) r ON r.factor_catalog_id = a.id
        LEFT JOIN qe_factor_classification c ON c.factor_catalog_id = a.id
        WHERE a.factor_name = 'chip_concentration_price_position'
    """)
    rows = cur.fetchall()
    for r in rows:
        print("id                       :", r[0])
        print("factor_name              :", r[1])
        print("is_available             :", r[2])
        print("disable_reason           :", r[3])
        print("disable_batch_id         :", r[4])
        print("official_grade           :", r[5])
        print("official_score           :", r[6])
        print("ic_value                 :", r[7])
        print("ic_sign_consistency_12m  :", r[8])
        print("ts_info_density          :", r[9])
        print("cross_horizon_consistency:", r[10])
        print("cluster_id               :", r[11])
        print("cluster_role             :", r[12])
        print()

conn.close()
