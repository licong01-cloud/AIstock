"""诊断 dynamic_pe_profit_growth_synergy 为何 Rule B 没命中."""
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
        WITH rated AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings ORDER BY factor_catalog_id, graded_at DESC
        ),
        metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) *
            FROM aistock_factor_metrics
            WHERE eval_window='out_sample' AND return_horizon='1d'
            ORDER BY factor_catalog_id, calculated_at DESC
        )
        SELECT a.id, a.factor_name, a.is_available,
               r.official_grade, r.official_score,
               c.ic_value, c.ic_sign_consistency_12m, c.ts_info_density,
               c.cross_horizon_consistency, c.cluster_id, c.cluster_role,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir,
               m.ic_positive_ratio, m.coverage,
               m.top_excess_annual_return, m.group_return_monotonicity
        FROM aistock_factor_catalog a
        LEFT JOIN rated r ON r.factor_catalog_id=a.id
        LEFT JOIN qe_factor_classification c ON c.factor_catalog_id=a.id
        LEFT JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.factor_name = 'dynamic_pe_profit_growth_synergy'
    """)
    for r in cur.fetchall():
        labels = ["id","name","is_available","grade","score",
                  "ic_value(cls)","sign_cons_12m","ts_density","cross_horizon","cluster_id","cluster_role",
                  "ic_mean(m)","rank_ic_mean","icir","rank_icir",
                  "ic_pos_ratio","coverage","excess_annual","monotonicity"]
        for l, v in zip(labels, r):
            print(f"  {l:<20}: {v}")

conn.close()
