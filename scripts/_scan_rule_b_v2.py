"""扫描新 Rule B v2 命中的纯噪声因子.

Rule B v2: grade='D'
           AND |ic_mean|<0.003 AND |rank_ic_mean|<0.003
           AND 0.45<=ic_positive_ratio<=0.55
           AND |rank_icir|<0.1
"""
import os
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

IC_TH = 0.003
RANK_IC_TH = 0.003
POS_LO = 0.45
POS_HI = 0.55
RICIR_TH = 0.1

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor() as cur:
    cur.execute(f"""
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
        SELECT a.id, a.factor_name, r.official_grade, r.official_score,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir,
               m.ic_positive_ratio, m.coverage
        FROM aistock_factor_catalog a
        JOIN rated r ON r.factor_catalog_id=a.id
        JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.is_available=true
          AND r.official_grade='D'
          AND ABS(m.ic_mean) < {IC_TH}
          AND ABS(m.rank_ic_mean) < {RANK_IC_TH}
          AND m.ic_positive_ratio BETWEEN {POS_LO} AND {POS_HI}
          AND ABS(m.rank_icir) < {RICIR_TH}
        ORDER BY ABS(m.ic_mean) + ABS(m.rank_ic_mean)
    """)
    rows = cur.fetchall()

print(f"Rule B v2 候选: {len(rows)}\n")
print(f"{'factor_name':<50} {'ic':>8} {'rank_ic':>9} {'icir':>7} {'rank_icir':>10} {'pos%':>6} {'cov%':>6}")
print("-" * 110)
for r in rows[:30]:
    ic_str = f"{r[4]:+.4f}" if r[4] is not None else "  NULL"
    ric_str = f"{r[5]:+.4f}" if r[5] is not None else "  NULL"
    icir_str = f"{r[6]:+.3f}" if r[6] is not None else "NULL"
    ricir_str = f"{r[7]:+.3f}" if r[7] is not None else "NULL"
    pos_str = f"{r[8]*100:.1f}" if r[8] is not None else "NULL"
    cov_str = f"{r[9]*100:.1f}" if r[9] is not None else "NULL"
    print(f"{r[1]:<50} {ic_str:>8} {ric_str:>9} {icir_str:>7} {ricir_str:>10} {pos_str:>6} {cov_str:>6}")
if len(rows) > 30:
    print(f"... 还有 {len(rows)-30} 个")

# 重点验证: dynamic_pe_profit_growth_synergy 是否命中?
hit_ids = {r[0] for r in rows}
print(f"\ndynamic_pe_profit_growth_synergy (id=471) 命中: {471 in hit_ids}")
print(f"chip_concentration_price_position (id=472) 命中: {472 in hit_ids}")

conn.close()
