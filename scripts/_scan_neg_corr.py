"""找 corr ≤ -0.999 的因子对, 用于 Rule A' 去重 (保留 IC 更正的)."""
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
    # 加载启用因子 id + ic_mean
    cur.execute("""
        WITH metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, ic_mean
            FROM aistock_factor_metrics
            WHERE eval_window='out_sample' AND return_horizon='1d'
            ORDER BY factor_catalog_id, calculated_at DESC
        )
        SELECT a.id, a.factor_name, COALESCE(m.ic_mean, 0)::float
        FROM aistock_factor_catalog a
        LEFT JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.is_available=true
    """)
    factors = {r[0]: {"name": r[1], "ic": r[2]} for r in cur.fetchall()}

    # 找 corr ≤ -0.999 的对 (双向启用)
    cur.execute("""
        SELECT DISTINCT ON (factor_a_id, factor_b_id) factor_a_id, factor_b_id, correlation
        FROM qe_factor_correlations
        ORDER BY factor_a_id, factor_b_id, as_of_date DESC
    """)
    neg_pairs = [(a, b, float(c)) for a, b, c in cur.fetchall()
                 if c <= -0.999 and a in factors and b in factors]

print(f"corr ≤ -0.999 的启用对: {len(neg_pairs)}\n")

# 保留规则: IC 更正 (或 |IC| 更大) 的保留, 另一个 disable
# 但如果两者都是噪声 (|IC|<0.003), 按 id ASC 保留
to_disable = set()
seen_groups = {}  # 维护一个 union-find 防重

for a, b, c in neg_pairs:
    ic_a = factors[a]["ic"]
    ic_b = factors[b]["ic"]
    # 规则: 保留 ic 更正的 (优先正值), 平局按 id ASC
    if ic_a > ic_b + 1e-9:
        loser = b
    elif ic_b > ic_a + 1e-9:
        loser = a
    else:
        loser = max(a, b)
    to_disable.add(loser)

# 排除 IC 本身已经极小的 (两边都接近 0, 留哪个都行, 但可能在 Rule B 已被清)
# 和 Rule B v2 去重
with conn.cursor() as cur:
    cur.execute("""
        WITH rated AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, official_grade
            FROM qe_factor_official_ratings ORDER BY factor_catalog_id, graded_at DESC
        ),
        metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) *
            FROM aistock_factor_metrics
            WHERE eval_window='out_sample' AND return_horizon='1d'
            ORDER BY factor_catalog_id, calculated_at DESC
        )
        SELECT a.id FROM aistock_factor_catalog a
        JOIN rated r ON r.factor_catalog_id=a.id
        JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.is_available=true
          AND r.official_grade='D'
          AND ABS(m.ic_mean) < 0.003
          AND ABS(m.rank_ic_mean) < 0.003
          AND m.ic_positive_ratio BETWEEN 0.45 AND 0.55
          AND ABS(m.rank_icir) < 0.1
    """)
    rule_b_ids = {r[0] for r in cur.fetchall()}

print(f"Rule B v2 命中  : {len(rule_b_ids)}")
print(f"Rule A' (corr=-1): {len(to_disable)}")
print(f"A' 中被 B 已覆盖: {len(to_disable & rule_b_ids)}")
print(f"A' 新增 (仅 A')  : {len(to_disable - rule_b_ids)}")
print(f"合计 disable     : {len(to_disable | rule_b_ids)}\n")

# 展示 A' 新增的 (非 B 覆盖)
print("=== Rule A' 样本 (corr=-1 中 IC 更负的被 disable) ===")
for a, b, c in sorted(neg_pairs, key=lambda x: x[2])[:20]:
    ic_a = factors[a]["ic"]
    ic_b = factors[b]["ic"]
    keep = a if ic_a > ic_b else b
    drop = b if keep == a else a
    print(f"  corr={c:+.4f}  保留 {factors[keep]['name']:<40} ic={factors[keep]['ic']:+.4f}  "
          f"→ 禁 {factors[drop]['name']:<40} ic={factors[drop]['ic']:+.4f}")

conn.close()
