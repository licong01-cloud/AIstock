"""扫描 657 启用因子, 评估增强规则可能新增 disable 的数量.

候选规则:
  Rule B1 (极低 IC): grade=D + |ic|<0.005 (无论 sign/ts)
  Rule C  (backtest): grade=D + latest out-sample top_excess_annual_return < 0
                       AND top_excess_sharpe < 0
  Rule D  (反向分组): grade=D + group_return_monotonicity < 0 + rank_icir < 0
"""
import os
from collections import Counter
from dotenv import load_dotenv
load_dotenv(r"F:\Dev\AIstock\.env")
import psycopg2

conn = psycopg2.connect(
    host=os.environ["TDX_DB_HOST"], port=int(os.environ.get("TDX_DB_PORT", "5432")),
    user=os.environ["TDX_DB_USER"], password=os.environ["TDX_DB_PASSWORD"],
    dbname=os.environ["TDX_DB_NAME"],
)

with conn.cursor() as cur:
    # 选每个因子最新的 out-sample 1d 指标
    cur.execute("""
        WITH rated AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id, official_grade, official_score
            FROM qe_factor_official_ratings ORDER BY factor_catalog_id, graded_at DESC
        ),
        metrics AS (
            SELECT DISTINCT ON (factor_catalog_id) factor_catalog_id,
                   ic_mean, rank_ic_mean, icir, rank_icir, ic_positive_ratio,
                   top_annual_return, top_excess_annual_return, top_excess_sharpe,
                   group_return_monotonicity, eval_window, return_horizon, calculated_at
            FROM aistock_factor_metrics
            WHERE eval_window='out_sample' AND return_horizon='1d'
            ORDER BY factor_catalog_id, calculated_at DESC
        )
        SELECT a.id, a.factor_name, r.official_grade,
               c.ic_value, c.ic_sign_consistency_12m, c.ts_info_density,
               c.cluster_role,
               m.ic_mean, m.rank_ic_mean, m.icir, m.rank_icir, m.ic_positive_ratio,
               m.top_excess_annual_return, m.top_excess_sharpe,
               m.group_return_monotonicity
        FROM aistock_factor_catalog a
        JOIN rated r ON r.factor_catalog_id=a.id
        LEFT JOIN qe_factor_classification c ON c.factor_catalog_id=a.id
        LEFT JOIN metrics m ON m.factor_catalog_id=a.id
        WHERE a.is_available=true
    """)
    rows = cur.fetchall()
print(f"扫描启用因子: {len(rows)}")

# 分类计数
grade_cnt = Counter(r[2] for r in rows)
print(f"按 grade: {dict(grade_cnt)}")

# 覆盖
has_m = sum(1 for r in rows if r[7] is not None)
print(f"有 out-sample 1d 指标: {has_m}\n")

# Rule B1 候选 (当前启用 + D + |ic|<0.005)
rule_b1 = [r for r in rows
           if r[2]=='D' and r[3] is not None and abs(r[3])<0.005]
print(f"Rule B1 (grade=D + |ic|<0.005): {len(rule_b1)}")

# Rule C 候选 (grade=D + excess<0 + excess_sharpe<0)
rule_c = [r for r in rows
          if r[2]=='D' and r[12] is not None and r[13] is not None
          and r[12] < 0 and r[13] < 0]
print(f"Rule C (grade=D + excess_return<0 + excess_sharpe<0): {len(rule_c)}")

# Rule D (grade=D + monotonicity<0 + rank_icir<0)
rule_d = [r for r in rows
          if r[2]=='D' and r[14] is not None and r[10] is not None
          and r[14] < 0 and r[10] < 0]
print(f"Rule D (grade=D + monotonicity<0 + rank_icir<0): {len(rule_d)}")

# 和 Rule A (member) 的重叠
with conn.cursor() as cur:
    cur.execute("""
        SELECT a.id FROM aistock_factor_catalog a
        JOIN qe_factor_classification c ON c.factor_catalog_id=a.id
        WHERE a.is_available=true AND c.cluster_role='member'
    """)
    rule_a_ids = {r[0] for r in cur.fetchall()}

b1_ids = {r[0] for r in rule_b1}
c_ids = {r[0] for r in rule_c}
d_ids = {r[0] for r in rule_d}

print(f"\nRule A (cluster member): {len(rule_a_ids)} (注: 当前启用集已无 member, 因聚类未跑本批)")
print(f"B1 ∪ C ∪ D             : {len(b1_ids | c_ids | d_ids)}")
print(f"B1 ∩ C                  : {len(b1_ids & c_ids)}")
print(f"B1 ∩ D                  : {len(b1_ids & d_ids)}")
print(f"C  ∩ D                  : {len(c_ids & d_ids)}")
print(f"B1 ∩ C ∩ D              : {len(b1_ids & c_ids & d_ids)}")

# chip_concentration_price_position (id=472) 命中哪些?
print(f"\nchip_concentration_price_position (id=472) 命中:")
print(f"  Rule B1: {472 in b1_ids}")
print(f"  Rule C : {472 in c_ids}")
print(f"  Rule D : {472 in d_ids}")

# 样本: Rule C 命中但 B1 没命中的 (说明不是极低 IC 但实盘亏)
only_c = [r for r in rule_c if r[0] not in b1_ids]
print(f"\n只 Rule C 命中 (非低 IC 但实盘亏): {len(only_c)} 个, 样本 10:")
for r in sorted(only_c, key=lambda x: x[12])[:10]:
    print(f"  {r[1]:<48} ic={r[3]:+.4f} excess={r[12]:+.3f} sharpe={r[13]:+.2f}")

conn.close()
