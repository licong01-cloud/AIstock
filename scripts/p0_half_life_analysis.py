"""P0: IC Decay Half-Life 数据分析脚本"""
import os, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from dotenv import load_dotenv
load_dotenv(os.path.join(os.path.dirname(__file__), "..", ".env"))
import psycopg2

conn = psycopg2.connect(
    host=os.environ['TDX_DB_HOST'],
    port=os.environ['TDX_DB_PORT'],
    dbname=os.environ['TDX_DB_NAME'],
    user=os.environ['TDX_DB_USER'],
    password=os.environ['TDX_DB_PASSWORD'],
)
cur = conn.cursor()

# 1. S/A/B 因子半衰期详细列表
cur.execute("""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d, rank_ic_mean
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT m.factor_name, m.ic_decay_half_life, c.grade, c.category,
           m.rank_ic_1d, m.rank_ic_5d, m.rank_ic_10d, m.rank_ic_20d, m.rank_ic_mean
    FROM latest_metrics m
    JOIN qe_factor_classification c ON c.factor_name = m.factor_name
    WHERE c.grade IN ('S', 'A', 'B')
      AND m.ic_decay_half_life IS NOT NULL
    ORDER BY m.ic_decay_half_life
""")
rows = cur.fetchall()
print(f"--- S/A/B 因子半衰期 (N={len(rows)}) ---")

def f(v):
    return f"{v:.4f}" if v is not None else "  N/A "

for name, hl, grade, cat, ic1, ic5, ic10, ic20, icm in rows:
    cls = "short" if hl < 10 else ("medium" if hl <= 30 else "long")
    print(f"  {cls:6s} {name:45s} hl={hl:6.1f}d {grade} {cat:5s}  "
          f"ic1d={f(ic1)} ic5d={f(ic5)} ic10d={f(ic10)} ic20d={f(ic20)}")

# 2. 按 grade 统计
print()
for g in ["S", "A", "B"]:
    sub = [r for r in rows if r[2] == g]
    if not sub:
        continue
    s = sum(1 for r in sub if r[1] < 10)
    m = sum(1 for r in sub if 10 <= r[1] <= 30)
    l = sum(1 for r in sub if r[1] > 30)
    print(f"  Grade {g}: total={len(sub)}, short={s}, medium={m}, long={l}")

# 3. 所有因子按分类的平均 |IC|
cur.execute("""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_mean, ic_mean
        FROM aistock_factor_metrics
        WHERE eval_window = 'full' AND ic_decay_half_life IS NOT NULL
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT
        CASE WHEN ic_decay_half_life < 10 THEN 'short'
             WHEN ic_decay_half_life <= 30 THEN 'medium'
             ELSE 'long' END as cls,
        COUNT(*),
        AVG(ABS(rank_ic_mean)),
        AVG(ABS(ic_mean))
    FROM latest_metrics
    WHERE rank_ic_mean IS NOT NULL
    GROUP BY cls
    ORDER BY cls
""")
print(f"\n--- 各分类平均 |IC| ---")
print(f"  {'class':>8s} {'count':>6s} {'avg|RankIC|':>12s} {'avg|IC|':>10s}")
for cls, cnt, ric, ic in cur.fetchall():
    print(f"  {cls:>8s} {cnt:6d} {ric:12.5f} {ic:10.5f}")

# 4. 多持有期IC的衰减模式 (取 rank_ic_1d 有值的因子)
cur.execute("""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d
        FROM aistock_factor_metrics
        WHERE eval_window = 'full' AND rank_ic_1d IS NOT NULL
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT
        CASE WHEN ic_decay_half_life < 10 THEN 'short'
             WHEN ic_decay_half_life <= 30 THEN 'medium'
             ELSE 'long' END as cls,
        COUNT(*),
        AVG(ABS(rank_ic_1d)),
        AVG(ABS(rank_ic_5d)),
        AVG(ABS(rank_ic_10d)),
        AVG(ABS(rank_ic_20d))
    FROM latest_metrics
    WHERE ic_decay_half_life IS NOT NULL
    GROUP BY cls
    ORDER BY cls
""")
print(f"\n--- 各分类平均多持有期 |RankIC| 衰减曲线 ---")
print(f"  {'class':>8s} {'count':>6s} {'|IC_1d|':>8s} {'|IC_5d|':>8s} {'|IC_10d|':>9s} {'|IC_20d|':>9s}")
for cls, cnt, ic1, ic5, ic10, ic20 in cur.fetchall():
    print(f"  {cls:>8s} {cnt:6d} {ic1:8.5f} {ic5:8.5f} {ic10:9.5f} {ic20:9.5f}")

# 5. 缺失半衰期的因子 (NULL) — 检查是否需要补算
cur.execute("""
    WITH latest_metrics AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_mean, calculated_at
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT COUNT(*) as total,
           SUM(CASE WHEN ic_decay_half_life IS NULL THEN 1 ELSE 0 END) as null_cnt
    FROM latest_metrics
""")
total, null_cnt = cur.fetchone()
print(f"\n--- 半衰期 NULL 统计 ---")
print(f"  总因子: {total}, NULL: {null_cnt} ({null_cnt/total*100:.1f}%)")

conn.close()
