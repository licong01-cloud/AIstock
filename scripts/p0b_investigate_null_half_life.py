"""调查 130 个 ic_decay_half_life = NULL 因子的原因"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from dotenv import load_dotenv
load_dotenv()
import psycopg2

conn = psycopg2.connect(
    host=os.environ['TDX_DB_HOST'],
    port=os.environ['TDX_DB_PORT'],
    dbname=os.environ['TDX_DB_NAME'],
    user=os.environ['TDX_DB_USER'],
    password=os.environ['TDX_DB_PASSWORD'],
)
cur = conn.cursor()

# 1. NULL vs HAS_VALUE 的时间分布和 rank_ic_1d 是否也NULL
cur.execute("""
    WITH latest AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_1d, calculated_at, ic_mean, rank_ic_mean
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT
        CASE WHEN ic_decay_half_life IS NULL THEN 'NULL' ELSE 'HAS_VALUE' END as hl_status,
        COUNT(*) as cnt,
        MIN(calculated_at)::date as earliest,
        MAX(calculated_at)::date as latest,
        SUM(CASE WHEN rank_ic_1d IS NULL THEN 1 ELSE 0 END) as rank_ic_1d_null_cnt
    FROM latest
    GROUP BY hl_status
""")
print("--- ic_decay_half_life: NULL vs HAS_VALUE ---")
for hl_status, cnt, earliest, latest, ric_null in cur.fetchall():
    print(f"  {hl_status:>10s}: {cnt:5d}  earliest={earliest}  latest={latest}  rank_ic_1d_null={ric_null}")

# 2. NULL 因子按 calculated_at 日期分布
cur.execute("""
    WITH latest AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life, calculated_at
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT calculated_at::date as calc_date, COUNT(*) as cnt
    FROM latest
    WHERE ic_decay_half_life IS NULL
    GROUP BY calc_date
    ORDER BY calc_date
""")
print("\n--- NULL 因子按 calculated_at 日期分布 ---")
for dt, cnt in cur.fetchall():
    print(f"  {dt}: {cnt}")

# 3. 样例: NULL 因子的具体数据
cur.execute("""
    WITH latest AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life,
               rank_ic_1d, rank_ic_5d, rank_ic_10d, rank_ic_20d,
               ic_mean, rank_ic_mean, calculated_at, source_task_id
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT factor_name, ic_mean, rank_ic_mean, rank_ic_1d, rank_ic_5d,
           calculated_at::date, source_task_id
    FROM latest
    WHERE ic_decay_half_life IS NULL
    ORDER BY calculated_at DESC
    LIMIT 20
""")
print("\n--- NULL 因子样例 (最近20个) ---")
fmt_v = lambda v: f"{v:.4f}" if v is not None else "  NULL"
for name, ic, ric, ric1d, ric5d, calc_dt, task_id in cur.fetchall():
    print(f"  {name:45s} ic={fmt_v(ic)} ric1d={fmt_v(ric1d)} ric5d={fmt_v(ric5d)} date={calc_dt}  task={task_id}")

# 4. 对比: HAS_VALUE 因子的日期分布
cur.execute("""
    WITH latest AS (
        SELECT DISTINCT ON (factor_name) factor_name, ic_decay_half_life, calculated_at
        FROM aistock_factor_metrics
        WHERE eval_window = 'full'
        ORDER BY factor_name, calculated_at DESC
    )
    SELECT calculated_at::date as calc_date, COUNT(*) as cnt
    FROM latest
    WHERE ic_decay_half_life IS NOT NULL
    GROUP BY calc_date
    ORDER BY calc_date
""")
print("\n--- HAS_VALUE 因子按 calculated_at 日期分布 ---")
for dt, cnt in cur.fetchall():
    print(f"  {dt}: {cnt}")

conn.close()
