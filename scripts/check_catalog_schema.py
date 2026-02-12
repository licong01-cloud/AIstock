"""Check all catalog tables schema and data."""
import psycopg2, json, os, sys
from dotenv import load_dotenv
load_dotenv()

conn = psycopg2.connect(
    host=os.getenv('PG_HOST','localhost'),
    port=int(os.getenv('PG_PORT','5432')),
    dbname=os.getenv('PG_DATABASE','aistock'),
    user=os.getenv('PG_USER','postgres'),
    password=os.getenv('PG_PASSWORD','')
)
cur = conn.cursor()

# Find all tables with 'catalog' in name
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%%catalog%%' ORDER BY table_name")
tables = [r[0] for r in cur.fetchall()]
print("=== CATALOG TABLES ===")
for t in tables:
    print(f"\nTable: {t}")
    cur.execute("SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (t,))
    for col in cur.fetchall():
        print(f"  {col[0]:40s} {col[1]:20s} nullable={col[2]} default={col[3]}")
    cur.execute(f"SELECT count(*) FROM {t}")
    cnt = cur.fetchone()[0]
    print(f"  ROW COUNT: {cnt}")

# Check factor_catalog data sample
print("\n\n=== FACTOR CATALOG SAMPLE (first 3 rows, key fields) ===")
cur.execute("""
    SELECT factor_name, source, tags,
           is_sota_factor,
           source_task_id,
           annualized_return, max_drawdown, sharpe, ic,
           performance_metrics IS NOT NULL as has_perf_json,
           code_text IS NOT NULL as has_code,
           expression
    FROM aistock_factor_catalog
    WHERE is_sota_factor = true
    LIMIT 3
""")
cols = [d[0] for d in cur.description]
for row in cur.fetchall():
    d = dict(zip(cols, row))
    # truncate expression for readability
    if d.get('expression') and len(str(d['expression'])) > 80:
        d['expression'] = str(d['expression'])[:80] + '...'
    print(json.dumps(d, ensure_ascii=False, default=str))

# Check loop_catalog
print("\n\n=== LOOP CATALOG ===")
cur.execute("SELECT count(*) FROM aistock_loop_catalog")
print(f"aistock_loop_catalog row count: {cur.fetchone()[0]}")

# Check factor counts by source
print("\n\n=== FACTOR COUNTS BY SOURCE ===")
cur.execute("SELECT source, count(*), count(annualized_return) as has_ann_ret FROM aistock_factor_catalog GROUP BY source ORDER BY source")
for row in cur.fetchall():
    print(f"  source={row[0]:30s}  total={row[1]}  has_ann_ret={row[2]}")

# Check factor counts by tags
print("\n\n=== FACTOR COUNTS BY TAG STATUS ===")
cur.execute("SELECT CASE WHEN tags IS NULL THEN 'NULL' WHEN tags::text = '[]' THEN 'EMPTY' ELSE 'HAS_TAGS' END as tag_status, count(*) FROM aistock_factor_catalog GROUP BY 1")
for row in cur.fetchall():
    print(f"  {row[0]:20s}  count={row[1]}")

# Check if alpha158 tag exists
cur.execute("SELECT count(*) FROM aistock_factor_catalog WHERE tags ? 'alpha158'")
print(f"\n  Factors with 'alpha158' tag: {cur.fetchone()[0]}")

cur.execute("SELECT count(*) FROM aistock_factor_catalog WHERE is_sota_factor = true")
print(f"  SOTA factors: {cur.fetchone()[0]}")

conn.close()
