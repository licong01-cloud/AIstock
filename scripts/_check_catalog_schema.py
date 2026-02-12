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
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' AND table_name LIKE '%catalog%' ORDER BY table_name")
tables = [r[0] for r in cur.fetchall()]
print("=== CATALOG TABLES ===")
for t in tables:
    print(f"\nTable: {t}")
    cur.execute(f"SELECT column_name, data_type, is_nullable, column_default FROM information_schema.columns WHERE table_name=%s ORDER BY ordinal_position", (t,))
    for col in cur.fetchall():
        print(f"  {col[0]:40s} {col[1]:20s} nullable={col[2]} default={col[3]}")
    cur.execute(f"SELECT count(*) FROM {t}")
    cnt = cur.fetchone()[0]
    print(f"  ROW COUNT: {cnt}")

# Check factor_catalog data sample
print("\n\n=== FACTOR CATALOG SAMPLE (first 3 rows) ===")
cur.execute("""
    SELECT factor_name, source, expression, is_sota_factor,
           source_task_id, source_code_relpath,
           annualized_return, max_drawdown, sharpe, ic,
           performance_metrics IS NOT NULL as has_perf_json,
           code_text IS NOT NULL as has_code
    FROM aistock_factor_catalog
    LIMIT 3
""")
cols = [d[0] for d in cur.description]
for row in cur.fetchall():
    print(dict(zip(cols, row)))

# Check loop_catalog data
print("\n\n=== LOOP CATALOG SAMPLE ===")
cur.execute("SELECT count(*) FROM aistock_loop_catalog")
print(f"aistock_loop_catalog row count: {cur.fetchone()[0]}")
cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name='aistock_loop_catalog' ORDER BY ordinal_position")
for col in cur.fetchall():
    print(f"  {col[0]:40s} {col[1]:20s}")

conn.close()
