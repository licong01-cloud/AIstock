"""Direct DB query to check sync_status values."""
import os
import psycopg2
from dotenv import load_dotenv

# Load env
load_dotenv(r"f:\Dev\AIstock\.env")

conn = psycopg2.connect(
    host=os.getenv('PG_HOST', 'localhost'),
    port=int(os.getenv('PG_PORT', '5432')),
    dbname=os.getenv('PG_DATABASE', 'aistock'),
    user=os.getenv('PG_USER', 'postgres'),
    password=os.getenv('PG_PASSWORD', '')
)
cur = conn.cursor()

# Check actual sync_status values
print("=== aistock_task_catalog sync_status values ===")
cur.execute("""
    SELECT sync_status, COUNT(*)
    FROM aistock_task_catalog
    GROUP BY sync_status
    ORDER BY sync_status
""")
for row in cur.fetchall():
    print(f"  {repr(row[0])}: {row[1]}")

# Check a]a] tasks specifically
print("\n=== a]a] tasks details ===")
cur.execute("""
    SELECT task_id, sync_status, task_dir, updated_at_utc
    FROM aistock_task_catalog
    WHERE task_id LIKE '%a]a]%'
""")
for row in cur.fetchall():
    print(f"  task_id: {row[0]}")
    print(f"  sync_status: {repr(row[1])}")
    print(f"  task_dir: {row[2]}")
    print(f"  updated_at_utc: {row[3]}")
    print()

# Check table column default
print("=== Column defaults ===")
cur.execute("""
    SELECT column_name, column_default, is_nullable
    FROM information_schema.columns
    WHERE table_name = 'aistock_task_catalog' AND column_name = 'sync_status'
""")
for row in cur.fetchall():
    print(f"  column: {row[0]}, default: {repr(row[1])}, nullable: {row[2]}")

conn.close()
