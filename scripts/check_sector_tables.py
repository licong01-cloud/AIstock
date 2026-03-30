import os
from dotenv import load_dotenv
load_dotenv('/mnt/f/Dev/AIstock/.env')
import psycopg2

conn = psycopg2.connect(
    host=os.getenv('TDX_DB_HOST', '127.0.0.1'),
    port=int(os.getenv('TDX_DB_PORT', 5432)),
    user=os.getenv('TDX_DB_USER', 'postgres'),
    password=os.getenv('TDX_DB_PASSWORD', ''),
    dbname=os.getenv('TDX_DB_NAME', 'aistock'),
)
cur = conn.cursor()

# List all tables in market schema
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='market' ORDER BY table_name")
print('market schema tables:', [r[0] for r in cur.fetchall()])

# Check sw_index_member columns
try:
    cur.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_schema='market' AND table_name='sw_index_member' ORDER BY ordinal_position")
    print('sw_index_member columns:', cur.fetchall())
    cur.execute("SELECT COUNT(*), MIN(in_date), MAX(in_date) FROM market.sw_index_member")
    print('sw_index_member stats:', cur.fetchone())
except Exception as e:
    print('sw_index_member error:', e)

conn.close()
