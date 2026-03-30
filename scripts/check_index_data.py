import psycopg2, os
from dotenv import load_dotenv
load_dotenv('/mnt/f/Dev/AIstock/.env')

conn = psycopg2.connect(
    host=os.getenv('TDX_DB_HOST', '127.0.0.1'),
    port=int(os.getenv('TDX_DB_PORT', 5432)),
    user=os.getenv('TDX_DB_USER', 'postgres'),
    password=os.getenv('TDX_DB_PASSWORD', ''),
    dbname=os.getenv('TDX_DB_NAME', 'aistock'),
)
cur = conn.cursor()

# 检查指数分钟线
cur.execute("""
    SELECT ts_code, COUNT(*), MIN(trade_time), MAX(trade_time)
    FROM market.kline_minute_raw
    WHERE ts_code IN ('000001.SH', '399001.SZ', '399006.SZ')
    GROUP BY ts_code
""")
rows = cur.fetchall()
print('Index minute data in DB:')
for r in rows:
    print(f'  {r[0]}: {r[1]:,} rows, {r[2]} ~ {r[3]}')

# 检查指数日线
cur.execute("""
    SELECT ts_code, COUNT(*), MIN(trade_date), MAX(trade_date)
    FROM market.index_daily
    WHERE ts_code IN ('000001.SH', '399001.SZ', '399006.SZ')
    GROUP BY ts_code
""")
rows = cur.fetchall()
print('Index daily data in DB:')
for r in rows:
    print(f'  {r[0]}: {r[1]:,} rows, {r[2]} ~ {r[3]}')

conn.close()
