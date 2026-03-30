"""验证写入的涨跌停 bin 文件是否正确"""
import qlib
from qlib.data import D
import pandas as pd
import psycopg2

qlib.init(provider_uri='/mnt/f/Dev/AIstock/qlib_bin/qlib_bin_20260311', region='cn')

# 用 Qlib 读取新写入的字段
df = D.features(
    ['000001.SZ', '300001.SZ', '688001.SZ'],
    ['$close', '$up_limit_price', '$down_limit_price'],
    '2025-01-02', '2025-01-10'
)
print('=== Qlib 读取涨跌停价格字段 ===')
print(df.to_string())

# 与数据库原始数据对比
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()
cur.execute("""
    SELECT ts_code, trade_date, up_limit, down_limit
    FROM market.stk_limit
    WHERE ts_code IN ('000001.SZ', '300001.SZ', '688001.SZ')
    AND trade_date BETWEEN '2025-01-02' AND '2025-01-10'
    ORDER BY ts_code, trade_date
""")
print('\n=== 数据库原始数据 ===')
for row in cur.fetchall():
    print(f'  {row[0]} | {row[1]} | up={row[2]} | down={row[3]}')
conn.close()

# 验证300和688的涨停幅度是否为20%
print('\n=== 板块涨幅验证 (up_limit/pre_close - 1) ===')
conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()
cur.execute("""
    SELECT ts_code, trade_date,
           ROUND((up_limit/pre_close - 1)*100, 1) as up_pct,
           ROUND((1 - down_limit/pre_close)*100, 1) as down_pct
    FROM market.stk_limit
    WHERE ts_code IN ('000001.SZ', '300001.SZ', '688001.SZ')
    AND trade_date = '2025-01-02'
""")
for row in cur.fetchall():
    print(f'  {row[0]} | {row[1]} | +{row[2]}% / -{row[3]}%')
conn.close()
