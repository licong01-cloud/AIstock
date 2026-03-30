import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
conn.set_session(autocommit=True)
cur = conn.cursor()

# 1. 总行数
cur.execute('SELECT count(*) FROM market.kline_minute_raw')
print(f'总行数: {cur.fetchone()[0]:,}')

# 2. NULL 检查
cur.execute('SELECT count(*) FROM market.kline_minute_raw WHERE open_li IS NULL')
print(f'open_li NULL: {cur.fetchone()[0]:,}')

# 3. 按月统计
cur.execute("""
SELECT
  to_char(date_trunc('month', trade_time + interval '8 hours'), 'YYYY-MM') as month,
  count(DISTINCT date(trade_time + interval '8 hours')) as trading_days,
  count(*) as total_rows,
  count(DISTINCT ts_code) as stocks
FROM market.kline_minute_raw
GROUP BY 1 ORDER BY 1
""")
print()
print('月份        交易日  总行数           股票数')
print('-' * 55)
for r in cur.fetchall():
    print(f'{r[0]}    {r[1]:3d}日  {r[2]:>15,}  {r[3]:>5}只')

# 4. 来源分布
cur.execute("""
SELECT source, count(*), count(DISTINCT ts_code)
FROM market.kline_minute_raw
GROUP BY source ORDER BY source
""")
print()
print('=== 数据来源分布 ===')
for r in cur.fetchall():
    print(f'  {r[0]}: {r[1]:,} 行, {r[2]} 只股票')

conn.close()
print('\n检查完成')
