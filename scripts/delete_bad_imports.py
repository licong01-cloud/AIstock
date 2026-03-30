"""删除错误导入的 8 天数据 — 用时间戳范围避免 ::date 全表扫描"""
import psycopg2

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
conn.autocommit = True

dates = ['2024-01-05', '2024-01-26', '2024-01-30', '2024-01-31',
         '2024-02-20', '2024-02-23', '2024-02-29', '2024-03-14']

for d in dates:
    cur = conn.cursor()
    cur.execute("SET statement_timeout = '600s'")
    cur.execute(f"""
        DELETE FROM market.kline_minute_raw
        WHERE trade_time >= '{d} 00:00:00+08'
          AND trade_time < '{d} 23:59:59+08'
    """)
    print(f'{d}: 删除 {cur.rowcount:,} 行')

conn.close()
print('完成')
