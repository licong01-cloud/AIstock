"""验证分钟线数据完整性：选 000001.SZ，检查每天是否有 240 条记录"""
import psycopg2
import pandas as pd

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()

stock = '000001.SZ'

# 1. 该股票每天的记录数
cur.execute("""
    SELECT trade_time::date as dt, COUNT(*) as cnt
    FROM market.kline_minute_raw
    WHERE ts_code = %s
    GROUP BY dt
    ORDER BY dt
""", (stock,))
rows = cur.fetchall()
print(f'=== {stock} 分钟线完整性检查 ===')
print(f'总交易日数: {len(rows)}')
if rows:
    print(f'日期范围: {rows[0][0]} ~ {rows[-1][0]}')

# 2. 非 240 条的天
bad = [(str(r[0]), r[1]) for r in rows if r[1] != 240 and r[1] != 241]
print(f'\n非 240/241 条的天数: {len(bad)}')
if bad:
    for d, c in bad[:30]:
        print(f'  {d}: {c} 条')
    if len(bad) > 30:
        print(f'  ... 还有 {len(bad)-30} 天')

# 3. 按来源区分：ZIP 补齐期 vs 原有数据
# ZIP 补齐的日期大致是 2024-01 ~ 2025-03 中的缺失天
# 原有数据是 2025-07 之后
print(f'\n=== 按时间段统计 ===')
for period, start, end in [
    ('2024-01~06 (ZIP补齐)', '2024-01-01', '2024-06-30'),
    ('2024-07~12 (ZIP补齐+原有)', '2024-07-01', '2024-12-31'),
    ('2025-01~03 (ZIP补齐)', '2025-01-01', '2025-03-31'),
    ('2025-04~06 (原有/过渡)', '2025-04-01', '2025-06-30'),
    ('2025-07~09 (原有)', '2025-07-01', '2025-09-30'),
    ('2025-10~12 (原有)', '2025-10-01', '2025-12-31'),
    ('2026-01~03 (原有)', '2026-01-01', '2026-03-31'),
]:
    period_rows = [(d, c) for d, c in rows if start <= str(d) <= end]
    if not period_rows:
        print(f'  {period}: 无数据')
        continue
    total_days = len(period_rows)
    bad_days = [(d, c) for d, c in period_rows if c != 240 and c != 241]
    print(f'  {period}: {total_days} 天, 异常 {len(bad_days)} 天')
    for d, c in bad_days[:5]:
        print(f'    {d}: {c} 条')

# 4. 对比交易日历，找缺失天
cur.execute("""
    SELECT tc.cal_date
    FROM market.trading_calendar tc
    LEFT JOIN (
        SELECT DISTINCT trade_time::date as td
        FROM market.kline_minute_raw
        WHERE ts_code = %s
    ) m ON tc.cal_date = m.td
    WHERE tc.is_trading = true
      AND tc.cal_date >= '2024-01-01'
      AND tc.cal_date <= '2026-03-19'
      AND m.td IS NULL
    ORDER BY tc.cal_date
""", (stock,))
missing = cur.fetchall()
print(f'\n=== 对照交易日历缺失天 ===')
print(f'缺失交易日: {len(missing)} 天')
for r in missing:
    print(f'  {r[0]}')

# 5. 抽样验证 241 条的天（可能含集合竞价 15:00 bar）
cur.execute("""
    SELECT trade_time::date as dt, COUNT(*) as cnt
    FROM market.kline_minute_raw
    WHERE ts_code = %s
    GROUP BY dt
    HAVING COUNT(*) = 241
    ORDER BY dt
    LIMIT 5
""", (stock,))
rows_241 = cur.fetchall()
if rows_241:
    print(f'\n=== 241 条的天（含 15:00 集合竞价 bar）===')
    for d, c in rows_241:
        cur.execute("""
            SELECT MIN(trade_time::time), MAX(trade_time::time)
            FROM market.kline_minute_raw
            WHERE ts_code = %s AND trade_time::date = %s
        """, (stock, d))
        t_min, t_max = cur.fetchone()
        print(f'  {d}: {c} 条, 时间 {t_min} ~ {t_max}')

conn.close()
