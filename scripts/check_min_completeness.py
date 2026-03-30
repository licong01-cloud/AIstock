"""检查 market.kline_minute_raw 分钟线数据完整性"""
import psycopg2
import pandas as pd
from datetime import datetime, timedelta

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
cur = conn.cursor()

# 1. 基本信息
cur.execute("SELECT MIN(trade_time::date), MAX(trade_time::date), COUNT(DISTINCT trade_time::date) FROM market.kline_minute_raw")
min_d, max_d, n_days = cur.fetchone()
print(f'=== 基本信息 ===')
print(f'  日期范围: {min_d} ~ {max_d}')
print(f'  有数据的交易日: {n_days}')

# 2. 对比日线表的交易日
cur.execute("""
    SELECT COUNT(DISTINCT trade_date) FROM market.kline_daily_raw
    WHERE trade_date >= '2024-01-01' AND trade_date <= %s
""", (max_d,))
daily_days = cur.fetchone()[0]
print(f'  日线表同期交易日: {daily_days}')
print(f'  差异: {daily_days - n_days} 天')

# 3. 找出日线有但分钟线缺失的交易日
cur.execute("""
    SELECT d.trade_date
    FROM (SELECT DISTINCT trade_date FROM market.kline_daily_raw
          WHERE trade_date >= '2024-01-01' AND trade_date <= %s) d
    LEFT JOIN (SELECT DISTINCT trade_time::date as trade_date FROM market.kline_minute_raw) m
    ON d.trade_date = m.trade_date
    WHERE m.trade_date IS NULL
    ORDER BY d.trade_date
""", (max_d,))
missing_days = cur.fetchall()
if missing_days:
    print(f'\n=== 缺失交易日 ({len(missing_days)} 天) ===')
    for row in missing_days:
        print(f'  {row[0]}')
else:
    print(f'\n  无缺失交易日')

# 4. 每日分钟数分布（检查是否有不完整的天）
cur.execute("""
    SELECT trade_time::date as td, COUNT(*) as min_count, COUNT(DISTINCT ts_code) as stock_count
    FROM market.kline_minute_raw
    GROUP BY td
    ORDER BY td
""")
daily_stats = cur.fetchall()
df = pd.DataFrame(daily_stats, columns=['date', 'min_count', 'stock_count'])

print(f'\n=== 每日数据量统计 ===')
print(f'  总交易日: {len(df)}')
print(f'  每日分钟数 — 中位数: {df["min_count"].median():,.0f}, 均值: {df["min_count"].mean():,.0f}')
print(f'  每日股票数 — 中位数: {df["stock_count"].median():.0f}, 均值: {df["stock_count"].mean():.0f}')
print(f'  每日分钟数范围: {df["min_count"].min():,} ~ {df["min_count"].max():,}')

# A 股每天 240 分钟（9:30-11:30=120 + 13:00-15:00=120）
# 正常 = 240 × 股票数
expected_per_stock = 240
median_stocks = df['stock_count'].median()
expected_total = expected_per_stock * median_stocks
print(f'  期望每日分钟数: ~{expected_total:,.0f} (240分钟 × {median_stocks:.0f}只)')

# 5. 找异常天（分钟数过少）
threshold = df['min_count'].median() * 0.5
abnormal = df[df['min_count'] < threshold]
if len(abnormal) > 0:
    print(f'\n=== 异常天（数据量 < 中位数50%）===')
    for _, row in abnormal.iterrows():
        print(f'  {row["date"]}: {row["min_count"]:,} 条, {row["stock_count"]} 只股票')
else:
    print(f'\n  无异常天（所有交易日数据量正常）')

# 6. 按月汇总
df['month'] = pd.to_datetime(df['date']).dt.to_period('M')
monthly = df.groupby('month').agg(
    days=('date', 'count'),
    avg_stocks=('stock_count', 'mean'),
    avg_minutes=('min_count', 'mean')
)
print(f'\n=== 按月汇总 ===')
print(f'  {"月份":<10} {"交易日":>6} {"日均股票":>8} {"日均分钟数":>12}')
for m, row in monthly.iterrows():
    print(f'  {str(m):<10} {row["days"]:>6} {row["avg_stocks"]:>8.0f} {row["avg_minutes"]:>12,.0f}')

# 7. 抽样检查单只股票的分钟完整性
print(f'\n=== 单股分钟完整性抽样 ===')
sample_stocks = ['000001.SZ', '600519.SH', '300750.SZ', '688001.SH']
for stock in sample_stocks:
    cur.execute("""
        SELECT trade_time::date as td, COUNT(*) as bars
        FROM market.kline_minute_raw
        WHERE ts_code = %s AND trade_time >= '2024-07-01'
        GROUP BY td
        ORDER BY bars
        LIMIT 5
    """, (stock,))
    rows = cur.fetchall()
    cur.execute("""
        SELECT COUNT(DISTINCT trade_time::date)
        FROM market.kline_minute_raw
        WHERE ts_code = %s AND trade_time >= '2024-07-01'
    """, (stock,))
    total_days = cur.fetchone()[0]

    if rows:
        min_bars = rows[0][1]
        max_bars_date = rows[0][0]
        print(f'  {stock}: {total_days} 交易日, 最少 {min_bars} bars ({max_bars_date})')
        if min_bars < 240:
            print(f'    异常天:')
            for d, b in rows:
                if b < 240:
                    print(f'      {d}: {b} bars')

conn.close()
