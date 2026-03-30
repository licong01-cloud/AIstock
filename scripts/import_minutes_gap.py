"""强制补齐 2025-03-10 ~ 2025-07-07 期间的分钟线数据
这些天 DB 中只有极少量股票（1~几十只），需要从 ZIP 月归档补齐全市场"""
import os, io, zipfile, psycopg2, psycopg2.extras, pandas as pd

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')
ZIP_ARCHIVE_DIR = r'F:\Dev\A股历史数据\A股分时数据\A股\1分钟_按月归档'
BATCH_SIZE = 50000

conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()

# 获取需要补齐的日期（交易日历中 2025-03-10 ~ 2025-07-07）
cur.execute("""
    SELECT cal_date FROM market.trading_calendar
    WHERE is_trading = true AND cal_date >= '2025-03-10' AND cal_date <= '2025-07-07'
    ORDER BY cal_date
""")
dates = [str(r[0]).replace('-','') for r in cur.fetchall()]
print(f'待补齐日期: {len(dates)} 天')

total_inserted = 0
for i, d in enumerate(dates):
    month = f'{d[:4]}-{d[4:6]}'
    zip_path = os.path.join(ZIP_ARCHIVE_DIR, month, f'{d}_1min.zip')
    if not os.path.exists(zip_path):
        print(f'[{i+1}/{len(dates)}] {d}: ZIP不存在')
        continue

    z = zipfile.ZipFile(zip_path)
    all_rows = []
    for fname in z.namelist():
        if not fname.endswith('.csv') or fname.startswith('bj'):
            continue
        try:
            df = pd.read_csv(io.BytesIO(z.read(fname)), encoding='utf-8')
        except:
            continue
        if len(df) < 2:
            continue
        cols = df.columns.tolist()
        if len(cols) < 9:
            continue
        df.columns = ['trade_time','ts_code_raw','name','open','close','high','low','volume','amount'] + [f'x{i}' for i in range(len(cols)-9)]
        code_raw = df['ts_code_raw'].iloc[0]
        exchange = code_raw[:2].upper()
        number = code_raw[2:]
        if exchange == 'BJ':
            continue
        ts_code = f'{number}.{exchange}'
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df['ts_code'] = ts_code
        df['open_li'] = (df['open'] * 1000).round().astype(int)
        df['high_li'] = (df['high'] * 1000).round().astype(int)
        df['low_li'] = (df['low'] * 1000).round().astype(int)
        df['close_li'] = (df['close'] * 1000).round().astype(int)
        df['volume_hand'] = df['volume'].astype(int)
        df['amount_li'] = (df['amount'] * 1000).round().astype(int)
        for t, o, h, l, c, v, a in zip(df['trade_time'], df['open_li'], df['high_li'], df['low_li'], df['close_li'], df['volume_hand'], df['amount_li']):
            all_rows.append((t, ts_code, '1m', o, h, l, c, v, a, 'none', 'tdx_vipdoc'))
    z.close()

    if not all_rows:
        print(f'[{i+1}/{len(dates)}] {d}: 无数据')
        continue

    n_stocks = len(set(r[1] for r in all_rows))
    sql = """INSERT INTO market.kline_minute_raw
        (trade_time, ts_code, freq, open_li, high_li, low_li, close_li, volume_hand, amount_li, adjust_type, source)
        VALUES %s ON CONFLICT DO NOTHING"""
    psycopg2.extras.execute_values(cur, sql, all_rows, page_size=BATCH_SIZE)
    conn.commit()
    total_inserted += len(all_rows)
    print(f'[{i+1}/{len(dates)}] {d}: {len(all_rows):,} 行, {n_stocks} 只')

conn.close()
print(f'\n完成! 总导入: {total_inserted:,} 行')
