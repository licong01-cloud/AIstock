"""修复 2025-04-30 和 2025-12-01 的 open_li=NULL 坏数据
策略: DELETE NULL 行 + INSERT 正确数据（绕过 JOIN 时区匹配问题）
2025-04-30: 从 ZIP 文件获取
2025-12-01: 从 TDX API 获取
"""
import os, io, zipfile, requests, time
import psycopg2, psycopg2.extras, pandas as pd
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')
TDX_BASE = 'http://localhost:19080'
ZIP_ARCHIVE_DIR = r'F:\Dev\A股历史数据\A股分时数据\A股\1分钟_按月归档'

def to_tdx(ts_code):
    num, ex = ts_code.split('.')
    return ex + num

def ts_to_str(t):
    """pandas Timestamp -> '2025-04-30 09:31:00+08:00' 字符串"""
    return t.strftime('%Y-%m-%d %H:%M:%S') + '+08:00'

conn = psycopg2.connect(**DB_CONFIG)
conn.autocommit = False
cur = conn.cursor()
cur.execute("SET statement_timeout = 0")
cur.execute("SET timezone = 'Asia/Shanghai'")
cur.execute("SET timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
conn.commit()

# ===================== Phase 1: 2025-04-30 从 ZIP =====================
print('=== Phase 1: 修复 2025-04-30 (ZIP) ===', flush=True)
zip_path = os.path.join(ZIP_ARCHIVE_DIR, '2025-04', '20250430_1min.zip')
z = zipfile.ZipFile(zip_path)
rows_0430 = []
for fname in z.namelist():
    if not fname.endswith('.csv') or fname.startswith('bj'):
        continue
    try:
        df = pd.read_csv(io.BytesIO(z.read(fname)), encoding='utf-8')
    except:
        continue
    if len(df) < 2 or len(df.columns) < 9:
        continue
    df.columns = ['trade_time','ts_code_raw','name','open','close','high','low','volume','amount'] + [f'x{i}' for i in range(len(df.columns)-9)]
    code_raw = df['ts_code_raw'].iloc[0]
    exchange = code_raw[:2].upper()
    number = code_raw[2:]
    if exchange == 'BJ':
        continue
    ts_code = f'{number}.{exchange}'
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    for t, o, h, l, c, v, a in zip(
        df['trade_time'],
        (df['open'] * 1000).round().astype(int),
        (df['high'] * 1000).round().astype(int),
        (df['low'] * 1000).round().astype(int),
        (df['close'] * 1000).round().astype(int),
        df['volume'].astype(int),
        (df['amount'] * 1000).round().astype(int)):
        if t.hour == 9 and t.minute == 30:
            continue  # 跳过集合竞价 bar（tdx_vipdoc 已有）
        rows_0430.append((ts_to_str(t), ts_code, '1m', int(o), int(h), int(l), int(c),
                          int(v), int(a), 'tdx_api'))
z.close()
print(f'  ZIP 解析完成: {len(rows_0430):,} 行 (不含集合竞价)', flush=True)

# Step 1a: DELETE NULL rows
t0 = time.time()
cur.execute("""DELETE FROM market.kline_minute_raw
    WHERE trade_time >= '2025-04-30 00:00:00+08' AND trade_time < '2025-05-01 00:00:00+08'
      AND source = 'tdx_api' AND open_li IS NULL""")
deleted = cur.rowcount
conn.commit()
print(f'  已删除 NULL 行: {deleted:,} ({time.time()-t0:.1f}s)', flush=True)

# Step 1b: INSERT correct data
t0 = time.time()
psycopg2.extras.execute_values(cur,
    """INSERT INTO market.kline_minute_raw
       (trade_time, ts_code, freq, open_li, high_li, low_li, close_li,
        volume_hand, amount_li, source)
       VALUES %s ON CONFLICT (trade_time, ts_code, freq) DO UPDATE SET
        open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li,
        low_li=EXCLUDED.low_li, close_li=EXCLUDED.close_li,
        volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li,
        source=EXCLUDED.source""",
    rows_0430, page_size=50000)
conn.commit()
print(f'  已插入正确数据: {len(rows_0430):,} 行 ({time.time()-t0:.1f}s)', flush=True)

# ===================== Phase 2: 2025-12-01 从 TDX API =====================
print('\n=== Phase 2: 修复 2025-12-01 (TDX API) ===', flush=True)
TARGET = '2025-12-01'

cur.execute("SELECT ts_code FROM market.stock_basic WHERE list_status='L' AND exchange IN ('SSE','SZSE')")
all_codes = [r[0] for r in cur.fetchall()]
print(f'  股票总数: {len(all_codes)}', flush=True)

def fetch_one(ts_code):
    tdx_code = to_tdx(ts_code)
    try:
        resp = requests.get(f'{TDX_BASE}/api/kline-all/tdx',
                          params={'code': tdx_code, 'type': 'minute1'}, timeout=20)
        items = resp.json().get('data', {}).get('list', [])
    except:
        return ts_code, None
    day = [it for it in items if it.get('Time', '').startswith(TARGET)]
    if not day:
        return ts_code, []
    rows = []
    for bar in day:
        rows.append((bar['Time'], ts_code, '1m',
             int(bar.get('Open', 0)), int(bar.get('High', 0)),
             int(bar.get('Low', 0)), int(bar.get('Close', 0)),
             int(bar.get('Volume', 0)), int(bar.get('Amount', 0)),
             'tdx_api'))
    return ts_code, rows

all_rows_1201 = []
done = 0
errors = 0
empty = 0
t0 = time.time()
with ThreadPoolExecutor(max_workers=4) as pool:
    futures = {pool.submit(fetch_one, code): code for code in all_codes}
    for f in as_completed(futures):
        done += 1
        ts_code, rows = f.result()
        if rows is None:
            errors += 1
        elif not rows:
            empty += 1
        else:
            all_rows_1201.extend(rows)
        if done % 500 == 0:
            print(f'  [{done}/{len(all_codes)}] 收集 {len(all_rows_1201):,} 行, 空={empty}, 错误={errors}', flush=True)

print(f'  TDX 获取完成: {len(all_rows_1201):,} 行, 空={empty}, 错误={errors} ({time.time()-t0:.1f}s)', flush=True)

if all_rows_1201:
    # Step 2a: DELETE NULL rows
    t0 = time.time()
    cur.execute("""DELETE FROM market.kline_minute_raw
        WHERE trade_time >= '2025-12-01 00:00:00+08' AND trade_time < '2025-12-02 00:00:00+08'
          AND source = 'tdx_api' AND open_li IS NULL""")
    deleted2 = cur.rowcount
    conn.commit()
    print(f'  已删除 NULL 行: {deleted2:,} ({time.time()-t0:.1f}s)', flush=True)

    # Step 2b: INSERT correct data
    t0 = time.time()
    psycopg2.extras.execute_values(cur,
        """INSERT INTO market.kline_minute_raw
           (trade_time, ts_code, freq, open_li, high_li, low_li, close_li,
            volume_hand, amount_li, source)
           VALUES %s ON CONFLICT (trade_time, ts_code, freq) DO UPDATE SET
            open_li=EXCLUDED.open_li, high_li=EXCLUDED.high_li,
            low_li=EXCLUDED.low_li, close_li=EXCLUDED.close_li,
            volume_hand=EXCLUDED.volume_hand, amount_li=EXCLUDED.amount_li,
            source=EXCLUDED.source""",
        all_rows_1201, page_size=50000)
    conn.commit()
    print(f'  已插入正确数据: {len(all_rows_1201):,} 行 ({time.time()-t0:.1f}s)', flush=True)
else:
    print('  WARNING: TDX API 未获取到 2025-12-01 数据！', flush=True)

# ===================== Phase 3: 验证 =====================
print('\n=== Phase 3: 验证 ===', flush=True)
for d, d_next in [('2025-04-30', '2025-05-01'), ('2025-12-01', '2025-12-02')]:
    cur.execute(f"""SELECT source, COUNT(*),
        SUM(CASE WHEN open_li IS NULL THEN 1 ELSE 0 END) as nulls
        FROM market.kline_minute_raw
        WHERE trade_time >= '{d} 00:00:00+08' AND trade_time < '{d_next} 00:00:00+08'
        GROUP BY source ORDER BY source""")
    rows = cur.fetchall()
    for r in rows:
        status = 'OK' if r[2] == 0 else f'NULL={r[2]}'
        print(f'  {d} [{r[0]}]: {r[1]:,} 行, {status}')

cur.execute("SELECT count(*) FROM market.kline_minute_raw WHERE open_li IS NULL")
total_null = cur.fetchone()[0]
print(f'\n  全库剩余 NULL: {total_null:,} 行')

conn.close()
print('\n完成!')
