"""补齐 2025-12-02 单天的全市场 1 分钟 K 线数据
从 TDX 后端 API 获取，写入 market.kline_minute_raw
使用 4 线程并行加速"""
import requests, psycopg2, psycopg2.extras, sys
from concurrent.futures import ThreadPoolExecutor, as_completed

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')
TDX_BASE = 'http://localhost:19080'
TARGET_DATE = '2025-12-02'
WORKERS = 4

# ts_code (000001.SZ) -> TDX code (SZ000001)
def to_tdx(ts_code):
    num, ex = ts_code.split('.')
    return ex + num

def fetch_one(ts_code):
    """获取单只股票目标日期的分钟线，返回 (ts_code, rows) 或 (ts_code, None)"""
    tdx_code = to_tdx(ts_code)
    try:
        resp = requests.get(f'{TDX_BASE}/api/kline-all/tdx',
                          params={'code': tdx_code, 'type': 'minute1'}, timeout=20)
        data = resp.json().get('data', {})
        items = data.get('list') or data.get('List') or []
    except Exception:
        return ts_code, None

    day_bars = [it for it in items if it.get('Time', '').startswith(TARGET_DATE)]
    if not day_bars:
        return ts_code, []

    rows = []
    for bar in day_bars:
        rows.append((bar['Time'], ts_code, '1m',
                     bar.get('Open', 0), bar.get('High', 0),
                     bar.get('Low', 0), bar.get('Close', 0),
                     bar.get('Volume', 0), bar.get('Amount', 0),
                     'none', 'tdx_api'))
    return ts_code, rows

# 获取所有上市股票代码
conn = psycopg2.connect(**DB_CONFIG)
cur = conn.cursor()
cur.execute("""
    SELECT ts_code FROM market.stock_basic
    WHERE list_status = 'L' AND exchange IN ('SSE','SZSE')
""")
all_codes = [r[0] for r in cur.fetchall()]
print(f'上市股票: {len(all_codes)} 只', flush=True)

total_inserted = 0
errors = 0
empty = 0
done = 0

sql = """INSERT INTO market.kline_minute_raw
    (trade_time, ts_code, freq, open_li, high_li, low_li, close_li,
     volume_hand, amount_li, adjust_type, source)
    VALUES %s ON CONFLICT DO NOTHING"""

with ThreadPoolExecutor(max_workers=WORKERS) as pool:
    futures = {pool.submit(fetch_one, code): code for code in all_codes}
    for future in as_completed(futures):
        done += 1
        ts_code, rows = future.result()
        if rows is None:
            errors += 1
        elif len(rows) == 0:
            empty += 1
        else:
            psycopg2.extras.execute_values(cur, sql, rows, page_size=5000)
            conn.commit()
            total_inserted += len(rows)

        if done % 100 == 0:
            print(f'[{done}/{len(all_codes)}] 插入 {total_inserted:,} 行, 空 {empty}, 错 {errors}', flush=True)

conn.close()
print(f'\n完成! 总插入: {total_inserted:,} 行, 空 {empty} 只, 错误 {errors} 只')
