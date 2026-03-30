"""
补齐 market.kline_minute_raw 分钟线数据。
数据来源：
  1. ZIP 文件 (F:\Dev\A股历史数据\A股分时数据\A股\1分钟\YYYYMMDD_1min.zip)
  2. Tushare 缓存 parquet (scripts/tushare_min_cache/YYYYMMDD_1min.parquet)

只导入 DB 中缺失的交易日，跳过北交所。
"""
import os
import io
import zipfile
import time
import psycopg2
import psycopg2.extras
import pandas as pd
import numpy as np
from datetime import datetime

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')
ZIP_DIR = r'F:\Dev\A股历史数据\A股分时数据\A股\1分钟'
ZIP_ARCHIVE_DIR = r'F:\Dev\A股历史数据\A股分时数据\A股\1分钟_按月归档'
TUSHARE_CACHE_DIR = r'F:\Dev\AIstock\scripts\tushare_min_cache'
BATCH_SIZE = 50000


def get_missing_dates(conn):
    """获取 DB 中缺失的交易日（对照交易日历）"""
    cur = conn.cursor()
    cur.execute("""
        SELECT tc.cal_date
        FROM market.trading_calendar tc
        LEFT JOIN (
            SELECT DISTINCT trade_time::date as td FROM market.kline_minute_raw
        ) m ON tc.cal_date = m.td
        WHERE tc.is_trading = true
          AND tc.cal_date >= '2024-01-01'
          AND tc.cal_date <= CURRENT_DATE
          AND m.td IS NULL
        ORDER BY tc.cal_date
    """)
    return [str(r[0]).replace('-', '') for r in cur.fetchall()]


def find_zip_path(date_str):
    """在日文件目录和月归档目录中查找 ZIP"""
    # 优先日文件目录
    p1 = os.path.join(ZIP_DIR, f'{date_str}_1min.zip')
    if os.path.exists(p1):
        return p1
    # 月归档目录
    month = f'{date_str[:4]}-{date_str[4:6]}'
    p2 = os.path.join(ZIP_ARCHIVE_DIR, month, f'{date_str}_1min.zip')
    if os.path.exists(p2):
        return p2
    return None


def load_from_zip(date_str):
    """从 ZIP 文件加载分钟线数据"""
    zip_path = find_zip_path(date_str)
    if zip_path is None:
        return None

    z = zipfile.ZipFile(zip_path)
    all_rows = []
    for fname in z.namelist():
        if not fname.endswith('.csv'):
            continue
        # 过滤北交所 (bj 开头)
        if fname.startswith('bj'):
            continue

        try:
            raw = z.read(fname)
            df = pd.read_csv(io.BytesIO(raw), encoding='utf-8')
        except Exception:
            continue

        if len(df) < 2:
            continue

        # 重命名列（按位置，列名可能是乱码）
        cols = df.columns.tolist()
        if len(cols) < 9:
            continue
        df.columns = ['trade_time', 'ts_code_raw', 'name', 'open', 'close',
                      'high', 'low', 'volume', 'amount'] + [f'extra_{i}' for i in range(len(cols)-9)]

        # 转换 ts_code: sh600000 → 600000.SH
        code_raw = df['ts_code_raw'].iloc[0]
        exchange = code_raw[:2].upper()
        number = code_raw[2:]
        ts_code = f'{number}.{exchange}'

        # 过滤北交所
        if exchange == 'BJ':
            continue

        # 转换 trade_time: 2024/07/01 09:30 → timestamp
        df['trade_time'] = pd.to_datetime(df['trade_time'])
        df['ts_code'] = ts_code

        # 转换为 DB 格式
        # price: ZIP 元 → DB 厘 (×1000)
        df['open_li'] = (df['open'] * 1000).round().astype(int)
        df['high_li'] = (df['high'] * 1000).round().astype(int)
        df['low_li'] = (df['low'] * 1000).round().astype(int)
        df['close_li'] = (df['close'] * 1000).round().astype(int)
        # volume: ZIP 已经是手，直接使用
        df['volume_hand'] = df['volume'].astype(int)
        # amount: ZIP 元 → DB 厘 (×1000)
        df['amount_li'] = (df['amount'] * 1000).round().astype(int)

        for t, o, h, l, c, v, a in zip(
            df['trade_time'], df['open_li'], df['high_li'], df['low_li'],
            df['close_li'], df['volume_hand'], df['amount_li']
        ):
            all_rows.append((t, ts_code, '1m', o, h, l, c, v, a, 'none', 'tdx_vipdoc'))

    z.close()
    return all_rows


def load_from_tushare_cache(date_str):
    """从 Tushare 缓存 parquet 加载"""
    cache_path = os.path.join(TUSHARE_CACHE_DIR, f'{date_str}_1min.parquet')
    if not os.path.exists(cache_path):
        return None

    df = pd.read_parquet(cache_path)
    if df.empty:
        return None

    # 过滤北交所
    df = df[~df['ts_code'].str.endswith('.BJ')]

    all_rows = []
    for _, row in df.iterrows():
        trade_time = pd.to_datetime(row['trade_time'])
        ts_code = row['ts_code']
        # Tushare stk_mins: close/open/high/low 是元, vol 是股, amount 是元
        open_li = int(round(row['open'] * 1000))
        high_li = int(round(row['high'] * 1000))
        low_li = int(round(row['low'] * 1000))
        close_li = int(round(row['close'] * 1000))
        volume_hand = int(round(row['vol'] / 100))
        amount_li = int(round(row['amount'] * 1000))

        all_rows.append((
            trade_time, ts_code, '1m',
            open_li, high_li, low_li, close_li,
            volume_hand, amount_li, 'none', 'tdx_vipdoc'
        ))

    return all_rows


def insert_batch(conn, rows):
    """批量插入"""
    if not rows:
        return 0
    cur = conn.cursor()
    sql = """
        INSERT INTO market.kline_minute_raw
        (trade_time, ts_code, freq, open_li, high_li, low_li, close_li,
         volume_hand, amount_li, adjust_type, source)
        VALUES %s
        ON CONFLICT DO NOTHING
    """
    psycopg2.extras.execute_values(cur, sql, rows, page_size=BATCH_SIZE)
    conn.commit()
    return len(rows)


def main():
    conn = psycopg2.connect(**DB_CONFIG)
    missing = get_missing_dates(conn)
    print(f'DB 缺失交易日: {len(missing)} 天')

    total_inserted = 0
    zip_days = 0
    tushare_days = 0
    failed_days = []

    for i, d in enumerate(missing):
        print(f'[{i+1}/{len(missing)}] {d}: ', end='', flush=True)

        # 优先尝试 ZIP
        rows = load_from_zip(d)
        source = 'ZIP'
        if rows is None:
            # 尝试 Tushare 缓存
            rows = load_from_tushare_cache(d)
            source = 'Tushare'

        if rows is None or len(rows) == 0:
            print('无数据源')
            failed_days.append(d)
            continue

        n_stocks = len(set(r[1] for r in rows))
        n_rows = len(rows)

        # 检查数据量是否合理（至少 100 只股票 × 12 条以上）
        if n_stocks < 100:
            print(f'{source} 仅 {n_stocks} 只股票，跳过')
            failed_days.append(d)
            continue

        inserted = insert_batch(conn, rows)
        total_inserted += inserted

        if source == 'ZIP':
            zip_days += 1
        else:
            tushare_days += 1

        print(f'{source} → {n_rows:,} 行, {n_stocks} 只股票')

    conn.close()

    print(f'\n=== 导入完成 ===')
    print(f'  总导入: {total_inserted:,} 行')
    print(f'  ZIP 导入: {zip_days} 天')
    print(f'  Tushare 导入: {tushare_days} 天')
    print(f'  失败/跳过: {len(failed_days)} 天')
    if failed_days:
        print(f'  失败日期: {failed_days}')


if __name__ == '__main__':
    main()
