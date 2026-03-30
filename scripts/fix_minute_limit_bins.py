"""只更新分钟线 limit_up/limit_down bin 文件，不重新导出其他字段。

逻辑：
  1. 读取现有 bin 的 calendar 和 instruments
  2. 从 DB 读取 kline_minute_raw (close_li) + stk_limit (up_limit/down_limit)
  3. 计算 limit_up = (close_li/1000 >= up_limit), limit_down = (close_li/1000 <= down_limit)
  4. 按现有 bin 的 start_index 对齐写入
"""
import os, struct, time, gc
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import psycopg2

DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')

QLIB_DIR = Path('/home/lc999/data/qlib_minute_bin')
FEATURES_DIR = QLIB_DIR / 'features'
BATCH_SIZE = 100  # DB query batch


def load_calendar():
    cal_path = QLIB_DIR / 'calendars' / '1min.txt'
    cal = pd.to_datetime(pd.read_csv(cal_path, header=None)[0])
    return pd.DatetimeIndex(cal)


def load_instruments():
    inst_path = QLIB_DIR / 'instruments' / 'all.txt'
    stocks = set()
    with open(inst_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if parts:
                stocks.add(parts[0].upper())
    return sorted(stocks)


def read_bin_start_index(bin_path):
    with open(bin_path, 'rb') as f:
        return int(struct.unpack('<f', f.read(4))[0])


def write_bin(path, values, start_index):
    with open(path, 'wb') as f:
        f.write(struct.pack('<f', float(start_index)))
        f.write(values.astype('<f').tobytes())


def get_stock_dir(stock_code):
    parts = stock_code.split('.')
    return FEATURES_DIR / f"{parts[0].lower()}.{parts[1].lower()}"


def process_stock(stock_code, close_data, limit_data, calendar):
    """处理单只股票：计算 limit_up/limit_down 并写入 bin。

    close_data: DataFrame with columns [trade_time, close_li]
    limit_data: DataFrame with columns [trade_date, up_limit, down_limit]
    """
    stock_dir = get_stock_dir(stock_code)
    if not stock_dir.exists():
        return stock_code, 'skip_no_dir'

    close_bin = stock_dir / 'close.1min.bin'
    if not close_bin.exists():
        return stock_code, 'skip_no_close'

    start_idx = read_bin_start_index(close_bin)

    if close_data is None or len(close_data) == 0:
        return stock_code, 'skip_no_data'

    # 准备数据
    close_data = close_data.copy()
    close_data['trade_time'] = pd.to_datetime(close_data['trade_time'], utc=True)
    # 转为无时区的本地时间（与 calendar 匹配）
    close_data['trade_time'] = close_data['trade_time'].dt.tz_convert('Asia/Shanghai').dt.tz_localize(None)
    close_data = close_data.set_index('trade_time')
    close_data['close_yuan'] = close_data['close_li'].astype(float) / 1000.0

    # limit_data: per-day up_limit/down_limit
    limit_data = limit_data.copy()
    limit_data['trade_date'] = pd.to_datetime(limit_data['trade_date'])
    limit_data['up_limit'] = pd.to_numeric(limit_data['up_limit'], errors='coerce').astype(np.float64)
    limit_data['down_limit'] = pd.to_numeric(limit_data['down_limit'], errors='coerce').astype(np.float64)
    limit_data = limit_data.set_index('trade_date')

    # 给每个分钟 bar 关联当天的 limit
    close_data['date'] = close_data.index.normalize()
    close_data = close_data.join(limit_data[['up_limit', 'down_limit']], on='date', how='left')

    # 计算 limit_up/limit_down（基于收盘价）
    close_data['limit_up'] = (close_data['close_yuan'] >= close_data['up_limit']).astype(np.float32)
    close_data['limit_down'] = (close_data['close_yuan'] <= close_data['down_limit']).astype(np.float32)
    # NaN limit → 0（允许交易）
    close_data.loc[close_data['up_limit'].isna(), 'limit_up'] = 0.0
    close_data.loc[close_data['down_limit'].isna(), 'limit_down'] = 0.0

    # 对齐 calendar
    cal_slice = calendar[start_idx:]
    up_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    down_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)

    common = close_data.index.intersection(cal_slice)
    if len(common) > 0:
        up_series.loc[common] = close_data.loc[common, 'limit_up'].values
        down_series.loc[common] = close_data.loc[common, 'limit_down'].values

    write_bin(stock_dir / 'limit_up.1min.bin', up_series.values, start_idx)
    write_bin(stock_dir / 'limit_down.1min.bin', down_series.values, start_idx)

    n_lu = int((up_series == 1.0).sum())
    n_ld = int((down_series == 1.0).sum())
    return stock_code, 'ok', n_lu, n_ld


def main():
    t0 = time.time()

    calendar = load_calendar()
    print(f"Calendar: {len(calendar)} bars, {calendar[0]} ~ {calendar[-1]}")

    instruments = load_instruments()
    print(f"Instruments: {len(instruments)} stocks")

    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # 加载全量 stk_limit（一次性）
    print("Loading stk_limit...")
    cur.execute("""
        SELECT ts_code, trade_date, up_limit, down_limit
        FROM market.stk_limit
    """)
    limit_rows = cur.fetchall()
    limit_df = pd.DataFrame(limit_rows, columns=['ts_code', 'trade_date', 'up_limit', 'down_limit'])
    print(f"  stk_limit: {len(limit_df):,} rows")

    # 按股票分组
    limit_by_stock = {code: grp[['trade_date', 'up_limit', 'down_limit']]
                      for code, grp in limit_df.groupby('ts_code')}
    del limit_df
    gc.collect()

    # 分批查询分钟线 close_li 并处理
    ok_count = 0
    skip_counts = {}
    total_lu = total_ld = 0

    for batch_idx in range(0, len(instruments), BATCH_SIZE):
        batch_codes = instruments[batch_idx:batch_idx + BATCH_SIZE]

        # 查询这批股票的分钟线 close
        cur.execute("""
            SELECT ts_code, trade_time, close_li
            FROM market.kline_minute_raw
            WHERE ts_code = ANY(%s) AND freq = '1m'
              AND trade_time >= '2024-01-01 00:00:00+08'
              AND trade_time <= '2026-03-20 00:00:00+08'
            ORDER BY ts_code, trade_time
        """, (batch_codes,))
        rows = cur.fetchall()
        if rows:
            close_df = pd.DataFrame(rows, columns=['ts_code', 'trade_time', 'close_li'])
            close_by_stock = {code: grp[['trade_time', 'close_li']]
                              for code, grp in close_df.groupby('ts_code')}
            del close_df
        else:
            close_by_stock = {}

        # 处理每只股票
        for stock in batch_codes:
            close_data = close_by_stock.get(stock)
            limit_data = limit_by_stock.get(stock)
            if limit_data is None:
                limit_data = pd.DataFrame(columns=['trade_date', 'up_limit', 'down_limit'])

            result = process_stock(stock, close_data, limit_data, calendar)
            if result[1] == 'ok':
                ok_count += 1
                total_lu += result[2]
                total_ld += result[3]
            else:
                skip_counts[result[1]] = skip_counts.get(result[1], 0) + 1

        del close_by_stock, rows
        gc.collect()

        processed = min(batch_idx + BATCH_SIZE, len(instruments))
        if processed % 500 == 0 or processed == len(instruments):
            elapsed = time.time() - t0
            print(f"  Progress: {processed}/{len(instruments)} (ok={ok_count}, "
                  f"lu_bars={total_lu:,}, ld_bars={total_ld:,}, {elapsed:.0f}s)")

    conn.close()

    elapsed = time.time() - t0
    total_skip = sum(skip_counts.values())
    print(f"\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min)")
    print(f"  ok={ok_count}, skipped={total_skip}")
    for reason, cnt in sorted(skip_counts.items()):
        print(f"    {reason}: {cnt}")
    print(f"  Total limit_up bars: {total_lu:,}")
    print(f"  Total limit_down bars: {total_ld:,}")
    print(f"  Files updated in: {FEATURES_DIR}")


if __name__ == '__main__':
    main()
