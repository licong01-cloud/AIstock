"""将真实涨跌停价格写入分钟级 Qlib bin 格式。

数据来源：market.stk_limit（Tushare 每日涨跌停价格表）
  - pre_close: 昨日收盘价（元）
  - up_limit:  今日涨停价（元）
  - down_limit: 今日跌停价（元）

逻辑：日级价格展开到分钟级（同一天内所有分钟 bar 值相同）。
对齐现有 close.1min.bin 的 start_index。

每只股票生成三个文件：
  {features}/{stock}/prev_close.1min.bin
  {features}/{stock}/up_limit_price.1min.bin
  {features}/{stock}/down_limit_price.1min.bin

用法：
  python dump_limit_price_minute_bins.py [--qlib_dir /path/to/qlib_minute_bin] [--workers 4]
"""

import argparse
import os
import struct
import time
import gc
from pathlib import Path

import numpy as np
import pandas as pd
import psycopg2


DB_CONFIG = dict(
    host=os.environ.get('AISTOCK_DB_HOST', '127.0.0.1'),
    port=5432, dbname='aistock',
    user='postgres', password='lc78080808',
)


def load_calendar(qlib_dir: Path) -> pd.DatetimeIndex:
    cal_path = qlib_dir / 'calendars' / '1min.txt'
    cal = pd.to_datetime(pd.read_csv(cal_path, header=None)[0])
    return pd.DatetimeIndex(cal)


def load_instruments(qlib_dir: Path) -> list:
    inst_path = qlib_dir / 'instruments' / 'all.txt'
    stocks = set()
    with open(inst_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if parts:
                stocks.add(parts[0].upper())
    return sorted(stocks)


def read_bin_start_index(bin_path: Path) -> int:
    with open(bin_path, 'rb') as f:
        return int(struct.unpack('<f', f.read(4))[0])


def write_bin(path: Path, values: np.ndarray, start_index: int):
    with open(path, 'wb') as f:
        f.write(struct.pack('<f', float(start_index)))
        f.write(values.astype('<f').tobytes())


def get_stock_dir(features_dir: Path, stock_code: str) -> Path:
    parts = stock_code.split('.')
    return features_dir / f"{parts[0].lower()}.{parts[1].lower()}"


def process_stock(stock_code, limit_data, calendar, features_dir):
    """处理单只股票：将日级价格展开到分钟级 bin。

    limit_data: DataFrame with columns [trade_date, pre_close, up_limit, down_limit]
    """
    stock_dir = get_stock_dir(features_dir, stock_code)
    if not stock_dir.exists():
        return stock_code, 'skip_no_dir'

    close_bin = stock_dir / 'close.1min.bin'
    if not close_bin.exists():
        return stock_code, 'skip_no_close'

    start_idx = read_bin_start_index(close_bin)

    if limit_data is None or len(limit_data) == 0:
        return stock_code, 'skip_no_data'

    # 准备 limit 数据
    limit_data = limit_data.copy()
    limit_data['trade_date'] = pd.to_datetime(limit_data['trade_date'])
    for col in ['pre_close', 'up_limit', 'down_limit']:
        limit_data[col] = pd.to_numeric(limit_data[col], errors='coerce').astype(np.float64)
    limit_data = limit_data.set_index('trade_date')

    # 对齐 calendar
    cal_slice = calendar[start_idx:]
    pc_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    up_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    dn_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)

    # 每个分钟 bar 的日期 → 关联当天的 limit 价格
    cal_dates = cal_slice.normalize()
    # 构建日期到值的映射
    for trade_date, row in limit_data.iterrows():
        mask = cal_dates == trade_date
        if mask.any():
            if not np.isnan(row['pre_close']):
                pc_series.loc[mask] = np.float32(row['pre_close'])
            if not np.isnan(row['up_limit']):
                up_series.loc[mask] = np.float32(row['up_limit'])
            if not np.isnan(row['down_limit']):
                dn_series.loc[mask] = np.float32(row['down_limit'])

    write_bin(stock_dir / 'prev_close.1min.bin', pc_series.values, start_idx)
    write_bin(stock_dir / 'up_limit_price.1min.bin', up_series.values, start_idx)
    write_bin(stock_dir / 'down_limit_price.1min.bin', dn_series.values, start_idx)

    n_filled = int((~np.isnan(pc_series.values)).sum())
    return stock_code, 'ok', n_filled


def main():
    parser = argparse.ArgumentParser(description='导出真实涨跌停价格到分钟级 Qlib bin')
    parser.add_argument('--qlib_dir', default='/home/lc999/data/qlib_minute_bin',
                        help='Qlib minute bin directory')
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()

    qlib_dir = Path(args.qlib_dir)
    features_dir = qlib_dir / 'features'
    assert qlib_dir.exists(), f'{qlib_dir} not found'

    t0 = time.time()

    calendar = load_calendar(qlib_dir)
    print(f'Calendar: {len(calendar)} bars, {calendar[0]} ~ {calendar[-1]}')

    instruments = load_instruments(qlib_dir)
    print(f'Instruments: {len(instruments)} stocks')

    # 加载全量 stk_limit
    print('Loading stk_limit (pre_close, up_limit, down_limit) from database...')
    conn = psycopg2.connect(**DB_CONFIG)
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute("""
        SELECT ts_code, trade_date, pre_close, up_limit, down_limit
        FROM market.stk_limit
    """)
    rows = cur.fetchall()
    limit_df = pd.DataFrame(rows, columns=['ts_code', 'trade_date', 'pre_close', 'up_limit', 'down_limit'])
    conn.close()
    print(f'  stk_limit: {len(limit_df):,} rows')

    # 按股票分组
    limit_by_stock = {
        code: grp[['trade_date', 'pre_close', 'up_limit', 'down_limit']]
        for code, grp in limit_df.groupby('ts_code')
    }
    del limit_df, rows
    gc.collect()

    # 处理每只股票
    ok_count = 0
    skip_counts = {}
    total_filled = 0

    for i, stock in enumerate(instruments):
        limit_data = limit_by_stock.get(stock)
        result = process_stock(stock, limit_data, calendar, features_dir)
        if result[1] == 'ok':
            ok_count += 1
            total_filled += result[2]
        else:
            skip_counts[result[1]] = skip_counts.get(result[1], 0) + 1

        if (i + 1) % 500 == 0 or (i + 1) == len(instruments):
            elapsed = time.time() - t0
            print(f'  Progress: {i+1}/{len(instruments)} (ok={ok_count}, '
                  f'filled_bars={total_filled:,}, {elapsed:.0f}s)')

    elapsed = time.time() - t0
    total_skip = sum(skip_counts.values())
    print(f'\nDone in {elapsed:.0f}s ({elapsed/60:.1f} min)')
    print(f'  ok={ok_count}, skipped={total_skip}')
    for reason, cnt in sorted(skip_counts.items()):
        print(f'    {reason}: {cnt}')
    print(f'  Total filled minute bars: {total_filled:,}')
    print(f'  Files written to: {features_dir}')
    print(f'  Total bin files: {ok_count * 3} (prev_close + up_limit_price + down_limit_price)')


if __name__ == '__main__':
    main()
