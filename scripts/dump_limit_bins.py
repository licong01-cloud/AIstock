"""
将涨跌停**布尔标记**写入 Qlib bin 格式。

核心逻辑（基于收盘价判断）：
  涨停禁买（limit_up=1）条件：close >= up_limit（收盘价达到涨停价）
  跌停禁卖（limit_down=1）条件：close <= down_limit（收盘价达到跌停价）

  用不复权价格（kline_daily_raw）与不复权涨跌停价格比较，避免复权不匹配。

每只股票生成两个文件：
  {qlib_bin}/features/{stock_lower}/limit_up.day.bin   -- 1.0=涨停(不可买入), 0.0=正常, NaN=无数据
  {qlib_bin}/features/{stock_lower}/limit_down.day.bin -- 1.0=跌停(不可卖出), 0.0=正常, NaN=无数据

Qlib bin 格式：
  [4 bytes float32: start_index] [float32 x N: daily values]

用法：
  python dump_limit_bins.py --qlib_dir /path/to/qlib_bin [--test] [--workers 8]
"""

import argparse
import os
import struct
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import psycopg2


DB_CONFIG = dict(host='127.0.0.1', port=5432, dbname='aistock',
                 user='postgres', password='lc78080808')

OLD_FILES = ['up_limit_price.day.bin', 'down_limit_price.day.bin',
             'limit_buy.day.bin', 'limit_sell.day.bin']


def load_calendar(qlib_dir: Path) -> pd.DatetimeIndex:
    cal_path = qlib_dir / 'calendars' / 'day.txt'
    dates = pd.to_datetime(pd.read_csv(cal_path, header=None)[0])
    return pd.DatetimeIndex(dates)


def load_instruments(qlib_dir: Path) -> list[str]:
    """从 instruments/all.txt 读取股票池，确保与 bin 完全匹配。"""
    inst_path = qlib_dir / 'instruments' / 'all.txt'
    stocks = set()
    with open(inst_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if parts:
                stocks.add(parts[0].upper())
    return sorted(stocks)


def load_limit_and_ohlc(conn, instruments: list[str]) -> pd.DataFrame:
    """JOIN kline_daily_raw (close) + stk_limit (up_limit/down_limit)，
    仅加载 instruments 中的股票。"""
    print('Loading stk_limit + kline_daily_raw (OHLC) from database...')
    in_clause = ','.join(f"'{s}'" for s in instruments)
    sql = f"""
    SELECT
        sl.ts_code,
        sl.trade_date,
        sl.up_limit,
        sl.down_limit,
        k.open_li,
        k.high_li,
        k.low_li,
        k.close_li
    FROM market.stk_limit sl
    LEFT JOIN market.kline_daily_raw k
        ON sl.ts_code = k.ts_code AND sl.trade_date = k.trade_date
    WHERE sl.ts_code IN ({in_clause})
    """
    df = pd.read_sql(sql, conn)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    df = df.set_index(['ts_code', 'trade_date']).sort_index()
    n_stocks = df.index.get_level_values(0).nunique()
    print(f'  Loaded {len(df):,} rows, {n_stocks} stocks (instruments: {len(instruments)})')
    return df


def write_bin(path: Path, values: np.ndarray, start_index: int):
    """写入 Qlib bin 文件：[start_index float32] + [values float32...]"""
    with open(path, 'wb') as f:
        f.write(struct.pack('<f', float(start_index)))
        f.write(values.astype('<f').tobytes())


def process_stock(args):
    stock_code, df_stock, calendar, features_dir = args

    # Qlib 目录格式：sz000001 或 sh600000 — 需要检查实际目录名
    parts = stock_code.split('.')
    if len(parts) == 2:
        stock_lower = parts[0].lower() + '.' + parts[1].lower()
    else:
        stock_lower = stock_code.lower()

    stock_dir = features_dir / stock_lower
    if not stock_dir.exists():
        return stock_code, 'skip_no_dir'

    # 清理旧文件
    for old_name in OLD_FILES:
        old_path = stock_dir / old_name
        if old_path.exists():
            old_path.unlink()

    # 读取该股票已有的 close.day.bin 的 start_index 来对齐
    close_bin = stock_dir / 'close.day.bin'
    if not close_bin.exists():
        return stock_code, 'skip_no_close'

    with open(close_bin, 'rb') as f:
        close_start_idx = int(struct.unpack('<f', f.read(4))[0])

    # close_li 是厘（元 × 1000），up_limit/down_limit 是元
    stock_dates = df_stock.index
    common_dates = stock_dates.intersection(calendar)

    if len(common_dates) == 0:
        return stock_code, 'skip_no_data'

    df_common = df_stock.loc[common_dates].copy()

    # 转元（仅需 close 和 limit 价格）
    close_yuan = df_common['close_li'].astype(float) / 1000.0
    up_limit = df_common['up_limit'].astype(float)
    down_limit = df_common['down_limit'].astype(float)

    # 涨停：收盘价达到涨停价（close >= up_limit）
    is_limit_up = (close_yuan >= up_limit).astype(float)

    # 跌停：收盘价达到跌停价（close <= down_limit）
    is_limit_down = (close_yuan <= down_limit).astype(float)

    # OHLC 或 limit 价格为 NaN 时，布尔值也应为 NaN
    ohlc_nan = (df_common['close_li'].isna() | df_common['open_li'].isna() |
                df_common['high_li'].isna() | df_common['low_li'].isna())
    is_limit_up[ohlc_nan | df_common['up_limit'].isna()] = np.nan
    is_limit_down[ohlc_nan | df_common['down_limit'].isna()] = np.nan

    # 对齐日历：从 close_start_idx 开始到日历末尾
    cal_slice = calendar[close_start_idx:]
    up_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    down_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)

    # 填充有数据的日期
    valid_dates = common_dates.intersection(cal_slice)
    up_series.loc[valid_dates] = is_limit_up.loc[valid_dates].values
    down_series.loc[valid_dates] = is_limit_down.loc[valid_dates].values

    write_bin(stock_dir / 'limit_up.day.bin', up_series.values, close_start_idx)
    write_bin(stock_dir / 'limit_down.day.bin', down_series.values, close_start_idx)

    return stock_code, 'ok'


def main():
    parser = argparse.ArgumentParser(description='导出涨跌停布尔标记到 Qlib bin')
    parser.add_argument('--qlib_dir', default=r'F:\Dev\AIstock\qlib_bin\qlib_bin_20260311',
                        help='Qlib bin directory')
    parser.add_argument('--test', action='store_true',
                        help='Only process first 50 stocks for testing')
    parser.add_argument('--workers', type=int, default=4)
    args = parser.parse_args()

    qlib_dir = Path(args.qlib_dir)
    features_dir = qlib_dir / 'features'
    assert qlib_dir.exists(), f'{qlib_dir} not found'

    # 加载日历
    calendar = load_calendar(qlib_dir)
    print(f'Calendar: {len(calendar)} days, {calendar[0].date()} ~ {calendar[-1].date()}')

    # 从 instruments/all.txt 读取股票池
    instruments = load_instruments(qlib_dir)
    print(f'Instruments from all.txt: {len(instruments)} stocks')

    # 加载数据（仅 instruments 中的股票）
    conn = psycopg2.connect(**DB_CONFIG)
    df_all = load_limit_and_ohlc(conn, instruments)
    conn.close()

    # 准备任务（以 instruments 为基准）
    stocks_in_db = set(df_all.index.get_level_values(0).unique())
    tasks = []
    no_data_count = 0
    for stock in instruments:
        if stock in stocks_in_db:
            df_stock = df_all.xs(stock, level=0)
            tasks.append((stock, df_stock, calendar, features_dir))
        else:
            no_data_count += 1

    if args.test:
        tasks = tasks[:50]
        print(f'TEST MODE: processing {len(tasks)} stocks')
    else:
        print(f'Processing {len(tasks)} stocks (no DB data: {no_data_count})')

    # 并行处理
    ok_count = 0
    skip_counts = {}
    with ProcessPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(process_stock, t): t[0] for t in tasks}
        for i, future in enumerate(as_completed(futures)):
            stock_code, status = future.result()
            if status == 'ok':
                ok_count += 1
            else:
                skip_counts[status] = skip_counts.get(status, 0) + 1
            if (i + 1) % 500 == 0:
                print(f'  Progress: {i+1}/{len(tasks)} (ok={ok_count})')

    total_skip = sum(skip_counts.values())
    print(f'\nDone! ok={ok_count}, skipped={total_skip}')
    for reason, cnt in sorted(skip_counts.items()):
        print(f'  {reason}: {cnt}')
    print(f'Files written to: {features_dir}')
    print(f'Total bin files: {ok_count * 2} (limit_up + limit_down)')
    print()
    print('Next steps:')
    print('  1. WSL test: python test_limit_bins.py')
    print('  2. Qlib usage: D.features(["SZ000001"], ["$limit_up", "$limit_down"])')


if __name__ == '__main__':
    main()
