"""
将真实涨跌停价格写入 Qlib bin 格式。

数据来源：market.stk_limit（Tushare 每日涨跌停价格表）
  - pre_close: 昨日收盘价（元）
  - up_limit:  今日涨停价（元）
  - down_limit: 今日跌停价（元）

注意：stk_limit 价格单位为元，与 kline_daily_raw 的厘(×1000)不同。
      本脚本输出的 bin 文件价格单位统一为元（与前复权价格一致）。

每只股票生成三个文件：
  {qlib_bin}/features/{stock_lower}/prev_close.day.bin     -- 昨日收盘价(元), NaN=无数据
  {qlib_bin}/features/{stock_lower}/up_limit_price.day.bin -- 今日涨停价(元), NaN=无数据
  {qlib_bin}/features/{stock_lower}/down_limit_price.day.bin -- 今日跌停价(元), NaN=无数据

Qlib bin 格式：
  [4 bytes float32: start_index] [float32 x N: daily values]

已知数据问题：
  - 2024-07-23 全市场 pre_close 为 NULL（Tushare 采集问题），保留为 NaN，不做静默填充
  - up_limit / down_limit 无 NULL

用法：
  python dump_limit_price_bins.py --qlib_dir /path/to/qlib_bin [--test] [--workers 8]
"""

import argparse
import struct
from pathlib import Path
from concurrent.futures import ProcessPoolExecutor, as_completed

import numpy as np
import pandas as pd
import psycopg2


DB_CONFIG = dict(
    host='127.0.0.1', port=5432, dbname='aistock',
    user='postgres', password='lc78080808',
)


def load_calendar(qlib_dir: Path) -> pd.DatetimeIndex:
    cal_path = qlib_dir / 'calendars' / 'day.txt'
    dates = pd.to_datetime(pd.read_csv(cal_path, header=None)[0])
    return pd.DatetimeIndex(dates)


def load_instruments(qlib_dir: Path) -> list[str]:
    """从 instruments/all.txt 读取股票池。"""
    inst_path = qlib_dir / 'instruments' / 'all.txt'
    stocks = set()
    with open(inst_path, 'r') as f:
        for line in f:
            parts = line.strip().split('\t')
            if parts:
                stocks.add(parts[0].upper())
    return sorted(stocks)


def load_stk_limit(conn, instruments: list[str]) -> pd.DataFrame:
    """从 market.stk_limit 加载真实涨跌停价格。"""
    print('Loading stk_limit (pre_close, up_limit, down_limit) from database...')
    in_clause = ','.join(f"'{s}'" for s in instruments)
    sql = f"""
    SELECT ts_code, trade_date, pre_close, up_limit, down_limit
    FROM market.stk_limit
    WHERE ts_code IN ({in_clause})
    """
    df = pd.read_sql(sql, conn)
    df['trade_date'] = pd.to_datetime(df['trade_date'])
    # 强制转 float（数据库为 numeric/Decimal）
    for col in ['pre_close', 'up_limit', 'down_limit']:
        df[col] = pd.to_numeric(df[col], errors='coerce').astype('float64')
    df = df.set_index(['ts_code', 'trade_date']).sort_index()
    n_stocks = df.index.get_level_values(0).nunique()
    n_null_pre = df['pre_close'].isna().sum()
    print(f'  Loaded {len(df):,} rows, {n_stocks} stocks')
    print(f'  pre_close NULL: {n_null_pre} (will be NaN in bin)')
    return df


def write_bin(path: Path, values: np.ndarray, start_index: int):
    """写入 Qlib bin 文件：[start_index float32] + [values float32...]"""
    with open(path, 'wb') as f:
        f.write(struct.pack('<f', float(start_index)))
        f.write(values.astype('<f').tobytes())


def process_stock(args):
    stock_code, df_stock, calendar, features_dir = args

    # Qlib 目录格式：000001.sz
    parts = stock_code.split('.')
    if len(parts) == 2:
        stock_lower = parts[0].lower() + '.' + parts[1].lower()
    else:
        stock_lower = stock_code.lower()

    stock_dir = features_dir / stock_lower
    if not stock_dir.exists():
        return stock_code, 'skip_no_dir'

    # 读取该股票已有的 close.day.bin 的 start_index 来对齐
    close_bin = stock_dir / 'close.day.bin'
    if not close_bin.exists():
        return stock_code, 'skip_no_close'

    with open(close_bin, 'rb') as f:
        close_start_idx = int(struct.unpack('<f', f.read(4))[0])

    stock_dates = df_stock.index
    common_dates = stock_dates.intersection(calendar)

    if len(common_dates) == 0:
        return stock_code, 'skip_no_data'

    df_common = df_stock.loc[common_dates]

    # 对齐日历：从 close_start_idx 开始到日历末尾
    cal_slice = calendar[close_start_idx:]
    prev_close_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    up_limit_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)
    down_limit_series = pd.Series(np.nan, index=cal_slice, dtype=np.float32)

    # 填充有数据的日期
    valid_dates = common_dates.intersection(cal_slice)
    prev_close_series.loc[valid_dates] = df_common.loc[valid_dates, 'pre_close'].values.astype(np.float32)
    up_limit_series.loc[valid_dates] = df_common.loc[valid_dates, 'up_limit'].values.astype(np.float32)
    down_limit_series.loc[valid_dates] = df_common.loc[valid_dates, 'down_limit'].values.astype(np.float32)

    write_bin(stock_dir / 'prev_close.day.bin', prev_close_series.values, close_start_idx)
    write_bin(stock_dir / 'up_limit_price.day.bin', up_limit_series.values, close_start_idx)
    write_bin(stock_dir / 'down_limit_price.day.bin', down_limit_series.values, close_start_idx)

    return stock_code, 'ok'


def main():
    parser = argparse.ArgumentParser(description='导出真实涨跌停价格到 Qlib bin')
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

    # 加载数据
    conn = psycopg2.connect(**DB_CONFIG)
    df_all = load_stk_limit(conn, instruments)
    conn.close()

    # 准备任务
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
    print(f'Total bin files: {ok_count * 3} (prev_close + up_limit_price + down_limit_price)')
    print()
    print('Qlib usage:')
    print('  D.features(["SZ000001"], ["$prev_close", "$up_limit_price", "$down_limit_price"])')


if __name__ == '__main__':
    main()
