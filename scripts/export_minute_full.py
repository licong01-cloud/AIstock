"""全量分钟线导出 → bin → Qlib 回测
Step 1: 直接从 DB 导出 CSV（绕过 db_reader，避免全量 JOIN 太慢）
"""
import os, sys, time, gc
os.environ.setdefault('TDX_DB_PASSWORD', 'lc78080808')

import psycopg2
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import date

# ========== 配置 ==========
# Alpha158 需要 ~60 天历史窗口，导出 2025-11 ~ 2026-02
START = '2025-11-01'
END = '2026-02-28'
OUTPUT_DIR = Path('F:/Dev/AIstock/qlib_minute_full')
CSV_DIR = OUTPUT_DIR / 'csv'
BIN_DIR = OUTPUT_DIR / 'bin'

CSV_DIR.mkdir(parents=True, exist_ok=True)
BIN_DIR.mkdir(parents=True, exist_ok=True)

conn = psycopg2.connect(host='127.0.0.1', port=5432, dbname='aistock',
                        user='postgres', password='lc78080808')
conn.set_session(autocommit=True)
cur = conn.cursor()

# ========== Step 1: 获取股票列表 ==========
print('=== Step 1: 获取股票列表 ===')
cur.execute("""
    SELECT DISTINCT ts_code FROM market.kline_minute_raw
    WHERE trade_time >= %s AND trade_time < %s AND freq='1m'
""", (f'{START} 00:00:00+08', f'2026-03-01 00:00:00+08'))
all_codes = sorted([r[0] for r in cur.fetchall()])
print(f'  股票数: {len(all_codes)}')

# ========== Step 2: 加载 stk_limit（一次性） ==========
print('=== Step 2: 加载 stk_limit ===')
cur.execute("""
    SELECT ts_code, trade_date, up_limit, down_limit
    FROM market.stk_limit
    WHERE trade_date >= %s AND trade_date <= %s
""", (START, END))
limit_rows = cur.fetchall()
limit_df = pd.DataFrame(limit_rows, columns=['ts_code', 'trade_date', 'up_limit', 'down_limit'])
limit_df['trade_date'] = pd.to_datetime(limit_df['trade_date']).dt.date
print(f'  stk_limit 行数: {len(limit_df):,}')

# ========== Step 3: 加载复权因子（一次性） ==========
print('=== Step 3: 加载复权因子 ===')
cur.execute("""
    SELECT ts_code, trade_date, adj_factor
    FROM market.adj_factor
    WHERE trade_date >= %s AND trade_date <= %s
""", (START, END))
adj_rows = cur.fetchall()
adj_df = pd.DataFrame(adj_rows, columns=['ts_code', 'trade_date', 'adj_factor'])
adj_df['trade_date'] = pd.to_datetime(adj_df['trade_date']).dt.date
# 计算前复权因子: qfq = adj_factor / max(adj_factor per stock)
max_adj = adj_df.groupby('ts_code')['adj_factor'].transform('max')
adj_df['qfq_factor'] = adj_df['adj_factor'] / max_adj
print(f'  adj_factor 行数: {len(adj_df):,}')

# ========== Step 4: 分批导出 CSV ==========
print('=== Step 4: 分批导出 CSV ===')
BATCH_SIZE = 100  # 每批 100 只股票
total_rows = 0
t0 = time.time()

for batch_idx in range(0, len(all_codes), BATCH_SIZE):
    batch_codes = all_codes[batch_idx:batch_idx + BATCH_SIZE]

    # 读取分钟线
    cur.execute("""
        SELECT trade_time, ts_code, open_li, high_li, low_li, close_li,
               volume_hand, amount_li
        FROM market.kline_minute_raw
        WHERE ts_code = ANY(%s) AND freq='1m'
          AND trade_time >= %s AND trade_time < %s
        ORDER BY ts_code, trade_time
    """, (batch_codes, f'{START} 00:00:00+08', '2026-03-01 00:00:00+08'))
    rows = cur.fetchall()
    if not rows:
        continue

    df = pd.DataFrame(rows, columns=['trade_time', 'ts_code', 'open_li', 'high_li',
                                     'low_li', 'close_li', 'volume_hand', 'amount_li'])
    df['trade_date'] = pd.to_datetime(df['trade_time']).dt.date

    # 合并复权因子
    df = df.merge(adj_df[['ts_code', 'trade_date', 'qfq_factor']],
                  on=['ts_code', 'trade_date'], how='left')
    # 缺失复权因子用 1.0
    df['qfq_factor'] = df['qfq_factor'].fillna(1.0)

    # 合并涨跌停价
    df = df.merge(limit_df[['ts_code', 'trade_date', 'up_limit', 'down_limit']],
                  on=['ts_code', 'trade_date'], how='left')

    # 计算各字段
    SCALE = 1000.0
    df['open']   = (df['open_li']  / SCALE * df['qfq_factor']).astype(np.float32)
    df['high']   = (df['high_li']  / SCALE * df['qfq_factor']).astype(np.float32)
    df['low']    = (df['low_li']   / SCALE * df['qfq_factor']).astype(np.float32)
    df['close']  = (df['close_li'] / SCALE * df['qfq_factor']).astype(np.float32)
    df['volume'] = (df['volume_hand'] * 100.0 / df['qfq_factor']).astype(np.float32)
    df['amount'] = (df['amount_li'] / SCALE).astype(np.float32)
    df['factor'] = df['qfq_factor'].astype(np.float32)

    # 涨跌停标志
    open_yuan  = df['open_li']  / SCALE
    high_yuan  = df['high_li']  / SCALE
    low_yuan   = df['low_li']   / SCALE
    close_yuan = df['close_li'] / SCALE
    df['limit_up'] = ((close_yuan >= df['up_limit']) &
                      (open_yuan  >= df['up_limit']) &
                      (low_yuan   >= df['up_limit'])).astype(np.float32)
    df['limit_down'] = ((close_yuan <= df['down_limit']) &
                        (open_yuan  <= df['down_limit']) &
                        (high_yuan  <= df['down_limit'])).astype(np.float32)
    # 无 stk_limit 数据时置 0
    nan_mask = df['up_limit'].isna()
    df.loc[nan_mask, 'limit_up'] = 0.0
    df.loc[nan_mask, 'limit_down'] = 0.0

    # 格式化
    df['date'] = pd.to_datetime(df['trade_time']).dt.strftime('%Y-%m-%d %H:%M:%S')
    df['symbol'] = df['ts_code'].str.lower()

    csv_cols = ['date', 'symbol', 'open', 'high', 'low', 'close',
                'volume', 'amount', 'factor', 'limit_up', 'limit_down']

    # 写 CSV（按 symbol 拆分）
    for sym, grp in df.groupby('symbol'):
        csv_path = CSV_DIR / f'{sym}.csv'
        if csv_path.exists():
            grp[csv_cols].to_csv(csv_path, index=False, mode='a', header=False)
        else:
            grp[csv_cols].to_csv(csv_path, index=False)

    total_rows += len(df)
    elapsed = time.time() - t0
    progress = (batch_idx + BATCH_SIZE) / len(all_codes) * 100
    print(f'  [{min(batch_idx+BATCH_SIZE, len(all_codes))}/{len(all_codes)}] '
          f'{total_rows:,} rows, {elapsed:.0f}s ({progress:.0f}%)', flush=True)

    del df, rows
    gc.collect()

conn.close()
print(f'\n  导出完成: {total_rows:,} 行, {len(list(CSV_DIR.glob("*.csv")))} 个 CSV 文件')

# ========== Step 5: 输出 dump_bin 命令 ==========
csv_wsl = str(CSV_DIR).replace('F:', '/mnt/f').replace('\\', '/')
bin_wsl = str(BIN_DIR).replace('F:', '/mnt/f').replace('\\', '/')
cmd = (f'source ~/miniconda3/etc/profile.d/conda.sh && '
       f'conda activate rdagent-gpu && '
       f'python /mnt/f/Dev/RD-Agent-main/scripts/dump_bin.py dump_all '
       f'--data_path {csv_wsl} '
       f'--qlib_dir {bin_wsl} '
       f'--freq 1min '
       f'--date_field_name date '
       f'--symbol_field_name symbol '
       f'--exclude_fields date,symbol')
print(f'\n=== Step 5: dump_bin 命令 ===')
print(f'wsl bash -c "{cmd}"')
